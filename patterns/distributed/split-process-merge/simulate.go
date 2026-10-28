package splitprocessmerge

import (
	"container/heap"
	"fmt"
	"math/rand/v2"
	"time"
)

// WorkerCrash kills one simulated worker at a point in simulated time. The
// chunk it was processing is lost and goes back on the queue once the crash
// is detected (its lease expires).
type WorkerCrash struct {
	Worker int
	At     time.Duration
}

// SimConfig describes a simulated job. All times are simulated; nothing
// sleeps. Chunk costs depend only on (Seed, chunk, attempt number), so the
// same chunk costs the same whatever the worker count, and comparisons
// between configurations are fair.
type SimConfig struct {
	Chunks  []Chunk
	Workers int
	// CostPerMediaSecond is processing time per second of media (3 means a
	// 4s chunk takes 12s to encode into all resolutions).
	CostPerMediaSecond float64
	// ChunkOverhead is added to every attempt: fetch the segment, start the
	// encoder, upload the output.
	ChunkOverhead time.Duration
	// Jitter spreads cost uniformly by ±Jitter (0.2 = ±20%).
	Jitter float64
	// StragglerRate is the chance an attempt runs StragglerFactor times slower
	// (a noisy neighbour, a throttled disk, a hard scene).
	StragglerRate, StragglerFactor float64
	// FailRate is the chance an attempt fails halfway; it is then retried, up
	// to MaxAttempts failures per chunk (0 means 3).
	FailRate    float64
	MaxAttempts int
	Speculate   bool
	// MergePerChunk is the serial cost of appending one chunk to the output.
	MergePerChunk time.Duration
	// StreamingMerge merges the contiguous prefix while later chunks are still
	// processing, instead of starting after the last chunk.
	StreamingMerge bool
	Crashes        []WorkerCrash
	DetectAfter    time.Duration // lease timeout before a crashed worker's chunk is requeued
	// StopAt, if > 0, stops the job at that simulated time: a deadline, a
	// cancel, or the coordinator crashing. Done reports what was checkpointed.
	StopAt time.Duration
	// Resume marks chunks completed by an earlier run (a checkpoint).
	Resume []bool
	Seed   uint64
}

// SimReport is the outcome of Simulate.
type SimReport struct {
	Makespan      time.Duration // wall time until the merged output exists
	ProcessingEnd time.Duration // last chunk accepted
	MergeTail     time.Duration // Makespan - ProcessingEnd: merge work left after processing
	Busy          time.Duration // worker time spent, including wasted work
	Wasted        time.Duration // failed attempts, lost attempts and cancelled backup losers
	Stats         Stats
	Stopped       bool   // StopAt was reached before the job finished
	Failed        bool   // a chunk used up its attempts, or every worker died
	InFlight      int    // attempts running when the job stopped (their work is lost)
	Done          []bool // per chunk: accepted (the checkpoint)
}

// SerialTime is the time to process the whole input as one job on one
// machine: no per-chunk overhead, no merge, no stragglers or failures.
func SerialTime(cfg SimConfig) time.Duration {
	var media time.Duration
	for _, c := range cfg.Chunks {
		media += c.Duration()
	}
	return time.Duration(float64(media) * cfg.CostPerMediaSecond)
}

type simEvent struct {
	at     time.Duration
	seq    int
	kind   int // evFinish, evCrash, evDetect
	worker int
	id     int
	ok     bool
}

const (
	evFinish = iota
	evCrash
	evDetect
)

type eventHeap []simEvent

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	return h[i].seq < h[j].seq
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)   { *h = append(*h, x.(simEvent)) }
func (h *eventHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	*h = old[:len(old)-1]
	return e
}

func (cfg SimConfig) attemptCost(c Chunk, number int) (time.Duration, bool) {
	r := rand.New(rand.NewPCG(cfg.Seed, uint64(c.Index)<<16|uint64(number)))
	f := cfg.CostPerMediaSecond * (1 + cfg.Jitter*(2*r.Float64()-1))
	if r.Float64() < cfg.StragglerRate {
		f *= cfg.StragglerFactor
	}
	cost := cfg.ChunkOverhead + time.Duration(float64(c.Duration())*f)
	if r.Float64() < cfg.FailRate {
		return cost / 2, false
	}
	return cost, true
}

// Simulate runs the job in simulated time with the same scheduler as Run.
func Simulate(cfg SimConfig) (SimReport, error) {
	if err := validateChunks(cfg.Chunks); err != nil {
		return SimReport{}, err
	}
	if cfg.Workers < 1 || cfg.CostPerMediaSecond <= 0 || cfg.Jitter < 0 || cfg.Jitter >= 1 ||
		cfg.StragglerRate < 0 || cfg.FailRate < 0 || cfg.FailRate >= 1 || cfg.MaxAttempts < 0 ||
		(cfg.StragglerRate > 0 && cfg.StragglerFactor < 1) || cfg.MergePerChunk < 0 || cfg.ChunkOverhead < 0 ||
		(cfg.Resume != nil && len(cfg.Resume) != len(cfg.Chunks)) {
		return SimReport{}, fmt.Errorf("%w: bad simulation parameters", ErrBadConfig)
	}
	for _, c := range cfg.Crashes {
		if c.Worker < 0 || c.Worker >= cfg.Workers || c.At < 0 {
			return SimReport{}, fmt.Errorf("%w: crash of unknown worker %d", ErrBadConfig, c.Worker)
		}
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 3
	}
	n := len(cfg.Chunks)
	s := newScheduler(n, cfg.MaxAttempts, cfg.Speculate)
	doneAt := make([]time.Duration, n)
	for i, d := range cfg.Resume {
		if d {
			s.markDone(i)
		}
	}

	type running struct {
		worker int
		start  time.Duration
	}
	var (
		rep    SimReport
		now    time.Duration
		seq    int
		events eventHeap
		alive  = make([]bool, cfg.Workers)
		busy   = make([]int, cfg.Workers) // attempt id, 0 = idle
		runs   = map[int]running{}
	)
	push := func(e simEvent) { seq++; e.seq = seq; heap.Push(&events, e) }
	for w := range alive {
		alive[w] = true
	}
	for _, c := range cfg.Crashes {
		push(simEvent{at: c.At, kind: evCrash, worker: c.Worker})
	}
	stopWork := func(id int) { // free a worker and account its time as wasted
		r := runs[id]
		delete(runs, id)
		busy[r.worker] = 0
		rep.Busy += now - r.start
		rep.Wasted += now - r.start
	}
	dispatch := func() {
		for w := range cfg.Workers {
			if !alive[w] || busy[w] != 0 {
				continue
			}
			id, ci, num, _, ok := s.next()
			if !ok {
				return
			}
			cost, success := cfg.attemptCost(cfg.Chunks[ci], num)
			busy[w] = id
			runs[id] = running{worker: w, start: now}
			push(simEvent{at: now + cost, kind: evFinish, worker: w, id: id, ok: success})
		}
	}

	dispatch()
loop:
	for !s.finished() && events.Len() > 0 {
		e := heap.Pop(&events).(simEvent)
		if cfg.StopAt > 0 && e.at > cfg.StopAt {
			now, rep.Stopped = cfg.StopAt, true
			break
		}
		now = e.at
		switch e.kind {
		case evFinish:
			r, live := runs[e.id]
			if !live {
				continue // cancelled loser or crashed worker
			}
			if !e.ok {
				stopWork(e.id)
				if s.fail(e.id) {
					rep.Failed = true
					break loop
				}
				break
			}
			delete(runs, e.id)
			busy[r.worker] = 0
			rep.Busy += now - r.start
			ci := s.live[e.id].chunk
			accepted, losers := s.complete(e.id)
			if accepted {
				doneAt[ci] = now
			} else {
				rep.Wasted += now - r.start
			}
			for _, l := range losers {
				if _, ok := runs[l]; ok {
					stopWork(l)
				}
			}
		case evCrash:
			alive[e.worker] = false
			if id := busy[e.worker]; id != 0 {
				stopWork(id)
				push(simEvent{at: now + cfg.DetectAfter, kind: evDetect, id: id})
			}
		case evDetect:
			s.release(e.id)
		}
		dispatch()
	}
	if !s.finished() && !rep.Stopped {
		rep.Failed = true // a dead chunk, or no live workers left
	}

	rep.InFlight = len(runs)
	for id := range runs {
		stopWork(id) // work in progress when the job stopped is lost
	}
	rep.Stats = s.stats
	rep.Done = make([]bool, n)
	for i := range n {
		rep.Done[i] = s.chunks[i].done
		if s.chunks[i].done {
			rep.ProcessingEnd = max(rep.ProcessingEnd, doneAt[i])
		}
	}
	if !s.finished() {
		rep.Makespan = now
		return rep, nil
	}
	if cfg.StreamingMerge {
		var m time.Duration
		for i := range n {
			m = max(m, doneAt[i]) + cfg.MergePerChunk
		}
		rep.Makespan = m
	} else {
		rep.Makespan = rep.ProcessingEnd + time.Duration(n)*cfg.MergePerChunk
	}
	rep.MergeTail = rep.Makespan - rep.ProcessingEnd
	return rep, nil
}
