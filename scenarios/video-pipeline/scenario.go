// Package videopipeline simulates the "creators' uploads never finish
// processing" incident on a video platform.
//
// Creators upload long videos during an evening peak. The upload API encodes
// the video inside the POST handler, on one machine, from start to finish. An
// encode costs three times the video's length, so every request outlives the
// load balancer's 60-second timeout. The app sees a gateway timeout and
// uploads again, and the server, which never noticed the client left, starts
// a second, third and fourth encode of the same video. A two-hour keynote
// takes six hours on one machine; when that machine dies at hour five, the
// encode starts again from zero. The encode servers fill up with duplicates
// and long videos, and short uploads queue behind them for hours.
//
// Broken models exactly that. Fixed keeps the same uploads, the same worker
// count and the same worker crashes, and changes the design:
//
//   - The upload API is an async request-reply endpoint
//     (patterns/architecture/async-request-reply): it answers 202 with a job
//     ID at once, de-duplicates retries with an Idempotency-Key, and POSTs a
//     signed callback when the video is ready. Clients that cannot take a
//     callback poll with backoff.
//   - The encode is a split-process-merge job
//     (patterns/distributed/split-process-merge): 4-second chunks spread over
//     the whole worker pool, a crashed worker's chunk is requeued when its
//     lease expires, finished chunks are checkpointed so a restarted job only
//     redoes what was in flight, idle workers run backup copies of
//     stragglers, and the output is merged in order as a stream.
//
// Everything runs in simulated time: Broken is a small discrete-event model,
// Fixed uses splitprocessmerge.Simulate for the cluster and drives the real
// asyncrequestreply Server and Client over loopback HTTP with a simulated
// clock.
package videopipeline

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http/httptest"
	"slices"
	"strconv"
	"sync"
	"time"

	arr "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/async-request-reply"
	wh "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/webhook-delivery"
	spm "github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/split-process-merge"
)

// The traffic, the cluster and the failures. Both designs get all of them.
const (
	Uploads    = 80
	PeakWindow = 3 * time.Hour // uploads arrive over this window
	Workers    = 48            // encode slots in the cluster
	// EncodeCost is encode time per second of video (all resolutions).
	EncodeCost   = 3.0
	LBTimeout    = 60 * time.Second
	ClientTries  = 4 // the app's first upload plus 3 retries
	RetryPause   = 10 * time.Second
	RestartAfter = 2 * time.Minute // a crashed machine or coordinator is back after this
	Keynote      = 2 * time.Hour   // upload 0, at t=0
	// KeynoteCrashAt is how far through the keynote's encode the machine
	// running it dies: hour 5 of a 6-hour serial encode.
	KeynoteCrashAt = 5.0 / 6

	// Fixed pipeline parameters.
	ChunkSize     = 4 * time.Second
	ChunkOverhead = 2 * time.Second // fetch segment, start encoder, upload output
	Jitter        = 0.2
	StragglerRate = 0.02
	Straggler     = 4.0 // a straggling chunk runs this many times slower
	MergePerChunk = 20 * time.Millisecond
	LeaseTimeout  = 30 * time.Second // a dead worker's chunk is requeued after this
	PollBase      = 5 * time.Second
	PollMax       = 2 * time.Minute
	// LongVideoBound is the invariant for long videos: processing finishes
	// within this factor of (total encode work / Workers).
	LongVideoBound = 2.0
	Seed           = 42
)

// Crashes are the worker deaths both designs suffer, by worker index and
// time since the start of the peak. Broken also loses the machine running the
// keynote at KeynoteCrashAt; Fixed loses the keynote's coordinator at the
// same fraction of its (much shorter) processing.
var Crashes = []spm.WorkerCrash{
	{Worker: 3, At: 33 * time.Minute},
	{Worker: 21, At: 41 * time.Minute},
	{Worker: 7, At: 77 * time.Minute},
	{Worker: 30, At: 118 * time.Minute},
	{Worker: 12, At: 150 * time.Minute},
	{Worker: 44, At: 171 * time.Minute},
}

// Upload is one creator upload.
type Upload struct {
	ID      int
	Arrival time.Duration // since the start of the peak
	Length  time.Duration // video length
	// LostAck marks uploads whose first answer is lost on the creator's
	// network, so the app sends the upload again.
	LostAck bool
}

// Work is the encode work the upload needs: length × EncodeCost.
func (u Upload) Work() time.Duration { return time.Duration(float64(u.Length) * EncodeCost) }

// Traffic returns the seeded evening of uploads: a 2-hour keynote at t=0,
// then a mix of short clips, mid-length videos and a few long streams.
func Traffic() []Upload {
	r := rand.New(rand.NewPCG(Seed, 7))
	ups := []Upload{{ID: 0, Length: Keynote}}
	for i := 1; i < Uploads; i++ {
		var length time.Duration
		switch p := r.Float64(); {
		case p < 0.75:
			length = time.Duration(60+r.IntN(9*60)) * time.Second // 1-10 min
		case p < 0.95:
			length = time.Duration(15+r.IntN(30)) * time.Minute
		default:
			length = time.Duration(90+r.IntN(60)) * time.Minute
		}
		ups = append(ups, Upload{
			ID:      i,
			Arrival: time.Duration(r.Int64N(int64(PeakWindow))).Truncate(time.Second),
			Length:  length,
			LostAck: r.Float64() < 0.1,
		})
	}
	slices.SortStableFunc(ups, func(a, b Upload) int { return int(a.Arrival - b.Arrival) })
	return ups
}

// Report is what a run measured.
type Report struct {
	Name string
	// Ready[i] is upload i's time from upload to playable (all resolutions).
	Ready   []time.Duration
	Lengths []time.Duration
	// Encodes counts whole-video encode jobs started; Uploads of them were needed.
	Encodes int
	// Requests counts upload POSTs; Timeouts those cut at the load balancer.
	Requests, Timeouts int
	// MaxHeld is the longest any upload request was held open.
	MaxHeld time.Duration
	// Busy is worker time spent (including wasted work), Useful the encode
	// work the uploads needed, Horizon the time the last video became ready.
	Busy, Useful, Horizon time.Duration
	// Redone is encode work thrown away by the worker crashes in Crashes. For
	// Fixed it is an upper bound: chunks lost × the most a chunk attempt can
	// cost (RedoneBound).
	Redone      time.Duration
	RedoneBound bool
	// ChunksRedone counts chunk attempts lost to crashes (Fixed).
	ChunksRedone int
	// KeynoteRedone is the work lost when the keynote's machine (Broken) or
	// its job coordinator (Fixed) died KeynoteCrashAt of the way through.
	KeynoteRedone time.Duration
	// KeynoteProcessing is the keynote's encode wall time, excluding queueing
	// (Broken: of the encode that finished first);
	// KeynoteIdeal is its encode work / Workers.
	KeynoteProcessing, KeynoteIdeal time.Duration
	// LongWorst is the worst ratio, over videos of an hour or more, of
	// processing time (excluding queueing) to (encode work / Workers).
	LongWorst float64

	// Fixed only.
	Polls                  int
	PollLatency            []time.Duration // per upload: ready -> a polling client saw it
	CallbacksSent          int
	Backups, BackupWins    int
	SpeculativeWaste       time.Duration // worker time on cancelled backups and lost attempts
	MergeTail              time.Duration // keynote: merge time left after its last chunk
	KeynoteChunks          int
	KeynoteChunksResumed   int
	KeynoteInFlightAtCrash int
}

// Utilisation is Busy / (Workers × Horizon).
func (r Report) Utilisation() float64 {
	return float64(r.Busy) / float64(time.Duration(Workers)*r.Horizon)
}

// DuplicateEncodes is encodes beyond one per upload.
func (r Report) DuplicateEncodes() int { return r.Encodes - len(r.Ready) }

// Percentile returns the p-th percentile (0-100) of ds, nearest rank.
func Percentile(ds []time.Duration, p float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	s := slices.Clone(ds)
	slices.Sort(s)
	i := int(p/100*float64(len(s))+0.5) - 1
	return s[min(max(i, 0), len(s)-1)]
}

func fmtD(d time.Duration) string {
	if d < time.Minute {
		return d.Round(100 * time.Millisecond).String()
	}
	return d.Round(time.Minute).String()
}

func hours(d time.Duration) string { return fmt.Sprintf("%.1fh", d.Hours()) }

// Print writes the summary.
func (r Report) Print(w io.Writer) {
	fmt.Fprintf(w, "%s\n", r.Name)
	fmt.Fprintf(w, "%d uploads over %v (a %v keynote at t=0), %d encode workers, encode cost %.0fx video length, LB timeout %v, %d worker crashes\n\n",
		len(r.Ready), PeakWindow, Keynote, Workers, EncodeCost, LBTimeout, len(Crashes))
	fmt.Fprintf(w, "  upload requests: %d, cut at the load balancer: %d, longest held: %v\n", r.Requests, r.Timeouts, fmtD(r.MaxHeld))
	fmt.Fprintf(w, "  encodes started: %d for %d uploads (%d duplicates)\n", r.Encodes, len(r.Ready), r.DuplicateEncodes())
	fmt.Fprintf(w, "  time to ready: p50 %v, p95 %v, max %v\n", fmtD(Percentile(r.Ready, 50)), fmtD(Percentile(r.Ready, 95)), fmtD(Percentile(r.Ready, 100)))
	var short []time.Duration
	for i, l := range r.Lengths {
		if l <= 10*time.Minute {
			short = append(short, r.Ready[i])
		}
	}
	fmt.Fprintf(w, "  time to ready for clips of 10 min or less (%d): p50 %v, max %v\n", len(short), fmtD(Percentile(short, 50)), fmtD(Percentile(short, 100)))
	fmt.Fprintf(w, "  keynote: encode took %v (ideal work/workers %v); worst long video %.1fx its ideal\n",
		fmtD(r.KeynoteProcessing), fmtD(r.KeynoteIdeal), r.LongWorst)
	bound := ""
	if r.RedoneBound {
		bound = "at most "
	}
	fmt.Fprintf(w, "  encode work redone after the %d worker crashes: %s%v", len(Crashes), bound, fmtD(r.Redone))
	if r.ChunksRedone > 0 {
		fmt.Fprintf(w, " (%d chunks lost with their worker)", r.ChunksRedone)
	}
	fmt.Fprintf(w, "\n  keynote crash at %.0f%% of its encode: %v of work lost", KeynoteCrashAt*100, fmtD(r.KeynoteRedone))
	if r.KeynoteChunks > 0 {
		fmt.Fprintf(w, " (%d of %d chunks already checkpointed, %d in flight redone)", r.KeynoteChunksResumed, r.KeynoteChunks, r.KeynoteInFlightAtCrash)
	}
	fmt.Fprintf(w, "\n  workers: %.0f%% busy until the last video was ready (t=%v); %v of worker time for %v of needed encode work (%.0f%%)\n",
		100*r.Utilisation(), fmtD(r.Horizon), hours(r.Busy), hours(r.Useful), 100*float64(r.Useful)/float64(r.Busy))
	if r.Polls > 0 {
		fmt.Fprintf(w, "  callbacks sent: %d (sent the moment a video is ready)\n", r.CallbacksSent)
		fmt.Fprintf(w, "  polling clients instead: %d status polls, saw ready p50 %v / max %v after it was\n",
			r.Polls, fmtD(Percentile(r.PollLatency, 50)), fmtD(Percentile(r.PollLatency, 100)))
		fmt.Fprintf(w, "  speculation: %d backup chunks, %d won; %v of worker time on lost attempts and cancelled copies\n",
			r.Backups, r.BackupWins, fmtD(r.SpeculativeWaste))
		fmt.Fprintf(w, "  keynote merge tail after its last chunk: %v\n", fmtD(r.MergeTail))
	}
}

// ---------------------------------------------------------------- broken

type bEvent struct {
	at     time.Duration
	seq    int
	kind   int // bAttempt, bDone, bCrash, bRestart
	upload int
	server int
	enc    int
	gen    int
}

const (
	bAttempt = iota
	bDone
	bCrash
	bRestart
)

type bHeap []bEvent

func (h bHeap) Len() int { return len(h) }
func (h bHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	return h[i].seq < h[j].seq
}
func (h bHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *bHeap) Push(x any)   { *h = append(*h, x.(bEvent)) }
func (h *bHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	*h = old[:len(old)-1]
	return e
}

// Broken: POST /upload encodes the whole video inline on one server. The
// load balancer cuts the request after 60s; the app retries; the orphaned
// encode keeps running and the retry starts another one. A crashed server
// restarts its encode from zero.
func Broken() Report {
	ups := Traffic()
	r := Report{Name: "BROKEN: synchronous POST encodes the whole video on one server; the app retries on timeout",
		Ready: make([]time.Duration, len(ups)), Lengths: make([]time.Duration, len(ups))}
	type encode struct {
		upload int
		start  time.Duration
		gen    int // bumps when the encode is restarted, to ignore stale finish events
	}
	var (
		h       bHeap
		seq     int
		queue   []int // encode ids waiting for a server
		encodes []encode
		running = make([]int, Workers) // encode id + 1, 0 = idle
		down    = make([]bool, Workers)
		ready   = make([]time.Duration, len(ups))
		began   = make([]time.Duration, len(ups)) // start of the encode that finished first
		done    = make([]bool, len(ups))
	)
	push := func(e bEvent) { seq++; e.seq = seq; heap.Push(&h, e) }
	for i, u := range ups {
		r.Useful += u.Work()
		r.Lengths[i] = u.Length
		// No attempt can succeed (the shortest encode is 3 minutes), so the
		// app always makes all its attempts.
		for t := range ClientTries {
			push(bEvent{at: u.Arrival + time.Duration(t)*(LBTimeout+RetryPause), kind: bAttempt, upload: i})
		}
	}
	for _, c := range Crashes {
		push(bEvent{at: c.At, kind: bCrash, server: c.Worker})
	}
	// The keynote is the first upload and lands on server 0 at t=0.
	push(bEvent{at: time.Duration(float64(ups[0].Work()) * KeynoteCrashAt), kind: bCrash, server: 0, upload: -1})

	var now time.Duration
	start := func(s, id int) {
		running[s] = id + 1
		encodes[id].start = now
		push(bEvent{at: now + ups[encodes[id].upload].Work(), kind: bDone, server: s, enc: id, gen: encodes[id].gen})
	}
	dispatch := func() {
		for s := range Workers {
			if len(queue) == 0 {
				return
			}
			if running[s] == 0 && !down[s] {
				id := queue[0]
				queue = queue[1:]
				start(s, id)
			}
		}
	}
	var attemptOf []time.Duration // encode id -> when its request arrived
	for h.Len() > 0 {
		e := heap.Pop(&h).(bEvent)
		now = e.at
		switch e.kind {
		case bAttempt:
			r.Requests++
			encodes = append(encodes, encode{upload: e.upload})
			id := len(encodes) - 1
			attemptOf = append(attemptOf, now)
			queue = append(queue, id)
			r.Encodes++
		case bDone:
			if running[e.server] != e.enc+1 || encodes[e.enc].gen != e.gen {
				continue // the server crashed during this encode
			}
			running[e.server] = 0
			en := encodes[e.enc]
			r.Busy += now - en.start
			if !done[en.upload] {
				done[en.upload] = true
				ready[en.upload], began[en.upload] = now, en.start
			}
		case bCrash:
			if id := running[e.server]; id != 0 {
				en := &encodes[id-1]
				lost := now - en.start
				r.Busy += lost
				if e.upload == -1 && en.upload == 0 {
					r.KeynoteRedone = lost
				} else {
					r.Redone += lost
				}
				en.gen++
				running[e.server] = 0
				queue = append([]int{id - 1}, queue...) // restarts from zero, first in line
			}
			down[e.server] = true
			push(bEvent{at: now + RestartAfter, kind: bRestart, server: e.server})
		case bRestart:
			down[e.server] = false
		}
		dispatch()
	}
	// Every request is held until its encode finishes or the LB cuts it.
	for id, en := range encodes {
		held := en.start + ups[en.upload].Work() - attemptOf[id]
		if held > LBTimeout {
			r.Timeouts++
			held = LBTimeout
		}
		r.MaxHeld = max(r.MaxHeld, held)
	}
	for i, u := range ups {
		r.Ready[i] = ready[i] - u.Arrival
		r.Horizon = max(r.Horizon, ready[i])
		if u.Length >= time.Hour {
			r.LongWorst = max(r.LongWorst, float64(ready[i]-began[i])/float64(u.Work()/Workers))
		}
	}
	if ups[0].Arrival != 0 || ups[0].Length != Keynote {
		panic("videopipeline: the keynote must be the first upload")
	}
	r.KeynoteProcessing = ready[0] - began[0]
	r.KeynoteIdeal = ups[0].Work() / Workers
	return r
}

// ---------------------------------------------------------------- fixed

// Fixed: async request-reply in front, split-process-merge behind it.
func Fixed() Report { return fixed(true) }

func simConfig(u Upload, speculate bool) spm.SimConfig {
	chunks, err := spm.SplitFixed(u.Length, ChunkSize)
	if err != nil {
		panic(err)
	}
	return spm.SimConfig{
		Chunks:             chunks,
		Workers:            Workers,
		CostPerMediaSecond: EncodeCost,
		ChunkOverhead:      ChunkOverhead,
		Jitter:             Jitter,
		StragglerRate:      StragglerRate,
		StragglerFactor:    Straggler,
		Speculate:          speculate,
		MergePerChunk:      MergePerChunk,
		StreamingMerge:     true,
		DetectAfter:        LeaseTimeout,
		Seed:               Seed + uint64(u.ID),
	}
}

// maxAttempt is the most one chunk attempt can cost.
func maxAttempt() time.Duration {
	return ChunkOverhead + time.Duration(float64(ChunkSize)*EncodeCost*(1+Jitter)*Straggler)
}

func simulate(cfg spm.SimConfig) spm.SimReport {
	rep, err := spm.Simulate(cfg)
	if err != nil {
		panic(err)
	}
	if rep.Failed {
		panic("videopipeline: simulated encode failed")
	}
	return rep
}

// crashesFrom returns the crashes at or after start, relative to start.
func crashesFrom(start time.Duration) []spm.WorkerCrash {
	var out []spm.WorkerCrash
	for _, c := range Crashes {
		if c.At >= start {
			out = append(out, spm.WorkerCrash{Worker: c.Worker, At: c.At - start})
		}
	}
	return out
}

func fixed(speculate bool) Report {
	ups := Traffic()
	name := "FIXED: 202 + job status + idempotency key + callback; chunked encode with retry, checkpoints, backups, ordered merge"
	if !speculate {
		name = "FIXED without speculative backups"
	}
	r := Report{Name: name, Ready: make([]time.Duration, len(ups)), Lengths: make([]time.Duration, len(ups)), RedoneBound: true}

	// 1. The cluster: each accepted job is split into chunks and spread over
	// every worker; jobs start in the order they were accepted. A job's merge
	// runs on the coordinator, so the next job's chunks can start as soon as
	// the last chunk of this one is accepted.
	readyAt := make([]time.Duration, len(ups))
	var free time.Duration // when the pool can take the next job
	for i, u := range ups {
		r.Lengths[i] = u.Length
		r.Useful += u.Work()
		start := max(u.Arrival, free)
		cfg := simConfig(u, speculate)
		cfg.Crashes = crashesFrom(start)
		var processing, end time.Duration
		var rep spm.SimReport
		if i == 0 {
			// The keynote's coordinator dies KeynoteCrashAt of the way through
			// processing and comes back RestartAfter later; it resumes from the
			// chunk checkpoint.
			dry := simulate(cfg)
			cfg.StopAt = time.Duration(float64(dry.ProcessingEnd) * KeynoteCrashAt)
			first := simulate(cfg)
			resumed := 0
			for _, d := range first.Done {
				if d {
					resumed++
				}
			}
			r.KeynoteChunks, r.KeynoteChunksResumed, r.KeynoteInFlightAtCrash = len(cfg.Chunks), resumed, first.InFlight
			if first.Stats.Lost != 0 || first.Stats.Backups != 0 {
				panic("videopipeline: keynote's first run should only lose its in-flight chunks")
			}
			// With no worker crash and no backups before the stop, everything
			// wasted is the in-flight work the coordinator crash threw away.
			r.KeynoteRedone = first.Wasted
			r.Busy += first.Busy
			restart := cfg.StopAt + RestartAfter
			cfg.StopAt, cfg.Resume = 0, first.Done
			cfg.Crashes = crashesFrom(start + restart)
			rep = simulate(cfg)
			processing = restart + rep.Makespan
			end = start + restart + rep.ProcessingEnd
			r.MergeTail = rep.MergeTail
			r.KeynoteProcessing = processing
			r.KeynoteIdeal = u.Work() / Workers
		} else {
			rep = simulate(cfg)
			processing, end = rep.Makespan, start+rep.ProcessingEnd
		}
		r.Busy += rep.Busy
		r.SpeculativeWaste += rep.Wasted
		r.ChunksRedone += rep.Stats.Lost
		r.Backups += rep.Stats.Backups
		r.BackupWins += rep.Stats.BackupWins
		readyAt[i] = start + processing
		r.Ready[i] = readyAt[i] - u.Arrival
		r.Horizon = max(r.Horizon, readyAt[i])
		if u.Length >= time.Hour {
			r.LongWorst = max(r.LongWorst, float64(processing)/float64(u.Work()/Workers))
		}
		free = end
	}
	r.Redone = time.Duration(r.ChunksRedone) * maxAttempt()

	// 2. The API: every upload goes through the real async request-reply
	// server and client, with the simulated clock.
	api := runAPI(ups, readyAt)
	r.Requests, r.Encodes, r.Polls, r.CallbacksSent = api.requests, api.executions, api.polls, api.callbacks
	r.PollLatency = api.pollLatency
	// The handler never waits for the encode; it is held for no simulated time.
	r.MaxHeld = api.maxHeld
	return r
}

type apiResult struct {
	requests, executions, polls, callbacks int
	maxHeld                                time.Duration
	pollLatency                            []time.Duration
}

// clock is the simulated clock shared by the server, the client's sleeps and
// the encode cluster's completions.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *clock) set(t time.Time)     { c.mu.Lock(); c.now = t; c.mu.Unlock() }
func (c *clock) add(d time.Duration) { c.mu.Lock(); c.now = c.now.Add(d); c.mu.Unlock() }

var epoch = time.Date(2026, 10, 28, 19, 0, 0, 0, time.UTC)

// runAPI plays each upload's conversation with the API in turn: submit (and,
// when the first answer is lost, resubmit with the same key), then poll with
// backoff until the job is done. The job's Work blocks until the simulated
// clock reaches the time the cluster finished that video. Uploads are played
// one after another, each on its own stretch of the clock, because at the API
// layer they do not interact: the cluster schedule above already accounts
// for the contention between them.
func runAPI(ups []Upload, readyAt []time.Duration) apiResult {
	ctx := context.Background()
	clk := &clock{now: epoch}
	var res apiResult

	var mu sync.Mutex
	release := map[string]chan struct{}{}
	gate := func(id string) chan struct{} {
		mu.Lock()
		defer mu.Unlock()
		ch, ok := release[id]
		if !ok {
			ch = make(chan struct{})
			release[id] = ch
		}
		return ch
	}
	work := func(ctx context.Context, in []byte, _ func(float64)) ([]byte, error) {
		select {
		case <-gate(string(in)):
			return []byte("hls manifest for upload " + string(in)), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	secret := []byte("whsec_creator_app")
	callbacks := make(chan string, len(ups))
	receiver := httptest.NewServer(&wh.Receiver{
		Secrets: [][]byte{secret},
		Handle: func(_ context.Context, _ string, body []byte) error {
			var st arr.Status
			if err := json.Unmarshal(body, &st); err != nil {
				return err
			}
			callbacks <- st.ID
			return nil
		},
	})
	defer receiver.Close()

	updates := make(chan arr.Status, 4*len(ups)+16)
	srv, err := arr.New(work,
		arr.WithWorkers(4),
		arr.WithClock(clk.Now),
		arr.WithRetryAfter(PollBase),
		arr.WithCallbacks(&wh.Sender{Secret: secret}),
		arr.WithOnUpdate(func(st arr.Status) { updates <- st }),
	)
	if err != nil {
		panic(err)
	}
	hs := httptest.NewServer(srv)
	defer hs.Close()
	defer srv.Close()

	for i, u := range ups {
		id := strconv.Itoa(u.ID)
		submitAt := epoch.Add(u.Arrival)
		ready := epoch.Add(readyAt[i])
		clk.set(submitAt)
		released := false
		var jobID string
		// Sleep advances the clock; crossing the ready time lets the job finish.
		sleep := func(_ context.Context, d time.Duration) error {
			target := clk.Now().Add(d)
			if !released && !target.Before(ready) {
				clk.set(ready)
				close(gate(id))
				released = true
				for st := range updates {
					if st.ID == jobID && st.State.Terminal() {
						break
					}
				}
			}
			clk.set(target)
			return nil
		}
		client := &arr.Client{BaseURL: hs.URL, BaseDelay: PollBase, MaxDelay: PollMax, Sleep: sleep}
		key := "upload-" + id
		sub, err := client.Submit(ctx, []byte(id), key, receiver.URL)
		if err != nil {
			panic(err)
		}
		res.requests++
		jobID = sub.Status.ID
		res.maxHeld = max(res.maxHeld, clk.Now().Sub(submitAt))
		if u.LostAck {
			// The 202 never reached the app; it uploads again with the same key.
			_ = sleep(ctx, RetryPause)
			again, err := client.Submit(ctx, []byte(id), key, receiver.URL)
			if err != nil {
				panic(err)
			}
			res.requests++
			if !again.Replayed || again.Status.ID != jobID {
				panic("videopipeline: resubmit was not de-duplicated")
			}
		}
		out, err := client.Await(ctx, sub)
		if err != nil {
			panic(err)
		}
		res.polls += out.Polls
		res.pollLatency = append(res.pollLatency, clk.Now().Sub(ready))
		if got := <-callbacks; got != jobID {
			panic("videopipeline: callback for the wrong job")
		}
	}
	srv.Close()
	st := srv.Stats()
	res.executions, res.callbacks = int(st.Executions), int(st.CallbacksSent)
	return res
}
