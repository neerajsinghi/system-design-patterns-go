// Package splitprocessmerge parallelises one huge job by cutting it into
// independent chunks, processing the chunks on many workers, and stitching the
// results back together in order.
//
// The motivating case is video encoding. Encoding a two-hour upload into
// several resolutions in one process takes hours, and a crash at 95% throws
// all of that away. Cut the video into short segments instead (four seconds
// each, ideally at keyframes so every segment decodes on its own), and the job
// becomes 1,800 small tasks that a pool of workers can share like any other
// work queue. A failed segment is retried on its own, a finished segment is
// checkpointed so a restarted job only redoes what was unfinished, and a
// final merge concatenates the segments in index order.
//
// Three details decide how well this works in practice:
//
//   - Merge is ordered and needs every chunk. Results arrive in any order, so
//     they are slotted by index and merged as a contiguous prefix: either all
//     at the end, or streamed as soon as chunk 0..k are all done.
//   - Stragglers set the finish time. With many workers the job ends when its
//     slowest chunk ends, so once the queue is empty idle workers start backup
//     copies of the oldest running chunks (MapReduce "backup tasks"). The first
//     result wins; the loser is cancelled and a late duplicate is ignored, so
//     there is exactly one result per chunk.
//   - The job has a deadline. Cancelling the context stops handing out work,
//     cancels in-flight attempts, and keeps the checkpoint for a later resume.
//
// Run executes a job for real with goroutines. Simulate drives the same
// scheduler with simulated chunk costs, so speedup numbers are deterministic.
package splitprocessmerge

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Errors returned by the package.
var (
	ErrBadConfig   = errors.New("splitprocessmerge: invalid config")
	ErrChunkFailed = errors.New("splitprocessmerge: chunk failed after max attempts")
)

// Chunk is one independent piece of the job: a time range of the source.
type Chunk struct {
	Index      int
	Start, End time.Duration
}

// Duration is the length of media the chunk covers.
func (c Chunk) Duration() time.Duration { return c.End - c.Start }

// SplitFixed cuts [0, total) into chunks of size; the last one may be shorter.
func SplitFixed(total, size time.Duration) ([]Chunk, error) {
	if total <= 0 || size <= 0 {
		return nil, fmt.Errorf("%w: total and chunk size must be > 0", ErrBadConfig)
	}
	var out []Chunk
	for start := time.Duration(0); start < total; start += size {
		out = append(out, Chunk{Index: len(out), Start: start, End: min(start+size, total)})
	}
	return out, nil
}

// SplitAtKeyframes cuts [0, total) only at keyframe timestamps, starting a new
// chunk at the first keyframe at least target after the current chunk start.
// Each chunk then begins with a keyframe and can be decoded without its
// neighbours, at the cost of uneven chunk lengths. keyframes must start at 0
// and be strictly increasing and below total.
func SplitAtKeyframes(total time.Duration, keyframes []time.Duration, target time.Duration) ([]Chunk, error) {
	if total <= 0 || target <= 0 {
		return nil, fmt.Errorf("%w: total and target must be > 0", ErrBadConfig)
	}
	if len(keyframes) == 0 || keyframes[0] != 0 {
		return nil, fmt.Errorf("%w: keyframes must start at 0", ErrBadConfig)
	}
	for i, k := range keyframes {
		if k >= total || (i > 0 && k <= keyframes[i-1]) {
			return nil, fmt.Errorf("%w: keyframes must be increasing and < total", ErrBadConfig)
		}
	}
	var out []Chunk
	start := time.Duration(0)
	for _, k := range keyframes[1:] {
		if k-start >= target {
			out = append(out, Chunk{Index: len(out), Start: start, End: k})
			start = k
		}
	}
	return append(out, Chunk{Index: len(out), Start: start, End: total}), nil
}

// Attempt is one execution of one chunk handed to a worker.
type Attempt struct {
	Chunk  Chunk
	Number int  // 1 for the first attempt of this chunk; backups and retries count up
	Backup bool // a speculative duplicate of a chunk that is still running
	id     int
}

// Stats counts what the scheduler did.
type Stats struct {
	Chunks     int // chunks in the job
	Resumed    int // already complete in the checkpoint when the job started
	Completed  int // chunks with an accepted result (including resumed ones)
	Attempts   int // attempts started, including retries, reruns and backups
	Failures   int // attempts that returned an error
	Lost       int // attempts lost with a crashed worker
	Reruns     int // non-backup attempts on a chunk that had been started before
	Backups    int // speculative duplicates started
	BackupWins int // chunks whose accepted result came from a backup
	Duplicates int // results for an already finished chunk, ignored
}

// ---------------------------------------------------------------------------
// scheduler: the queue and bookkeeping shared by Run and Simulate. Not safe
// for concurrent use; Run calls it from a single coordinator goroutine.

type liveAttempt struct {
	chunk, number int
	backup        bool
	seq           int // start order, used to find the longest-running chunk
}

type chunkState struct {
	done, dead bool
	started    bool
	failures   int
	attempts   int
	running    []int // live attempt ids
}

type scheduler struct {
	maxAttempts int
	speculate   bool
	chunks      []chunkState
	pending     []int // FIFO of chunk indexes; retries go to the front
	live        map[int]*liveAttempt
	nextID      int
	stats       Stats
}

func newScheduler(n, maxAttempts int, speculate bool) *scheduler {
	s := &scheduler{maxAttempts: maxAttempts, speculate: speculate,
		chunks: make([]chunkState, n), live: map[int]*liveAttempt{}}
	s.pending = make([]int, n)
	for i := range n {
		s.pending[i] = i
	}
	s.stats.Chunks = n
	return s
}

func (s *scheduler) markDone(i int) {
	if c := &s.chunks[i]; !c.done {
		c.done = true
		s.stats.Completed++
		s.stats.Resumed++
	}
}

func (s *scheduler) finished() bool { return s.stats.Completed == len(s.chunks) }

func (s *scheduler) start(i int, backup bool) (int, int) {
	c := &s.chunks[i]
	s.nextID++
	c.attempts++
	if c.started && !backup {
		s.stats.Reruns++
	}
	c.started = true
	s.stats.Attempts++
	if backup {
		s.stats.Backups++
	}
	s.live[s.nextID] = &liveAttempt{chunk: i, number: c.attempts, backup: backup, seq: s.nextID}
	c.running = append(c.running, s.nextID)
	return s.nextID, c.attempts
}

// next hands out the next attempt: the oldest pending chunk, or, when the
// queue is empty and speculation is on, a backup of the longest-running chunk
// that has no backup yet.
func (s *scheduler) next() (id, chunk, number int, backup, ok bool) {
	for len(s.pending) > 0 {
		i := s.pending[0]
		s.pending = s.pending[1:]
		if c := &s.chunks[i]; c.done || c.dead || len(c.running) > 0 {
			continue
		}
		id, n := s.start(i, false)
		return id, i, n, false, true
	}
	if !s.speculate {
		return 0, 0, 0, false, false
	}
	best, bestSeq := -1, 0
	for _, a := range s.live {
		if c := &s.chunks[a.chunk]; len(c.running) == 1 && !a.backup && (best < 0 || a.seq < bestSeq) {
			best, bestSeq = a.chunk, a.seq
		}
	}
	if best < 0 {
		return 0, 0, 0, false, false
	}
	id, n := s.start(best, true)
	return id, best, n, true, true
}

func (s *scheduler) drop(id int) (*liveAttempt, bool) {
	a, ok := s.live[id]
	if !ok {
		return nil, false
	}
	delete(s.live, id)
	c := &s.chunks[a.chunk]
	for j, r := range c.running {
		if r == id {
			c.running = append(c.running[:j], c.running[j+1:]...)
			break
		}
	}
	return a, true
}

// complete records a successful attempt. It returns whether the result was
// accepted (first result for the chunk) and the ids of other attempts of the
// same chunk, which the caller should cancel; they are forgotten here, so
// anything they report later counts as a duplicate or is ignored.
func (s *scheduler) complete(id int) (accepted bool, losers []int) {
	a, ok := s.drop(id)
	if !ok || s.chunks[a.chunk].done {
		s.stats.Duplicates++
		return false, nil
	}
	c := &s.chunks[a.chunk]
	c.done = true
	s.stats.Completed++
	if a.backup {
		s.stats.BackupWins++
	}
	losers = append(losers, c.running...)
	for _, l := range losers {
		s.drop(l)
	}
	return true, losers
}

// fail records a failed attempt and requeues the chunk at the front of the
// queue (it blocks the merge prefix). It returns true when the chunk has used
// up its attempts and the job cannot finish.
func (s *scheduler) fail(id int) (dead bool) {
	a, ok := s.drop(id)
	if !ok {
		return false // a cancelled loser reporting back
	}
	c := &s.chunks[a.chunk]
	c.failures++
	s.stats.Failures++
	if len(c.running) > 0 {
		return false // a backup may still succeed
	}
	if c.failures >= s.maxAttempts {
		c.dead = true
		return true
	}
	s.pending = append([]int{a.chunk}, s.pending...)
	return false
}

// release returns the chunk of a lost attempt (its worker died) to the front
// of the queue without counting a failure.
func (s *scheduler) release(id int) {
	a, ok := s.drop(id)
	if !ok {
		return
	}
	s.stats.Lost++
	if c := &s.chunks[a.chunk]; !c.done && len(c.running) == 0 {
		s.pending = append([]int{a.chunk}, s.pending...)
	}
}

// ---------------------------------------------------------------------------
// Checkpoint

// Checkpoint durably records finished chunks so a restarted job can skip them.
type Checkpoint[R any] interface {
	Load() (map[int]R, error)
	Save(index int, result R) error
}

// MemoryCheckpoint is an in-memory, concurrency-safe Checkpoint. A real one
// would write to a database or object storage.
type MemoryCheckpoint[R any] struct {
	mu sync.Mutex
	m  map[int]R
}

// NewMemoryCheckpoint returns an empty checkpoint.
func NewMemoryCheckpoint[R any]() *MemoryCheckpoint[R] { return &MemoryCheckpoint[R]{m: map[int]R{}} }

// Load returns a copy of the saved results.
func (c *MemoryCheckpoint[R]) Load() (map[int]R, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[int]R, len(c.m))
	for k, v := range c.m {
		out[k] = v
	}
	return out, nil
}

// Save records one finished chunk.
func (c *MemoryCheckpoint[R]) Save(index int, r R) error {
	c.mu.Lock()
	c.m[index] = r
	c.mu.Unlock()
	return nil
}

// Len is the number of saved chunks.
func (c *MemoryCheckpoint[R]) Len() int { c.mu.Lock(); defer c.mu.Unlock(); return len(c.m) }

// ---------------------------------------------------------------------------
// Run

// ProcessFunc processes one attempt. It must return promptly once ctx is
// cancelled (the chunk was won by another attempt, or the job was cancelled).
type ProcessFunc[R any] func(ctx context.Context, a Attempt) (R, error)

// Config configures Run.
type Config[R any] struct {
	Workers     int  // concurrent workers, >= 1
	MaxAttempts int  // failed attempts allowed per chunk; 0 means 3
	Speculate   bool // start backup copies of running chunks once the queue is empty
	// Checkpoint, if set, is loaded at start (those chunks are skipped) and
	// written after every accepted result.
	Checkpoint Checkpoint[R]
	// OnMerge, if set, is called for each chunk strictly in index order as soon
	// as every earlier chunk is done (a streaming merge of the contiguous
	// prefix). Resumed chunks are merged too.
	OnMerge func(c Chunk, r R) error
}

// Report is the outcome of Run.
type Report[R any] struct {
	Results []R // indexed by chunk; complete only when Run returned nil
	Merged  int // chunks passed to OnMerge (or that would have been)
	Stats   Stats
}

// ChunkError reports the chunk that exhausted its attempts.
type ChunkError struct {
	Index    int
	Attempts int
	Err      error // the last attempt's error
}

func (e *ChunkError) Error() string {
	return fmt.Sprintf("splitprocessmerge: chunk %d failed after %d attempts: %v", e.Index, e.Attempts, e.Err)
}

// Unwrap lets errors.Is match both ErrChunkFailed and the cause.
func (e *ChunkError) Unwrap() []error { return []error{ErrChunkFailed, e.Err} }

func validateChunks(chunks []Chunk) error {
	if len(chunks) == 0 {
		return fmt.Errorf("%w: no chunks", ErrBadConfig)
	}
	for i, c := range chunks {
		if c.Index != i || c.End <= c.Start {
			return fmt.Errorf("%w: chunk %d must have Index %d and End > Start", ErrBadConfig, i, i)
		}
	}
	return nil
}

type task struct {
	a   Attempt
	ctx context.Context
}

type outcome[R any] struct {
	id  int
	r   R
	err error
}

// Run processes chunks with cfg.Workers competing workers and returns the
// results in index order. It returns a *ChunkError if a chunk fails
// cfg.MaxAttempts times, or the context's error (wrapped) on cancel or
// deadline. Run waits for every in-flight attempt to return before it does.
func Run[R any](ctx context.Context, chunks []Chunk, process ProcessFunc[R], cfg Config[R]) (Report[R], error) {
	if err := validateChunks(chunks); err != nil {
		return Report[R]{}, err
	}
	if process == nil || cfg.Workers < 1 || cfg.MaxAttempts < 0 {
		return Report[R]{}, fmt.Errorf("%w: need a process func, workers >= 1, max attempts >= 0", ErrBadConfig)
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 3
	}
	n := len(chunks)
	s := newScheduler(n, cfg.MaxAttempts, cfg.Speculate)
	rep := Report[R]{Results: make([]R, n)}
	if cfg.Checkpoint != nil {
		saved, err := cfg.Checkpoint.Load()
		if err != nil {
			return rep, fmt.Errorf("splitprocessmerge: load checkpoint: %w", err)
		}
		for i, r := range saved {
			if i >= 0 && i < n {
				rep.Results[i] = r
				s.markDone(i)
			}
		}
	}
	mergeReady := func() error {
		for rep.Merged < n && s.chunks[rep.Merged].done {
			if cfg.OnMerge != nil {
				if err := cfg.OnMerge(chunks[rep.Merged], rep.Results[rep.Merged]); err != nil {
					return fmt.Errorf("splitprocessmerge: merge chunk %d: %w", rep.Merged, err)
				}
			}
			rep.Merged++
		}
		return nil
	}

	runCtx, cancelAll := context.WithCancel(ctx)
	defer cancelAll()
	tasks := make(chan task, cfg.Workers)
	outcomes := make(chan outcome[R], cfg.Workers)
	var wg sync.WaitGroup
	for range cfg.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range tasks {
				r, err := process(t.ctx, t.a)
				outcomes <- outcome[R]{t.a.id, r, err}
			}
		}()
	}

	cancels := map[int]context.CancelFunc{}
	idle, inflight := cfg.Workers, 0
	jobErr := mergeReady()
	for jobErr == nil && !s.finished() {
		if err := ctx.Err(); err != nil {
			jobErr = err
			break
		}
		for idle > 0 {
			id, ci, num, backup, ok := s.next()
			if !ok {
				break
			}
			actx, cancel := context.WithCancel(runCtx)
			cancels[id] = cancel
			tasks <- task{Attempt{Chunk: chunks[ci], Number: num, Backup: backup, id: id}, actx}
			idle--
			inflight++
		}
		if inflight == 0 {
			break // nothing running and nothing to hand out (cannot happen unless dead)
		}
		select {
		case <-ctx.Done():
			jobErr = ctx.Err()
		case o := <-outcomes:
			idle++
			inflight--
			cancels[o.id]()
			delete(cancels, o.id)
			if o.err == nil {
				ci := -1
				if a, ok := s.live[o.id]; ok {
					ci = a.chunk
				}
				accepted, losers := s.complete(o.id)
				if !accepted {
					continue
				}
				rep.Results[ci] = o.r
				for _, l := range losers {
					cancels[l]()
				}
				if cfg.Checkpoint != nil {
					if err := cfg.Checkpoint.Save(ci, o.r); err != nil {
						jobErr = fmt.Errorf("splitprocessmerge: save checkpoint: %w", err)
						break
					}
				}
				jobErr = mergeReady()
				continue
			}
			a, ok := s.live[o.id]
			if !ok {
				continue // a cancelled loser
			}
			if s.fail(o.id) {
				jobErr = &ChunkError{Index: a.chunk, Attempts: s.chunks[a.chunk].attempts, Err: o.err}
			}
		}
	}

	cancelAll()
	close(tasks)
	for ; inflight > 0; inflight-- {
		o := <-outcomes
		if _, live := s.live[o.id]; !live && o.err == nil {
			s.stats.Duplicates++ // a cancelled loser that finished anyway
		}
	}
	wg.Wait()
	rep.Stats = s.stats
	if jobErr != nil {
		if errors.Is(jobErr, context.Canceled) || errors.Is(jobErr, context.DeadlineExceeded) {
			jobErr = fmt.Errorf("splitprocessmerge: job stopped with %d/%d chunks done: %w", s.stats.Completed, n, jobErr)
		}
		return rep, jobErr
	}
	return rep, nil
}
