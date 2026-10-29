package splitprocessmerge

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

func mustChunks(t *testing.T, n int) []Chunk {
	t.Helper()
	c, err := SplitFixed(time.Duration(n)*4*time.Second, 4*time.Second)
	if err != nil || len(c) != n {
		t.Fatalf("SplitFixed: %v, %d chunks", err, len(c))
	}
	return c
}

func label(i int) string { return fmt.Sprintf("r%d", i) }

func TestSplitFixed(t *testing.T) {
	tests := []struct {
		total, size time.Duration
		want        int
		lastLen     time.Duration
	}{
		{120 * time.Minute, 4 * time.Second, 1800, 4 * time.Second},
		{10 * time.Second, 4 * time.Second, 3, 2 * time.Second},
		{3 * time.Second, 4 * time.Second, 1, 3 * time.Second},
	}
	for _, tt := range tests {
		c, err := SplitFixed(tt.total, tt.size)
		if err != nil || len(c) != tt.want {
			t.Fatalf("SplitFixed(%v,%v) = %d chunks, %v", tt.total, tt.size, len(c), err)
		}
		if err := validateChunks(c); err != nil {
			t.Fatal(err)
		}
		if c[len(c)-1].End != tt.total || c[len(c)-1].Duration() != tt.lastLen {
			t.Fatalf("last chunk %+v", c[len(c)-1])
		}
	}
	for _, bad := range [][2]time.Duration{{0, time.Second}, {time.Second, 0}, {-1, time.Second}} {
		if _, err := SplitFixed(bad[0], bad[1]); !errors.Is(err, ErrBadConfig) {
			t.Errorf("SplitFixed(%v) err=%v", bad, err)
		}
	}
}

func TestSplitAtKeyframes(t *testing.T) {
	s := time.Second
	kf := []time.Duration{0, 2 * s, 4500 * time.Millisecond, 5 * s, 9 * s, 13 * s}
	c, err := SplitAtKeyframes(15*s, kf, 4*s)
	if err != nil {
		t.Fatal(err)
	}
	want := [][2]time.Duration{{0, 4500 * time.Millisecond}, {4500 * time.Millisecond, 9 * s}, {9 * s, 13 * s}, {13 * s, 15 * s}}
	if len(c) != len(want) {
		t.Fatalf("got %+v", c)
	}
	for i, w := range want {
		if c[i].Index != i || c[i].Start != w[0] || c[i].End != w[1] {
			t.Fatalf("chunk %d = %+v, want %v", i, c[i], w)
		}
		if !contains(kf, c[i].Start) {
			t.Fatalf("chunk %d starts at %v, not a keyframe", i, c[i].Start)
		}
	}
	bad := []struct {
		total, target time.Duration
		kf            []time.Duration
	}{
		{10 * s, 4 * s, nil},
		{10 * s, 4 * s, []time.Duration{s, 2 * s}},
		{10 * s, 4 * s, []time.Duration{0, 3 * s, 3 * s}},
		{10 * s, 4 * s, []time.Duration{0, 11 * s}},
		{10 * s, 0, []time.Duration{0}},
		{0, 4 * s, []time.Duration{0}},
	}
	for _, b := range bad {
		if _, err := SplitAtKeyframes(b.total, b.kf, b.target); !errors.Is(err, ErrBadConfig) {
			t.Errorf("SplitAtKeyframes(%v, %v, %v) err=%v", b.total, b.kf, b.target, err)
		}
	}
}

func contains(xs []time.Duration, x time.Duration) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func okProcess(_ context.Context, a Attempt) (string, error) { return label(a.Chunk.Index), nil }

func TestRunValidation(t *testing.T) {
	good := mustChunks(t, 3)
	tests := []struct {
		name    string
		chunks  []Chunk
		process ProcessFunc[string]
		cfg     Config[string]
	}{
		{"no chunks", nil, okProcess, Config[string]{Workers: 1}},
		{"bad index", []Chunk{{Index: 1, End: time.Second}}, okProcess, Config[string]{Workers: 1}},
		{"empty chunk", []Chunk{{Index: 0}}, okProcess, Config[string]{Workers: 1}},
		{"nil process", good, nil, Config[string]{Workers: 1}},
		{"no workers", good, okProcess, Config[string]{}},
		{"negative attempts", good, okProcess, Config[string]{Workers: 1, MaxAttempts: -1}},
	}
	for _, tt := range tests {
		if _, err := Run(context.Background(), tt.chunks, tt.process, tt.cfg); !errors.Is(err, ErrBadConfig) {
			t.Errorf("%s: err=%v", tt.name, err)
		}
	}
}

func TestMergeOrderRegardlessOfCompletionOrder(t *testing.T) {
	const n = 8
	chunks := mustChunks(t, n)
	// chunk i may only finish after chunk i+1, so completion order is n-1..0.
	finished := make([]chan struct{}, n)
	for i := range finished {
		finished[i] = make(chan struct{})
	}
	var mu sync.Mutex
	var completion, merged []int
	process := func(ctx context.Context, a Attempt) (string, error) {
		i := a.Chunk.Index
		if i+1 < n {
			select {
			case <-finished[i+1]:
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		mu.Lock()
		completion = append(completion, i)
		mu.Unlock()
		close(finished[i])
		return label(i), nil
	}
	rep, err := Run(context.Background(), chunks, process, Config[string]{Workers: n,
		OnMerge: func(c Chunk, r string) error {
			if r != label(c.Index) {
				t.Errorf("merge got %q for chunk %d", r, c.Index)
			}
			merged = append(merged, c.Index)
			return nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	for i := range n {
		if completion[i] != n-1-i || merged[i] != i || rep.Results[i] != label(i) {
			t.Fatalf("completion=%v merged=%v results=%v", completion, merged, rep.Results)
		}
	}
	if rep.Merged != n {
		t.Fatalf("merged %d", rep.Merged)
	}
}

func TestOnlyFailedChunksRerun(t *testing.T) {
	chunks := mustChunks(t, 20)
	var mu sync.Mutex
	calls := map[int]int{}
	failOnce := map[int]bool{3: true, 9: true, 17: true}
	process := func(_ context.Context, a Attempt) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		calls[a.Chunk.Index]++
		if failOnce[a.Chunk.Index] && a.Number == 1 {
			return "", errors.New("transient")
		}
		return label(a.Chunk.Index), nil
	}
	rep, err := Run(context.Background(), chunks, process, Config[string]{Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	for i := range chunks {
		want := 1
		if failOnce[i] {
			want = 2
		}
		if calls[i] != want {
			t.Errorf("chunk %d processed %d times, want %d", i, calls[i], want)
		}
		if rep.Results[i] != label(i) {
			t.Errorf("result %d = %q", i, rep.Results[i])
		}
	}
	if s := rep.Stats; s.Failures != 3 || s.Reruns != 3 || s.Attempts != 23 || s.Completed != 20 {
		t.Fatalf("stats %+v", s)
	}
}

func TestChunkFailsAfterMaxAttempts(t *testing.T) {
	chunks := mustChunks(t, 10)
	boom := errors.New("corrupt input")
	var mu sync.Mutex
	calls := 0
	process := func(_ context.Context, a Attempt) (string, error) {
		if a.Chunk.Index == 4 {
			mu.Lock()
			calls++
			mu.Unlock()
			return "", boom
		}
		return label(a.Chunk.Index), nil
	}
	_, err := Run(context.Background(), chunks, process, Config[string]{Workers: 3, MaxAttempts: 3})
	var ce *ChunkError
	if !errors.As(err, &ce) || ce.Index != 4 || ce.Attempts != 3 {
		t.Fatalf("err=%v", err)
	}
	if !errors.Is(err, ErrChunkFailed) || !errors.Is(err, boom) {
		t.Fatalf("error should match ErrChunkFailed and the cause: %v", err)
	}
	if calls != 3 {
		t.Fatalf("chunk 4 tried %d times", calls)
	}
}

func TestCheckpointResume(t *testing.T) {
	chunks := mustChunks(t, 20)
	cp := NewMemoryCheckpoint[string]()
	for i := range 10 {
		_ = cp.Save(i, "cp-"+label(i))
	}
	var mu sync.Mutex
	processed := map[int]bool{}
	var merged []int
	rep, err := Run(context.Background(), chunks, func(_ context.Context, a Attempt) (string, error) {
		mu.Lock()
		processed[a.Chunk.Index] = true
		mu.Unlock()
		return label(a.Chunk.Index), nil
	}, Config[string]{Workers: 4, Checkpoint: cp, OnMerge: func(c Chunk, _ string) error {
		merged = append(merged, c.Index)
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		want := label(i)
		if i < 10 {
			want = "cp-" + label(i)
		}
		if processed[i] != (i >= 10) || rep.Results[i] != want || merged[i] != i {
			t.Fatalf("chunk %d: processed=%v result=%q merged=%v", i, processed[i], rep.Results[i], merged)
		}
	}
	if rep.Stats.Resumed != 10 || rep.Stats.Attempts != 10 || cp.Len() != 20 {
		t.Fatalf("stats %+v cp=%d", rep.Stats, cp.Len())
	}
}

func TestCancelThenResumeRunsOnlyUnfinished(t *testing.T) {
	chunks := mustChunks(t, 30)
	cp := NewMemoryCheckpoint[string]()
	ctx, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	firstRun := map[int]bool{}
	_, err := Run(ctx, chunks, func(ctx context.Context, a Attempt) (string, error) {
		if a.Chunk.Index >= 10 { // the job is cancelled while these are in flight
			<-ctx.Done()
			return "", ctx.Err()
		}
		mu.Lock()
		firstRun[a.Chunk.Index] = true
		mu.Unlock()
		return label(a.Chunk.Index), nil
	}, Config[string]{Workers: 4, Checkpoint: cp, OnMerge: func(c Chunk, _ string) error {
		if c.Index == 9 {
			cancel()
		}
		return nil
	}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	if cp.Len() != 10 {
		t.Fatalf("checkpoint has %d chunks", cp.Len())
	}
	second := map[int]int{}
	rep, err := Run(context.Background(), chunks, func(_ context.Context, a Attempt) (string, error) {
		mu.Lock()
		second[a.Chunk.Index]++
		mu.Unlock()
		return label(a.Chunk.Index), nil
	}, Config[string]{Workers: 4, Checkpoint: cp})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 30 {
		if (second[i] == 1) != (i >= 10) || second[i] > 1 || rep.Results[i] != label(i) {
			t.Fatalf("chunk %d rerun %d times", i, second[i])
		}
	}
}

func TestDeadline(t *testing.T) {
	chunks := mustChunks(t, 5)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	rep, err := Run(ctx, chunks, func(ctx context.Context, a Attempt) (string, error) {
		if a.Chunk.Index == 2 {
			<-ctx.Done() // a stuck encoder that at least honours cancellation
			return "", ctx.Err()
		}
		return label(a.Chunk.Index), nil
	}, Config[string]{Workers: 2})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v", err)
	}
	if rep.Stats.Completed != 4 || rep.Merged != 2 {
		t.Fatalf("completed=%d merged=%d", rep.Stats.Completed, rep.Merged)
	}
}

func TestSpeculativeDuplicateIgnored(t *testing.T) {
	chunks := mustChunks(t, 6)
	var mu sync.Mutex
	calls := map[int]int{}
	merges := map[int]int{}
	process := func(ctx context.Context, a Attempt) (string, error) {
		mu.Lock()
		calls[a.Chunk.Index]++
		mu.Unlock()
		if a.Chunk.Index == 2 && !a.Backup {
			<-ctx.Done()           // the straggler: cancelled once the backup wins...
			return "original", nil // ...but it reports a result anyway
		}
		if a.Backup {
			return "backup", nil
		}
		return label(a.Chunk.Index), nil
	}
	rep, err := Run(context.Background(), chunks, process, Config[string]{Workers: 2, Speculate: true,
		OnMerge: func(c Chunk, _ string) error { merges[c.Index]++; return nil }})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Results[2] != "backup" || calls[2] != 2 {
		t.Fatalf("result=%q calls=%d", rep.Results[2], calls[2])
	}
	if s := rep.Stats; s.Backups != 1 || s.BackupWins != 1 || s.Duplicates != 1 {
		t.Fatalf("stats %+v", s)
	}
	for i := range 6 {
		if merges[i] != 1 {
			t.Fatalf("chunk %d merged %d times", i, merges[i])
		}
	}
}

func TestConcurrentRunWithSpeculationAndFailures(t *testing.T) {
	const n = 500
	chunks := mustChunks(t, n)
	var merged []int
	results := map[int]int{}
	rep, err := Run(context.Background(), chunks, func(ctx context.Context, a Attempt) (string, error) {
		runtime.Gosched()
		if a.Chunk.Index%7 == 0 && a.Number == 1 {
			return "", errors.New("flaky")
		}
		return label(a.Chunk.Index), nil
	}, Config[string]{Workers: 16, Speculate: true, Checkpoint: NewMemoryCheckpoint[string](),
		OnMerge: func(c Chunk, r string) error {
			merged = append(merged, c.Index)
			results[c.Index]++
			return nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	for i := range n {
		if merged[i] != i || results[i] != 1 || rep.Results[i] != label(i) {
			t.Fatalf("chunk %d: merged at %d, %d results, %q", i, merged[i], results[i], rep.Results[i])
		}
	}
	if rep.Stats.Completed != n {
		t.Fatalf("stats %+v", rep.Stats)
	}
}

// ---------------------------------------------------------------------------
// Simulation

func simBase(t *testing.T, workers int) SimConfig {
	return SimConfig{Chunks: mustChunks(t, 600), Workers: workers, CostPerMediaSecond: 3,
		ChunkOverhead: time.Second, Jitter: 0.25, StragglerRate: 0.01, StragglerFactor: 20,
		MergePerChunk: 50 * time.Millisecond, DetectAfter: 30 * time.Second, Seed: 7}
}

func mustSim(t *testing.T, cfg SimConfig) SimReport {
	t.Helper()
	r, err := Simulate(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestSimulateValidation(t *testing.T) {
	mods := map[string]func(*SimConfig){
		"no workers":        func(c *SimConfig) { c.Workers = 0 },
		"no cost":           func(c *SimConfig) { c.CostPerMediaSecond = 0 },
		"jitter >= 1":       func(c *SimConfig) { c.Jitter = 1 },
		"fail rate 1":       func(c *SimConfig) { c.FailRate = 1 },
		"straggler < 1x":    func(c *SimConfig) { c.StragglerFactor = 0.5 },
		"resume length":     func(c *SimConfig) { c.Resume = []bool{true} },
		"unknown crash":     func(c *SimConfig) { c.Crashes = []WorkerCrash{{Worker: 99}} },
		"no chunks":         func(c *SimConfig) { c.Chunks = nil },
		"negative merge":    func(c *SimConfig) { c.MergePerChunk = -1 },
		"negative tries":    func(c *SimConfig) { c.MaxAttempts = -1 },
		"negative straggle": func(c *SimConfig) { c.StragglerRate = -0.1 },
	}
	for name, mod := range mods {
		c := simBase(t, 4)
		mod(&c)
		if _, err := Simulate(c); !errors.Is(err, ErrBadConfig) {
			t.Errorf("%s: err=%v", name, err)
		}
	}
}

func TestSimSpeedupGrowsWithWorkers(t *testing.T) {
	serial := SerialTime(simBase(t, 1))
	prev := time.Duration(1 << 62)
	prevSpeedup := 0.0
	for _, n := range []int{1, 4, 16, 64} {
		r := mustSim(t, simBase(t, n))
		again := mustSim(t, simBase(t, n))
		if r.Makespan != again.Makespan || r.Busy != again.Busy {
			t.Fatalf("simulation not deterministic for N=%d", n)
		}
		sp := float64(serial) / float64(r.Makespan)
		if r.Makespan >= prev || sp <= prevSpeedup || sp > float64(n) {
			t.Fatalf("N=%d makespan %v (prev %v) speedup %.2f", n, r.Makespan, prev, sp)
		}
		if r.Stats.Completed != 600 || r.MergeTail != 600*50*time.Millisecond {
			t.Fatalf("N=%d %+v", n, r)
		}
		prev, prevSpeedup = r.Makespan, sp
	}
}

func TestSimSpeculationTrimsTailWithOneResultPerChunk(t *testing.T) {
	base := mustSim(t, simBase(t, 64))
	c := simBase(t, 64)
	c.Speculate = true
	spec := mustSim(t, c)
	if spec.ProcessingEnd >= base.ProcessingEnd || spec.Stats.Backups == 0 || spec.Stats.BackupWins == 0 {
		t.Fatalf("baseline %v, speculative %v (%+v)", base.ProcessingEnd, spec.ProcessingEnd, spec.Stats)
	}
	if spec.Stats.Completed != 600 || spec.Stats.Duplicates != 0 || spec.Stats.Attempts != 600+spec.Stats.Backups {
		t.Fatalf("each chunk must be accepted exactly once: %+v", spec.Stats)
	}
}

func TestSimStreamingMerge(t *testing.T) {
	c := simBase(t, 64)
	end := mustSim(t, c)
	c.StreamingMerge = true
	stream := mustSim(t, c)
	floor := 600 * c.MergePerChunk
	if stream.Makespan >= end.Makespan || stream.Makespan < floor || stream.Makespan < stream.ProcessingEnd {
		t.Fatalf("end-merge %v streaming %v floor %v", end.Makespan, stream.Makespan, floor)
	}
}

func TestSimWorkerCrashRerunsOnlyInFlight(t *testing.T) {
	for _, k := range []int{1, 3} {
		c := simBase(t, 16)
		for w := range k {
			c.Crashes = append(c.Crashes, WorkerCrash{Worker: w, At: 5 * time.Minute})
		}
		r := mustSim(t, c)
		if r.Failed || r.Stats.Completed != 600 || r.Stats.Lost != k || r.Stats.Reruns != k || r.Stats.Attempts != 600+k {
			t.Fatalf("%d crashes: %+v", k, r.Stats)
		}
	}
	c := simBase(t, 2)
	c.Crashes = []WorkerCrash{{Worker: 0, At: time.Minute}, {Worker: 1, At: time.Minute}}
	if r := mustSim(t, c); !r.Failed || r.Stats.Completed == 600 {
		t.Fatalf("all workers dead should fail the job: %+v", r)
	}
}

func TestSimResumeFromCheckpoint(t *testing.T) {
	c := simBase(t, 16)
	c.StopAt = 5 * time.Minute
	first := mustSim(t, c)
	done := 0
	for _, d := range first.Done {
		if d {
			done++
		}
	}
	if !first.Stopped || done == 0 || done == 600 || first.InFlight != 16 {
		t.Fatalf("first run: stopped=%v done=%d inflight=%d", first.Stopped, done, first.InFlight)
	}
	c.StopAt, c.Resume = 0, first.Done
	second := mustSim(t, c)
	full := mustSim(t, simBase(t, 16))
	if second.Stats.Resumed != done || second.Stats.Attempts != 600-done || second.Stats.Completed != 600 {
		t.Fatalf("resume must process only unfinished chunks: %+v", second.Stats)
	}
	if second.Makespan >= full.Makespan {
		t.Fatalf("resume %v should beat a full rerun %v", second.Makespan, full.Makespan)
	}
}

func TestSimRetries(t *testing.T) {
	c := simBase(t, 8)
	c.FailRate, c.MaxAttempts = 0.2, 10
	r := mustSim(t, c)
	if r.Failed || r.Stats.Completed != 600 || r.Stats.Failures == 0 || r.Stats.Attempts != 600+r.Stats.Failures || r.Wasted == 0 {
		t.Fatalf("%+v", r.Stats)
	}
	c.FailRate, c.MaxAttempts = 0.5, 1
	if r := mustSim(t, c); !r.Failed {
		t.Fatal("a chunk failing its only attempt should fail the job")
	}
}
