package scattergather

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// src returns a source that answers v after d, or fails if err is set. It
// honours cancellation.
func src(name string, d time.Duration, v int, err error) Source[int] {
	return Source[int]{Name: name, Fetch: func(ctx context.Context) (int, error) {
		select {
		case <-time.After(d):
			return v, err
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}}
}

func names(rs []Response[int]) string {
	s := ""
	for _, r := range rs {
		s += r.Name + ","
	}
	return s
}

func TestGather(t *testing.T) {
	boom := errors.New("boom")
	fast, slow := 5*time.Millisecond, 500*time.Millisecond
	tests := []struct {
		name        string
		sources     []Source[int]
		opts        Options
		wantOK      string
		wantFailed  string
		wantMissing string
		maxTime     time.Duration
	}{
		{"all succeed", []Source[int]{src("a", fast, 1, nil), src("b", fast, 2, nil)}, Options{Timeout: time.Second}, "a,b,", "", "", 200 * time.Millisecond},
		{"slow shard is missing, rest returned", []Source[int]{src("a", fast, 1, nil), src("slow", slow, 2, nil), src("c", fast, 3, nil)}, Options{Timeout: 50 * time.Millisecond}, "a,c,", "", "slow,", 300 * time.Millisecond},
		{"error is reported, not fatal", []Source[int]{src("a", fast, 1, nil), src("bad", fast, 0, boom)}, Options{Timeout: time.Second}, "a,", "bad,", "", 200 * time.Millisecond},
		{"need 2 of 3 returns early", []Source[int]{src("a", fast, 1, nil), src("b", fast, 2, nil), src("slow", slow, 3, nil)}, Options{Timeout: time.Second, Need: 2}, "a,b,", "", "slow,", 300 * time.Millisecond},
		{"errors don't count towards need", []Source[int]{src("bad", fast, 0, boom), src("a", 20*time.Millisecond, 1, nil)}, Options{Need: 1}, "a,", "bad,", "", 300 * time.Millisecond},
		{"nil fetch", []Source[int]{{Name: "nil"}}, Options{}, "", "nil,", "", 100 * time.Millisecond},
		{"panicking source", []Source[int]{{Name: "p", Fetch: func(context.Context) (int, error) { panic("oops") }}}, Options{}, "", "p,", "", 100 * time.Millisecond},
		{"no sources", nil, Options{}, "", "", "", 50 * time.Millisecond},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Now()
			res := Gather(context.Background(), tc.sources, tc.opts)
			if took := time.Since(start); took > tc.maxTime {
				t.Errorf("took %v, want < %v", took, tc.maxTime)
			}
			if got := names(res.Responses); got != tc.wantOK {
				t.Errorf("ok = %q, want %q", got, tc.wantOK)
			}
			if got := names(res.Failed); got != tc.wantFailed {
				t.Errorf("failed = %q, want %q", got, tc.wantFailed)
			}
			missing := ""
			for _, m := range res.Missing {
				missing += m + ","
			}
			if missing != tc.wantMissing {
				t.Errorf("missing = %q, want %q", missing, tc.wantMissing)
			}
			if res.Complete() != (tc.wantFailed == "" && tc.wantMissing == "") {
				t.Error("Complete() disagrees")
			}
		})
	}
}

func TestPanicErrorType(t *testing.T) {
	res := Gather(context.Background(), []Source[int]{{Name: "p", Fetch: func(context.Context) (int, error) { panic(42) }}}, Options{})
	var pe *PanicError
	if len(res.Failed) != 1 || !errors.As(res.Failed[0].Err, &pe) || pe.Value != 42 {
		t.Errorf("got %+v", res.Failed)
	}
}

func TestParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	res := Gather(ctx, []Source[int]{src("a", time.Second, 1, nil), src("b", time.Second, 2, nil)}, Options{})
	if len(res.Missing) != 2 {
		t.Errorf("missing = %v", res.Missing)
	}
}

func TestStragglersAreCancelledAndDontLeak(t *testing.T) {
	var cancelled atomic.Int32
	slow := Source[int]{Name: "slow", Fetch: func(ctx context.Context) (int, error) {
		<-ctx.Done() // only returns if Gather cancels it
		cancelled.Add(1)
		return 0, ctx.Err()
	}}
	before := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		Gather(context.Background(), []Source[int]{slow, src("fast", 0, 1, nil)}, Options{Need: 1})
	}
	deadline := time.Now().Add(time.Second)
	for cancelled.Load() < 20 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if cancelled.Load() != 20 {
		t.Fatalf("only %d of 20 stragglers were cancelled", cancelled.Load())
	}
	for runtime.NumGoroutine() > before+2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before+2 {
		t.Errorf("goroutines: before %d, after %d", before, n)
	}
}

func TestDuplicateNames(t *testing.T) {
	var sources []Source[int]
	for i := 0; i < 3; i++ {
		sources = append(sources, src("same", 0, i, nil))
	}
	if res := Gather(context.Background(), sources, Options{Timeout: time.Second}); len(res.Responses) != 3 {
		t.Errorf("got %d responses", len(res.Responses))
	}
}
