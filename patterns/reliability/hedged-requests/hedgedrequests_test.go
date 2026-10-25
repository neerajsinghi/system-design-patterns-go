package hedgedrequests

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// manualTimer lets a test decide exactly when the hedge delay "elapses".
type manualTimer struct {
	mu    sync.Mutex
	chans []chan time.Time
}

func (m *manualTimer) After(time.Duration) <-chan time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := make(chan time.Time, 1)
	m.chans = append(m.chans, c)
	return c
}

// fire triggers the most recently armed timer.
func (m *manualTimer) fire() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n := len(m.chans); n > 0 {
		m.chans[n-1] <- time.Time{}
	}
}

func (m *manualTimer) armed() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.chans)
}

func TestFastPrimaryNoHedge(t *testing.T) {
	var calls atomic.Int32
	v, winner, err := Do(context.Background(), Config{Delay: time.Hour, MaxAttempts: 3},
		func(context.Context, int) (string, error) { calls.Add(1); return "ok", nil })
	if err != nil || v != "ok" || winner != 0 || calls.Load() != 1 {
		t.Fatalf("v=%q winner=%d err=%v calls=%d", v, winner, err, calls.Load())
	}
}

func TestSlowPrimaryHedgeWinsAndLoserCancelled(t *testing.T) {
	mt := &manualTimer{}
	loserCancelled := make(chan struct{})
	go func() {
		for mt.armed() == 0 {
			time.Sleep(100 * time.Microsecond)
		}
		mt.fire() // the delay elapses with attempt 0 still running
	}()
	v, winner, err := Do(context.Background(), Config{Delay: time.Second, MaxAttempts: 2, After: mt.After},
		func(ctx context.Context, attempt int) (int, error) {
			if attempt == 0 {
				<-ctx.Done() // slow replica; only returns once cancelled
				close(loserCancelled)
				return 0, ctx.Err()
			}
			return 42, nil
		})
	if err != nil || v != 42 || winner != 1 {
		t.Fatalf("v=%d winner=%d err=%v", v, winner, err)
	}
	select {
	case <-loserCancelled:
	case <-time.After(time.Second):
		t.Fatal("losing attempt was not cancelled")
	}
}

func TestErrorTriggersImmediateHedge(t *testing.T) {
	// Delay is an hour and the timer never fires: attempt 1 must still start
	// because attempt 0 failed.
	mt := &manualTimer{}
	v, winner, err := Do(context.Background(), Config{Delay: time.Hour, MaxAttempts: 2, After: mt.After},
		func(_ context.Context, attempt int) (string, error) {
			if attempt == 0 {
				return "", errors.New("replica 0 down")
			}
			return "from replica 1", nil
		})
	if err != nil || winner != 1 || v != "from replica 1" {
		t.Fatalf("v=%q winner=%d err=%v", v, winner, err)
	}
}

func TestAllFail(t *testing.T) {
	tests := []struct {
		name     string
		delay    time.Duration
		attempts int
	}{
		{"sequential failures", time.Hour, 3},
		{"parallel (zero delay)", 0, 3},
		{"default attempts", time.Hour, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			_, winner, err := Do(context.Background(), Config{Delay: tt.delay, MaxAttempts: tt.attempts},
				func(_ context.Context, a int) (int, error) { calls.Add(1); return 0, fmt.Errorf("fail %d", a) })
			want := tt.attempts
			if want <= 0 {
				want = 2
			}
			if err == nil || winner != -1 || int(calls.Load()) != want {
				t.Fatalf("err=%v winner=%d calls=%d want %d", err, winner, calls.Load(), want)
			}
		})
	}
}

func TestZeroDelayLaunchesAll(t *testing.T) {
	var started sync.WaitGroup
	started.Add(3)
	release := make(chan struct{})
	go func() { started.Wait(); close(release) }()
	_, _, err := Do(context.Background(), Config{Delay: 0, MaxAttempts: 3},
		func(ctx context.Context, a int) (int, error) {
			started.Done()
			<-release // only proceeds once all 3 are running concurrently
			return a, nil
		})
	if err != nil {
		t.Fatal(err)
	}
}

func TestParentCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(2 * time.Millisecond); cancel() }()
	_, _, err := Do(ctx, Config{Delay: time.Hour, MaxAttempts: 2},
		func(ctx context.Context, _ int) (int, error) { <-ctx.Done(); return 0, ctx.Err() })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want Canceled", err)
	}
}

func TestLatencyTracker(t *testing.T) {
	tr := NewLatencyTracker(100)
	if got := tr.Quantile(0.95, time.Second); got != time.Second {
		t.Fatalf("empty quantile = %v, want fallback", got)
	}
	for i := 1; i <= 100; i++ {
		tr.Observe(time.Duration(i) * time.Millisecond)
	}
	tests := []struct {
		q    float64
		want time.Duration
	}{{0, time.Millisecond}, {0.5, 50 * time.Millisecond}, {0.95, 95 * time.Millisecond}, {1, 100 * time.Millisecond}, {7, 100 * time.Millisecond}}
	for _, tt := range tests {
		if got := tr.Quantile(tt.q, 0); got != tt.want {
			t.Fatalf("q%.2f = %v, want %v", tt.q, got, tt.want)
		}
	}
	// Ring buffer: newer samples replace the oldest.
	for i := 0; i < 100; i++ {
		tr.Observe(time.Second)
	}
	if got := tr.Quantile(0, 0); got != time.Second {
		t.Fatalf("after wrap min = %v, want 1s", got)
	}
}
