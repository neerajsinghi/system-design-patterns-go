package refreshahead

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (f *fakeClock) Now() time.Time          { f.mu.Lock(); defer f.mu.Unlock(); return f.t }
func (f *fakeClock) Advance(d time.Duration) { f.mu.Lock(); f.t = f.t.Add(d); f.mu.Unlock() }

func fixedRand(v float64) func() float64 { return func() float64 { return v } }

func TestNewValidation(t *testing.T) {
	load := func(context.Context, string) (int, error) { return 0, nil }
	tests := []struct {
		name string
		load Loader[string, int]
		ttl  time.Duration
		opts []Option
	}{
		{"nil loader", nil, time.Second, nil},
		{"zero ttl", load, 0, nil},
		{"negative jitter", load, time.Second, []Option{WithJitter(-0.1)}},
		{"jitter of one", load, time.Second, []Option{WithJitter(1)}},
		{"negative beta", load, time.Second, []Option{WithEarlyRefresh(-1)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.load, tt.ttl, tt.opts...); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestJitterBounds(t *testing.T) {
	tests := []struct {
		name string
		r    float64
		want time.Duration // effective TTL
	}{
		{"rand 0 keeps the full ttl", 0, 100 * time.Second},
		{"rand 0.5 trims 10%", 0.5, 90 * time.Second},
		{"rand near 1 trims almost 20%", 0.999, 80*time.Second + 20*time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clk := &fakeClock{t: time.Unix(0, 0)}
			var loads atomic.Int64
			c, _ := New(func(context.Context, string) (int, error) { return int(loads.Add(1)), nil },
				100*time.Second, WithClock(clk.Now), WithJitter(0.2), WithRand(fixedRand(tt.r)))
			ctx := context.Background()
			c.Get(ctx, "k")
			clk.Advance(tt.want - time.Millisecond)
			if c.Get(ctx, "k"); loads.Load() != 1 {
				t.Fatalf("reloaded before the effective ttl %v", tt.want)
			}
			clk.Advance(time.Millisecond)
			if c.Get(ctx, "k"); loads.Load() != 2 {
				t.Fatalf("did not reload at the effective ttl %v", tt.want)
			}
		})
	}
}

func TestEarlyRefreshProbability(t *testing.T) {
	// ttl 60s, recompute cost 2s, beta 1. XFetch refreshes when
	// remaining <= cost * beta * -ln(1-r).
	tests := []struct {
		name      string
		remaining time.Duration
		r         float64 // so -ln(1-r) is known
		want      bool
	}{
		{"far from expiry, typical draw", 30 * time.Second, 0.5, false},        // gap 1.39s
		{"near expiry, typical draw", time.Second, 0.5, true},                  // gap 1.39s >= 1s
		{"near expiry, low draw", time.Second, 0.1, false},                     // gap 0.21s
		{"far from expiry, very rare draw", 30 * time.Second, 0.9999999, true}, // gap 32s
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clk := &fakeClock{t: time.Unix(0, 0)}
			var loads atomic.Int64
			c, _ := New(func(context.Context, string) (int, error) { return int(loads.Add(1)), nil },
				60*time.Second, WithClock(clk.Now), WithRand(fixedRand(tt.r)), WithEarlyRefresh(1),
				WithLoadCost(func(string) time.Duration { return 2 * time.Second }))
			ctx := context.Background()
			c.Get(ctx, "k")
			clk.Advance(60*time.Second - tt.remaining)
			v, err := c.Get(ctx, "k")
			refreshed := loads.Load() == 2
			if err != nil || refreshed != tt.want {
				t.Fatalf("refreshed = %v (value %d, err %v), want %v", refreshed, v, err, tt.want)
			}
			if refreshed && (v != 2 || c.Stats().EarlyRefreshes != 1) {
				t.Fatalf("early refresh returned %d with stats %+v", v, c.Stats())
			}
		})
	}
}

func TestEarlyRefreshUsesMeasuredLoadTime(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	var loads atomic.Int64
	c, _ := New(func(context.Context, string) (int, error) {
		clk.Advance(3 * time.Second) // the query takes 3s of clock time
		return int(loads.Add(1)), nil
	}, 60*time.Second, WithClock(clk.Now), WithRand(fixedRand(0.5)), WithEarlyRefresh(1))
	ctx := context.Background()
	c.Get(ctx, "k")               // entry expires 60s after the load finished
	clk.Advance(58 * time.Second) // 2s left; gap = 3s * ln 2 = 2.08s
	c.Get(ctx, "k")
	if loads.Load() != 2 {
		t.Fatalf("loads = %d, want an early refresh based on the measured 3s cost", loads.Load())
	}
}

func TestFailedEarlyRefreshServesCachedValue(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	fail := false
	c, _ := New(func(context.Context, string) (string, error) {
		if fail {
			return "", errors.New("db down")
		}
		return "v1", nil
	}, time.Minute, WithClock(clk.Now), WithRand(fixedRand(0.99)), WithEarlyRefresh(1),
		WithLoadCost(func(string) time.Duration { return time.Second }))
	ctx := context.Background()
	c.Get(ctx, "k")
	fail = true
	clk.Advance(59 * time.Second)
	if v, err := c.Get(ctx, "k"); err != nil || v != "v1" {
		t.Fatalf("got (%q, %v), want the cached v1", v, err)
	}
	if s := c.Stats(); s.EarlyRefreshes != 1 {
		t.Fatalf("stats %+v, want one attempted early refresh", s)
	}
}

func TestConcurrentMissesShareOneLoad(t *testing.T) {
	var loads atomic.Int64
	release := make(chan struct{})
	c, _ := New(func(context.Context, string) (int, error) {
		loads.Add(1)
		<-release
		return 7, nil
	}, time.Minute)
	ctx := context.Background()

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() { defer wg.Done(); c.Get(ctx, "k") }()
	}
	for c.Stats().Misses < 50 {
		time.Sleep(time.Millisecond)
	}
	close(release)
	wg.Wait()
	if loads.Load() != 1 {
		t.Fatalf("loads = %d, want 1", loads.Load())
	}
}

// expirySpread warms n keys at t=0, then reads every key once a second for
// 70 seconds and returns the largest number of loads in any one second.
func expirySpread(t *testing.T, opts ...Option) (peak int) {
	t.Helper()
	clk := &fakeClock{t: time.Unix(0, 0)}
	perSecond := map[int64]int{}
	c, _ := New(func(context.Context, string) (int, error) {
		perSecond[clk.Now().Unix()]++
		return 0, nil
	}, 60*time.Second, append([]Option{WithClock(clk.Now)}, opts...)...)
	ctx := context.Background()
	for range 70 {
		for i := range 200 {
			c.Get(ctx, fmt.Sprint(i))
		}
		clk.Advance(time.Second)
	}
	delete(perSecond, 0) // the initial warm-up isn't an expiry spike
	for _, n := range perSecond {
		peak = max(peak, n)
	}
	return peak
}

func TestJitterSpreadsSynchronisedExpiry(t *testing.T) {
	seq := 0.0
	spread := func() float64 { seq += 0.618033988749895; return seq - float64(int(seq)) } // evenly spread draws

	fixed := expirySpread(t)
	jittered := expirySpread(t, WithJitter(0.2), WithRand(spread))
	if fixed != 200 {
		t.Fatalf("fixed TTL peak = %d, want all 200 keys reloading in one second", fixed)
	}
	if jittered > 40 {
		t.Fatalf("jittered peak = %d, want the reloads spread across the 12s jitter window", jittered)
	}
}

func TestInvalidateDuringLoadIsNotCached(t *testing.T) {
	var loads atomic.Int64
	started, release := make(chan struct{}), make(chan struct{})
	c, _ := New(func(context.Context, string) (int, error) {
		if loads.Add(1) == 1 {
			close(started)
			<-release
		}
		return int(loads.Load()), nil
	}, time.Minute)
	ctx := context.Background()
	done := make(chan struct{})
	go func() { c.Get(ctx, "k"); close(done) }()
	<-started
	c.Invalidate("k")
	close(release)
	<-done
	if v, _ := c.Get(ctx, "k"); v != 2 {
		t.Fatalf("got %d, want a fresh load after invalidation", v)
	}
}
