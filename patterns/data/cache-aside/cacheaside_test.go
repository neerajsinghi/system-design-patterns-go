package cacheaside

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

func TestNewValidation(t *testing.T) {
	load := func(context.Context, string) (int, error) { return 0, nil }
	tests := []struct {
		name string
		load Loader[string, int]
		ttl  time.Duration
	}{
		{"nil loader", nil, time.Second},
		{"zero ttl", load, 0},
		{"negative ttl", load, -time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.load, tt.ttl); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestHitMissExpiry(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	var loads atomic.Int64
	c, err := New(func(_ context.Context, k string) (string, error) {
		n := loads.Add(1)
		return k + "-v" + string(rune('0'+n)), nil
	}, time.Minute, WithClock(clk.Now))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	steps := []struct {
		name    string
		advance time.Duration
		want    string
		loads   int64
	}{
		{"first read misses and loads", 0, "a-v1", 1},
		{"second read hits", 30 * time.Second, "a-v1", 1},
		{"read just before ttl hits", 29 * time.Second, "a-v1", 1},
		{"read at ttl reloads", time.Second, "a-v2", 2},
	}
	for _, s := range steps {
		clk.Advance(s.advance)
		got, err := c.Get(ctx, "a")
		if err != nil || got != s.want || loads.Load() != s.loads {
			t.Fatalf("%s: got (%q, %v) loads=%d, want %q loads=%d", s.name, got, err, loads.Load(), s.want, s.loads)
		}
	}
}

func TestInvalidateForcesReload(t *testing.T) {
	db := map[string]int{"x": 1}
	var mu sync.Mutex
	c, _ := New(func(_ context.Context, k string) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return db[k], nil
	}, time.Hour)
	ctx := context.Background()
	if v, _ := c.Get(ctx, "x"); v != 1 {
		t.Fatalf("got %d", v)
	}
	mu.Lock()
	db["x"] = 2 // write to source of truth...
	mu.Unlock()
	if v, _ := c.Get(ctx, "x"); v != 1 {
		t.Fatalf("expected stale cached 1 before invalidate, got %d", v)
	}
	c.Invalidate("x") // ...then invalidate
	if v, _ := c.Get(ctx, "x"); v != 2 {
		t.Fatalf("expected 2 after invalidate, got %d", v)
	}
}

func TestErrorsAreNotCached(t *testing.T) {
	var calls atomic.Int64
	boom := errors.New("db down")
	c, _ := New(func(context.Context, string) (int, error) {
		if calls.Add(1) == 1 {
			return 0, boom
		}
		return 7, nil
	}, time.Hour)
	if _, err := c.Get(context.Background(), "k"); !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
	if v, err := c.Get(context.Background(), "k"); err != nil || v != 7 {
		t.Fatalf("want 7, got %d %v", v, err)
	}
}

func TestStampedeProtection(t *testing.T) {
	const callers = 50
	release := make(chan struct{})
	var loads atomic.Int64
	c, _ := New(func(context.Context, string) (int, error) {
		loads.Add(1)
		<-release
		return 42, nil
	}, time.Hour)

	var wg sync.WaitGroup
	results := make(chan int, callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := c.Get(context.Background(), "hot")
			if err != nil {
				t.Error(err)
			}
			results <- v
		}()
	}
	// Wait until every caller has joined the single in-flight load.
	deadline := time.Now().Add(time.Second)
	for c.Stats().Misses < callers {
		if time.Now().After(deadline) {
			t.Fatal("callers did not all arrive")
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	wg.Wait()
	close(results)
	for v := range results {
		if v != 42 {
			t.Fatalf("got %d", v)
		}
	}
	if n := loads.Load(); n != 1 {
		t.Fatalf("loader ran %d times, want 1", n)
	}
	if s := c.Stats(); s.Shared != callers-1 {
		t.Fatalf("shared=%d, want %d", s.Shared, callers-1)
	}
}

func TestWaiterContextCancel(t *testing.T) {
	release := make(chan struct{})
	c, _ := New(func(context.Context, string) (int, error) {
		<-release
		return 1, nil
	}, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Get(ctx, "k"); !errors.Is(err, context.Canceled) {
		t.Fatalf("want Canceled, got %v", err)
	}
	close(release)
	// The detached load still completes and populates the cache.
	if v, err := c.Get(context.Background(), "k"); err != nil || v != 1 {
		t.Fatalf("got %d %v", v, err)
	}
}

func TestInvalidateDuringLoadDoesNotCacheStale(t *testing.T) {
	release := make(chan struct{})
	var loads atomic.Int64
	c, _ := New(func(context.Context, string) (int, error) {
		if loads.Add(1) == 1 {
			<-release
			return 1, nil // value read before the write
		}
		return 2, nil
	}, time.Hour)

	done := make(chan int)
	go func() { v, _ := c.Get(context.Background(), "k"); done <- v }()
	for c.Stats().Loads == 0 {
		time.Sleep(time.Millisecond)
	}
	c.Invalidate("k") // a write happened while the load was in flight
	close(release)
	if v := <-done; v != 1 {
		t.Fatalf("in-flight waiter got %d", v)
	}
	if v, _ := c.Get(context.Background(), "k"); v != 2 {
		t.Fatalf("stale value was cached: got %d", v)
	}
}

func TestOptionValidation(t *testing.T) {
	load := func(context.Context, string) (int, error) { return 0, nil }
	tests := []struct {
		name string
		opt  Option
	}{
		{"negative negative-ttl", WithNegativeTTL(-time.Second)},
		{"negative jitter", WithTTLJitter(-0.1)},
		{"jitter of one", WithTTLJitter(1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(load, time.Minute, tt.opt); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestNegativeCaching(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	var mu sync.Mutex
	rows := map[string]string{}
	var loads atomic.Int64
	load := func(_ context.Context, k string) (string, error) {
		loads.Add(1)
		mu.Lock()
		defer mu.Unlock()
		v, ok := rows[k]
		if !ok {
			return "", fmt.Errorf("row %q: %w", k, ErrNotFound)
		}
		return v, nil
	}
	ctx := context.Background()

	t.Run("without the option, every lookup of a missing key loads", func(t *testing.T) {
		loads.Store(0)
		c, _ := New(load, time.Minute, WithClock(clk.Now))
		for range 10 {
			c.Get(ctx, "ghost")
		}
		if loads.Load() != 10 {
			t.Fatalf("loads = %d, want 10", loads.Load())
		}
	})

	t.Run("with the option, a miss is cached for the negative ttl", func(t *testing.T) {
		loads.Store(0)
		c, _ := New(load, time.Minute, WithClock(clk.Now), WithNegativeTTL(5*time.Second))
		for range 10 {
			if _, err := c.Get(ctx, "ghost"); !errors.Is(err, ErrNotFound) {
				t.Fatalf("err = %v, want ErrNotFound", err)
			}
		}
		if loads.Load() != 1 || c.Stats().NegativeHits != 9 {
			t.Fatalf("loads = %d, stats %+v; want 1 load and 9 negative hits", loads.Load(), c.Stats())
		}

		mu.Lock()
		rows["ghost"] = "now exists"
		mu.Unlock()
		if _, err := c.Get(ctx, "ghost"); err == nil {
			t.Fatal("a newly created key should stay hidden until the negative ttl ends")
		}
		clk.Advance(5 * time.Second)
		if v, err := c.Get(ctx, "ghost"); err != nil || v != "now exists" {
			t.Fatalf("after the negative ttl got (%q, %v)", v, err)
		}
	})

	t.Run("invalidate clears a cached miss", func(t *testing.T) {
		mu.Lock()
		delete(rows, "late")
		mu.Unlock()
		c, _ := New(load, time.Minute, WithClock(clk.Now), WithNegativeTTL(time.Hour))
		c.Get(ctx, "late")
		mu.Lock()
		rows["late"] = "created"
		mu.Unlock()
		c.Invalidate("late")
		if v, err := c.Get(ctx, "late"); err != nil || v != "created" {
			t.Fatalf("got (%q, %v), want the created row", v, err)
		}
	})

	t.Run("other errors are still never cached", func(t *testing.T) {
		var calls atomic.Int64
		c, _ := New(func(context.Context, string) (string, error) {
			calls.Add(1)
			return "", errors.New("timeout")
		}, time.Minute, WithNegativeTTL(time.Hour))
		c.Get(ctx, "k")
		c.Get(ctx, "k")
		if calls.Load() != 2 {
			t.Fatalf("calls = %d, want 2", calls.Load())
		}
	})
}

func TestTTLJitter(t *testing.T) {
	tests := []struct {
		name string
		r    float64
		want time.Duration
	}{
		{"rand 0 keeps the full ttl", 0, 100 * time.Second},
		{"rand 0.5 with 20% jitter trims 10%", 0.5, 90 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clk := &fakeClock{t: time.Unix(0, 0)}
			var loads atomic.Int64
			c, _ := New(func(context.Context, string) (int, error) { return int(loads.Add(1)), nil },
				100*time.Second, WithClock(clk.Now), WithTTLJitter(0.2), WithRand(func() float64 { return tt.r }))
			ctx := context.Background()
			c.Get(ctx, "k")
			clk.Advance(tt.want - time.Millisecond)
			c.Get(ctx, "k")
			clk.Advance(time.Millisecond)
			c.Get(ctx, "k")
			if loads.Load() != 2 {
				t.Fatalf("loads = %d, want an expiry exactly at %v", loads.Load(), tt.want)
			}
		})
	}
}
