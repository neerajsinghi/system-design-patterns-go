package cacheaside

import (
	"context"
	"errors"
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
