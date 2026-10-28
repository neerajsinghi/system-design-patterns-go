package servestale

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

var errDown = errors.New("origin down")

// origin is a fake backend that returns "v<n>" for the n-th successful call.
type origin struct {
	down  atomic.Bool
	calls atomic.Int64
	n     atomic.Int64
}

func (o *origin) load(context.Context, string) (string, error) {
	o.calls.Add(1)
	if o.down.Load() {
		return "", errDown
	}
	return "v" + string(rune('0'+o.n.Add(1))), nil
}

func TestNewValidation(t *testing.T) {
	load := (&origin{}).load
	tests := []struct {
		name       string
		load       Loader[string, string]
		ttl, stale time.Duration
		opts       []Option
	}{
		{"nil loader", nil, time.Second, time.Second, nil},
		{"zero ttl", load, 0, time.Second, nil},
		{"negative stale window", load, time.Second, -time.Second, nil},
		{"negative retry", load, time.Second, time.Second, []Option{WithRetryInterval(-1)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.load, tt.ttl, tt.stale, tt.opts...); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestLifecycle(t *testing.T) {
	// ttl 10s, stale-if-error 50s: a value is usable for 60s while the origin is down.
	clk := &fakeClock{t: time.Unix(0, 0)}
	o := &origin{}
	c, _ := New(o.load, 10*time.Second, 50*time.Second, WithClock(clk.Now))
	ctx := context.Background()

	steps := []struct {
		name      string
		advance   time.Duration
		down      bool
		want      string
		wantStale bool
		wantErr   bool
	}{
		{"cold read fetches", 0, false, "v1", false, false},
		{"fresh hit", 5 * time.Second, false, "v1", false, false},
		{"expired, origin up: refresh", 5 * time.Second, false, "v2", false, false},
		{"expired, origin down: stale", 10 * time.Second, true, "v2", true, false},
		{"still inside the grace window", 49 * time.Second, true, "v2", true, false},
		{"past ttl+grace: error", time.Second, true, "", false, true},
		{"origin recovers", time.Second, false, "v3", false, false},
	}
	for _, s := range steps {
		clk.Advance(s.advance)
		o.down.Store(s.down)
		r, err := c.Get(ctx, "k")
		if (err != nil) != s.wantErr || r.Value != s.want || r.Stale != s.wantStale {
			t.Fatalf("%s: got %+v, err %v; want %q stale=%v err=%v", s.name, r, err, s.want, s.wantStale, s.wantErr)
		}
		if s.wantStale && !errors.Is(r.Cause, errDown) {
			t.Fatalf("%s: stale result should carry the origin error, got %v", s.name, r.Cause)
		}
	}
}

func TestStaleAgeReported(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	o := &origin{}
	c, _ := New(o.load, time.Second, time.Minute, WithClock(clk.Now))
	ctx := context.Background()
	c.Get(ctx, "k")
	o.down.Store(true)
	clk.Advance(30 * time.Second)
	if r, _ := c.Get(ctx, "k"); r.Age != 30*time.Second {
		t.Fatalf("Age = %v, want 30s", r.Age)
	}
}

func TestNoEntryAndOriginDownIsAnError(t *testing.T) {
	o := &origin{}
	o.down.Store(true)
	c, _ := New(o.load, time.Second, time.Hour)
	if _, err := c.Get(context.Background(), "k"); !errors.Is(err, errDown) {
		t.Fatalf("err = %v, want the origin error", err)
	}
}

func TestRetryIntervalSparesTheOrigin(t *testing.T) {
	tests := []struct {
		name      string
		retry     time.Duration
		wantCalls int64 // origin calls during 100 reads, one every 100ms, while down
	}{
		{"no retry interval: every read hits the failing origin", 0, 100},
		{"1s retry interval: about one call per second", time.Second, 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clk := &fakeClock{t: time.Unix(0, 0)}
			o := &origin{}
			c, _ := New(o.load, time.Second, time.Hour, WithClock(clk.Now), WithRetryInterval(tt.retry))
			ctx := context.Background()
			c.Get(ctx, "k")
			clk.Advance(time.Second)
			o.down.Store(true)
			before := o.calls.Load()
			for range 100 {
				if r, err := c.Get(ctx, "k"); err != nil || !r.Stale {
					t.Fatalf("got %+v, %v; want a stale value", r, err)
				}
				clk.Advance(100 * time.Millisecond)
			}
			if got := o.calls.Load() - before; got != tt.wantCalls {
				t.Fatalf("origin calls = %d, want %d", got, tt.wantCalls)
			}
		})
	}
}

func TestStats(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	o := &origin{}
	c, _ := New(o.load, time.Second, time.Second, WithClock(clk.Now))
	ctx := context.Background()
	c.Get(ctx, "k") // refreshed (first fetch)
	c.Get(ctx, "k") // fresh
	o.down.Store(true)
	clk.Advance(time.Second)
	c.Get(ctx, "k") // stale
	clk.Advance(time.Second)
	c.Get(ctx, "k") // error
	want := Stats{Fresh: 1, Refreshed: 1, Stale: 1, Errors: 1, OriginCalls: 3}
	if got := c.Stats(); got != want {
		t.Fatalf("Stats = %+v, want %+v", got, want)
	}
}

func TestConcurrentReads(t *testing.T) {
	o := &origin{}
	c, _ := New(o.load, time.Millisecond, time.Hour)
	ctx := context.Background()
	c.Get(ctx, "k")
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 200 {
				o.down.Store((i+g)%3 == 0)
				if r, err := c.Get(ctx, "k"); err != nil || r.Value == "" {
					t.Errorf("got %+v, %v", r, err)
					return
				}
			}
		}()
	}
	wg.Wait()
}
