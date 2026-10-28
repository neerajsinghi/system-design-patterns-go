package tieredcache

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

// origin counts calls and blocks until released, so tests can line up
// concurrent misses deterministically.
type origin struct {
	calls   atomic.Int64
	release chan struct{}
	fail    atomic.Bool
}

func newOrigin() *origin { return &origin{release: make(chan struct{})} }

func (o *origin) fetch(_ context.Context, key string) (string, error) {
	o.calls.Add(1)
	<-o.release
	if o.fail.Load() {
		return "", errors.New("origin 503")
	}
	return "content:" + key, nil
}

func instantOrigin() *origin { o := newOrigin(); close(o.release); return o }

func TestNewTierValidation(t *testing.T) {
	o := instantOrigin()
	if _, err := NewTier[string]("edge", time.Minute, nil); err == nil {
		t.Fatal("expected error for nil parent")
	}
	if _, err := NewTier("edge", 0, o.fetch); err == nil {
		t.Fatal("expected error for zero ttl")
	}
}

// burst sends n concurrent requests for key to each edge, waits until every
// request is either waiting or done, then releases the origin.
func burst(t *testing.T, o *origin, edges []*Tier[string], key string, n int) {
	t.Helper()
	var wg sync.WaitGroup
	for _, e := range edges {
		for range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if v, err := e.Fetch(context.Background(), key); err != nil || v != "content:"+key {
					t.Errorf("%s: got (%q, %v)", e.Name(), v, err)
				}
			}()
		}
	}
	for {
		var misses uint64
		for _, e := range edges {
			misses += e.Stats().Misses + e.Stats().Hits
		}
		if misses == uint64(len(edges)*n) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(o.release)
	wg.Wait()
}

func TestOriginLoadDuringLaunch(t *testing.T) {
	tests := []struct {
		name            string
		shield, prewarm bool
		wantOriginCalls int64
	}{
		{"edges straight to origin: one call per edge", false, false, 5},
		{"origin shield: one call in total", true, false, 1},
		{"prewarmed: no calls during the launch", true, true, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := newOrigin()
			parent := Fetcher[string](o.fetch)
			var tiers []*Tier[string]
			if tt.shield {
				shield, _ := NewTier("shield", time.Hour, o.fetch)
				parent = shield.Fetch
				tiers = append(tiers, shield)
			}
			edges := make([]*Tier[string], 5)
			for i := range edges {
				edges[i], _ = NewTier(fmt.Sprintf("edge%d", i), time.Hour, parent)
			}
			if tt.prewarm {
				warm := instantOrigin()
				if err := Prewarm(context.Background(), warm.fetch, []string{"ep1"}, append(tiers, edges...)...); err != nil {
					t.Fatal(err)
				}
			}
			burst(t, o, edges, "ep1", 200)
			if got := o.calls.Load(); got != tt.wantOriginCalls {
				t.Fatalf("origin calls = %d, want %d", got, tt.wantOriginCalls)
			}
		})
	}
}

func TestEdgeCoalescesItsOwnMisses(t *testing.T) {
	o := newOrigin()
	edge, _ := NewTier("edge", time.Hour, o.fetch)
	burst(t, o, []*Tier[string]{edge}, "k", 100)
	s := edge.Stats()
	if s.Fetches != 1 || s.Shared != 99 || o.calls.Load() != 1 {
		t.Fatalf("stats %+v, origin calls %d; want 1 fetch shared by 99 waiters", s, o.calls.Load())
	}
}

func TestTTLExpiryRefetches(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	o := instantOrigin()
	shield, _ := NewTier("shield", time.Hour, o.fetch, WithClock(clk.Now))
	edge, _ := NewTier("edge", time.Minute, shield.Fetch, WithClock(clk.Now))
	ctx := context.Background()

	edge.Fetch(ctx, "k")
	clk.Advance(time.Minute) // edge entry expires; shield's is still fresh
	edge.Fetch(ctx, "k")
	if o.calls.Load() != 1 || edge.Stats().Fetches != 2 || shield.Stats().Hits != 1 {
		t.Fatalf("origin %d, edge %+v, shield %+v: want the shield to absorb the edge's refetch",
			o.calls.Load(), edge.Stats(), shield.Stats())
	}
}

func TestErrorsAreNotCached(t *testing.T) {
	o := instantOrigin()
	o.fail.Store(true)
	edge, _ := NewTier("edge", time.Hour, o.fetch)
	ctx := context.Background()
	if _, err := edge.Fetch(ctx, "k"); err == nil {
		t.Fatal("expected the origin error")
	}
	o.fail.Store(false)
	if v, err := edge.Fetch(ctx, "k"); err != nil || v != "content:k" {
		t.Fatalf("got (%q, %v) after recovery", v, err)
	}
}

func TestPurgeForcesRefetch(t *testing.T) {
	o := instantOrigin()
	edge, _ := NewTier("edge", time.Hour, o.fetch)
	ctx := context.Background()
	edge.Fetch(ctx, "k")
	edge.Purge("k")
	edge.Fetch(ctx, "k")
	if o.calls.Load() != 2 {
		t.Fatalf("origin calls = %d, want 2", o.calls.Load())
	}
}

func TestCallerCancellationDoesNotFailOthers(t *testing.T) {
	o := newOrigin()
	edge, _ := NewTier("edge", time.Hour, o.fetch)
	ctx, cancel := context.WithCancel(context.Background())
	firstDone := make(chan error)
	go func() { _, err := edge.Fetch(ctx, "k"); firstDone <- err }()
	for edge.Stats().Fetches == 0 {
		time.Sleep(time.Millisecond)
	}
	secondDone := make(chan string)
	go func() { v, _ := edge.Fetch(context.Background(), "k"); secondDone <- v }()
	for edge.Stats().Shared == 0 {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("first caller err = %v, want context.Canceled", err)
	}
	close(o.release)
	if v := <-secondDone; v != "content:k" {
		t.Fatalf("second caller got %q", v)
	}
}

func TestHitRatio(t *testing.T) {
	tests := []struct {
		s    Stats
		want float64
	}{
		{Stats{}, 0},
		{Stats{Hits: 3, Misses: 1}, 0.75},
	}
	for _, tt := range tests {
		if got := tt.s.HitRatio(); got != tt.want {
			t.Fatalf("HitRatio(%+v) = %v, want %v", tt.s, got, tt.want)
		}
	}
}
