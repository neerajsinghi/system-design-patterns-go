package bulkhead

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAcquireLimits(t *testing.T) {
	tests := []struct {
		name         string
		cfg          Config
		holders      int // slots taken before the probe
		wantCapacity int
		wantErr      bool
	}{
		{"free slot", Config{MaxConcurrent: 2}, 1, 2, false},
		{"full no queue", Config{MaxConcurrent: 2}, 2, 2, true},
		{"zero config defaults to 1", Config{}, 1, 1, true},
		{"negative waiting treated as 0", Config{MaxConcurrent: 1, MaxWaiting: -3}, 1, 1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := New(tt.cfg)
			for i := 0; i < tt.holders; i++ {
				if err := b.Acquire(context.Background()); err != nil {
					t.Fatalf("holder %d: %v", i, err)
				}
			}
			err := b.Acquire(context.Background())
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && !errors.Is(err, ErrFull) {
				t.Fatalf("err = %v, want ErrFull", err)
			}
			if c := b.Stats().Capacity; c != tt.wantCapacity {
				t.Fatalf("capacity = %d, want %d", c, tt.wantCapacity)
			}
		})
	}
}

func TestWaiterGetsSlotOnRelease(t *testing.T) {
	b := New(Config{MaxConcurrent: 1, MaxWaiting: 1})
	if err := b.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := make(chan error, 1)
	go func() { got <- b.Acquire(context.Background()) }()

	// Wait until the goroutine is queued.
	for b.Stats().Waiting != 1 {
		time.Sleep(time.Millisecond)
	}
	// Queue is full now: a third caller is rejected immediately.
	if err := b.Acquire(context.Background()); !errors.Is(err, ErrFull) {
		t.Fatalf("third caller err = %v, want ErrFull", err)
	}
	b.Release()
	if err := <-got; err != nil {
		t.Fatalf("waiter err = %v", err)
	}
	b.Release()
	if s := b.Stats(); s.InFlight != 0 || s.Waiting != 0 || s.Rejected != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestWaiterTimesOut(t *testing.T) {
	b := New(Config{MaxConcurrent: 1, MaxWaiting: 5})
	_ = b.Acquire(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	err := b.Acquire(ctx)
	if !errors.Is(err, ErrFull) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want ErrFull + DeadlineExceeded", err)
	}
}

func TestReleaseWithoutAcquireIsNoop(t *testing.T) {
	b := New(Config{MaxConcurrent: 1})
	b.Release()
	if err := b.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := b.Acquire(context.Background()); err == nil {
		t.Fatal("extra Release must not create capacity")
	}
}

func TestExecuteNeverExceedsLimit(t *testing.T) {
	const limit = 3
	b := New(Config{MaxConcurrent: limit, MaxWaiting: 100})
	var cur, peak atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = b.Execute(context.Background(), func(context.Context) error {
				n := cur.Add(1)
				for {
					p := peak.Load()
					if n <= p || peak.CompareAndSwap(p, n) {
						break
					}
				}
				time.Sleep(100 * time.Microsecond)
				cur.Add(-1)
				return nil
			})
		}()
	}
	wg.Wait()
	if p := peak.Load(); p > limit {
		t.Fatalf("peak concurrency %d > limit %d", p, limit)
	}
}

func TestGroupIsolatesDependencies(t *testing.T) {
	g := NewGroup(Config{MaxConcurrent: 1}, map[string]Config{"search": {MaxConcurrent: 2}})
	if g.Get("search") != g.Get("search") {
		t.Fatal("Get must return the same bulkhead per name")
	}
	if c := g.Get("search").Stats().Capacity; c != 2 {
		t.Fatalf("search capacity = %d, want 2", c)
	}
	_ = g.Get("payments").Acquire(context.Background())
	if err := g.Get("payments").Acquire(context.Background()); !errors.Is(err, ErrFull) {
		t.Fatalf("payments should be full, got %v", err)
	}
	if err := g.Get("search").Acquire(context.Background()); err != nil {
		t.Fatalf("search must be unaffected by payments: %v", err)
	}
}
