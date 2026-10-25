package backpressure

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestConfig(t *testing.T) {
	cases := []struct{ capacity, high int }{{0, 1}, {3, 0}, {3, 4}, {-1, -1}}
	for _, c := range cases {
		if _, err := New[int](c.capacity, c.high); !errors.Is(err, ErrBadConfig) {
			t.Errorf("New(%d,%d) err=%v", c.capacity, c.high, err)
		}
	}
}

func TestTryPutDropsWhenFull(t *testing.T) {
	q, _ := New[int](3, 3)
	accepted := 0
	for i := 0; i < 10; i++ {
		if q.TryPut(i) {
			accepted++
		}
	}
	if accepted != 3 || q.Dropped() != 7 || q.Len() != 3 {
		t.Fatalf("accepted=%d dropped=%d len=%d", accepted, q.Dropped(), q.Len())
	}
}

func TestOfferSignals(t *testing.T) {
	q, _ := New[int](4, 3)
	want := []Signal{Accepted, Accepted, SlowDown, SlowDown, Rejected, Rejected}
	for i, w := range want {
		if got := q.Offer(i); got != w {
			t.Fatalf("offer %d: got %v want %v", i, got, w)
		}
	}
	if q.Rejected() != 2 {
		t.Fatalf("rejected=%d", q.Rejected())
	}
	// Draining relieves pressure.
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := q.Get(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if got := q.Offer(99); got != Accepted {
		t.Fatalf("after drain got %v", got)
	}
}

func TestPutBlocksUntilSpace(t *testing.T) {
	q, _ := New[int](1, 1)
	ctx := context.Background()
	if err := q.Put(ctx, 1); err != nil {
		t.Fatal(err)
	}
	unblocked := make(chan error, 1)
	go func() { unblocked <- q.Put(ctx, 2) }()
	select {
	case err := <-unblocked:
		t.Fatalf("Put should block on full queue, returned %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if v, _ := q.Get(ctx); v != 1 {
		t.Fatalf("got %d", v)
	}
	if err := <-unblocked; err != nil {
		t.Fatal(err)
	}
	if v, _ := q.Get(ctx); v != 2 {
		t.Fatalf("got %d", v)
	}
}

func TestPutHonoursContextAndClose(t *testing.T) {
	q, _ := New[int](1, 1)
	_ = q.Put(context.Background(), 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := q.Put(ctx, 2); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline exceeded, got %v", err)
	}

	blocked := make(chan error, 1)
	go func() { blocked <- q.Put(context.Background(), 3) }()
	time.Sleep(5 * time.Millisecond)
	q.Close()
	q.Close() // idempotent
	if err := <-blocked; !errors.Is(err, ErrClosed) {
		t.Fatalf("blocked Put after Close: %v", err)
	}
	if v, err := q.Get(context.Background()); err != nil || v != 1 {
		t.Fatalf("drain after close: %d %v", v, err)
	}
	if _, err := q.Get(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("empty closed queue: %v", err)
	}
	if q.TryPut(4) || q.Offer(5) != Rejected {
		t.Fatal("closed queue must not accept items")
	}
}

func TestBlockingPipelineLosesNothing(t *testing.T) {
	q, _ := New[int](4, 3)
	const n = 500
	var wg sync.WaitGroup
	for p := 0; p < 4; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n/4; i++ {
				if err := q.Put(context.Background(), i); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	go func() { wg.Wait(); q.Close() }()
	got := 0
	for {
		if _, err := q.Get(context.Background()); err != nil {
			break
		}
		got++
	}
	if got != n {
		t.Fatalf("got %d want %d", got, n)
	}
}
