package backpressure

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSendQueueConfig(t *testing.T) {
	cases := []struct {
		name     string
		capacity int
		policy   OverflowPolicy
		opts     []SendQueueOption
	}{
		{"zero capacity", 0, DropNewest, nil},
		{"unknown policy", 1, OverflowPolicy(9), nil},
		{"negative policy", 1, OverflowPolicy(-1), nil},
		{"zero block timeout", 1, Block, []SendQueueOption{WithBlockTimeout(0)}},
	}
	for _, c := range cases {
		if _, err := NewSendQueue[int](c.capacity, c.policy, c.opts...); !errors.Is(err, ErrSendQueueConfig) {
			t.Errorf("%s: err=%v", c.name, err)
		}
	}
}

// drain pops everything currently queued without blocking.
func drain(q *SendQueue[int]) []int {
	var out []int
	for {
		v, ok := q.TryPop()
		if !ok {
			return out
		}
		out = append(out, v)
	}
}

func equal(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestSendQueueOverflowPolicies(t *testing.T) {
	tests := []struct {
		policy      OverflowPolicy
		wantErr     error // from the pushes that overflow
		wantQueued  []int
		wantDropped int64
		wantDisc    bool
	}{
		{DropNewest, ErrQueueFull, []int{0, 1, 2}, 3, false},
		{DropOldest, nil, []int{3, 4, 5}, 3, false},
		{Block, ErrSendTimeout, []int{0, 1, 2}, 3, false},
		{Disconnect, ErrDisconnected, nil, 1, true}, // later pushes are refused, not counted: the subscriber is gone
	}
	for _, tt := range tests {
		t.Run(tt.policy.String(), func(t *testing.T) {
			q, err := NewSendQueue[int](3, tt.policy, WithBlockTimeout(time.Millisecond))
			if err != nil {
				t.Fatal(err)
			}
			for i := range 6 {
				err := q.Push(i)
				if i < 3 && err != nil {
					t.Fatalf("push %d into a non-full queue: %v", i, err)
				}
				if i >= 3 && !errors.Is(err, tt.wantErr) {
					t.Fatalf("push %d: err=%v want %v", i, err, tt.wantErr)
				}
			}
			if got := drain(q); !equal(got, tt.wantQueued) {
				t.Fatalf("queued %v want %v", got, tt.wantQueued)
			}
			st := q.Stats()
			if st.Dropped != tt.wantDropped || st.Disconnected != tt.wantDisc {
				t.Fatalf("stats %+v", st)
			}
		})
	}
}

func TestBlockWaitsForRoom(t *testing.T) {
	q, _ := NewSendQueue[int](1, Block, WithBlockTimeout(10*time.Second))
	_ = q.Push(1)
	done := make(chan error, 1)
	go func() { done <- q.Push(2) }()
	select {
	case err := <-done:
		t.Fatalf("Push on a full queue returned early: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if v, err := q.Pop(context.Background()); err != nil || v != 1 {
		t.Fatalf("pop = %d, %v", v, err)
	}
	if err := <-done; err != nil {
		t.Fatalf("blocked push should succeed once there is room: %v", err)
	}
	if v, _ := q.Pop(context.Background()); v != 2 {
		t.Fatalf("pop = %d", v)
	}
}

func TestBlockReleasedByClose(t *testing.T) {
	q, _ := NewSendQueue[int](1, Block, WithBlockTimeout(10*time.Second))
	_ = q.Push(1)
	done := make(chan error, 1)
	go func() { done <- q.Push(2) }()
	time.Sleep(5 * time.Millisecond)
	q.Close()
	if err := <-done; !errors.Is(err, ErrClosed) {
		t.Fatalf("err=%v", err)
	}
}

func TestDisconnectStopsQueue(t *testing.T) {
	var calls atomic.Int32
	q, _ := NewSendQueue[int](2, Disconnect, WithOnDisconnect(func() { calls.Add(1) }))
	_ = q.Push(1)
	_ = q.Push(2)
	for range 3 {
		if err := q.Push(3); !errors.Is(err, ErrDisconnected) {
			t.Fatalf("err=%v", err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("OnDisconnect called %d times, want 1", calls.Load())
	}
	if _, err := q.Pop(context.Background()); !errors.Is(err, ErrDisconnected) {
		t.Fatalf("pop err=%v", err)
	}
	if st := q.Stats(); st.Dropped != 1 || !st.Disconnected || st.Pushed != 2 {
		t.Fatalf("stats %+v", st)
	}
}

func TestSendQueueCloseDrains(t *testing.T) {
	q, _ := NewSendQueue[int](3, DropNewest)
	_ = q.Push(1)
	_ = q.Push(2)
	q.Close()
	q.Close() // idempotent
	if err := q.Push(3); !errors.Is(err, ErrClosed) {
		t.Fatalf("push after close: %v", err)
	}
	ctx := context.Background()
	for _, want := range []int{1, 2} {
		if v, err := q.Pop(ctx); err != nil || v != want {
			t.Fatalf("pop = %d, %v", v, err)
		}
	}
	if _, err := q.Pop(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("err=%v", err)
	}
}

func TestPopHonoursContext(t *testing.T) {
	q, _ := NewSendQueue[int](1, DropNewest)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if _, err := q.Pop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v", err)
	}
}

// TestSlowSubscriberDoesNotBlockFast publishes to a fast subscriber (a
// goroutine popping as fast as it can, with room for the whole burst so it
// never overflows however the scheduler interleaves) and a slow one (small
// queue, nobody popping at all). Under every policy the publisher must finish
// and the fast subscriber must receive everything, in order, while the slow
// subscriber absorbs the overflow.
func TestSlowSubscriberDoesNotBlockFast(t *testing.T) {
	const n = 50
	for _, policy := range []OverflowPolicy{Block, DropOldest, DropNewest, Disconnect} {
		t.Run(policy.String(), func(t *testing.T) {
			fast, _ := NewSendQueue[int](n, policy, WithBlockTimeout(time.Millisecond))
			slow, _ := NewSendQueue[int](4, policy, WithBlockTimeout(time.Millisecond))

			got := make(chan []int, 1)
			go func() {
				var seen []int
				for {
					v, err := fast.Pop(context.Background())
					if err != nil {
						got <- seen
						return
					}
					seen = append(seen, v)
				}
			}()

			finished := make(chan struct{})
			go func() {
				defer close(finished)
				defer fast.Close()
				for i := range n {
					_ = slow.Push(i) // its error is the slow subscriber's problem, not ours
					if err := fast.Push(i); err != nil {
						t.Errorf("fast push %d: %v", i, err)
						return
					}
				}
			}()
			select {
			case <-finished:
			case <-time.After(5 * time.Second):
				t.Fatal("publisher stalled behind the slow subscriber")
			}
			seen := <-got
			if len(seen) != n {
				t.Fatalf("fast subscriber got %d/%d", len(seen), n)
			}
			for i, v := range seen {
				if v != i {
					t.Fatalf("fast subscriber out of order at %d: %d", i, v)
				}
			}
			st := slow.Stats()
			if want := int64(n - 4); policy != Disconnect && st.Dropped != want {
				t.Fatalf("slow subscriber dropped %d, want %d: %+v", st.Dropped, want, st)
			}
			if policy == Disconnect && (!st.Disconnected || st.Dropped != 1) {
				t.Fatalf("slow subscriber should be disconnected: %+v", st)
			}
		})
	}
}

func TestSendQueueConcurrent(t *testing.T) {
	for _, policy := range []OverflowPolicy{Block, DropOldest, DropNewest} {
		t.Run(policy.String(), func(t *testing.T) {
			q, _ := NewSendQueue[int](8, policy, WithBlockTimeout(time.Millisecond))
			const producers, each = 4, 200
			var popped atomic.Int64
			ctx, cancel := context.WithCancel(context.Background())
			var consumers sync.WaitGroup
			for range 2 {
				consumers.Add(1)
				go func() {
					defer consumers.Done()
					for {
						if _, err := q.Pop(ctx); err != nil {
							return
						}
						popped.Add(1)
					}
				}()
			}
			var wg sync.WaitGroup
			for range producers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := range each {
						_ = q.Push(i)
					}
				}()
			}
			wg.Wait()
			q.Close()
			consumers.Wait()
			cancel()
			st := q.Stats()
			// Every push is either still accounted as pushed or dropped, and
			// under DropOldest an evicted message was first counted as pushed.
			accounted := st.Pushed + st.Dropped
			if policy == DropOldest {
				accounted = st.Pushed
			}
			if accounted != producers*each {
				t.Fatalf("accounted for %d of %d pushes: %+v", accounted, producers*each, st)
			}
			if st.Popped != popped.Load() {
				t.Fatalf("popped stat %d, consumers saw %d", st.Popped, popped.Load())
			}
			wantPopped := st.Pushed
			if policy == DropOldest {
				wantPopped = st.Pushed - st.Dropped
			}
			if st.Popped != wantPopped {
				t.Fatalf("popped %d, want %d: %+v", st.Popped, wantPopped, st)
			}
		})
	}
}
