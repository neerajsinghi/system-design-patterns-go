// Package backpressure provides a bounded queue that offers producers three
// ways to deal with a consumer that cannot keep up:
//
//   - Put blocks the producer until there is room (or its context ends).
//     Nothing is lost; the slowness propagates upstream.
//   - TryPut never blocks and drops the item when full. The producer stays
//     fast; data is lost and counted.
//   - Offer never blocks and returns a Signal: Accepted, SlowDown (accepted,
//     but the queue is above its high-water mark) or Rejected (full). The
//     producer decides how to adapt, e.g. by reducing its rate or returning
//     "429 Too Many Requests" to its own caller.
//
// An unbounded queue has none of these properties: it hides overload until
// the process runs out of memory.
//
// For fan-out to many subscribers, SendQueue is a per-connection variant
// whose OverflowPolicy (Block with a deadline, DropOldest, DropNewest or
// Disconnect) keeps one slow subscriber from stalling the publisher and
// everyone else.
package backpressure

import (
	"context"
	"errors"
	"sync/atomic"
)

// Signal is the producer-facing result of Offer.
type Signal int

const (
	// Accepted means the item was queued and the queue is healthy.
	Accepted Signal = iota
	// SlowDown means the item was queued but the queue is above the high-water mark.
	SlowDown
	// Rejected means the queue was full and the item was not queued.
	Rejected
)

// String returns the signal name.
func (s Signal) String() string {
	switch s {
	case Accepted:
		return "accepted"
	case SlowDown:
		return "slow-down"
	case Rejected:
		return "rejected"
	default:
		return "unknown"
	}
}

// Errors returned by the queue.
var (
	ErrClosed    = errors.New("backpressure: queue closed")
	ErrBadConfig = errors.New("backpressure: capacity must be >= 1 and 0 < highWater <= capacity")
)

// Queue is a bounded, concurrency-safe FIFO queue.
type Queue[T any] struct {
	ch        chan T
	done      chan struct{}
	closed    atomic.Bool
	highWater int
	dropped   atomic.Int64
	rejected  atomic.Int64
}

// New returns a queue holding at most capacity items. Offer reports SlowDown
// once the queue holds highWater or more items.
func New[T any](capacity, highWater int) (*Queue[T], error) {
	if capacity < 1 || highWater < 1 || highWater > capacity {
		return nil, ErrBadConfig
	}
	return &Queue[T]{ch: make(chan T, capacity), done: make(chan struct{}), highWater: highWater}, nil
}

// Put blocks until v is queued, ctx is done, or the queue is closed.
func (q *Queue[T]) Put(ctx context.Context, v T) error {
	if q.closed.Load() {
		return ErrClosed
	}
	select {
	case q.ch <- v:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-q.done:
		return ErrClosed
	}
}

// TryPut queues v if there is room and otherwise drops it, returning false.
func (q *Queue[T]) TryPut(v T) bool {
	if q.closed.Load() {
		return false
	}
	select {
	case q.ch <- v:
		return true
	default:
		q.dropped.Add(1)
		return false
	}
}

// Offer queues v if there is room and tells the producer how loaded the
// queue is. It never blocks.
func (q *Queue[T]) Offer(v T) Signal {
	if q.closed.Load() {
		q.rejected.Add(1)
		return Rejected
	}
	select {
	case q.ch <- v:
		if len(q.ch) >= q.highWater {
			return SlowDown
		}
		return Accepted
	default:
		q.rejected.Add(1)
		return Rejected
	}
}

// Get blocks until an item is available, ctx is done, or the queue is closed
// and drained.
func (q *Queue[T]) Get(ctx context.Context) (T, error) {
	var zero T
	select {
	case v := <-q.ch:
		return v, nil
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-q.done:
		select { // drain whatever is left before reporting closed
		case v := <-q.ch:
			return v, nil
		default:
			return zero, ErrClosed
		}
	}
}

// Close stops accepting new items. Consumers drain the remaining items and
// then receive ErrClosed. Like closing a channel, Close should be called by
// the producer side once producers are finished; items put concurrently with
// Close may be rejected.
func (q *Queue[T]) Close() {
	if q.closed.CompareAndSwap(false, true) {
		close(q.done)
	}
}

// Len returns the number of queued items.
func (q *Queue[T]) Len() int { return len(q.ch) }

// Cap returns the queue capacity.
func (q *Queue[T]) Cap() int { return cap(q.ch) }

// Dropped returns how many items TryPut discarded.
func (q *Queue[T]) Dropped() int64 { return q.dropped.Load() }

// Rejected returns how many items Offer refused.
func (q *Queue[T]) Rejected() int64 { return q.rejected.Load() }
