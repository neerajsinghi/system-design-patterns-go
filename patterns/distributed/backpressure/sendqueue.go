package backpressure

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// OverflowPolicy says what Push does when a SendQueue is full.
type OverflowPolicy int

const (
	// Block waits for room up to the block timeout (see WithBlockTimeout).
	// If the queue is still full, the message is dropped and Push returns
	// ErrSendTimeout. The publisher is slowed, but never by more than the
	// timeout per message.
	Block OverflowPolicy = iota
	// DropOldest evicts the oldest queued message to make room. The
	// subscriber skips ahead and always sees the most recent messages.
	DropOldest
	// DropNewest discards the message being pushed. The subscriber keeps the
	// backlog it already had and misses whatever came later.
	DropNewest
	// Disconnect gives up on the subscriber: the queue is shut, the
	// OnDisconnect callback runs, and Pop returns ErrDisconnected so the
	// writer can close the connection. The client reconnects and resyncs.
	Disconnect
)

// String returns the policy name.
func (p OverflowPolicy) String() string {
	switch p {
	case Block:
		return "block"
	case DropOldest:
		return "drop-oldest"
	case DropNewest:
		return "drop-newest"
	case Disconnect:
		return "disconnect"
	default:
		return "unknown"
	}
}

// Errors returned by SendQueue.
var (
	ErrSendQueueConfig = errors.New("backpressure: send queue needs capacity >= 1, a known policy and a positive block timeout")
	ErrSendTimeout     = errors.New("backpressure: send queue full until the block timeout; message dropped")
	ErrQueueFull       = errors.New("backpressure: send queue full; message dropped")
	ErrDisconnected    = errors.New("backpressure: subscriber disconnected for falling behind")
)

// DefaultBlockTimeout is the Block policy's wait when WithBlockTimeout is not given.
const DefaultBlockTimeout = time.Second

// SendQueueOption configures a SendQueue.
type SendQueueOption func(*sendQueueConfig)

type sendQueueConfig struct {
	blockTimeout time.Duration
	onDisconnect func()
}

// WithBlockTimeout sets how long Push waits for room under the Block policy.
func WithBlockTimeout(d time.Duration) SendQueueOption {
	return func(c *sendQueueConfig) { c.blockTimeout = d }
}

// WithOnDisconnect registers a callback that runs once, in the goroutine of
// the Push that overflowed, when the Disconnect policy drops the subscriber.
func WithOnDisconnect(fn func()) SendQueueOption {
	return func(c *sendQueueConfig) { c.onDisconnect = fn }
}

// SendQueueStats is a snapshot of a SendQueue's counters.
type SendQueueStats struct {
	Pushed       int64 // messages accepted into the queue
	Popped       int64 // messages handed to the consumer
	Dropped      int64 // messages lost to the overflow policy (evicted, refused, timed out, or the one that triggered a disconnect); pushes after a disconnect or Close are refused but not counted
	Disconnected bool  // true once the Disconnect policy fired
}

// SendQueue is a bounded per-connection outbox for fan-out: a publisher
// pushes the same message into one SendQueue per subscriber, and each
// subscriber's writer goroutine pops from its own queue at its own pace.
//
// The point is isolation. A shared channel, or a publisher that writes to
// every socket in turn, lets one slow subscriber stall the publisher and
// therefore every other subscriber. With a SendQueue per connection the
// publisher only ever touches memory, and the OverflowPolicy decides what a
// full queue costs: a bounded wait, a lost message, or the connection.
//
// SendQueue is safe for concurrent use by any number of pushers and poppers.
type SendQueue[T any] struct {
	ch     chan T
	done   chan struct{} // closed by Close or by a disconnect
	policy OverflowPolicy
	cfg    sendQueueConfig

	evictMu      sync.Mutex // serialises DropOldest's evict-then-send
	stopOnce     sync.Once
	closed       atomic.Bool
	disconnected atomic.Bool
	pushed       atomic.Int64
	popped       atomic.Int64
	dropped      atomic.Int64
}

// NewSendQueue returns a queue that holds at most capacity messages and
// applies policy when full.
func NewSendQueue[T any](capacity int, policy OverflowPolicy, opts ...SendQueueOption) (*SendQueue[T], error) {
	cfg := sendQueueConfig{blockTimeout: DefaultBlockTimeout}
	for _, o := range opts {
		o(&cfg)
	}
	if capacity < 1 || policy < Block || policy > Disconnect || cfg.blockTimeout <= 0 {
		return nil, ErrSendQueueConfig
	}
	return &SendQueue[T]{ch: make(chan T, capacity), done: make(chan struct{}), policy: policy, cfg: cfg}, nil
}

// Push enqueues v, applying the overflow policy if the queue is full. It
// returns nil when v was queued (under DropOldest, possibly by evicting an
// older message), ErrSendTimeout or ErrQueueFull when v was dropped,
// ErrDisconnected once the subscriber has been disconnected, and ErrClosed
// after Close.
func (q *SendQueue[T]) Push(v T) error {
	if err := q.stopped(); err != nil {
		return err
	}
	select {
	case q.ch <- v:
		q.pushed.Add(1)
		return nil
	default:
	}
	switch q.policy {
	case DropNewest:
		q.dropped.Add(1)
		return ErrQueueFull
	case DropOldest:
		q.evictMu.Lock()
		defer q.evictMu.Unlock()
		for {
			select {
			case q.ch <- v:
				q.pushed.Add(1)
				return nil
			default:
			}
			select {
			case <-q.ch:
				q.dropped.Add(1)
			default: // a consumer made room in the meantime; retry the send
			}
		}
	case Disconnect:
		q.dropped.Add(1)
		if q.disconnected.CompareAndSwap(false, true) {
			q.stop()
			if q.cfg.onDisconnect != nil {
				q.cfg.onDisconnect()
			}
		}
		return ErrDisconnected
	default: // Block
		t := time.NewTimer(q.cfg.blockTimeout)
		defer t.Stop()
		select {
		case q.ch <- v:
			q.pushed.Add(1)
			return nil
		case <-t.C:
			q.dropped.Add(1)
			return ErrSendTimeout
		case <-q.done:
			if err := q.stopped(); err != nil {
				return err
			}
			return ErrClosed
		}
	}
}

// Pop blocks until a message is available, ctx is done, or the queue stops.
// After Close it drains what is left, then returns ErrClosed. After a
// disconnect it returns ErrDisconnected at once: the connection is being torn
// down, and the client will resync when it reconnects.
func (q *SendQueue[T]) Pop(ctx context.Context) (T, error) {
	var zero T
	if q.disconnected.Load() {
		return zero, ErrDisconnected
	}
	select {
	case v := <-q.ch:
		q.popped.Add(1)
		return v, nil
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-q.done:
		if q.disconnected.Load() {
			return zero, ErrDisconnected
		}
		select {
		case v := <-q.ch:
			q.popped.Add(1)
			return v, nil
		default:
			return zero, ErrClosed
		}
	}
}

// TryPop returns the next message without blocking; ok is false when the
// queue is empty or the subscriber has been disconnected.
func (q *SendQueue[T]) TryPop() (v T, ok bool) {
	if q.disconnected.Load() {
		return v, false
	}
	select {
	case v = <-q.ch:
		q.popped.Add(1)
		return v, true
	default:
		return v, false
	}
}

// Close stops accepting messages (the subscriber left normally). Pop drains
// the remaining messages and then returns ErrClosed. Close is idempotent.
func (q *SendQueue[T]) Close() {
	q.closed.Store(true)
	q.stop()
}

// Stats returns a snapshot of the counters.
func (q *SendQueue[T]) Stats() SendQueueStats {
	return SendQueueStats{
		Pushed:       q.pushed.Load(),
		Popped:       q.popped.Load(),
		Dropped:      q.dropped.Load(),
		Disconnected: q.disconnected.Load(),
	}
}

// Len returns the number of queued messages.
func (q *SendQueue[T]) Len() int { return len(q.ch) }

// Policy returns the overflow policy.
func (q *SendQueue[T]) Policy() OverflowPolicy { return q.policy }

func (q *SendQueue[T]) stop() { q.stopOnce.Do(func() { close(q.done) }) }

func (q *SendQueue[T]) stopped() error {
	if q.disconnected.Load() {
		return ErrDisconnected
	}
	if q.closed.Load() {
		return ErrClosed
	}
	return nil
}
