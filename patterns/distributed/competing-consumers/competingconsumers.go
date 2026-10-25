// Package competingconsumers implements a work queue in the style of Amazon
// SQS: many consumers compete for messages, each received message becomes
// invisible for a visibility timeout, and a message that is not acknowledged
// in time is redelivered to someone else. Messages that keep failing are
// moved to a dead-letter queue (DLQ) instead of being retried forever.
//
// Delivery is at-least-once: a consumer that is slow (not crashed) may finish
// after its message was redelivered, so handlers must be idempotent. Each
// delivery carries a unique receipt handle, and only the latest receipt can
// acknowledge the message, which stops a stale consumer from deleting a
// message another consumer is now working on.
package competingconsumers

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Clock abstracts time so tests can control visibility timeouts.
type Clock interface{ Now() time.Time }

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// ManualClock is a Clock that only moves when Advance is called.
type ManualClock struct {
	mu sync.Mutex
	t  time.Time
}

// NewManualClock returns a ManualClock starting at t.
func NewManualClock(t time.Time) *ManualClock { return &ManualClock{t: t} }

// Now returns the current fake time.
func (c *ManualClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }

// Advance moves the fake time forward.
func (c *ManualClock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

// Errors returned by the queue.
var (
	ErrStaleReceipt = errors.New("competingconsumers: receipt is stale or unknown")
	ErrBadConfig    = errors.New("competingconsumers: invalid config")
)

// Config controls redelivery.
type Config struct {
	VisibilityTimeout time.Duration // how long a received message stays hidden
	MaxReceives       int           // receives allowed before moving to the DLQ
	Clock             Clock         // defaults to the wall clock
}

// Delivery is one receipt of a message by a consumer.
type Delivery struct {
	MessageID string
	Body      string
	Receipt   string // pass to Ack/Nack; unique per delivery
	Attempt   int    // 1 for the first delivery
}

// DeadLetter is a message that exhausted its receives.
type DeadLetter struct {
	MessageID string
	Body      string
	Receives  int
}

type message struct {
	id        string
	body      string
	receives  int
	visibleAt time.Time
	receipt   string
}

// Queue is a concurrency-safe work queue with visibility timeouts.
type Queue struct {
	mu      sync.Mutex
	cfg     Config
	msgs    []*message // FIFO by enqueue order
	dlq     []DeadLetter
	nextID  int
	nextRcp int
}

// NewQueue validates cfg and returns an empty queue.
func NewQueue(cfg Config) (*Queue, error) {
	if cfg.VisibilityTimeout <= 0 || cfg.MaxReceives < 1 {
		return nil, fmt.Errorf("%w: visibility timeout must be > 0 and max receives >= 1", ErrBadConfig)
	}
	if cfg.Clock == nil {
		cfg.Clock = realClock{}
	}
	return &Queue{cfg: cfg}, nil
}

// Enqueue adds a message and returns its ID.
func (q *Queue) Enqueue(body string) string {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.nextID++
	m := &message{id: fmt.Sprintf("m%d", q.nextID), body: body, visibleAt: q.cfg.Clock.Now()}
	q.msgs = append(q.msgs, m)
	return m.id
}

// Receive returns the oldest visible message and hides it for the visibility
// timeout. ok is false when nothing is visible. Messages that have already
// been received MaxReceives times are moved to the DLQ instead.
func (q *Queue) Receive() (d Delivery, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.cfg.Clock.Now()
	for i := 0; i < len(q.msgs); {
		m := q.msgs[i]
		if now.Before(m.visibleAt) {
			i++
			continue
		}
		if m.receives >= q.cfg.MaxReceives {
			q.dlq = append(q.dlq, DeadLetter{MessageID: m.id, Body: m.body, Receives: m.receives})
			q.msgs = append(q.msgs[:i], q.msgs[i+1:]...)
			continue
		}
		m.receives++
		m.visibleAt = now.Add(q.cfg.VisibilityTimeout)
		q.nextRcp++
		m.receipt = fmt.Sprintf("%s#%d", m.id, q.nextRcp)
		return Delivery{MessageID: m.id, Body: m.body, Receipt: m.receipt, Attempt: m.receives}, true
	}
	return Delivery{}, false
}

// Ack deletes the message. It fails with ErrStaleReceipt if the message was
// redelivered since this receipt was issued, or was already acked.
func (q *Queue) Ack(receipt string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	i := q.findLocked(receipt)
	if i < 0 {
		return ErrStaleReceipt
	}
	q.msgs = append(q.msgs[:i], q.msgs[i+1:]...)
	return nil
}

// Nack makes the message visible again immediately (a fast retry).
func (q *Queue) Nack(receipt string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	i := q.findLocked(receipt)
	if i < 0 {
		return ErrStaleReceipt
	}
	q.msgs[i].visibleAt = q.cfg.Clock.Now()
	q.msgs[i].receipt = "" // the receipt is spent
	return nil
}

func (q *Queue) findLocked(receipt string) int {
	if receipt == "" {
		return -1
	}
	for i, m := range q.msgs {
		if m.receipt == receipt && q.cfg.Clock.Now().Before(m.visibleAt) {
			return i
		}
	}
	return -1
}

// Len returns the number of messages not yet acked (visible or in flight).
func (q *Queue) Len() int { q.mu.Lock(); defer q.mu.Unlock(); return len(q.msgs) }

// DeadLetters returns a copy of the DLQ.
func (q *Queue) DeadLetters() []DeadLetter {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]DeadLetter(nil), q.dlq...)
}

// Handler processes one delivery. Returning nil acks the message; returning
// an error nacks it so it becomes visible for another attempt.
type Handler func(worker int, d Delivery) error

// RunWorkers starts n competing consumers that poll q until ctx is cancelled.
// When the queue is empty a worker waits poll before trying again. The
// returned function blocks until every worker has exited.
func RunWorkers(ctx context.Context, q *Queue, n int, poll time.Duration, h Handler) (wait func()) {
	if poll <= 0 {
		poll = time.Millisecond
	}
	var wg sync.WaitGroup
	for w := 1; w <= n; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			t := time.NewTimer(poll)
			defer t.Stop()
			for ctx.Err() == nil {
				d, ok := q.Receive()
				if !ok {
					t.Reset(poll)
					select {
					case <-ctx.Done():
						return
					case <-t.C:
					}
					continue
				}
				if err := h(w, d); err != nil {
					_ = q.Nack(d.Receipt)
				} else {
					_ = q.Ack(d.Receipt) // may be stale if we were too slow; that is fine
				}
			}
		}(w)
	}
	return wg.Wait
}
