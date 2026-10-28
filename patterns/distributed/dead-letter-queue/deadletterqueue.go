// Package deadletterqueue implements a message queue with bounded retries and
// a dead-letter queue (DLQ) for messages that keep failing.
//
// Without a DLQ, a "poison" message (a malformed payload, a record that hits a
// bug in the consumer, a downstream that rejects it every time) is retried
// forever. In an ordered queue it blocks everything behind it; in an
// unordered one it quietly burns consumer capacity on attempts that can never
// succeed. Both failure modes are invisible until somebody notices the lag.
//
// This package gives every message a receive count. A failed attempt makes the
// message invisible for an exponentially growing backoff (a visibility-timeout
// style delayed redelivery), so other messages flow in the meantime. After
// MaxAttempts failures, or immediately for errors wrapped with Permanent, the
// message is moved to the DLQ together with why and when it failed. The DLQ
// depth is a metric to alert on, and Redrive puts dead letters back on the main
// queue after a fix, with their original IDs so idempotent consumers can still
// deduplicate them.
//
// Optional per-key ordering (like SQS FIFO message groups or Kafka partitions)
// delivers one message per key at a time. Parking a message then breaks that
// key's order unless WithParkKey is used, which parks the rest of the key too.
package deadletterqueue

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
)

// RetryForever, passed to WithMaxAttempts, disables dead-lettering: failed
// messages are retried indefinitely. It exists to show the problem.
const RetryForever = 0

// ErrUnknownReceipt is returned by Ack and Nack for a receipt that was never
// issued, was already settled, or whose visibility timeout expired.
var ErrUnknownReceipt = errors.New("deadletterqueue: unknown or expired receipt")

// Message is the unit of work. ID is preserved across retries and redrives.
type Message struct {
	ID      string
	Key     string // ordering key; only used with WithPerKeyOrder
	Payload string
	// SentAt is when the message was first sent; a redrive keeps it, so
	// end-to-end latency includes the time spent in the DLQ.
	SentAt time.Time
	// Attempts is the receive count since the message was sent or last redriven.
	Attempts int
	// Redrives counts how many times the message came back from the DLQ.
	Redrives int
}

// Delivery is a received message plus the receipt needed to settle it.
type Delivery struct {
	Message
	Receipt uint64
}

// DeadLetter is a parked message and the metadata needed to triage it.
type DeadLetter struct {
	Message
	Reason       string // last error, or why it was parked
	Permanent    bool   // failed with a Permanent error, no retries spent
	FirstFailure time.Time
	LastFailure  time.Time
	DeadAt       time.Time
}

// Stats is a snapshot of the queue's counters and gauges.
type Stats struct {
	Sent, Delivered, Acked, Retried, Expired, DeadLettered, Redriven uint64
	Depth                                                            int // messages in the main queue, including in-flight ones
	InFlight                                                         int
	DLQDepth                                                         int // the alerting metric: anything above zero needs a human
}

type permanentError struct{ err error }

func (p permanentError) Error() string { return "permanent: " + p.err.Error() }
func (p permanentError) Unwrap() error { return p.err }

// Permanent marks err as not worth retrying: a message nacked with it goes
// straight to the DLQ. Permanent(nil) returns nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err}
}

// IsPermanent reports whether err, or anything it wraps, came from Permanent.
func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}

type entry struct {
	msg       Message
	visibleAt time.Time
	inFlight  bool
	receipt   uint64
	deadline  time.Time // visibility timeout while in flight
	firstFail time.Time
	lastFail  time.Time
}

// Queue is a concurrency-safe in-memory queue with a dead-letter queue.
type Queue struct {
	now         func() time.Time
	maxAttempts int
	visibility  time.Duration
	baseBackoff time.Duration
	maxBackoff  time.Duration
	perKey      bool
	parkKey     bool

	mu          sync.Mutex
	main        []*entry // in send order
	byReceipt   map[uint64]*entry
	nextReceipt uint64
	dlq         []DeadLetter
	parked      map[string]string // key -> ID of the message that parked it
	stats       Stats
}

// Option configures a Queue.
type Option func(*options)

type options struct {
	now                  func() time.Time
	maxAttempts          int
	visibility           time.Duration
	baseBackoff, backoff time.Duration
	perKey, parkKey      bool
}

// WithClock injects a clock so backoff and visibility timeouts are testable.
func WithClock(now func() time.Time) Option {
	return func(o *options) {
		if now != nil {
			o.now = now
		}
	}
}

// WithMaxAttempts sets how many receives a message gets before it is
// dead-lettered (SQS calls this maxReceiveCount). Default 5. RetryForever (0)
// disables the DLQ; then Permanent errors are retried too, as there is nowhere
// to park them.
func WithMaxAttempts(n int) Option { return func(o *options) { o.maxAttempts = n } }

// WithVisibilityTimeout sets how long a received message stays hidden before it
// is assumed lost (consumer crashed) and counted as a failed attempt.
// Default 30s.
func WithVisibilityTimeout(d time.Duration) Option { return func(o *options) { o.visibility = d } }

// WithBackoff sets the redelivery delay after a failure: base after the first
// failed attempt, doubling each time, capped at max. Default 1s and 1m.
func WithBackoff(base, max time.Duration) Option {
	return func(o *options) { o.baseBackoff, o.backoff = base, max }
}

// WithPerKeyOrder delivers at most one message per Key at a time, in send
// order: a key's next message waits until the previous one is acked or
// dead-lettered.
func WithPerKeyOrder() Option { return func(o *options) { o.perKey = true } }

// WithParkKey, which requires WithPerKeyOrder, dead-letters every later
// message of a key once one of its messages is dead-lettered, so the key's
// order survives. Redriving any message of a parked key redrives the whole key.
func WithParkKey() Option { return func(o *options) { o.parkKey = true } }

// New returns an empty queue.
func New(opts ...Option) (*Queue, error) {
	o := options{
		now: time.Now, maxAttempts: 5, visibility: 30 * time.Second,
		baseBackoff: time.Second, backoff: time.Minute,
	}
	for _, opt := range opts {
		opt(&o)
	}
	switch {
	case o.maxAttempts < 0:
		return nil, errors.New("deadletterqueue: max attempts must not be negative")
	case o.visibility <= 0:
		return nil, errors.New("deadletterqueue: visibility timeout must be positive")
	case o.baseBackoff <= 0:
		return nil, errors.New("deadletterqueue: base backoff must be positive")
	case o.backoff < o.baseBackoff:
		return nil, errors.New("deadletterqueue: max backoff must be at least the base backoff")
	case o.parkKey && !o.perKey:
		return nil, errors.New("deadletterqueue: WithParkKey requires WithPerKeyOrder")
	}
	return &Queue{
		now: o.now, maxAttempts: o.maxAttempts, visibility: o.visibility,
		baseBackoff: o.baseBackoff, maxBackoff: o.backoff,
		perKey: o.perKey, parkKey: o.parkKey,
		byReceipt: make(map[uint64]*entry), parked: make(map[string]string),
	}, nil
}

// Send enqueues a message. The ID must be non-empty; it is what consumers
// deduplicate on, so producers should derive it from the business event.
func (q *Queue) Send(id, key, payload string) error {
	if id == "" {
		return errors.New("deadletterqueue: message ID must not be empty")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.now()
	q.stats.Sent++
	m := Message{ID: id, Key: key, Payload: payload, SentAt: now}
	if blocker, ok := q.parked[key]; ok && q.parkKey {
		q.stats.DeadLettered++
		q.dlq = append(q.dlq, DeadLetter{
			Message: m, Reason: fmt.Sprintf("key parked behind %s", blocker), DeadAt: now,
		})
		return nil
	}
	q.main = append(q.main, &entry{msg: m, visibleAt: now})
	return nil
}

// Receive returns the oldest message that is visible now (and, with per-key
// ordering, whose key has nothing earlier pending). It stays hidden for the
// visibility timeout; settle it with Ack or Nack before then.
func (q *Queue) Receive() (Delivery, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.now()
	q.expire(now)
	var blocked map[string]bool
	if q.perKey {
		blocked = make(map[string]bool)
	}
	for _, e := range q.main {
		if blocked[e.msg.Key] {
			continue
		}
		if e.inFlight || now.Before(e.visibleAt) {
			if q.perKey {
				blocked[e.msg.Key] = true
			}
			continue
		}
		q.nextReceipt++
		e.inFlight, e.receipt, e.deadline = true, q.nextReceipt, now.Add(q.visibility)
		e.msg.Attempts++
		q.byReceipt[e.receipt] = e
		q.stats.Delivered++
		return Delivery{Message: e.msg, Receipt: e.receipt}, true
	}
	return Delivery{}, false
}

// Ack removes a successfully processed message.
func (q *Queue) Ack(receipt uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	e, ok := q.settle(receipt)
	if !ok {
		return ErrUnknownReceipt
	}
	q.remove(e)
	q.stats.Acked++
	return nil
}

// Nack reports a failed attempt. A Permanent cause, or the last allowed
// attempt, dead-letters the message; otherwise it is redelivered after the
// backoff for its attempt count.
func (q *Queue) Nack(receipt uint64, cause error) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	e, ok := q.settle(receipt)
	if !ok {
		return ErrUnknownReceipt
	}
	reason := "nacked"
	if cause != nil {
		reason = cause.Error()
	}
	q.fail(e, q.now(), reason, IsPermanent(cause))
	return nil
}

// ProcessOne receives one message, runs h on it, and acks it on success or
// nacks it with h's error. It reports whether a message was received and
// returns h's error (or ErrUnknownReceipt if h outlived the visibility timeout).
func (q *Queue) ProcessOne(h func(Delivery) error) (bool, error) {
	d, ok := q.Receive()
	if !ok {
		return false, nil
	}
	if err := h(d); err != nil {
		if nerr := q.Nack(d.Receipt, err); nerr != nil {
			return true, nerr
		}
		return true, err
	}
	return true, q.Ack(d.Receipt)
}

// NextVisible returns the earliest time something in the main queue becomes
// receivable (or its lease expires). Simulations use it to advance a fake clock.
func (q *Queue) NextVisible() (time.Time, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var next time.Time
	seen := make(map[string]bool)
	for _, e := range q.main {
		if q.perKey {
			// Only a key's oldest message can become receivable.
			if seen[e.msg.Key] {
				continue
			}
			seen[e.msg.Key] = true
		}
		t := e.visibleAt
		if e.inFlight {
			t = e.deadline
		}
		if next.IsZero() || t.Before(next) {
			next = t
		}
	}
	return next, !next.IsZero()
}

// Redrive moves dead letters matching filter (nil means all) back to the end
// of the main queue, keeping their IDs, payloads and SentAt, resetting
// Attempts and incrementing Redrives. With WithParkKey, matching one message
// of a key redrives all of that key's dead letters, in their original order.
// It returns how many messages were redriven.
func (q *Queue) Redrive(filter func(DeadLetter) bool) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	pick := make([]bool, len(q.dlq))
	keys := make(map[string]bool)
	for i, dl := range q.dlq {
		if filter == nil || filter(dl) {
			pick[i] = true
			keys[dl.Key] = true
		}
	}
	if q.parkKey {
		for i, dl := range q.dlq {
			if keys[dl.Key] {
				pick[i] = true
			}
		}
	}
	now := q.now()
	kept := q.dlq[:0:0]
	n := 0
	for i, dl := range q.dlq {
		if !pick[i] {
			kept = append(kept, dl)
			continue
		}
		m := dl.Message
		m.Attempts = 0
		m.Redrives++
		q.main = append(q.main, &entry{msg: m, visibleAt: now})
		delete(q.parked, m.Key)
		n++
	}
	q.dlq = kept
	q.stats.Redriven += uint64(n)
	return n
}

// DeadLetters returns a copy of the DLQ, oldest first.
func (q *Queue) DeadLetters() []DeadLetter {
	q.mu.Lock()
	defer q.mu.Unlock()
	return slices.Clone(q.dlq)
}

// DLQDepth is the number of parked messages: the metric to alert on.
func (q *Queue) DLQDepth() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.dlq)
}

// Stats returns a snapshot of the counters and gauges.
func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	s := q.stats
	s.Depth, s.InFlight, s.DLQDepth = len(q.main), len(q.byReceipt), len(q.dlq)
	return s
}

// backoff returns the delay after the given number of failed attempts.
func (q *Queue) backoff(attempts int) time.Duration {
	d := q.baseBackoff
	for i := 1; i < attempts && d < q.maxBackoff; i++ {
		d *= 2
	}
	return min(d, q.maxBackoff)
}

// settle looks up and clears an in-flight receipt. The caller holds q.mu.
func (q *Queue) settle(receipt uint64) (*entry, bool) {
	e, ok := q.byReceipt[receipt]
	if !ok {
		return nil, false
	}
	delete(q.byReceipt, receipt)
	e.inFlight = false
	return e, true
}

// expire turns leases whose visibility timeout passed into failed attempts.
// The caller holds q.mu.
func (q *Queue) expire(now time.Time) {
	var expired []*entry
	for r, e := range q.byReceipt {
		if !now.Before(e.deadline) {
			delete(q.byReceipt, r)
			e.inFlight = false
			expired = append(expired, e)
		}
	}
	// Map iteration order is random; settle in send order to stay deterministic.
	slices.SortFunc(expired, func(a, b *entry) int { return q.index(a) - q.index(b) })
	for _, e := range expired {
		q.stats.Expired++
		q.fail(e, e.deadline, "visibility timeout expired", false)
	}
}

// fail records a failed attempt and either schedules a retry or dead-letters
// the message. The caller holds q.mu.
func (q *Queue) fail(e *entry, at time.Time, reason string, permanent bool) {
	if e.firstFail.IsZero() {
		e.firstFail = at
	}
	e.lastFail = at
	dlqEnabled := q.maxAttempts != RetryForever
	if dlqEnabled && (permanent || e.msg.Attempts >= q.maxAttempts) {
		q.deadLetter(e, at, reason, permanent)
		return
	}
	e.visibleAt = at.Add(q.backoff(e.msg.Attempts))
	q.stats.Retried++
}

// deadLetter moves e (and, with WithParkKey, the rest of its key) to the DLQ.
// The caller holds q.mu.
func (q *Queue) deadLetter(e *entry, at time.Time, reason string, permanent bool) {
	q.remove(e)
	q.dlq = append(q.dlq, DeadLetter{
		Message: e.msg, Reason: reason, Permanent: permanent,
		FirstFailure: e.firstFail, LastFailure: e.lastFail, DeadAt: at,
	})
	q.stats.DeadLettered++
	if !q.parkKey {
		return
	}
	q.parked[e.msg.Key] = e.msg.ID
	kept := q.main[:0]
	for _, o := range q.main {
		// With per-key order nothing later in this key can be in flight.
		if o.msg.Key != e.msg.Key || o.inFlight {
			kept = append(kept, o)
			continue
		}
		q.dlq = append(q.dlq, DeadLetter{
			Message: o.msg, Reason: fmt.Sprintf("key parked behind %s", e.msg.ID), DeadAt: at,
		})
		q.stats.DeadLettered++
	}
	clear(q.main[len(kept):])
	q.main = kept
}

func (q *Queue) index(e *entry) int { return slices.Index(q.main, e) }

func (q *Queue) remove(e *entry) {
	if i := q.index(e); i >= 0 {
		q.main = slices.Delete(q.main, i, i+1)
	}
}
