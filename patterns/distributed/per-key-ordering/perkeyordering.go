// Package perkeyordering keeps the events of one entity (one order, one
// account, one chat conversation) in sequence while events of different
// entities are processed in parallel.
//
// Global ordering is easy and slow: one queue, one consumer. As soon as
// events are spread over partitions or a pool of workers, two events for the
// same order can be handled at the same time and finish in the wrong order,
// so a consumer sees "shipped" before "paid". Most systems only need order
// per key, and that is much cheaper. This package shows two tools that work
// together:
//
//   - Partition by key. A Dispatcher hashes the key to one of N partitions,
//     and each partition has exactly one sequential worker. Events for one
//     key always meet the same worker in submission order, while different
//     keys run in parallel on different partitions. This is what Kafka
//     partition keys, SQS FIFO message group IDs and Kinesis shard keys do.
//
//   - Resequence. When order is already lost before the consumer (several
//     producers, retries, multi-region replication, a network that reorders),
//     the producer stamps each event with a per-key sequence number and a
//     Resequencer at the consumer buffers early arrivals and releases only
//     contiguous runs. If a sequence number never shows up, the key waits at
//     most a gap timeout, then the Resequencer skips the hole, emits a Gap
//     notification and carries on. State is per key, so a gap on one key
//     never delays another.
//
// A typical pipeline does both: route by key so one worker owns each key, and
// give that worker a Resequencer so it can repair upstream reordering.
package perkeyordering

import (
	"errors"
	"hash/fnv"
	"sort"
	"sync"
	"time"
)

// Event is one message about an entity.
type Event struct {
	// Key is the ordering key, for example an order ID. Order is only
	// guaranteed between events with the same key.
	Key string
	// Seq is the per-key sequence number assigned by the producer, starting
	// at 1 and increasing by exactly 1. The Dispatcher does not need it; the
	// Resequencer does.
	Seq uint64
	// Type describes the event, for example "created", "paid", "shipped".
	Type string
}

var (
	// ErrClosed is returned by Submit after Close.
	ErrClosed = errors.New("perkeyordering: dispatcher closed")
	// ErrBufferFull is returned by Offer when a key already holds the maximum
	// number of out-of-order events. The event was not accepted; redeliver it
	// later (by then the gap may have been filled or timed out).
	ErrBufferFull = errors.New("perkeyordering: reorder buffer full for key")
)

var errEmptyKey = errors.New("perkeyordering: event key must not be empty")

// PartitionFor maps a key to a partition in [0, partitions) with FNV-1a. The
// same key always maps to the same partition for a given partition count.
// Changing the count remaps most keys, which is why repartitioning breaks
// ordering while it happens. It returns 0 if partitions < 1.
func PartitionFor(key string, partitions int) int {
	if partitions <= 1 {
		return 0
	}
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(partitions))
}

// ---------------------------------------------------------------------------
// Dispatcher: partition by key, one sequential worker per partition.

// Handler processes one event. It is called from the partition's single
// worker goroutine, so calls for the same partition never overlap.
type Handler func(partition int, e Event)

// DispatcherOption configures a Dispatcher.
type DispatcherOption func(*dispatcherOptions)

type dispatcherOptions struct {
	queueSize int
}

// WithQueueSize sets how many events each partition can hold before Submit
// blocks (backpressure). The default is 64; it must be at least 1.
func WithQueueSize(n int) DispatcherOption {
	return func(o *dispatcherOptions) { o.queueSize = n }
}

// Dispatcher routes events to partitions by key. Each partition has one
// worker that handles its events strictly in the order they were submitted,
// so events with the same key are handled in order and never concurrently,
// while different partitions run in parallel.
//
// Order is the order of Submit calls for a key. If two goroutines submit
// events for the same key at the same time, which goes first is up to the
// scheduler: give each key a single producer, or sequence the events and add
// a Resequencer.
type Dispatcher struct {
	handle Handler
	queues []chan Event
	wg     sync.WaitGroup

	mu     sync.RWMutex // held for reading while sending, for writing while closing
	closed bool
}

// NewDispatcher starts one worker goroutine per partition.
func NewDispatcher(partitions int, handle Handler, opts ...DispatcherOption) (*Dispatcher, error) {
	if partitions < 1 {
		return nil, errors.New("perkeyordering: partitions must be at least 1")
	}
	if handle == nil {
		return nil, errors.New("perkeyordering: handler must not be nil")
	}
	o := dispatcherOptions{queueSize: 64}
	for _, opt := range opts {
		opt(&o)
	}
	if o.queueSize < 1 {
		return nil, errors.New("perkeyordering: queue size must be at least 1")
	}
	d := &Dispatcher{handle: handle, queues: make([]chan Event, partitions)}
	for p := range d.queues {
		q := make(chan Event, o.queueSize)
		d.queues[p] = q
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			for e := range q {
				d.handle(p, e)
			}
		}()
	}
	return d, nil
}

// Partitions returns the number of partitions.
func (d *Dispatcher) Partitions() int { return len(d.queues) }

// Submit queues e on the partition for its key. It blocks while that
// partition's queue is full.
func (d *Dispatcher) Submit(e Event) error {
	if e.Key == "" {
		return errEmptyKey
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return ErrClosed
	}
	d.queues[PartitionFor(e.Key, len(d.queues))] <- e
	return nil
}

// Close stops accepting events, lets every worker finish its queue and waits
// for them. It is safe to call more than once.
func (d *Dispatcher) Close() {
	d.mu.Lock()
	if !d.closed {
		d.closed = true
		for _, q := range d.queues {
			close(q)
		}
	}
	d.mu.Unlock()
	d.wg.Wait()
}

// ---------------------------------------------------------------------------
// Resequencer: repair per-key order lost upstream.

// Gap reports sequence numbers that were given up on: First..Last
// (inclusive) for Key never arrived within the gap timeout and were skipped.
type Gap struct {
	Key         string
	First, Last uint64
	// Waited is how long the key had been blocked on this gap.
	Waited time.Duration
}

// Item is one thing released by the Resequencer, in the order it must be
// applied: either an Event, or (when Gap is non-nil) a notice that some
// sequence numbers were skipped. Gap notices come before the events that
// follow the hole.
type Item struct {
	Event Event
	Gap   *Gap
}

// Stats counts what the Resequencer has done.
type Stats struct {
	Released   uint64 // events released in order
	Duplicates uint64 // events ignored: already buffered, already released, or skipped by a gap
	Rejected   uint64 // events refused with ErrBufferFull
	Gaps       uint64 // gap notifications emitted
	Skipped    uint64 // sequence numbers given up on across all gaps
	Buffered   int    // events currently held waiting for a gap to fill
}

// Option configures a Resequencer.
type Option func(*options)

type options struct {
	now       func() time.Time
	maxBuffer int
}

// WithClock injects a clock, which makes gap timeouts testable.
func WithClock(now func() time.Time) Option {
	return func(o *options) {
		if now != nil {
			o.now = now
		}
	}
}

// WithMaxBuffer bounds how many out-of-order events one key may hold. The
// default is 1024; it must be at least 1.
func WithMaxBuffer(n int) Option {
	return func(o *options) { o.maxBuffer = n }
}

type pending struct {
	e       Event
	arrived time.Time
}

type keyState struct {
	next uint64             // next sequence number to release
	buf  map[uint64]pending // early arrivals, all > next
}

// Resequencer restores per-key order using producer-assigned sequence
// numbers. Each key starts at sequence 1.
//
// Gap policy: when the next expected event of a key is missing, later events
// of that key are buffered for up to the gap timeout (measured from the
// earliest buffered arrival). After that the missing numbers are skipped, a
// Gap item is emitted in their place and the buffered run is released. A
// missing event that turns up after its gap was skipped is ignored and
// counted as a duplicate. Skipping keeps the key moving; the Gap item lets
// the consumer alert, fetch the missing state from the source of truth, or
// mark the entity for repair. (The alternative policy, halting the key until
// an operator intervenes, is safer for ledgers but stalls the entity.)
//
// Expired gaps are resolved whenever that key is offered an event, and for
// all keys by Tick, which the consumer should call periodically.
//
// Resequencer is safe for concurrent use. Items are returned to the caller
// of Offer or Tick, so to keep a key's output in order, apply each returned
// slice before the next call for that key, for example by giving each key a
// single owner (a Dispatcher partition).
type Resequencer struct {
	gapTimeout time.Duration
	maxBuffer  int
	now        func() time.Time

	mu      sync.Mutex
	keys    map[string]*keyState
	waiting map[string]struct{} // keys with a non-empty buffer
	stats   Stats
}

// NewResequencer returns a Resequencer that waits at most gapTimeout for a
// missing sequence number before skipping it.
func NewResequencer(gapTimeout time.Duration, opts ...Option) (*Resequencer, error) {
	if gapTimeout <= 0 {
		return nil, errors.New("perkeyordering: gap timeout must be positive")
	}
	o := options{now: time.Now, maxBuffer: 1024}
	for _, opt := range opts {
		opt(&o)
	}
	if o.maxBuffer < 1 {
		return nil, errors.New("perkeyordering: max buffer must be at least 1")
	}
	return &Resequencer{
		gapTimeout: gapTimeout,
		maxBuffer:  o.maxBuffer,
		now:        o.now,
		keys:       make(map[string]*keyState),
		waiting:    make(map[string]struct{}),
	}, nil
}

// Offer hands one delivered event to the Resequencer and returns whatever can
// now be released for that key, in order: the event itself and any buffered
// run behind it if it was the next expected one, nothing if it arrived early
// (it is buffered), or Gap notices and runs if the key's gap had expired.
// Duplicates are ignored. ErrBufferFull means the event was not accepted.
func (r *Resequencer) Offer(e Event) ([]Item, error) {
	if e.Key == "" {
		return nil, errEmptyKey
	}
	if e.Seq == 0 {
		return nil, errors.New("perkeyordering: sequence numbers start at 1")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()

	ks := r.keys[e.Key]
	if ks == nil {
		ks = &keyState{next: 1, buf: make(map[uint64]pending)}
		r.keys[e.Key] = ks
	}
	// Resolve an overdue gap first: it may make room, or make e a late arrival.
	out := r.expire(e.Key, ks, now, nil)

	if _, dup := ks.buf[e.Seq]; dup || e.Seq < ks.next {
		r.stats.Duplicates++
		return out, nil
	}
	if e.Seq == ks.next {
		out = append(out, Item{Event: e})
		ks.next++
		r.stats.Released++
		out = r.drain(ks, out)
		// The run may stop at a later gap whose wait already expired.
		return r.expire(e.Key, ks, now, out), nil
	}
	if len(ks.buf) >= r.maxBuffer {
		r.stats.Rejected++
		return out, ErrBufferFull
	}
	ks.buf[e.Seq] = pending{e: e, arrived: now}
	r.stats.Buffered++
	r.waiting[e.Key] = struct{}{}
	return out, nil
}

// Tick resolves every expired gap and returns the released items, grouped by
// key (keys in sorted order) and in order within each key.
func (r *Resequencer) Tick() []Item {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	keys := make([]string, 0, len(r.waiting))
	for k := range r.waiting {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out []Item
	for _, k := range keys {
		out = r.expire(k, r.keys[k], now, out)
	}
	return out
}

// NextSeq returns the next sequence number the Resequencer will release for
// key (1 for a key it has never seen).
func (r *Resequencer) NextSeq(key string) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ks := r.keys[key]; ks != nil {
		return ks.next
	}
	return 1
}

// Stats returns a snapshot of the counters.
func (r *Resequencer) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stats
}

// drain releases the contiguous run starting at ks.next.
func (r *Resequencer) drain(ks *keyState, out []Item) []Item {
	for {
		p, ok := ks.buf[ks.next]
		if !ok {
			return out
		}
		delete(ks.buf, ks.next)
		out = append(out, Item{Event: p.e})
		ks.next++
		r.stats.Released++
		r.stats.Buffered--
	}
}

// expire skips every gap of key whose wait has reached the timeout.
func (r *Resequencer) expire(key string, ks *keyState, now time.Time, out []Item) []Item {
	for {
		if len(ks.buf) == 0 {
			delete(r.waiting, key)
			return out
		}
		// The key has been blocked since its earliest buffered arrival.
		var since time.Time
		lowest := uint64(0)
		for seq, p := range ks.buf {
			if since.IsZero() || p.arrived.Before(since) {
				since = p.arrived
			}
			if lowest == 0 || seq < lowest {
				lowest = seq
			}
		}
		waited := now.Sub(since)
		if waited < r.gapTimeout {
			return out
		}
		out = append(out, Item{Gap: &Gap{Key: key, First: ks.next, Last: lowest - 1, Waited: waited}})
		r.stats.Gaps++
		r.stats.Skipped += lowest - ks.next
		ks.next = lowest
		out = r.drain(ks, out)
	}
}
