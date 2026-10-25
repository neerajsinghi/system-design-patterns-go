// Package writebehindcache implements a write-behind (write-back) cache.
//
// Writes are acknowledged as soon as they land in memory. A background
// flusher persists dirty keys to the store in batches, either on a timer or
// once enough keys are dirty. Repeated writes to the same key between
// flushes are coalesced into one store write.
//
// Durability caveat: anything not yet flushed is lost if the process dies.
// Use write-behind only for data you can afford to lose or rebuild, or put a
// write-ahead log in front of it.
package writebehindcache

import (
	"context"
	"errors"
	"sync"
	"time"
)

// BatchStore persists many key/value pairs in one round trip.
type BatchStore[V any] interface {
	PutBatch(ctx context.Context, batch map[string]V) error
}

// ErrClosed is returned by Set after Close or Crash.
var ErrClosed = errors.New("writebehindcache: cache closed")

// Config controls flushing.
type Config struct {
	// MaxBatch is both the dirty-key count that triggers an early flush and
	// the maximum number of keys per PutBatch call. Must be > 0.
	MaxBatch int
	// Interval between timed flushes. Ignored when Tick is set.
	Interval time.Duration
	// Tick, if non-nil, replaces the internal ticker (for deterministic tests).
	Tick <-chan time.Time
}

// Stats counts flusher activity.
type Stats struct {
	Writes      uint64 // Set calls accepted
	Coalesced   uint64 // Sets that overwrote a not-yet-flushed value
	Flushed     uint64 // keys persisted
	Batches     uint64 // successful PutBatch calls
	FlushErrors uint64 // failed PutBatch calls (keys were re-queued)
}

// Cache is a concurrency-safe write-behind cache.
type Cache[V any] struct {
	store    BatchStore[V]
	maxBatch int

	mu     sync.Mutex
	data   map[string]V // everything we know: the read path
	dirty  map[string]V // written but not yet persisted
	closed bool
	stats  Stats

	flushMu sync.Mutex // one flush at a time, so batches reach the store in order
	kick    chan struct{}
	stop    chan struct{}
	done    chan struct{}
	once    sync.Once
	ticker  *time.Ticker
}

// New starts a write-behind cache and its background flusher. Call Close to
// flush remaining writes and stop the flusher.
func New[V any](store BatchStore[V], cfg Config) (*Cache[V], error) {
	if store == nil {
		return nil, errors.New("writebehindcache: store must not be nil")
	}
	if cfg.MaxBatch <= 0 {
		return nil, errors.New("writebehindcache: MaxBatch must be positive")
	}
	if cfg.Tick == nil && cfg.Interval <= 0 {
		return nil, errors.New("writebehindcache: Interval must be positive")
	}
	c := &Cache[V]{
		store:    store,
		maxBatch: cfg.MaxBatch,
		data:     make(map[string]V),
		dirty:    make(map[string]V),
		kick:     make(chan struct{}, 1),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	tick := cfg.Tick
	if tick == nil {
		c.ticker = time.NewTicker(cfg.Interval)
		tick = c.ticker.C
	}
	go c.loop(tick)
	return c, nil
}

func (c *Cache[V]) loop(tick <-chan time.Time) {
	defer close(c.done)
	for {
		select {
		case <-tick:
		case <-c.kick:
		case <-c.stop:
			return
		}
		select {
		case <-c.stop: // shutting down: don't start a new flush
			return
		default:
		}
		// Errors are counted in Stats and the keys stay dirty for the next round.
		_ = c.Flush(context.Background())
	}
}

// Set stores val in memory and marks key dirty. It returns before the store
// is written.
func (c *Cache[V]) Set(key string, val V) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	if _, pending := c.dirty[key]; pending {
		c.stats.Coalesced++
	}
	c.data[key] = val
	c.dirty[key] = val
	c.stats.Writes++
	full := len(c.dirty) >= c.maxBatch
	c.mu.Unlock()

	if full {
		select {
		case c.kick <- struct{}{}: // wake the flusher early
		default: // a flush is already requested
		}
	}
	return nil
}

// Get returns the latest written value, flushed or not.
func (c *Cache[V]) Get(key string) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.data[key]
	return v, ok
}

// Dirty returns how many keys are waiting to be persisted. This is exactly
// the data at risk if the process crashes now.
func (c *Cache[V]) Dirty() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.dirty)
}

// Flush synchronously persists all dirty keys in batches of at most
// MaxBatch. On a store error the failed batch is re-queued, except for keys
// that were rewritten in the meantime, since their newer value wins.
func (c *Cache[V]) Flush(ctx context.Context) error {
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	for {
		c.mu.Lock()
		if len(c.dirty) == 0 {
			c.mu.Unlock()
			return nil
		}
		batch := make(map[string]V, min(len(c.dirty), c.maxBatch))
		for k, v := range c.dirty {
			if len(batch) == c.maxBatch {
				break
			}
			batch[k] = v
			delete(c.dirty, k)
		}
		c.mu.Unlock()

		err := c.store.PutBatch(ctx, batch)

		c.mu.Lock()
		if err != nil {
			c.stats.FlushErrors++
			for k, v := range batch {
				if _, newer := c.dirty[k]; !newer {
					c.dirty[k] = v
				}
			}
			c.mu.Unlock()
			return err
		}
		c.stats.Batches++
		c.stats.Flushed += uint64(len(batch))
		c.mu.Unlock()
	}
}

// Close stops accepting writes, stops the flusher and performs a final
// flush. It is safe to call more than once; later calls only retry the flush.
func (c *Cache[V]) Close(ctx context.Context) error {
	c.shutdown()
	return c.Flush(ctx)
}

// Crash simulates the process dying: the flusher stops and nothing more is
// persisted. It returns the number of dirty writes that were lost.
func (c *Cache[V]) Crash() int {
	c.shutdown()
	c.flushMu.Lock() // wait out any in-progress flush
	defer c.flushMu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	lost := len(c.dirty)
	c.dirty = make(map[string]V)
	return lost
}

func (c *Cache[V]) shutdown() {
	c.once.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		close(c.stop)
		<-c.done
		if c.ticker != nil {
			c.ticker.Stop()
		}
	})
}

// Stats returns a snapshot of the counters.
func (c *Cache[V]) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

// MemStore is an in-memory BatchStore that records each batch, for tests and demos.
type MemStore[V any] struct {
	mu       sync.Mutex
	rows     map[string]V
	batches  []int
	failNext int
}

// NewMemStore returns an empty MemStore.
func NewMemStore[V any]() *MemStore[V] { return &MemStore[V]{rows: make(map[string]V)} }

// ErrInjected is returned by MemStore when a failure was injected.
var ErrInjected = errors.New("memstore: injected failure")

// FailNext makes the next n PutBatch calls fail.
func (m *MemStore[V]) FailNext(n int) { m.mu.Lock(); m.failNext = n; m.mu.Unlock() }

// PutBatch implements BatchStore.
func (m *MemStore[V]) PutBatch(_ context.Context, batch map[string]V) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failNext > 0 {
		m.failNext--
		return ErrInjected
	}
	for k, v := range batch {
		m.rows[k] = v
	}
	m.batches = append(m.batches, len(batch))
	return nil
}

// Get returns the persisted value for key.
func (m *MemStore[V]) Get(key string) (V, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.rows[key]
	return v, ok
}

// Len returns the number of persisted keys.
func (m *MemStore[V]) Len() int { m.mu.Lock(); defer m.mu.Unlock(); return len(m.rows) }

// Batches returns the size of every successful batch, in order.
func (m *MemStore[V]) Batches() []int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]int(nil), m.batches...)
}
