// Package cacheaside implements the cache-aside (lazy loading) pattern.
//
// The application owns the cache: reads check the cache first and, on a
// miss, load from the source of truth and populate the cache. Writes go to
// the source of truth and then invalidate the cached entry.
//
// This implementation adds the two things production cache-aside needs:
//
//   - TTL expiry, so stale data eventually ages out even if an
//     invalidation is missed.
//   - Stampede protection: concurrent misses for the same key share a
//     single in-flight load (singleflight-style) instead of all hitting
//     the database at once.
package cacheaside

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Loader fetches a value from the source of truth (database, API, ...).
type Loader[K comparable, V any] func(ctx context.Context, key K) (V, error)

// Stats counts cache activity. Loads is the number of times the Loader ran.
type Stats struct {
	Hits, Misses, Loads, Shared uint64
}

type entry[V any] struct {
	val     V
	expires time.Time
}

// call is one in-flight load shared by every concurrent miss on a key.
type call[V any] struct {
	done  chan struct{}
	val   V
	err   error
	stale bool // set by Invalidate while loading: don't cache the result
}

// Cache is a concurrency-safe cache-aside wrapper around a Loader.
type Cache[K comparable, V any] struct {
	load Loader[K, V]
	ttl  time.Duration
	now  func() time.Time

	mu       sync.Mutex
	entries  map[K]entry[V]
	inflight map[K]*call[V]
	stats    Stats
}

// Option configures a Cache.
type Option func(*options)

type options struct{ now func() time.Time }

// WithClock injects a clock, which makes TTL behaviour testable.
func WithClock(now func() time.Time) Option {
	return func(o *options) {
		if now != nil {
			o.now = now
		}
	}
}

// New returns a cache that loads misses with load and keeps values for ttl.
func New[K comparable, V any](load Loader[K, V], ttl time.Duration, opts ...Option) (*Cache[K, V], error) {
	if load == nil {
		return nil, errors.New("cacheaside: loader must not be nil")
	}
	if ttl <= 0 {
		return nil, errors.New("cacheaside: ttl must be positive")
	}
	o := options{now: time.Now}
	for _, opt := range opts {
		opt(&o)
	}
	return &Cache[K, V]{
		load:     load,
		ttl:      ttl,
		now:      o.now,
		entries:  make(map[K]entry[V]),
		inflight: make(map[K]*call[V]),
	}, nil
}

// Get returns the cached value for key, loading it on a miss or expiry.
// Concurrent misses for the same key wait for one shared load. Errors are
// returned to every waiter but are never cached. ctx bounds how long this
// caller waits; the shared load itself is not cancelled when one waiter gives up.
func (c *Cache[K, V]) Get(ctx context.Context, key K) (V, error) {
	c.mu.Lock()
	if e, ok := c.entries[key]; ok && c.now().Before(e.expires) {
		c.stats.Hits++
		c.mu.Unlock()
		return e.val, nil
	}
	delete(c.entries, key) // lazily drop the expired entry, if any
	c.stats.Misses++

	cl, loading := c.inflight[key]
	if loading {
		c.stats.Shared++
	} else {
		cl = &call[V]{done: make(chan struct{})}
		c.inflight[key] = cl
		c.stats.Loads++
		// Detach from this caller's cancellation so other waiters still
		// get a result if the first caller goes away.
		go c.doLoad(context.WithoutCancel(ctx), key, cl)
	}
	c.mu.Unlock()

	select {
	case <-cl.done:
		return cl.val, cl.err
	case <-ctx.Done():
		var zero V
		return zero, ctx.Err()
	}
}

func (c *Cache[K, V]) doLoad(ctx context.Context, key K, cl *call[V]) {
	val, err := c.load(ctx, key)

	c.mu.Lock()
	cl.val, cl.err = val, err
	if err == nil && !cl.stale {
		c.entries[key] = entry[V]{val: val, expires: c.now().Add(c.ttl)}
	}
	delete(c.inflight, key)
	c.mu.Unlock()
	close(cl.done)
}

// Invalidate removes key from the cache. Call it after writing to the source
// of truth. A load already in flight for key will still return to its
// waiters, but its (possibly pre-write) result will not be cached.
func (c *Cache[K, V]) Invalidate(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
	if cl, ok := c.inflight[key]; ok {
		cl.stale = true
	}
}

// Len returns the number of cached entries, including any not yet expired lazily.
func (c *Cache[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// Stats returns a snapshot of the counters.
func (c *Cache[K, V]) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}
