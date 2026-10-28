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
//
// Two optional extras guard the database further:
//
//   - Negative caching (WithNegativeTTL): a loader result of ErrNotFound is
//     cached briefly, so repeated lookups of a missing key don't all reach
//     the database.
//   - TTL jitter (WithTTLJitter): each entry's TTL is shortened by a random
//     fraction, so keys loaded together don't all expire together.
package cacheaside

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"
)

// ErrNotFound is what a Loader returns (or wraps) when the key doesn't exist
// in the source of truth. With WithNegativeTTL, that answer is cached too.
var ErrNotFound = errors.New("cacheaside: not found")

// Loader fetches a value from the source of truth (database, API, ...).
type Loader[K comparable, V any] func(ctx context.Context, key K) (V, error)

// Stats counts cache activity. Loads is the number of times the Loader ran.
// NegativeHits are hits on a cached "not found".
type Stats struct {
	Hits, Misses, Loads, Shared, NegativeHits uint64
}

type entry[V any] struct {
	val     V
	err     error // non-nil for a cached "not found"
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
	load   Loader[K, V]
	ttl    time.Duration
	negTTL time.Duration
	jitter float64
	now    func() time.Time
	rand   func() float64

	mu       sync.Mutex
	entries  map[K]entry[V]
	inflight map[K]*call[V]
	stats    Stats
}

// Option configures a Cache.
type Option func(*options)

type options struct {
	now    func() time.Time
	rand   func() float64
	negTTL time.Duration
	jitter float64
}

// WithNegativeTTL caches ErrNotFound results for d, so a flood of lookups for
// a key that doesn't exist (a deleted item, a typo'd URL, an enumeration
// attack) reaches the database once per d instead of on every request.
// Keep d short: a key created after a miss stays invisible for up to d,
// unless the writer calls Invalidate.
func WithNegativeTTL(d time.Duration) Option { return func(o *options) { o.negTTL = d } }

// WithTTLJitter shortens each entry's TTL by a random fraction in [0, f),
// with f in [0, 1), so keys loaded at the same moment expire across a window
// instead of all at once. See the refresh-ahead pattern for more.
func WithTTLJitter(f float64) Option { return func(o *options) { o.jitter = f } }

// WithRand injects the random source used for TTL jitter. It must return
// values in [0, 1); the cache calls it with its lock held.
func WithRand(r func() float64) Option {
	return func(o *options) {
		if r != nil {
			o.rand = r
		}
	}
}

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
	o := options{now: time.Now, rand: rand.Float64}
	for _, opt := range opts {
		opt(&o)
	}
	if o.negTTL < 0 {
		return nil, errors.New("cacheaside: negative ttl must not be negative")
	}
	if o.jitter < 0 || o.jitter >= 1 {
		return nil, errors.New("cacheaside: ttl jitter must be in [0, 1)")
	}
	return &Cache[K, V]{
		load:     load,
		ttl:      ttl,
		negTTL:   o.negTTL,
		jitter:   o.jitter,
		now:      o.now,
		rand:     o.rand,
		entries:  make(map[K]entry[V]),
		inflight: make(map[K]*call[V]),
	}, nil
}

// Get returns the cached value for key, loading it on a miss or expiry.
// Concurrent misses for the same key wait for one shared load. Errors are
// returned to every waiter and are not cached, except ErrNotFound when
// WithNegativeTTL is set. ctx bounds how long this
// caller waits; the shared load itself is not cancelled when one waiter gives up.
func (c *Cache[K, V]) Get(ctx context.Context, key K) (V, error) {
	c.mu.Lock()
	if e, ok := c.entries[key]; ok && c.now().Before(e.expires) {
		c.stats.Hits++
		if e.err != nil {
			c.stats.NegativeHits++
		}
		c.mu.Unlock()
		return e.val, e.err
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
	switch {
	case cl.stale:
	case err == nil:
		c.entries[key] = entry[V]{val: val, expires: c.now().Add(c.jitteredTTL())}
	case c.negTTL > 0 && errors.Is(err, ErrNotFound):
		c.entries[key] = entry[V]{val: val, err: err, expires: c.now().Add(c.negTTL)}
	}
	delete(c.inflight, key)
	c.mu.Unlock()
	close(cl.done)
}

// jitteredTTL returns the TTL shortened by a random fraction in [0, jitter).
// The caller must hold c.mu.
func (c *Cache[K, V]) jitteredTTL() time.Duration {
	if c.jitter == 0 {
		return c.ttl
	}
	return time.Duration(float64(c.ttl) * (1 - c.jitter*c.rand()))
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
