// Package refreshahead implements a cache that refreshes entries before they
// expire, so hot keys never fall out of the cache and keys loaded together
// don't all expire together.
//
// It combines two techniques:
//
//   - TTL jitter: each entry's lifetime is shortened by a random fraction, so
//     keys written in the same instant (a deploy, a cache warm-up, a restart)
//     expire across a window instead of in one spike.
//   - Probabilistic early refresh (XFetch, from Vattani, Chierichetti and
//     Lowenstein, "Optimal Probabilistic Cache Stampede Prevention", VLDB 2015):
//     on each hit, a reader refreshes the entry early with a probability that
//     rises as expiry approaches and as the value gets more expensive to
//     recompute. One reader pays for the refresh while everyone else keeps
//     getting the cached value, so a hot key is refreshed before it ever misses.
//
// Concurrent misses for the same key share one load, as in cache-aside.
package refreshahead

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

// Loader fetches a value from the source of truth.
type Loader[K comparable, V any] func(ctx context.Context, key K) (V, error)

// Stats counts cache activity. Misses are reads that had to wait for a load;
// EarlyRefreshes are loads started while the cached value was still valid.
type Stats struct {
	Hits, Misses, Shared, Loads, EarlyRefreshes uint64
}

type entry[V any] struct {
	val     V
	expires time.Time
	delta   time.Duration // how long the last recompute took
}

type call[V any] struct {
	done  chan struct{}
	val   V
	err   error
	stale bool // invalidated while loading: don't cache the result
}

// Cache is a concurrency-safe read-through cache with jittered TTLs and
// optional probabilistic early refresh.
type Cache[K comparable, V any] struct {
	load   Loader[K, V]
	ttl    time.Duration
	jitter float64
	beta   float64
	now    func() time.Time
	rand   func() float64
	cost   func(K) time.Duration

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
	jitter float64
	beta   float64
	cost   func(any) time.Duration
}

// WithClock injects a clock, which makes expiry and refresh testable.
func WithClock(now func() time.Time) Option {
	return func(o *options) {
		if now != nil {
			o.now = now
		}
	}
}

// WithRand injects the random source used for jitter and early refresh.
// It must return values in [0, 1). The cache calls it with its lock held.
func WithRand(r func() float64) Option {
	return func(o *options) {
		if r != nil {
			o.rand = r
		}
	}
}

// WithJitter shortens each entry's TTL by a random fraction in [0, f).
// f must be in [0, 1). Jitter never lengthens the TTL, so the configured TTL
// remains the upper bound on staleness.
func WithJitter(f float64) Option { return func(o *options) { o.jitter = f } }

// WithEarlyRefresh enables XFetch early refresh with the given beta. beta = 1
// is the paper's recommended default; larger values refresh earlier. 0 disables it.
func WithEarlyRefresh(beta float64) Option { return func(o *options) { o.beta = beta } }

// WithLoadCost overrides the measured recompute time used by early refresh.
// Use it when the clock is simulated, or when you already know a key's
// recompute cost from metrics.
func WithLoadCost[K comparable](cost func(K) time.Duration) Option {
	return func(o *options) {
		if cost != nil {
			o.cost = func(k any) time.Duration { return cost(k.(K)) }
		}
	}
}

// New returns a cache that loads with load and keeps values for at most ttl.
func New[K comparable, V any](load Loader[K, V], ttl time.Duration, opts ...Option) (*Cache[K, V], error) {
	if load == nil {
		return nil, errors.New("refreshahead: loader must not be nil")
	}
	if ttl <= 0 {
		return nil, errors.New("refreshahead: ttl must be positive")
	}
	o := options{now: time.Now, rand: rand.Float64}
	for _, opt := range opts {
		opt(&o)
	}
	if o.jitter < 0 || o.jitter >= 1 {
		return nil, errors.New("refreshahead: jitter must be in [0, 1)")
	}
	if o.beta < 0 {
		return nil, errors.New("refreshahead: beta must not be negative")
	}
	c := &Cache[K, V]{
		load:     load,
		ttl:      ttl,
		jitter:   o.jitter,
		beta:     o.beta,
		now:      o.now,
		rand:     o.rand,
		entries:  make(map[K]entry[V]),
		inflight: make(map[K]*call[V]),
	}
	if o.cost != nil {
		c.cost = func(k K) time.Duration { return o.cost(k) }
	}
	return c, nil
}

// Get returns the value for key. A fresh hit may trigger an early refresh,
// which this caller performs synchronously; if that refresh fails, the still
// valid cached value is returned. A miss (absent or expired) waits for one
// shared load. ctx bounds how long this caller waits.
func (c *Cache[K, V]) Get(ctx context.Context, key K) (V, error) {
	c.mu.Lock()
	now := c.now()
	if e, ok := c.entries[key]; ok && now.Before(e.expires) {
		if _, loading := c.inflight[key]; !loading && c.refreshEarly(now, e) {
			c.stats.EarlyRefreshes++
			cl := c.startLoad(key)
			c.mu.Unlock()
			if v, err := c.doLoad(ctx, key, cl); err == nil {
				return v, nil
			}
			return e.val, nil
		}
		c.stats.Hits++
		c.mu.Unlock()
		return e.val, nil
	}
	delete(c.entries, key)
	c.stats.Misses++
	if cl, loading := c.inflight[key]; loading {
		c.stats.Shared++
		c.mu.Unlock()
		select {
		case <-cl.done:
			return cl.val, cl.err
		case <-ctx.Done():
			var zero V
			return zero, ctx.Err()
		}
	}
	cl := c.startLoad(key)
	c.mu.Unlock()
	return c.doLoad(ctx, key, cl)
}

// refreshEarly is the XFetch test: refresh if now - delta*beta*ln(r) >= expiry,
// with r uniform in (0, 1]. -ln(r) is exponentially distributed, so the
// chance of refreshing grows smoothly as expiry approaches.
func (c *Cache[K, V]) refreshEarly(now time.Time, e entry[V]) bool {
	if c.beta == 0 || e.delta <= 0 {
		return false
	}
	r := 1 - c.rand() // (0, 1], so the log is finite
	gap := time.Duration(-float64(e.delta) * c.beta * math.Log(r))
	return !now.Add(gap).Before(e.expires)
}

// startLoad registers an in-flight load. The caller must hold c.mu.
func (c *Cache[K, V]) startLoad(key K) *call[V] {
	cl := &call[V]{done: make(chan struct{})}
	c.inflight[key] = cl
	c.stats.Loads++
	return cl
}

func (c *Cache[K, V]) doLoad(ctx context.Context, key K, cl *call[V]) (V, error) {
	start := c.now()
	val, err := c.load(ctx, key)

	c.mu.Lock()
	end := c.now()
	delta := end.Sub(start)
	if c.cost != nil {
		delta = c.cost(key)
	}
	cl.val, cl.err = val, err
	if err == nil && !cl.stale {
		c.entries[key] = entry[V]{val: val, expires: end.Add(c.jitteredTTL()), delta: delta}
	}
	delete(c.inflight, key)
	c.mu.Unlock()
	close(cl.done)
	return val, err
}

// jitteredTTL returns ttl shortened by a random fraction in [0, jitter).
// The caller must hold c.mu.
func (c *Cache[K, V]) jitteredTTL() time.Duration {
	if c.jitter == 0 {
		return c.ttl
	}
	return time.Duration(float64(c.ttl) * (1 - c.jitter*c.rand()))
}

// Invalidate removes key. A load already in flight still returns to its
// waiters, but its result is not cached.
func (c *Cache[K, V]) Invalidate(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
	if cl, ok := c.inflight[key]; ok {
		cl.stale = true
	}
}

// Stats returns a snapshot of the counters.
func (c *Cache[K, V]) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}
