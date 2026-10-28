// Package servestale implements serve-stale-on-error: a cache that keeps
// expired entries for a grace period and serves them when the origin fails,
// instead of turning an origin outage into user-facing errors.
//
// A normal TTL cache throws an entry away the moment it expires. If the
// origin is down at that moment, every request for that key fails, even
// though a perfectly usable value was in memory a millisecond earlier. Serve-
// stale keeps the value for an extra stale-if-error window: fresh values are
// served as usual, expired ones trigger a refresh, and only if that refresh
// fails is the stale value served, clearly flagged as stale.
//
// To avoid hammering an origin that is already failing, a failed refresh
// also starts a retry interval during which the key is served stale without
// asking the origin again.
package servestale

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Loader fetches a value from the origin.
type Loader[K comparable, V any] func(ctx context.Context, key K) (V, error)

// Result is a value plus where it came from.
type Result[V any] struct {
	Value V
	// Stale is true when the value is past its TTL and was served because the
	// origin failed (or is in its retry interval).
	Stale bool
	// Age is how long ago the value was fetched from the origin.
	Age time.Duration
	// Cause is the origin error that forced a stale answer, if any.
	Cause error
}

// Stats counts how requests were answered.
type Stats struct {
	Fresh, Refreshed, Stale, Errors, OriginCalls uint64
}

type entry[V any] struct {
	val       V
	fetched   time.Time
	retryAt   time.Time // don't call the origin for this key before then
	lastCause error
}

// Cache is a concurrency-safe serve-stale cache.
type Cache[K comparable, V any] struct {
	load          Loader[K, V]
	ttl           time.Duration
	staleIfError  time.Duration
	retryInterval time.Duration
	now           func() time.Time

	mu      sync.Mutex
	entries map[K]*entry[V]
	stats   Stats
}

// Option configures a Cache.
type Option func(*options)

type options struct {
	now   func() time.Time
	retry time.Duration
}

// WithClock injects a clock, which makes TTL and grace behaviour testable.
func WithClock(now func() time.Time) Option {
	return func(o *options) {
		if now != nil {
			o.now = now
		}
	}
}

// WithRetryInterval sets how long to wait after a failed refresh before
// asking the origin again for the same key. During that time the stale value
// is served without an origin call. The default, 0, retries on every request.
func WithRetryInterval(d time.Duration) Option { return func(o *options) { o.retry = d } }

// New returns a cache whose values are fresh for ttl and may be served stale,
// if the origin fails, for up to staleIfError after that.
func New[K comparable, V any](load Loader[K, V], ttl, staleIfError time.Duration, opts ...Option) (*Cache[K, V], error) {
	if load == nil {
		return nil, errors.New("servestale: loader must not be nil")
	}
	if ttl <= 0 {
		return nil, errors.New("servestale: ttl must be positive")
	}
	if staleIfError < 0 {
		return nil, errors.New("servestale: staleIfError must not be negative")
	}
	o := options{now: time.Now}
	for _, opt := range opts {
		opt(&o)
	}
	if o.retry < 0 {
		return nil, errors.New("servestale: retry interval must not be negative")
	}
	return &Cache[K, V]{
		load: load, ttl: ttl, staleIfError: staleIfError, retryInterval: o.retry,
		now: o.now, entries: make(map[K]*entry[V]),
	}, nil
}

// Get returns key's value. A fresh entry is returned directly. Otherwise the
// origin is asked; if it fails and the entry is within its stale-if-error
// window, the stale value is returned with Stale set and a nil error.
// Beyond the window, or with no entry at all, the origin's error is returned.
func (c *Cache[K, V]) Get(ctx context.Context, key K) (Result[V], error) {
	c.mu.Lock()
	now := c.now()
	e := c.entries[key]
	if e != nil && now.Sub(e.fetched) < c.ttl {
		c.stats.Fresh++
		c.mu.Unlock()
		return Result[V]{Value: e.val, Age: now.Sub(e.fetched)}, nil
	}
	if e != nil && c.servable(now, e) && now.Before(e.retryAt) {
		// The origin failed recently; don't ask it again yet.
		c.stats.Stale++
		c.mu.Unlock()
		return Result[V]{Value: e.val, Stale: true, Age: now.Sub(e.fetched), Cause: e.lastCause}, nil
	}
	c.stats.OriginCalls++
	c.mu.Unlock()

	val, err := c.load(ctx, key)

	c.mu.Lock()
	defer c.mu.Unlock()
	now = c.now()
	if err == nil {
		c.entries[key] = &entry[V]{val: val, fetched: now}
		c.stats.Refreshed++
		return Result[V]{Value: val}, nil
	}
	if e = c.entries[key]; e != nil && c.servable(now, e) {
		e.retryAt = now.Add(c.retryInterval)
		e.lastCause = err
		c.stats.Stale++
		return Result[V]{Value: e.val, Stale: true, Age: now.Sub(e.fetched), Cause: err}, nil
	}
	if e != nil {
		delete(c.entries, key) // past the grace period: useless now
	}
	c.stats.Errors++
	return Result[V]{}, err
}

// servable reports whether e is still inside its stale-if-error window.
// The caller must hold c.mu.
func (c *Cache[K, V]) servable(now time.Time, e *entry[V]) bool {
	return now.Sub(e.fetched) < c.ttl+c.staleIfError
}

// Stats returns a snapshot of the counters.
func (c *Cache[K, V]) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}
