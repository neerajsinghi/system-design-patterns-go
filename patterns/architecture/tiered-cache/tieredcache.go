// Package tieredcache implements a tiered (hierarchical) cache, the
// architecture CDNs use: many edge caches close to users, an optional origin
// shield in the middle, and the origin behind it.
//
// Each tier is a TTL cache whose misses are fetched from the tier behind it,
// and each tier coalesces concurrent misses for the same key into one fetch.
// That gives the two properties that protect an origin during a launch:
//
//   - Collapsing within a tier: a thousand viewers missing at one edge cause
//     one request to the shield.
//   - Collapsing across tiers: every edge missing at once causes one request
//     from the shield to the origin, instead of one per edge.
//
// Prewarm pushes content into the tiers before traffic arrives, so even the
// first viewer is a cache hit.
package tieredcache

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Fetcher loads a value for key. The origin is a Fetcher, and so is every Tier.
type Fetcher[V any] func(ctx context.Context, key string) (V, error)

// Stats counts one tier's activity. Fetches is how many requests it sent to
// its parent; Shared is how many misses waited on another caller's fetch.
type Stats struct {
	Hits, Misses, Shared, Fetches uint64
}

// HitRatio returns hits / (hits + misses), or 0 with no traffic.
func (s Stats) HitRatio() float64 {
	if s.Hits+s.Misses == 0 {
		return 0
	}
	return float64(s.Hits) / float64(s.Hits+s.Misses)
}

type entry[V any] struct {
	val     V
	expires time.Time
}

type call[V any] struct {
	done chan struct{}
	val  V
	err  error
}

// Tier is one caching layer: an edge, a regional cache or a shield.
type Tier[V any] struct {
	name   string
	ttl    time.Duration
	parent Fetcher[V]
	now    func() time.Time

	mu       sync.Mutex
	entries  map[string]entry[V]
	inflight map[string]*call[V]
	stats    Stats
}

// Option configures a Tier.
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

// NewTier returns a tier that caches values for ttl and fetches misses from parent.
func NewTier[V any](name string, ttl time.Duration, parent Fetcher[V], opts ...Option) (*Tier[V], error) {
	if parent == nil {
		return nil, errors.New("tieredcache: parent must not be nil")
	}
	if ttl <= 0 {
		return nil, errors.New("tieredcache: ttl must be positive")
	}
	o := options{now: time.Now}
	for _, opt := range opts {
		opt(&o)
	}
	return &Tier[V]{
		name: name, ttl: ttl, parent: parent, now: o.now,
		entries: make(map[string]entry[V]), inflight: make(map[string]*call[V]),
	}, nil
}

// Name returns the tier's name.
func (t *Tier[V]) Name() string { return t.name }

// Fetch returns key from this tier, or fetches it from the parent on a miss.
// Concurrent misses for the same key share one parent fetch, which keeps
// running even if the caller that started it gives up. Errors are passed to
// every waiter and never cached. Fetch has the Fetcher signature, so a tier
// can be the parent of another tier.
func (t *Tier[V]) Fetch(ctx context.Context, key string) (V, error) {
	t.mu.Lock()
	if e, ok := t.entries[key]; ok && t.now().Before(e.expires) {
		t.stats.Hits++
		t.mu.Unlock()
		return e.val, nil
	}
	t.stats.Misses++
	cl, loading := t.inflight[key]
	if loading {
		t.stats.Shared++
	} else {
		cl = &call[V]{done: make(chan struct{})}
		t.inflight[key] = cl
		t.stats.Fetches++
		go t.fetch(context.WithoutCancel(ctx), key, cl)
	}
	t.mu.Unlock()

	select {
	case <-cl.done:
		return cl.val, cl.err
	case <-ctx.Done():
		var zero V
		return zero, ctx.Err()
	}
}

func (t *Tier[V]) fetch(ctx context.Context, key string, cl *call[V]) {
	val, err := t.parent(ctx, key)
	t.mu.Lock()
	cl.val, cl.err = val, err
	if err == nil {
		t.entries[key] = entry[V]{val: val, expires: t.now().Add(t.ttl)}
	}
	delete(t.inflight, key)
	t.mu.Unlock()
	close(cl.done)
}

// Put stores a value directly in this tier (a push), resetting its TTL.
func (t *Tier[V]) Put(key string, val V) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.entries[key] = entry[V]{val: val, expires: t.now().Add(t.ttl)}
}

// Purge removes key from this tier.
func (t *Tier[V]) Purge(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.entries, key)
}

// Stats returns a snapshot of the tier's counters.
func (t *Tier[V]) Stats() Stats {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stats
}

// Prewarm fetches each key once from origin and pushes it into every tier,
// as a CDN does before a predictable launch. It stops at the first error.
func Prewarm[V any](ctx context.Context, origin Fetcher[V], keys []string, tiers ...*Tier[V]) error {
	for _, k := range keys {
		v, err := origin(ctx, k)
		if err != nil {
			return err
		}
		for _, t := range tiers {
			t.Put(k, v)
		}
	}
	return nil
}
