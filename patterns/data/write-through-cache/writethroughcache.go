// Package writethroughcache implements a write-through cache.
//
// Every write goes synchronously to the backing store and, only once the
// store has accepted it, to the cache. The cache therefore never holds a
// value the store does not have, and reads after a successful write always
// see that write. The price is that every write pays the store's latency.
package writethroughcache

import (
	"context"
	"errors"
	"hash/fnv"
	"sync"
)

// Store is the durable system of record behind the cache.
type Store[V any] interface {
	Get(ctx context.Context, key string) (V, bool, error)
	Put(ctx context.Context, key string, val V) error
	Delete(ctx context.Context, key string) error
}

// ErrNotFound is returned by Get when neither the cache nor the store has the key.
var ErrNotFound = errors.New("writethroughcache: not found")

// Stats counts cache activity.
type Stats struct {
	Hits, Misses, StoreWrites, StoreReads uint64
}

const stripes = 64

// Cache is a concurrency-safe write-through cache.
//
// Writes to the same key are serialised with a striped lock so that the
// order in which writes reach the store is the order in which they reach
// the cache. Without that, two concurrent Sets could leave the store with
// one value and the cache with the other.
type Cache[V any] struct {
	store Store[V]
	locks [stripes]sync.Mutex

	mu    sync.RWMutex
	data  map[string]V
	stats Stats
}

// New returns a write-through cache in front of store.
func New[V any](store Store[V]) (*Cache[V], error) {
	if store == nil {
		return nil, errors.New("writethroughcache: store must not be nil")
	}
	return &Cache[V]{store: store, data: make(map[string]V)}, nil
}

func (c *Cache[V]) lockFor(key string) *sync.Mutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return &c.locks[h.Sum32()%stripes]
}

// Set writes val to the store and then to the cache. If the store rejects
// the write, the cache is left untouched and the error is returned.
func (c *Cache[V]) Set(ctx context.Context, key string, val V) error {
	l := c.lockFor(key)
	l.Lock()
	defer l.Unlock()

	if err := c.store.Put(ctx, key, val); err != nil {
		return err
	}
	c.mu.Lock()
	c.data[key] = val
	c.stats.StoreWrites++
	c.mu.Unlock()
	return nil
}

// Delete removes key from the store and then from the cache.
func (c *Cache[V]) Delete(ctx context.Context, key string) error {
	l := c.lockFor(key)
	l.Lock()
	defer l.Unlock()

	if err := c.store.Delete(ctx, key); err != nil {
		return err
	}
	c.mu.Lock()
	delete(c.data, key)
	c.stats.StoreWrites++
	c.mu.Unlock()
	return nil
}

// Get serves from the cache, falling back to the store on a miss (for
// example after a restart, when the cache starts cold) and populating the
// cache with the result.
func (c *Cache[V]) Get(ctx context.Context, key string) (V, error) {
	c.mu.Lock()
	if v, ok := c.data[key]; ok {
		c.stats.Hits++
		c.mu.Unlock()
		return v, nil
	}
	c.stats.Misses++
	c.mu.Unlock()

	// Take the key's write lock so a concurrent Set can't be overwritten by
	// the older value we are about to read from the store.
	l := c.lockFor(key)
	l.Lock()
	defer l.Unlock()

	c.mu.RLock()
	v, ok := c.data[key] // a writer may have filled it while we waited
	c.mu.RUnlock()
	if ok {
		return v, nil
	}

	v, found, err := c.store.Get(ctx, key)
	c.mu.Lock()
	c.stats.StoreReads++
	if err == nil && found {
		c.data[key] = v
	}
	c.mu.Unlock()
	if err != nil {
		var zero V
		return zero, err
	}
	if !found {
		var zero V
		return zero, ErrNotFound
	}
	return v, nil
}

// Stats returns a snapshot of the counters.
func (c *Cache[V]) Stats() Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stats
}

// MemStore is an in-memory Store with failure injection, for tests and demos.
type MemStore[V any] struct {
	mu   sync.Mutex
	rows map[string]V
	// failPuts is the number of upcoming Put calls that will fail.
	failPuts int
}

// NewMemStore returns an empty in-memory store.
func NewMemStore[V any]() *MemStore[V] { return &MemStore[V]{rows: make(map[string]V)} }

// ErrInjected is returned by MemStore when a failure was injected.
var ErrInjected = errors.New("memstore: injected failure")

// FailNextPuts makes the next n Put calls return ErrInjected.
func (m *MemStore[V]) FailNextPuts(n int) { m.mu.Lock(); m.failPuts = n; m.mu.Unlock() }

// Get implements Store.
func (m *MemStore[V]) Get(_ context.Context, key string) (V, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.rows[key]
	return v, ok, nil
}

// Put implements Store.
func (m *MemStore[V]) Put(_ context.Context, key string, val V) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failPuts > 0 {
		m.failPuts--
		return ErrInjected
	}
	m.rows[key] = val
	return nil
}

// Delete implements Store.
func (m *MemStore[V]) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.rows, key)
	return nil
}
