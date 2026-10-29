// Package optimisticlocking implements optimistic concurrency control: a
// versioned key-value store with compare-and-set, plus a bounded
// read-modify-write retry helper.
//
// The bug it prevents is the lost update. Two buyers load the same seat, both
// see it free, and both write "booked by me". Each write succeeds on its own,
// the second silently overwrites the first, and two people hold tickets for
// one seat. Nothing failed, so nothing was logged.
//
// Optimistic locking takes no lock while the caller thinks. Every record
// carries a version number that goes up by one on every write. A writer
// remembers the version it read and writes with CompareAndSet(key,
// expectedVersion, newValue): the store applies the write only if the version
// is still the one the writer saw, and otherwise returns ErrConflict. The
// loser of a race learns that its decision was based on stale data and can
// re-read and decide again. Update wraps that loop: it re-reads, re-applies
// the caller's function, and gives up after maxAttempts with
// ErrTooManyConflicts. The function can also refuse (for example "seat
// already taken"), which ends the loop with the function's error.
//
// Optimism pays off when conflicts are rare. When many writers hammer the
// same key, most attempts conflict and the retries are wasted work; that is
// when a lock per key, or a queue that serialises writers, is the better tool.
package optimisticlocking

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrConflict is returned by CompareAndSet when the key's version is no
// longer the one the caller expected: someone else wrote in between.
var ErrConflict = errors.New("optimisticlocking: version conflict")

// ErrTooManyConflicts is returned by Update when every one of its attempts
// lost a race. The returned error also matches ErrConflict.
var ErrTooManyConflicts = errors.New("optimisticlocking: too many conflicts")

// Versioned is a value together with the version it was read at. Version 0
// means the key does not exist; the first write creates version 1.
type Versioned[V any] struct {
	Value   V
	Version uint64
}

// Stats counts what the store has been asked to do.
type Stats struct {
	Reads     uint64 // Get calls
	Writes    uint64 // successful writes (CompareAndSet or Put)
	Conflicts uint64 // CompareAndSet calls rejected with ErrConflict
}

// Store is a concurrency-safe in-memory versioned key-value store.
type Store[K comparable, V any] struct {
	mu    sync.Mutex
	items map[K]Versioned[V]
	stats Stats
}

// NewStore returns an empty store.
func NewStore[K comparable, V any]() *Store[K, V] {
	return &Store[K, V]{items: make(map[K]Versioned[V])}
}

// Get returns key's current value and version. A missing key returns the
// zero value at version 0.
func (s *Store[K, V]) Get(key K) Versioned[V] {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.Reads++
	return s.items[key]
}

// CompareAndSet writes newValue only if key is still at expectedVersion, and
// returns the new version. Use expectedVersion 0 to create a key that must
// not exist yet. On a mismatch nothing is written and the error is
// ErrConflict.
func (s *Store[K, V]) CompareAndSet(key K, expectedVersion uint64, newValue V) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.items[key]
	if cur.Version != expectedVersion {
		s.stats.Conflicts++
		return cur.Version, fmt.Errorf("%w: key at version %d, expected %d", ErrConflict, cur.Version, expectedVersion)
	}
	next := Versioned[V]{Value: newValue, Version: cur.Version + 1}
	s.items[key] = next
	s.stats.Writes++
	return next.Version, nil
}

// Put writes newValue unconditionally (last write wins) and returns the new
// version. It exists to show the naive read-then-write pattern that
// CompareAndSet replaces; don't use it for read-modify-write.
func (s *Store[K, V]) Put(key K, newValue V) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := Versioned[V]{Value: newValue, Version: s.items[key].Version + 1}
	s.items[key] = next
	s.stats.Writes++
	return next.Version
}

// Stats returns a snapshot of the store's counters.
func (s *Store[K, V]) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

// UpdateFunc computes the new value from the current one. It may be called
// several times, once per attempt, so it must not have side effects outside
// its return value. Returning an error aborts Update with that error.
type UpdateFunc[V any] func(current Versioned[V]) (V, error)

// Result describes a successful Update.
type Result[V any] struct {
	Value    V
	Version  uint64
	Attempts int // 1 means the first write went through
}

// Update performs an optimistic read-modify-write: read key, call fn, and
// CompareAndSet the result at the version that was read. On a conflict it
// re-reads and calls fn again, up to maxAttempts times in total.
//
// It returns fn's error unchanged if fn refuses, ctx.Err() if the context
// ends between attempts, and an error matching both ErrTooManyConflicts and
// ErrConflict if every attempt lost a race.
func (s *Store[K, V]) Update(ctx context.Context, key K, fn UpdateFunc[V], maxAttempts int) (Result[V], error) {
	if fn == nil {
		return Result[V]{}, errors.New("optimisticlocking: update func must not be nil")
	}
	if maxAttempts < 1 {
		return Result[V]{}, errors.New("optimisticlocking: maxAttempts must be at least 1")
	}
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return Result[V]{}, err
		}
		cur := s.Get(key)
		next, err := fn(cur)
		if err != nil {
			return Result[V]{}, err
		}
		v, err := s.CompareAndSet(key, cur.Version, next)
		if err == nil {
			return Result[V]{Value: next, Version: v, Attempts: attempt}, nil
		}
		// Only ErrConflict is possible here; loop to re-read.
	}
	return Result[V]{}, fmt.Errorf("%w after %d attempts: %w", ErrTooManyConflicts, maxAttempts, ErrConflict)
}
