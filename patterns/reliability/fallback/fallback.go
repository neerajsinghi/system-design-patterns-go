// Package fallback degrades gracefully: when the primary source fails, it
// serves the last known good value (if it is not too stale), then a static
// default, and only then an error. Every result says where it came from, so
// callers and metrics can tell a fresh answer from a degraded one.
package fallback

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Source says which tier produced a result.
type Source int

const (
	Primary Source = iota // fresh from the primary
	Stale                 // last known good value
	Default               // static default
)

func (s Source) String() string {
	switch s {
	case Primary:
		return "primary"
	case Stale:
		return "stale"
	case Default:
		return "default"
	default:
		return "unknown"
	}
}

// Result is a value plus provenance.
type Result[V any] struct {
	Value  V
	Source Source
	Age    time.Duration // age of a Stale value; 0 otherwise
	Cause  error         // the primary's error when Source != Primary
}

// Options configures a Fallback. All fields are optional.
type Options[K comparable, V any] struct {
	// MaxStale is the oldest last-known-good value that may be served.
	// 0 disables the stale tier.
	MaxStale time.Duration
	// Default returns a static fallback for k; ok=false means none.
	Default func(k K) (v V, ok bool)
	// ShouldFallback decides whether a primary error is eligible for fallback.
	// Defaults to any error except context.Canceled (the caller has gone, so
	// there is nobody to serve). Return false for errors such as "not found"
	// or "permission denied", which must reach the caller unchanged.
	ShouldFallback func(error) bool
	// MaxEntries bounds the last-known-good store; <=0 means 10000. When full,
	// the oldest entry is evicted.
	MaxEntries int
	// Now is the clock; defaults to time.Now.
	Now func() time.Time
}

type entry[V any] struct {
	v  V
	at time.Time
}

// Fallback wraps a primary lookup. Safe for concurrent use.
type Fallback[K comparable, V any] struct {
	primary func(context.Context, K) (V, error)
	opt     Options[K, V]

	mu   sync.Mutex
	good map[K]entry[V]
}

// New wraps primary with the given options.
func New[K comparable, V any](primary func(context.Context, K) (V, error), opt Options[K, V]) *Fallback[K, V] {
	if opt.ShouldFallback == nil {
		opt.ShouldFallback = func(err error) bool { return !errors.Is(err, context.Canceled) }
	}
	if opt.MaxEntries <= 0 {
		opt.MaxEntries = 10000
	}
	if opt.Now == nil {
		opt.Now = time.Now
	}
	return &Fallback[K, V]{primary: primary, opt: opt, good: map[K]entry[V]{}}
}

// Get calls the primary and falls back on eligible errors. The error is
// non-nil only when no tier could produce a value; it is the primary's error.
func (f *Fallback[K, V]) Get(ctx context.Context, k K) (Result[V], error) {
	v, err := f.primary(ctx, k)
	if err == nil {
		f.remember(k, v)
		return Result[V]{Value: v, Source: Primary}, nil
	}
	if !f.opt.ShouldFallback(err) {
		return Result[V]{}, err
	}
	if f.opt.MaxStale > 0 {
		f.mu.Lock()
		e, ok := f.good[k]
		f.mu.Unlock()
		if ok {
			if age := f.opt.Now().Sub(e.at); age <= f.opt.MaxStale {
				return Result[V]{Value: e.v, Source: Stale, Age: age, Cause: err}, nil
			}
		}
	}
	if f.opt.Default != nil {
		if dv, ok := f.opt.Default(k); ok {
			return Result[V]{Value: dv, Source: Default, Cause: err}, nil
		}
	}
	return Result[V]{}, err
}

func (f *Fallback[K, V]) remember(k K, v V) {
	if f.opt.MaxStale <= 0 {
		return // stale tier disabled: no need to store anything
	}
	now := f.opt.Now()
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.good[k]; !exists && len(f.good) >= f.opt.MaxEntries {
		// O(n) scan only when inserting a new key at capacity; a production
		// version would use an LRU list.
		var oldestK K
		var oldestAt time.Time
		first := true
		for key, e := range f.good {
			if first || e.at.Before(oldestAt) {
				oldestK, oldestAt, first = key, e.at, false
			}
		}
		delete(f.good, oldestK)
	}
	f.good[k] = entry[V]{v: v, at: now}
}
