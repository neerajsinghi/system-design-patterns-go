// Package bulkhead isolates dependencies by giving each one its own bounded
// pool of concurrent calls (and a bounded wait queue), so a slow dependency
// can exhaust only its own slots instead of every goroutine in the process.
package bulkhead

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// ErrFull is returned when no slot is free and the wait queue is full, or the
// caller gave up waiting. When the caller's context ended, the context error
// is joined with it.
var ErrFull = errors.New("bulkhead: full")

// Config sizes a bulkhead.
type Config struct {
	MaxConcurrent int // calls allowed to run at once; <=0 means 1
	MaxWaiting    int // callers allowed to queue for a slot; <0 means 0
}

// Bulkhead is a counting semaphore with a bounded wait queue.
// Safe for concurrent use.
type Bulkhead struct {
	slots      chan struct{}
	maxWaiting int64
	waiting    atomic.Int64
	rejected   atomic.Int64
}

// New returns a Bulkhead sized by cfg.
func New(cfg Config) *Bulkhead {
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = 1
	}
	if cfg.MaxWaiting < 0 {
		cfg.MaxWaiting = 0
	}
	return &Bulkhead{slots: make(chan struct{}, cfg.MaxConcurrent), maxWaiting: int64(cfg.MaxWaiting)}
}

// Acquire takes a slot, waiting while ctx allows if the queue has room.
// Every successful Acquire must be paired with exactly one Release.
func (b *Bulkhead) Acquire(ctx context.Context) error {
	select {
	case b.slots <- struct{}{}:
		return nil
	default:
	}
	// No free slot: join the wait queue if it has room.
	if b.waiting.Add(1) > b.maxWaiting {
		b.waiting.Add(-1)
		b.rejected.Add(1)
		return ErrFull
	}
	defer b.waiting.Add(-1)
	select {
	case b.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		b.rejected.Add(1)
		return errors.Join(ErrFull, ctx.Err())
	}
}

// Release frees a slot. Calling it without a matching Acquire is a no-op.
func (b *Bulkhead) Release() {
	select {
	case <-b.slots:
	default:
	}
}

// Execute runs fn inside the bulkhead.
func (b *Bulkhead) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	if err := b.Acquire(ctx); err != nil {
		return err
	}
	defer b.Release()
	return fn(ctx)
}

// Stats is a point-in-time snapshot.
type Stats struct {
	InFlight, Waiting, Capacity int
	Rejected                    int64
}

// Stats reports current usage.
func (b *Bulkhead) Stats() Stats {
	return Stats{
		InFlight: len(b.slots),
		Waiting:  int(b.waiting.Load()),
		Capacity: cap(b.slots),
		Rejected: b.rejected.Load(),
	}
}

// Group holds one Bulkhead per named dependency, created on first use.
type Group struct {
	mu       sync.Mutex
	def      Config
	perName  map[string]Config
	bulkhead map[string]*Bulkhead
}

// NewGroup returns a Group that uses overrides[name] when present and def otherwise.
func NewGroup(def Config, overrides map[string]Config) *Group {
	per := make(map[string]Config, len(overrides))
	for k, v := range overrides {
		per[k] = v
	}
	return &Group{def: def, perName: per, bulkhead: map[string]*Bulkhead{}}
}

// Get returns the bulkhead for a dependency.
func (g *Group) Get(name string) *Bulkhead {
	g.mu.Lock()
	defer g.mu.Unlock()
	if b, ok := g.bulkhead[name]; ok {
		return b
	}
	cfg, ok := g.perName[name]
	if !ok {
		cfg = g.def
	}
	b := New(cfg)
	g.bulkhead[name] = b
	return b
}
