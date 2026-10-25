// Package ratelimiterleakybucket implements the leaky bucket "as a queue":
// requests enter a bounded queue and leave it at a constant rate, so the
// output is perfectly smooth no matter how bursty the input is. When the queue
// is full, new requests overflow and are rejected.
//
// The queue is virtual: instead of a goroutine draining a channel, each
// admitted request is assigned the time at which it may proceed. That makes
// the limiter goroutine-free, deterministic under a fake clock, and cheap.
package ratelimiterleakybucket

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrOverflow is returned when the bucket's queue is full.
var ErrOverflow = errors.New("leakybucket: queue full")

// Limiter is a leaky bucket. Safe for concurrent use.
type Limiter struct {
	mu       sync.Mutex
	interval time.Duration // time between releases (1/rate)
	capacity int           // max requests allowed to wait
	last     time.Time     // release time of the most recently admitted request
	started  bool
	now      func() time.Time
}

// New returns a limiter releasing rate requests per second with up to
// capacity requests waiting. rate must be > 0; capacity < 0 is treated as 0
// (pure pacing: a request is admitted only if it can go right away).
// now may be nil to use time.Now.
func New(rate float64, capacity int, now func() time.Time) (*Limiter, error) {
	if !(rate > 0) { // also rejects NaN
		return nil, errors.New("leakybucket: rate must be > 0")
	}
	interval := time.Duration(float64(time.Second) / rate)
	if interval <= 0 {
		interval = 1
	}
	if capacity < 0 {
		capacity = 0
	}
	if now == nil {
		now = time.Now
	}
	return &Limiter{interval: interval, capacity: capacity, now: now}, nil
}

// queued returns how many admitted requests are still waiting at time now.
// mu must be held.
func (l *Limiter) queued(now time.Time) int {
	if !l.started || !l.last.After(now) {
		return 0
	}
	ahead := l.last.Sub(now)
	return int((ahead + l.interval - 1) / l.interval)
}

// Reserve admits a request into the queue and returns how long it must wait
// before proceeding, or ErrOverflow if the queue is full. A reservation is
// final: if the caller abandons it, its slot still leaks out on schedule,
// which errs on the side of protecting the downstream.
func (l *Limiter) Reserve() (time.Duration, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	next := now
	if l.started {
		if candidate := l.last.Add(l.interval); candidate.After(now) {
			next = candidate
		}
	}
	if next.After(now) && l.queued(now) >= l.capacity {
		return 0, ErrOverflow
	}
	l.last, l.started = next, true
	return next.Sub(now), nil
}

// Allow admits a request only if it may proceed immediately.
func (l *Limiter) Allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if l.started && l.last.Add(l.interval).After(now) {
		return false
	}
	l.last, l.started = now, true
	return true
}

// Wait reserves a slot and blocks until it is released or ctx is done.
// If ctx's deadline falls before the slot, it fails without blocking (the
// slot is still consumed; see Reserve).
func (l *Limiter) Wait(ctx context.Context) error {
	d, err := l.Reserve()
	if err != nil {
		return err
	}
	if d <= 0 {
		return ctx.Err()
	}
	if dl, ok := ctx.Deadline(); ok && time.Until(dl) < d {
		return context.DeadlineExceeded
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Queued reports how many admitted requests are currently waiting.
func (l *Limiter) Queued() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.queued(l.now())
}

// Interval returns the fixed spacing between releases.
func (l *Limiter) Interval() time.Duration { return l.interval }
