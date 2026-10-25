// Package ratelimitertokenbucket implements a token-bucket rate limiter: a
// bucket holds up to Burst tokens, refills at Rate tokens per second, and each
// request spends tokens. It allows short bursts while bounding the long-run rate.
package ratelimitertokenbucket

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrExceedsBurst is returned by Wait when n can never fit in the bucket.
var ErrExceedsBurst = errors.New("tokenbucket: request exceeds burst size")

// Limiter is a token bucket. Safe for concurrent use.
type Limiter struct {
	mu     sync.Mutex
	rate   float64 // tokens per second
	burst  float64
	tokens float64
	last   time.Time
	now    func() time.Time
}

// New returns a full bucket. rate<0 is treated as 0 (no refill), burst<1 as 1.
// now may be nil to use time.Now.
func New(rate float64, burst int, now func() time.Time) *Limiter {
	if rate < 0 {
		rate = 0
	}
	if burst < 1 {
		burst = 1
	}
	if now == nil {
		now = time.Now
	}
	return &Limiter{rate: rate, burst: float64(burst), tokens: float64(burst), last: now(), now: now}
}

// refill adds tokens earned since the last call. mu must be held.
func (l *Limiter) refill(now time.Time) {
	if elapsed := now.Sub(l.last); elapsed > 0 {
		l.tokens = min(l.burst, l.tokens+elapsed.Seconds()*l.rate)
		l.last = now
	}
}

// Allow reports whether one token is available and spends it if so.
func (l *Limiter) Allow() bool { return l.AllowN(1) }

// AllowN spends n tokens if all n are available; otherwise it spends nothing.
func (l *Limiter) AllowN(n int) bool {
	if n <= 0 {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refill(l.now())
	if l.tokens >= float64(n) {
		l.tokens -= float64(n)
		return true
	}
	return false
}

// Tokens returns the current token count (after refill).
func (l *Limiter) Tokens() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refill(l.now())
	return l.tokens
}

// Reserve takes n tokens now, letting the balance go negative, and returns how
// long the caller must wait before acting. The cancel func returns the tokens
// if the caller decides not to wait. ok is false if n can never be satisfied.
func (l *Limiter) Reserve(n int) (delay time.Duration, cancel func(), ok bool) {
	if n <= 0 {
		return 0, func() {}, true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if float64(n) > l.burst || (l.rate == 0 && l.tokens < float64(n)) {
		return 0, func() {}, false
	}
	l.refill(l.now())
	l.tokens -= float64(n)
	if l.tokens < 0 {
		delay = time.Duration(-l.tokens / l.rate * float64(time.Second))
	}
	var once sync.Once
	return delay, func() {
		once.Do(func() {
			l.mu.Lock()
			l.tokens = min(l.burst, l.tokens+float64(n))
			l.mu.Unlock()
		})
	}, true
}

// Wait blocks until n tokens are available or ctx is done. If ctx's deadline
// is earlier than the required wait, it returns immediately without spending.
func (l *Limiter) Wait(ctx context.Context, n int) error {
	delay, cancel, ok := l.Reserve(n)
	if !ok {
		return ErrExceedsBurst
	}
	if delay == 0 {
		return nil
	}
	if dl, has := ctx.Deadline(); has && time.Until(dl) < delay {
		cancel()
		return context.DeadlineExceeded
	}
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		cancel()
		return ctx.Err()
	}
}

// Keyed holds one Limiter per key (client ID, API key, IP), created on demand.
type Keyed struct {
	mu       sync.Mutex
	rate     float64
	burst    int
	now      func() time.Time
	limiters map[string]*keyedEntry
}

type keyedEntry struct {
	l        *Limiter
	lastSeen time.Time
}

// NewKeyed returns a per-key limiter factory.
func NewKeyed(rate float64, burst int, now func() time.Time) *Keyed {
	if now == nil {
		now = time.Now
	}
	return &Keyed{rate: rate, burst: burst, now: now, limiters: map[string]*keyedEntry{}}
}

// Allow spends one token from key's bucket.
func (k *Keyed) Allow(key string) bool {
	k.mu.Lock()
	e, ok := k.limiters[key]
	if !ok {
		e = &keyedEntry{l: New(k.rate, k.burst, k.now)}
		k.limiters[key] = e
	}
	e.lastSeen = k.now()
	k.mu.Unlock()
	return e.l.Allow()
}

// Sweep drops buckets idle for longer than idle, bounding memory. An idle
// bucket has refilled anyway, so dropping it loses nothing once idle is at
// least burst/rate. It returns the number of buckets removed.
func (k *Keyed) Sweep(idle time.Duration) int {
	k.mu.Lock()
	defer k.mu.Unlock()
	cutoff := k.now().Add(-idle)
	n := 0
	for key, e := range k.limiters {
		if e.lastSeen.Before(cutoff) {
			delete(k.limiters, key)
			n++
		}
	}
	return n
}

// Len returns the number of tracked keys.
func (k *Keyed) Len() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.limiters)
}
