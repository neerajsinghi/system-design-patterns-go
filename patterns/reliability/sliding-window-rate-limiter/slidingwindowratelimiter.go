// Package slidingwindowratelimiter enforces "at most Limit requests in any
// Window" quotas. It provides two classic variants:
//
//   - Log: stores a timestamp per accepted request. Exact, O(Limit) memory.
//   - Counter: keeps two fixed-window counters and weights the previous one by
//     how much of it still overlaps the sliding window. Approximate, O(1)
//     memory, and the usual choice at scale.
//
// Both avoid the fixed-window flaw where a client can send 2x Limit requests
// across a window boundary.
package slidingwindowratelimiter

import (
	"sync"
	"time"
)

func defaults(limit int, window time.Duration, now func() time.Time) (int, time.Duration, func() time.Time) {
	if limit < 0 {
		limit = 0
	}
	if window <= 0 {
		window = time.Second
	}
	if now == nil {
		now = time.Now
	}
	return limit, window, now
}

// Log is an exact sliding-window limiter. Safe for concurrent use.
type Log struct {
	mu     sync.Mutex
	limit  int
	window time.Duration
	now    func() time.Time
	times  []time.Time // accepted request times, oldest first
}

// NewLog returns an exact limiter. limit<=0 rejects everything; window<=0
// defaults to 1s; now may be nil to use time.Now.
func NewLog(limit int, window time.Duration, now func() time.Time) *Log {
	limit, window, now = defaults(limit, window, now)
	return &Log{limit: limit, window: window, now: now, times: make([]time.Time, 0, limit)}
}

// evict drops timestamps outside (now-window, now]. mu must be held.
func (l *Log) evict(now time.Time) {
	cutoff := now.Add(-l.window)
	i := 0
	for i < len(l.times) && !l.times[i].After(cutoff) {
		i++
	}
	l.times = l.times[i:]
}

// Allow records and accepts the request if fewer than Limit were accepted in
// the trailing window.
func (l *Log) Allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	l.evict(now)
	if len(l.times) >= l.limit {
		return false
	}
	l.times = append(l.times, now)
	return true
}

// RetryAfter reports how long until the next request would be accepted
// (0 if it would be accepted now). Useful for a Retry-After header.
func (l *Log) RetryAfter() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	l.evict(now)
	if len(l.times) < l.limit {
		return 0
	}
	if l.limit == 0 {
		return l.window
	}
	// The oldest entry that must expire to make room.
	oldest := l.times[len(l.times)-l.limit]
	return oldest.Add(l.window).Sub(now)
}

// Counter is an approximate sliding-window limiter using the weighted sum of
// the current and previous fixed windows. Safe for concurrent use.
type Counter struct {
	mu        sync.Mutex
	limit     int
	window    time.Duration
	now       func() time.Time
	currStart time.Time
	curr      int
	prev      int
}

// NewCounter returns an approximate limiter with the same parameters as NewLog.
func NewCounter(limit int, window time.Duration, now func() time.Time) *Counter {
	limit, window, now = defaults(limit, window, now)
	return &Counter{limit: limit, window: window, now: now, currStart: now().Truncate(window)}
}

// roll advances the fixed windows to contain now. mu must be held.
func (c *Counter) roll(now time.Time) {
	start := now.Truncate(c.window)
	switch {
	case !start.After(c.currStart):
		return // same window (or clock went backwards: keep counting)
	case start.Sub(c.currStart) == c.window:
		c.prev, c.curr = c.curr, 0
	default:
		c.prev, c.curr = 0, 0 // more than one window of silence
	}
	c.currStart = start
}

// estimate returns the weighted request count for the sliding window ending
// at now. mu must be held.
func (c *Counter) estimate(now time.Time) float64 {
	elapsed := now.Sub(c.currStart)
	weight := 1 - float64(elapsed)/float64(c.window) // share of prev window still in view
	if weight < 0 {
		weight = 0
	}
	return float64(c.prev)*weight + float64(c.curr)
}

// Allow accepts the request if the estimated count in the trailing window is
// below Limit.
func (c *Counter) Allow() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	c.roll(now)
	if c.estimate(now)+1 > float64(c.limit) {
		return false
	}
	c.curr++
	return true
}

// Estimate reports the current weighted count, for metrics and tests.
func (c *Counter) Estimate() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	c.roll(now)
	return c.estimate(now)
}
