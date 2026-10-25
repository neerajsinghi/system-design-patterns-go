// Package hedgedrequests cuts tail latency by sending a backup ("hedge")
// request when the first has not answered within a delay, typically the
// observed p95 latency. The first successful response wins and the others are
// cancelled. Because hedges fire only for the slowest few percent of requests,
// extra load stays small while p99 latency drops sharply.
package hedgedrequests

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

// Config controls hedging.
type Config struct {
	// Delay before each additional attempt is launched. <=0 launches all
	// attempts at once (pure "send to N, take the fastest").
	Delay time.Duration
	// MaxAttempts is the total number of attempts including the first; <=0 means 2.
	MaxAttempts int
	// After returns a channel that fires after d. Defaults to time.After.
	// Inject to control time in tests.
	After func(d time.Duration) <-chan time.Time
}

type result[T any] struct {
	v       T
	err     error
	attempt int
}

// Do calls fn, launching another attempt each time Delay passes without a
// success, or immediately when an attempt fails, up to MaxAttempts. fn
// receives a context that is cancelled as soon as any attempt succeeds (or Do
// returns), plus its attempt number, so implementations can pick a different
// replica per attempt. Do returns the winning value and attempt number, or the
// joined errors of all attempts. All goroutines it starts exit once fn
// returns; none block on sending results.
func Do[T any](ctx context.Context, cfg Config, fn func(ctx context.Context, attempt int) (T, error)) (T, int, error) {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 2
	}
	if cfg.After == nil {
		cfg.After = time.After
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // stops the losers

	results := make(chan result[T], cfg.MaxAttempts) // buffered: senders never block
	launched := 0
	launch := func() {
		n := launched
		launched++
		go func() {
			v, err := fn(ctx, n)
			results <- result[T]{v, err, n}
		}()
	}

	launch()
	var errs []error
	var zero T
	for received := 0; received < cfg.MaxAttempts; {
		var timer <-chan time.Time // nil (blocks forever) once all attempts are out
		if launched < cfg.MaxAttempts {
			if cfg.Delay <= 0 {
				launch()
				continue
			}
			timer = cfg.After(cfg.Delay)
		}
		select {
		case r := <-results:
			received++
			if r.err == nil {
				return r.v, r.attempt, nil
			}
			errs = append(errs, r.err)
			if launched < cfg.MaxAttempts {
				launch() // failed fast: hedge right away instead of waiting
			}
		case <-timer:
			launch()
		case <-ctx.Done():
			return zero, -1, errors.Join(append(errs, ctx.Err())...)
		}
	}
	return zero, -1, errors.Join(errs...)
}

// LatencyTracker keeps the most recent N latencies and reports quantiles, so
// the hedge delay can follow the observed p95 instead of a guessed constant.
// Safe for concurrent use.
type LatencyTracker struct {
	mu   sync.Mutex
	buf  []time.Duration
	next int
	full bool
}

// NewLatencyTracker keeps the last size samples (size<=0 means 1000).
func NewLatencyTracker(size int) *LatencyTracker {
	if size <= 0 {
		size = 1000
	}
	return &LatencyTracker{buf: make([]time.Duration, size)}
}

// Observe records one latency sample.
func (t *LatencyTracker) Observe(d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf[t.next] = d
	t.next = (t.next + 1) % len(t.buf)
	if t.next == 0 {
		t.full = true
	}
}

// Quantile returns the q-quantile (0..1, nearest-rank) of recorded samples,
// or fallback when there are none yet. It sorts a copy, O(n log n); fine for
// a few thousand samples, use a sketch (t-digest, HDR histogram) beyond that.
func (t *LatencyTracker) Quantile(q float64, fallback time.Duration) time.Duration {
	t.mu.Lock()
	n := t.next
	if t.full {
		n = len(t.buf)
	}
	samples := append([]time.Duration(nil), t.buf[:n]...)
	t.mu.Unlock()
	if len(samples) == 0 {
		return fallback
	}
	q = min(max(q, 0), 1)
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	return samples[int(q*float64(len(samples)-1))]
}
