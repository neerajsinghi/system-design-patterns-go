// Package retrybudget limits retries to a fraction of recent traffic, so that
// retrying cannot multiply the load on a dependency that is already failing.
//
// A per-request retry limit ("up to 3 attempts") looks harmless, but it caps
// each request, not the client. When a dependency fails 90% of its calls,
// every request uses all of its attempts and the dependency receives roughly
// 2.7 times its normal traffic exactly when it is least able to handle it: a
// retry storm. Retries at several layers multiply: three layers that each
// make 3 attempts send up to 27 calls to the bottom layer per user request.
//
// A retry budget is shared by every request a client sends to one
// destination. It is a token account in the style of Finagle's RetryBudget:
// each first attempt deposits a fraction of a token (the ratio, e.g. 0.1)
// and each retry withdraws one whole token. Retries therefore stay at or
// below ratio × requests however badly the dependency fails, while sporadic
// failures in healthy periods are still retried, because the account has
// plenty of tokens. Deposits are capped so that a long healthy period cannot
// bank an unlimited burst of retries, and a small time-based floor (minimum
// retries per second) keeps low-traffic clients able to retry at all.
//
// The clock is injectable so the floor can be tested and simulated without
// sleeping.
package retrybudget

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"
)

// ErrBudgetExhausted is returned by Do when a retry was needed but the budget
// refused it. It is joined with the last attempt's error.
var ErrBudgetExhausted = errors.New("retrybudget: retry budget exhausted")

// epsilon absorbs floating-point drift, so that ten deposits of 0.1 add up
// to one whole token.
const epsilon = 1e-9

// Stats counts what the budget has seen.
type Stats struct {
	Requests uint64 // first attempts recorded with OnRequest
	Retries  uint64 // retries allowed by Allow
	Denied   uint64 // retries refused by Allow
}

// Budget is a token-based retry budget shared by all requests from one client
// to one destination. It is safe for concurrent use.
type Budget struct {
	ratio      float64
	maxBalance float64
	minPerSec  float64
	floorCap   float64
	now        func() time.Time

	mu         sync.Mutex
	balance    float64   // tokens deposited by requests, capped at maxBalance
	floor      float64   // tokens from the minimum-retries-per-second floor
	lastRefill time.Time // when floor was last topped up
	stats      Stats
}

// Option configures a Budget.
type Option func(*options)

type options struct {
	maxBalance float64
	minPerSec  float64
	now        func() time.Time
}

// WithMaxBalance caps how many deposited tokens can be banked, which bounds
// the burst of retries allowed right after a long healthy period. The
// default is 10. It must be at least 1, or no retry could ever be paid for
// from deposits.
func WithMaxBalance(n float64) Option { return func(o *options) { o.maxBalance = n } }

// WithMinRetriesPerSecond sets a floor: n retries per second are allowed
// regardless of traffic, so that a client sending one request a minute can
// still retry it. The floor accrues up to one second's worth (at least one
// token). The default is 0, no floor.
func WithMinRetriesPerSecond(n float64) Option { return func(o *options) { o.minPerSec = n } }

// WithClock injects a clock, used only for the minimum-retries floor.
func WithClock(now func() time.Time) Option {
	return func(o *options) {
		if now != nil {
			o.now = now
		}
	}
}

// New returns a budget that allows retries up to ratio × first attempts (0.1
// means at most one retry per ten requests over time). ratio must be in
// [0, 10]; a ratio of 0 is valid only together with a minimum-retries floor.
func New(ratio float64, opts ...Option) (*Budget, error) {
	if math.IsNaN(ratio) || ratio < 0 || ratio > 10 {
		return nil, errors.New("retrybudget: ratio must be between 0 and 10")
	}
	o := options{maxBalance: 10, now: time.Now}
	for _, opt := range opts {
		opt(&o)
	}
	if math.IsNaN(o.maxBalance) || math.IsInf(o.maxBalance, 0) || o.maxBalance < 1 {
		return nil, errors.New("retrybudget: max balance must be a finite number >= 1")
	}
	if math.IsNaN(o.minPerSec) || math.IsInf(o.minPerSec, 0) || o.minPerSec < 0 {
		return nil, errors.New("retrybudget: min retries per second must be a finite number >= 0")
	}
	if ratio == 0 && o.minPerSec == 0 {
		return nil, errors.New("retrybudget: ratio and min retries per second are both 0, so no retry would ever be allowed")
	}
	floorCap := 0.0
	if o.minPerSec > 0 {
		floorCap = max(1, o.minPerSec)
	}
	return &Budget{
		ratio: ratio, maxBalance: o.maxBalance, minPerSec: o.minPerSec,
		floorCap: floorCap, now: o.now,
		floor: floorCap, lastRefill: o.now(),
	}, nil
}

// OnRequest records a first attempt and deposits ratio tokens. Call it once
// per logical request, not per retry: retries must not earn more retries.
func (b *Budget) OnRequest() {
	b.mu.Lock()
	b.stats.Requests++
	b.balance = min(b.maxBalance, b.balance+b.ratio)
	b.mu.Unlock()
}

// Allow reports whether one retry may be sent now and, if so, withdraws a
// token for it. The floor is spent first, because it refills with time
// whether it is used or not; deposited tokens are kept for bursts.
func (b *Budget) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillFloor()
	switch {
	case b.floor >= 1-epsilon:
		b.floor = max(0, b.floor-1)
	case b.balance >= 1-epsilon:
		b.balance = max(0, b.balance-1)
	default:
		b.stats.Denied++
		return false
	}
	b.stats.Retries++
	return true
}

// refillFloor tops up the time-based floor. Callers hold b.mu.
func (b *Budget) refillFloor() {
	if b.minPerSec == 0 {
		return
	}
	now := b.now()
	if elapsed := now.Sub(b.lastRefill); elapsed > 0 {
		b.floor = min(b.floorCap, b.floor+elapsed.Seconds()*b.minPerSec)
	}
	if now.After(b.lastRefill) {
		b.lastRefill = now // a clock that goes backwards never refills
	}
}

// Balance reports the tokens available for retries right now (deposits plus
// floor), without withdrawing any.
func (b *Budget) Balance() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillFloor()
	return b.balance + b.floor
}

// Stats returns a snapshot of the counters.
func (b *Budget) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stats
}

// Do calls fn up to maxAttempts times. The first call is recorded with
// OnRequest; each further call must be allowed by the budget. It stops on
// success, on a context error (from ctx or returned by fn), when attempts
// run out, or when the budget refuses a retry, in which case the result
// wraps both ErrBudgetExhausted and fn's last error.
//
// Do does not wait between attempts. In production, combine it with
// backoff and jitter (see ../retry-backoff) and with a per-attempt timeout.
func (b *Budget) Do(ctx context.Context, maxAttempts int, fn func(ctx context.Context) error) error {
	if maxAttempts < 1 {
		return errors.New("retrybudget: maxAttempts must be at least 1")
	}
	if fn == nil {
		return errors.New("retrybudget: fn must not be nil")
	}
	b.OnRequest()
	var err error
	for attempt := range maxAttempts {
		if cerr := ctx.Err(); cerr != nil {
			return errors.Join(cerr, err)
		}
		if attempt > 0 && !b.Allow() {
			return errors.Join(ErrBudgetExhausted, err)
		}
		if err = fn(ctx); err == nil {
			return nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
	}
	return err
}
