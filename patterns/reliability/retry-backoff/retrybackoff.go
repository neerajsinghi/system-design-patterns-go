// Package retrybackoff retries a failing operation with capped exponential
// backoff, optional jitter, and an optional retry budget that stops retry
// storms when a dependency is broadly unhealthy.
package retrybackoff

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"
)

// Jitter selects how randomness is applied to the computed backoff.
type Jitter int

const (
	// NoJitter sleeps exactly min(MaxDelay, BaseDelay*2^attempt).
	NoJitter Jitter = iota
	// FullJitter sleeps a uniform random duration in [0, backoff).
	FullJitter
	// EqualJitter sleeps backoff/2 plus a uniform random duration in [0, backoff/2).
	EqualJitter
)

// ErrBudgetExhausted is returned when the retry budget refuses a retry.
// The last operation error is joined with it so callers can inspect both.
var ErrBudgetExhausted = errors.New("retrybackoff: retry budget exhausted")

// permanentError marks an error that must not be retried.
type permanentError struct{ err error }

func (p *permanentError) Error() string { return p.err.Error() }
func (p *permanentError) Unwrap() error { return p.err }

// Permanent wraps err so that Do returns it immediately without retrying.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// Policy configures retries. The zero value is usable: 3 attempts, 100ms base,
// 10s cap, full jitter, real sleeps.
type Policy struct {
	MaxAttempts int           // total attempts including the first; <=0 means 3
	BaseDelay   time.Duration // delay before the first retry; <=0 means 100ms
	MaxDelay    time.Duration // cap on any single delay; <=0 means 10s
	Jitter      Jitter

	// Rand returns a float in [0,1). Defaults to math/rand. Inject for tests.
	Rand func() float64
	// Sleep waits for d or until ctx is done. Defaults to a timer. Inject for tests.
	Sleep func(ctx context.Context, d time.Duration) error
	// Retryable reports whether err should be retried. Defaults to "everything
	// except Permanent errors and context cancellation".
	Retryable func(error) bool
	// Budget, if set, is shared across calls and limits the retry rate.
	Budget *Budget
}

func (p Policy) withDefaults() Policy {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 3
	}
	if p.BaseDelay <= 0 {
		p.BaseDelay = 100 * time.Millisecond
	}
	if p.MaxDelay <= 0 {
		p.MaxDelay = 10 * time.Second
	}
	if p.MaxDelay < p.BaseDelay {
		p.MaxDelay = p.BaseDelay
	}
	if p.Rand == nil {
		p.Rand = rand.Float64
	}
	if p.Sleep == nil {
		p.Sleep = sleepCtx
	}
	return p
}

// Backoff returns the delay to wait before retry number retry (0-based: the
// delay after the first failed attempt is Backoff(0)).
func (p Policy) Backoff(retry int) time.Duration {
	p = p.withDefaults()
	if retry < 0 {
		retry = 0
	}
	d := p.BaseDelay
	for i := 0; i < retry && d < p.MaxDelay; i++ {
		d *= 2 // doubling until the cap avoids overflow of BaseDelay<<retry
	}
	if d > p.MaxDelay {
		d = p.MaxDelay
	}
	switch p.Jitter {
	case FullJitter:
		return time.Duration(p.Rand() * float64(d))
	case EqualJitter:
		half := d / 2
		return half + time.Duration(p.Rand()*float64(d-half))
	default:
		return d
	}
}

// Do calls fn until it succeeds, returns a non-retryable error, attempts run
// out, the budget refuses, or ctx is done. It returns the last error.
func (p Policy) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	p = p.withDefaults()
	var err error
	for attempt := 0; attempt < p.MaxAttempts; attempt++ {
		if cerr := ctx.Err(); cerr != nil {
			return errors.Join(cerr, err)
		}
		err = fn(ctx)
		if err == nil {
			p.Budget.recordSuccess()
			return nil
		}
		p.Budget.recordFailure()
		if !p.retryable(err) || attempt == p.MaxAttempts-1 {
			break
		}
		if !p.Budget.allowRetry() {
			return errors.Join(ErrBudgetExhausted, err)
		}
		if serr := p.Sleep(ctx, p.Backoff(attempt)); serr != nil {
			return errors.Join(serr, err)
		}
	}
	var perm *permanentError
	if errors.As(err, &perm) {
		return perm.err
	}
	return err
}

func (p Policy) retryable(err error) bool {
	var perm *permanentError
	if errors.As(err, &perm) {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if p.Retryable != nil {
		return p.Retryable(err)
	}
	return true
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
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

// Budget is a token-bucket retry throttle in the style of gRPC retry
// throttling. Every failure costs one token and every success refunds Ratio
// tokens; retries are only allowed while tokens > Max/2. When a dependency is
// failing for everyone, the bucket drains and clients stop amplifying load.
// A nil *Budget allows every retry. Safe for concurrent use.
type Budget struct {
	mu     sync.Mutex
	max    float64
	ratio  float64
	tokens float64
}

// NewBudget creates a full budget. max<=0 defaults to 10, ratio<=0 to 0.1.
func NewBudget(max, ratio float64) *Budget {
	if max <= 0 {
		max = 10
	}
	if ratio <= 0 {
		ratio = 0.1
	}
	return &Budget{max: max, ratio: ratio, tokens: max}
}

// Tokens reports the current token count.
func (b *Budget) Tokens() float64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.tokens
}

func (b *Budget) recordSuccess() {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.tokens = min(b.max, b.tokens+b.ratio)
	b.mu.Unlock()
}

func (b *Budget) recordFailure() {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.tokens = max(0, b.tokens-1)
	b.mu.Unlock()
}

func (b *Budget) allowRetry() bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.tokens > b.max/2
}
