// Package timeout bounds how long work may take and propagates a single
// end-to-end deadline across hops, giving each downstream call a slice of the
// remaining budget instead of a fixed, independent timeout.
package timeout

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// ErrInsufficientBudget means too little time remains for a downstream call to
// be worth starting. Failing fast here saves the dependency from doing work
// whose result would arrive after the caller has given up.
var ErrInsufficientBudget = errors.New("timeout: insufficient deadline budget for hop")

// HeaderName is the header used to propagate the remaining budget, in
// milliseconds, across process boundaries (gRPC does the same with grpc-timeout).
const HeaderName = "X-Request-Timeout-Ms"

// HopPolicy decides how much of the remaining budget one downstream call gets.
type HopPolicy struct {
	// Default is used when the incoming context has no deadline. <=0 means 1s.
	Default time.Duration
	// Max caps the hop timeout even if more budget remains. 0 means no cap.
	Max time.Duration
	// Reserve is kept back for local work after the hop returns (e.g. rendering
	// the response). It is subtracted from the remaining budget.
	Reserve time.Duration
	// Min is the smallest hop timeout worth attempting; below it the hop fails
	// fast with ErrInsufficientBudget. <=0 means 1ms.
	Min time.Duration
}

// Budget computes the hop timeout given the caller's deadline (if any) and the
// current time. It is a pure function, which keeps it trivially testable.
func (p HopPolicy) Budget(deadline time.Time, hasDeadline bool, now time.Time) (time.Duration, error) {
	minHop := p.Min
	if minHop <= 0 {
		minHop = time.Millisecond
	}
	var d time.Duration
	if hasDeadline {
		d = deadline.Sub(now) - p.Reserve
	} else {
		d = p.Default
		if d <= 0 {
			d = time.Second
		}
	}
	if p.Max > 0 && d > p.Max {
		d = p.Max
	}
	if d < minHop {
		return 0, fmt.Errorf("%w: %v left, need %v", ErrInsufficientBudget, max(d, 0), minHop)
	}
	return d, nil
}

// WithHop derives a context for one downstream call according to p.
// The returned cancel func must always be called; it is non-nil even on error.
func WithHop(ctx context.Context, p HopPolicy) (context.Context, context.CancelFunc, error) {
	deadline, ok := ctx.Deadline()
	d, err := p.Budget(deadline, ok, time.Now())
	if err != nil {
		return ctx, func() {}, err
	}
	hctx, cancel := context.WithTimeout(ctx, d)
	return hctx, cancel, nil
}

// Run calls fn and returns when fn returns or ctx is done, whichever comes
// first. It protects the caller from functions that ignore ctx. If ctx wins,
// fn keeps running in its goroutine until it returns on its own (its result is
// discarded into a buffered channel, so the goroutine does exit), which is why
// fn should still honour ctx whenever it can.
func Run[T any](ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	type result struct {
		v   T
		err error
	}
	if err := ctx.Err(); err != nil {
		var zero T
		return zero, err
	}
	ch := make(chan result, 1)
	go func() {
		v, err := fn(ctx)
		ch <- result{v, err}
	}()
	select {
	case r := <-ch:
		return r.v, r.err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

// FormatRemaining encodes the time left before ctx's deadline as a header
// value (whole milliseconds). ok is false when ctx has no deadline.
func FormatRemaining(ctx context.Context, now time.Time) (value string, ok bool) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return "", false
	}
	ms := deadline.Sub(now).Milliseconds()
	return strconv.FormatInt(max(ms, 0), 10), true
}

// FromHeader returns a child of parent whose deadline is the remaining budget
// in value. Empty, malformed or negative values leave parent unchanged; the
// parent's own deadline still applies if it is earlier.
func FromHeader(parent context.Context, value string) (context.Context, context.CancelFunc) {
	ms, err := strconv.ParseInt(value, 10, 64)
	if err != nil || ms < 0 || ms > int64(24*time.Hour/time.Millisecond) {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, time.Duration(ms)*time.Millisecond)
}
