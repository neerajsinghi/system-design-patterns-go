package fallback

// This file adds the second shape of fallback: an ordered failover chain of
// interchangeable providers (primary SMS -> secondary SMS -> voice call), as
// opposed to the primary -> stale -> default tiers in fallback.go.

import (
	"context"
	"errors"
	"fmt"
	"time"

	circuitbreaker "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/circuit-breaker"
)

var (
	// ErrDeadline is joined into the error when the chain's overall deadline
	// ran out before a provider could serve. It wraps context.DeadlineExceeded.
	ErrDeadline = fmt.Errorf("fallback: chain deadline exceeded: %w", context.DeadlineExceeded)
	// ErrNoProvider is joined into the error when every provider was skipped
	// because it was cooling down.
	ErrNoProvider = errors.New("fallback: no healthy provider")
)

// permanent marks an error that must not trigger failover.
type permanent struct{ err error }

func (p permanent) Error() string { return p.err.Error() }
func (p permanent) Unwrap() error { return p.err }

// Permanent marks err as non-retryable: the chain stops at the provider that
// returned it instead of trying the next one. Use it for errors that every
// provider would return too (invalid phone number, card declined), where
// failing over only costs money and time. errors.Is/As still see err.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanent{err}
}

// IsPermanent reports whether err (or anything it wraps) was marked Permanent.
func IsPermanent(err error) bool {
	var p permanent
	return errors.As(err, &p)
}

// Provider is one named, interchangeable way of serving a request.
type Provider[Req, Resp any] struct {
	Name string
	// Call serves req. It must honour ctx, which carries this attempt's timeout.
	Call func(ctx context.Context, req Req) (Resp, error)
	// Timeout bounds one attempt on this provider; 0 means only the chain's
	// overall deadline (if any) applies.
	Timeout time.Duration
}

// ChainOptions configures a Chain. All fields are optional.
type ChainOptions struct {
	// Deadline bounds the whole Do call across all attempts; 0 means only
	// the caller's context. Each attempt's timeout is clipped to what is left.
	Deadline time.Duration
	// FailureThreshold is how many consecutive retryable failures put a
	// provider into cooldown. 0 means 3; negative disables health tracking
	// (every provider is tried on every request).
	FailureThreshold int
	// Cooldown is how long an unhealthy provider is skipped before one probe
	// request is let through again. <=0 means 30s.
	Cooldown time.Duration
	// Retryable decides whether an error moves on to the next provider.
	// Defaults to: not Permanent and not context.Canceled.
	Retryable func(error) bool
	// Now is the clock (for cooldowns and the deadline check); defaults to
	// time.Now. Inject a fake clock for deterministic tests and simulations.
	Now func() time.Time
}

// Attempt records what happened at one provider during a Do call.
type Attempt struct {
	Provider string
	Skipped  bool          // provider was cooling down and was not called
	Err      error         // nil if this provider served
	Took     time.Duration // measured with ChainOptions.Now
}

// ChainResult says which provider served and what happened before it.
type ChainResult[Resp any] struct {
	Value    Resp
	Provider string    // name of the provider that served; "" on failure
	Attempts []Attempt // in chain order, including skipped providers
	// Errors is errors.Join of every failed attempt (each prefixed with the
	// provider name); nil if the first provider tried served.
	Errors error
}

type link[Req, Resp any] struct {
	p  Provider[Req, Resp]
	cb *circuitbreaker.Breaker // nil when health tracking is disabled
}

// Chain tries providers in order until one serves. Each provider sits behind
// its own circuit breaker, so one that keeps failing is skipped for Cooldown
// instead of costing every request its full timeout. Safe for concurrent use.
type Chain[Req, Resp any] struct {
	links []link[Req, Resp]
	opt   ChainOptions
}

// NewChain builds a chain from providers, tried in the order given.
func NewChain[Req, Resp any](providers []Provider[Req, Resp], opt ChainOptions) (*Chain[Req, Resp], error) {
	if len(providers) == 0 {
		return nil, errors.New("fallback: chain needs at least one provider")
	}
	if opt.Deadline < 0 {
		return nil, errors.New("fallback: negative chain deadline")
	}
	if opt.FailureThreshold == 0 {
		opt.FailureThreshold = 3
	}
	if opt.Cooldown <= 0 {
		opt.Cooldown = 30 * time.Second
	}
	if opt.Retryable == nil {
		opt.Retryable = func(err error) bool {
			return !IsPermanent(err) && !errors.Is(err, context.Canceled)
		}
	}
	if opt.Now == nil {
		opt.Now = time.Now
	}
	seen := map[string]bool{}
	links := make([]link[Req, Resp], len(providers))
	for i, p := range providers {
		switch {
		case p.Name == "":
			return nil, fmt.Errorf("fallback: provider %d has no name", i)
		case seen[p.Name]:
			return nil, fmt.Errorf("fallback: duplicate provider name %q", p.Name)
		case p.Call == nil:
			return nil, fmt.Errorf("fallback: provider %q has no Call", p.Name)
		case p.Timeout < 0:
			return nil, fmt.Errorf("fallback: provider %q has a negative timeout", p.Name)
		}
		seen[p.Name] = true
		links[i].p = p
		if opt.FailureThreshold > 0 {
			retryable := opt.Retryable
			links[i].cb = circuitbreaker.New(circuitbreaker.Settings{
				FailureThreshold: opt.FailureThreshold,
				OpenTimeout:      opt.Cooldown,
				Now:              opt.Now,
				// Only provider-side failures count against health. A
				// non-retryable answer ("invalid number") proves it is up.
				IsFailure: func(err error) bool { return err != nil && retryable(err) },
			})
		}
	}
	return &Chain[Req, Resp]{links: links, opt: opt}, nil
}

// Healthy reports whether the named provider would be tried right now
// (its breaker is not open). Unknown names report false.
func (c *Chain[Req, Resp]) Healthy(name string) bool {
	for _, l := range c.links {
		if l.p.Name == name {
			return l.cb == nil || l.cb.State() != circuitbreaker.Open
		}
	}
	return false
}

// Do tries each provider in order. It stops at the first success, at the
// first non-retryable error, when ctx is done, or when the overall deadline
// runs out. Providers that are cooling down are skipped. The returned error
// is nil exactly when a provider served; otherwise it is errors.Join of every
// failure (plus ErrDeadline or ErrNoProvider when they apply), and the result
// still lists the attempts.
func (c *Chain[Req, Resp]) Do(ctx context.Context, req Req) (ChainResult[Resp], error) {
	var res ChainResult[Resp]
	var errs []error
	start := c.opt.Now()
	if c.opt.Deadline > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.opt.Deadline)
		defer cancel()
	}
	called := 0
	for _, l := range c.links {
		if err := ctx.Err(); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				err = ErrDeadline
			}
			errs = append(errs, err)
			break
		}
		timeout := l.p.Timeout
		if c.opt.Deadline > 0 {
			// Checked on the injected clock too, so simulated time works.
			left := c.opt.Deadline - c.opt.Now().Sub(start)
			if left <= 0 {
				errs = append(errs, ErrDeadline)
				break
			}
			if timeout == 0 || left < timeout {
				timeout = left
			}
		}
		var done func(error)
		if l.cb != nil {
			var err error
			if done, err = l.cb.Allow(); err != nil {
				res.Attempts = append(res.Attempts, Attempt{Provider: l.p.Name, Skipped: true})
				continue
			}
		}
		called++
		actx, cancel := ctx, context.CancelFunc(func() {})
		if timeout > 0 {
			actx, cancel = context.WithTimeout(ctx, timeout)
		}
		t0 := c.opt.Now()
		v, err := l.p.Call(actx, req)
		cancel()
		took := c.opt.Now().Sub(t0)
		res.Attempts = append(res.Attempts, Attempt{Provider: l.p.Name, Err: err, Took: took})

		if err == nil {
			if done != nil {
				done(nil)
			}
			res.Value, res.Provider = v, l.p.Name
			res.Errors = errors.Join(errs...)
			return res, nil
		}
		errs = append(errs, fmt.Errorf("%s: %w", l.p.Name, err))
		if cerr := ctx.Err(); cerr != nil {
			// The caller left or the overall deadline cut this attempt short:
			// not the provider's fault, so do not count it as a failure.
			if done != nil {
				done(nil)
			}
			if errors.Is(cerr, context.DeadlineExceeded) {
				errs = append(errs, ErrDeadline)
			}
			break
		}
		if done != nil {
			done(err)
		}
		if !c.opt.Retryable(err) {
			break
		}
	}
	if called == 0 && len(errs) == 0 {
		errs = append(errs, ErrNoProvider)
	}
	res.Errors = errors.Join(errs...)
	return res, res.Errors
}
