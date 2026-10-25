// Package circuitbreaker implements the classic three-state circuit breaker
// (closed, open, half-open) that stops calling a failing dependency, lets it
// recover, and probes it carefully before resuming normal traffic.
package circuitbreaker

import (
	"errors"
	"sync"
	"time"
)

// State is the breaker state.
type State int

const (
	// Closed lets all calls through and counts consecutive failures.
	Closed State = iota
	// Open rejects all calls until OpenTimeout elapses.
	Open
	// HalfOpen lets a limited number of probe calls through.
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

var (
	// ErrOpen is returned while the breaker is open.
	ErrOpen = errors.New("circuitbreaker: circuit open")
	// ErrTooManyProbes is returned in half-open when all probe slots are taken.
	ErrTooManyProbes = errors.New("circuitbreaker: too many half-open probes")
)

// Settings configures a Breaker. Zero values get sensible defaults.
type Settings struct {
	FailureThreshold int           // consecutive failures that trip the breaker; default 5
	OpenTimeout      time.Duration // how long to stay open before probing; default 30s
	HalfOpenProbes   int           // concurrent probes allowed in half-open; default 1
	SuccessThreshold int           // probe successes needed to close; default 1

	// IsFailure decides whether an error counts against the dependency.
	// Defaults to err != nil. Return false for e.g. 4xx caller errors.
	IsFailure func(error) bool
	// Now is the clock; defaults to time.Now. Inject for tests.
	Now func() time.Time
	// OnStateChange is called (under the breaker lock, keep it fast) on transitions.
	OnStateChange func(from, to State)
}

// Breaker is a circuit breaker. Safe for concurrent use.
type Breaker struct {
	s Settings

	mu         sync.Mutex
	state      State
	generation uint64 // bumps on every transition; stale results are ignored
	failures   int    // consecutive failures while closed
	successes  int    // successful probes while half-open
	inFlight   int    // outstanding probes while half-open
	openedAt   time.Time
}

// New returns a closed Breaker.
func New(s Settings) *Breaker {
	if s.FailureThreshold <= 0 {
		s.FailureThreshold = 5
	}
	if s.OpenTimeout <= 0 {
		s.OpenTimeout = 30 * time.Second
	}
	if s.HalfOpenProbes <= 0 {
		s.HalfOpenProbes = 1
	}
	if s.SuccessThreshold <= 0 {
		s.SuccessThreshold = 1
	}
	if s.IsFailure == nil {
		s.IsFailure = func(err error) bool { return err != nil }
	}
	if s.Now == nil {
		s.Now = time.Now
	}
	return &Breaker{s: s}
}

// State returns the current state, applying the open->half-open timeout.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.currentState()
}

// Execute runs fn if the breaker allows it and records the outcome.
func (b *Breaker) Execute(fn func() error) error {
	done, err := b.Allow()
	if err != nil {
		return err
	}
	err = fn()
	done(err)
	return err
}

// Allow is the two-phase form of Execute: it reserves permission to call and
// returns a done callback that must be called exactly once with the call's
// result. Extra calls to done are ignored.
func (b *Breaker) Allow() (done func(error), err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.currentState() {
	case Open:
		return nil, ErrOpen
	case HalfOpen:
		if b.inFlight >= b.s.HalfOpenProbes {
			return nil, ErrTooManyProbes
		}
		b.inFlight++
	}
	gen := b.generation
	var once sync.Once
	return func(err error) {
		once.Do(func() { b.record(gen, err) })
	}, nil
}

func (b *Breaker) record(gen uint64, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	state := b.currentState()
	if gen != b.generation {
		return // the result belongs to a previous state; ignore it
	}
	failed := b.s.IsFailure(err)
	switch state {
	case Closed:
		if !failed {
			b.failures = 0
			return
		}
		b.failures++
		if b.failures >= b.s.FailureThreshold {
			b.setState(Open)
		}
	case HalfOpen:
		b.inFlight--
		if failed {
			b.setState(Open)
			return
		}
		b.successes++
		if b.successes >= b.s.SuccessThreshold {
			b.setState(Closed)
		}
	}
}

// currentState must be called with mu held.
func (b *Breaker) currentState() State {
	if b.state == Open && !b.s.Now().Before(b.openedAt.Add(b.s.OpenTimeout)) {
		b.setState(HalfOpen)
	}
	return b.state
}

// setState must be called with mu held.
func (b *Breaker) setState(to State) {
	from := b.state
	b.state = to
	b.generation++
	b.failures, b.successes, b.inFlight = 0, 0, 0
	if to == Open {
		b.openedAt = b.s.Now()
	}
	if b.s.OnStateChange != nil && from != to {
		b.s.OnStateChange(from, to)
	}
}
