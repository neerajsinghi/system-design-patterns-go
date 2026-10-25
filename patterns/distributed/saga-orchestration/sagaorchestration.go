// Package sagaorchestration implements an orchestrated saga: a central
// coordinator runs a sequence of local transactions in different services
// and, if one fails, runs the compensating actions of the steps that already
// succeeded, in reverse order.
//
// A saga gives up isolation (other requests can see intermediate states)
// in exchange for not needing a distributed lock or two-phase commit. Each
// compensation is a semantic undo (refund, cancel booking), not a rollback.
package sagaorchestration

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Step is one local transaction and its compensation. Compensate may be nil
// for steps with nothing to undo (e.g. a read or a final notification).
// Both functions should be idempotent: after a crash the orchestrator may
// call them again.
type Step struct {
	Name       string
	Action     func(ctx context.Context) error
	Compensate func(ctx context.Context) error
}

// Status is the final state of a saga run.
type Status int

const (
	// Completed means every step succeeded.
	Completed Status = iota
	// Compensated means a step failed and all prior steps were undone.
	Compensated
	// CompensationFailed means a compensation kept failing; the system is
	// inconsistent and needs a human or a later retry.
	CompensationFailed
	// Invalid means the saga definition was rejected and nothing ran.
	Invalid
)

// String returns the status name.
func (s Status) String() string {
	switch s {
	case Completed:
		return "completed"
	case Compensated:
		return "compensated"
	case CompensationFailed:
		return "compensation-failed"
	case Invalid:
		return "invalid"
	default:
		return "unknown"
	}
}

// Event is one entry in the saga log. A production orchestrator persists
// this log durably so it can resume after a crash.
type Event struct {
	Step string
	Kind string // "action-ok", "action-failed", "compensate-ok", "compensate-failed"
	Err  error
}

// Result describes a finished run.
type Result struct {
	Status     Status
	FailedStep string // the action that failed, if any
	Err        error  // the action's error, joined with any compensation error
	Log        []Event
}

// Orchestrator runs sagas.
type Orchestrator struct {
	// CompensationAttempts is how many times each compensation is tried
	// before giving up (default 3).
	CompensationAttempts int
	// RetryDelay is the pause between compensation attempts (default 0).
	RetryDelay time.Duration
}

// ErrInvalidSaga is returned for sagas with no steps or a step with no action.
var ErrInvalidSaga = errors.New("sagaorchestration: invalid saga definition")

// Run executes steps in order. If an action fails (or ctx is cancelled
// between steps) it compensates the completed steps in reverse order.
// Compensations run with a context detached from ctx's cancellation, because
// undoing work must not be skipped just because the caller gave up.
func (o *Orchestrator) Run(ctx context.Context, steps []Step) Result {
	if len(steps) == 0 {
		return Result{Status: Invalid, Err: ErrInvalidSaga}
	}
	for _, s := range steps {
		if s.Action == nil {
			return Result{Status: Invalid, Err: fmt.Errorf("%w: step %q has no action", ErrInvalidSaga, s.Name)}
		}
	}

	var res Result
	done := 0
	for _, s := range steps {
		err := ctx.Err()
		if err == nil {
			err = s.Action(ctx)
		}
		if err != nil {
			res.FailedStep = s.Name
			res.Err = fmt.Errorf("step %q: %w", s.Name, err)
			res.Log = append(res.Log, Event{Step: s.Name, Kind: "action-failed", Err: err})
			break
		}
		res.Log = append(res.Log, Event{Step: s.Name, Kind: "action-ok"})
		done++
	}
	if res.Err == nil {
		res.Status = Completed
		return res
	}

	res.Status = Compensated
	cctx := context.WithoutCancel(ctx)
	for i := done - 1; i >= 0; i-- {
		s := steps[i]
		if s.Compensate == nil {
			continue
		}
		if err := o.compensate(cctx, s); err != nil {
			res.Log = append(res.Log, Event{Step: s.Name, Kind: "compensate-failed", Err: err})
			res.Status = CompensationFailed
			res.Err = errors.Join(res.Err, fmt.Errorf("compensate %q: %w", s.Name, err))
			// Keep going: undo as much as possible, then surface for repair.
			continue
		}
		res.Log = append(res.Log, Event{Step: s.Name, Kind: "compensate-ok"})
	}
	return res
}

func (o *Orchestrator) compensate(ctx context.Context, s Step) error {
	attempts := o.CompensationAttempts
	if attempts <= 0 {
		attempts = 3
	}
	var err error
	for a := 0; a < attempts; a++ {
		if a > 0 && o.RetryDelay > 0 {
			time.Sleep(o.RetryDelay)
		}
		if err = s.Compensate(ctx); err == nil {
			return nil
		}
	}
	return err
}
