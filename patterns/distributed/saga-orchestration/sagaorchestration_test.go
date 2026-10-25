package sagaorchestration

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

type recorder struct{ calls []string }

// step builds a step whose action fails if failAction, and whose
// compensation fails the first compFailures times.
func (r *recorder) step(name string, failAction bool, compFailures int) Step {
	return Step{
		Name: name,
		Action: func(context.Context) error {
			r.calls = append(r.calls, "do:"+name)
			if failAction {
				return errors.New(name + " failed")
			}
			return nil
		},
		Compensate: func(context.Context) error {
			r.calls = append(r.calls, "undo:"+name)
			if compFailures > 0 {
				compFailures--
				return errors.New("undo " + name + " failed")
			}
			return nil
		},
	}
}

func TestRun(t *testing.T) {
	tests := []struct {
		name       string
		build      func(r *recorder) []Step
		wantStatus Status
		wantFailed string
		wantCalls  []string
	}{
		{
			name: "all steps succeed",
			build: func(r *recorder) []Step {
				return []Step{r.step("a", false, 0), r.step("b", false, 0)}
			},
			wantStatus: Completed,
			wantCalls:  []string{"do:a", "do:b"},
		},
		{
			name: "third step fails, first two compensated in reverse",
			build: func(r *recorder) []Step {
				return []Step{r.step("a", false, 0), r.step("b", false, 0), r.step("c", true, 0), r.step("d", false, 0)}
			},
			wantStatus: Compensated,
			wantFailed: "c",
			wantCalls:  []string{"do:a", "do:b", "do:c", "undo:b", "undo:a"},
		},
		{
			name: "first step fails, nothing to compensate",
			build: func(r *recorder) []Step {
				return []Step{r.step("a", true, 0), r.step("b", false, 0)}
			},
			wantStatus: Compensated,
			wantFailed: "a",
			wantCalls:  []string{"do:a"},
		},
		{
			name: "transient compensation failure is retried",
			build: func(r *recorder) []Step {
				return []Step{r.step("a", false, 2), r.step("b", true, 0)}
			},
			wantStatus: Compensated,
			wantFailed: "b",
			wantCalls:  []string{"do:a", "do:b", "undo:a", "undo:a", "undo:a"},
		},
		{
			name: "permanent compensation failure is surfaced, others still undone",
			build: func(r *recorder) []Step {
				return []Step{r.step("a", false, 0), r.step("b", false, 99), r.step("c", true, 0)}
			},
			wantStatus: CompensationFailed,
			wantFailed: "c",
			wantCalls:  []string{"do:a", "do:b", "do:c", "undo:b", "undo:b", "undo:b", "undo:a"},
		},
		{
			name: "nil compensation is skipped",
			build: func(r *recorder) []Step {
				s := r.step("a", false, 0)
				s.Compensate = nil
				return []Step{s, r.step("b", true, 0)}
			},
			wantStatus: Compensated,
			wantFailed: "b",
			wantCalls:  []string{"do:a", "do:b"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &recorder{}
			o := &Orchestrator{CompensationAttempts: 3}
			res := o.Run(context.Background(), tt.build(r))
			if res.Status != tt.wantStatus || res.FailedStep != tt.wantFailed {
				t.Fatalf("status=%v failed=%q err=%v", res.Status, res.FailedStep, res.Err)
			}
			if !reflect.DeepEqual(r.calls, tt.wantCalls) {
				t.Fatalf("calls=%v\nwant %v", r.calls, tt.wantCalls)
			}
			if (tt.wantStatus == Completed) != (res.Err == nil) {
				t.Fatalf("unexpected err %v", res.Err)
			}
		})
	}
}

func TestCancelledContextStillCompensates(t *testing.T) {
	r := &recorder{}
	ctx, cancel := context.WithCancel(context.Background())
	steps := []Step{
		r.step("a", false, 0),
		{Name: "cancel", Action: func(context.Context) error { cancel(); return nil },
			Compensate: func(c context.Context) error {
				if c.Err() != nil {
					return c.Err()
				}
				r.calls = append(r.calls, "undo:cancel")
				return nil
			}},
		r.step("never", false, 0),
	}
	res := (&Orchestrator{}).Run(ctx, steps)
	if res.Status != Compensated || res.FailedStep != "never" || !errors.Is(res.Err, context.Canceled) {
		t.Fatalf("res=%+v", res)
	}
	want := []string{"do:a", "undo:cancel", "undo:a"}
	if !reflect.DeepEqual(r.calls, want) {
		t.Fatalf("calls=%v", r.calls)
	}
}

func TestInvalidSaga(t *testing.T) {
	o := &Orchestrator{}
	for _, steps := range [][]Step{nil, {{Name: "no-action"}}} {
		res := o.Run(context.Background(), steps)
		if res.Status != Invalid || !errors.Is(res.Err, ErrInvalidSaga) {
			t.Fatalf("res=%+v", res)
		}
	}
	if !strings.Contains(Compensated.String(), "compensated") {
		t.Fatal("String")
	}
}
