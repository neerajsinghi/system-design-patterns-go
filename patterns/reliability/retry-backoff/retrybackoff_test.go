package retrybackoff

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	half := func() float64 { return 0.5 }
	tests := []struct {
		name   string
		policy Policy
		retry  int
		want   time.Duration
	}{
		{"no jitter first", Policy{BaseDelay: 10 * time.Millisecond, MaxDelay: time.Second}, 0, 10 * time.Millisecond},
		{"no jitter doubles", Policy{BaseDelay: 10 * time.Millisecond, MaxDelay: time.Second}, 3, 80 * time.Millisecond},
		{"capped", Policy{BaseDelay: 10 * time.Millisecond, MaxDelay: 50 * time.Millisecond}, 10, 50 * time.Millisecond},
		{"huge retry no overflow", Policy{BaseDelay: time.Second, MaxDelay: time.Minute}, 1000, time.Minute},
		{"negative retry", Policy{BaseDelay: 10 * time.Millisecond, MaxDelay: time.Second}, -5, 10 * time.Millisecond},
		{"full jitter", Policy{BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second, Jitter: FullJitter, Rand: half}, 1, 100 * time.Millisecond},
		{"equal jitter", Policy{BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second, Jitter: EqualJitter, Rand: half}, 1, 150 * time.Millisecond},
		{"max below base", Policy{BaseDelay: 100 * time.Millisecond, MaxDelay: time.Millisecond}, 2, 100 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.policy.Backoff(tt.retry); got != tt.want {
				t.Fatalf("Backoff(%d) = %v, want %v", tt.retry, got, tt.want)
			}
		})
	}
}

var errBoom = errors.New("boom")

func TestDo(t *testing.T) {
	tests := []struct {
		name      string
		failures  int   // number of leading failures
		err       error // error returned on failure
		attempts  int
		wantCalls int
		wantErr   error
		wantSleep []time.Duration
	}{
		{"success first try", 0, errBoom, 3, 1, nil, nil},
		{"success after two failures", 2, errBoom, 3, 3, nil, []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}},
		{"exhausted", 10, errBoom, 3, 3, errBoom, []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}},
		{"permanent not retried", 10, Permanent(errBoom), 5, 1, errBoom, nil},
		{"context error not retried", 10, context.DeadlineExceeded, 5, 1, context.DeadlineExceeded, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var slept []time.Duration
			p := Policy{
				MaxAttempts: tt.attempts,
				BaseDelay:   10 * time.Millisecond,
				MaxDelay:    time.Second,
				Sleep: func(_ context.Context, d time.Duration) error {
					slept = append(slept, d)
					return nil
				},
			}
			calls := 0
			err := p.Do(context.Background(), func(context.Context) error {
				calls++
				if calls <= tt.failures {
					return tt.err
				}
				return nil
			})
			if !errors.Is(err, tt.wantErr) || (tt.wantErr == nil && err != nil) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if calls != tt.wantCalls {
				t.Fatalf("calls = %d, want %d", calls, tt.wantCalls)
			}
			if len(slept) != len(tt.wantSleep) {
				t.Fatalf("slept %v, want %v", slept, tt.wantSleep)
			}
			for i := range slept {
				if slept[i] != tt.wantSleep[i] {
					t.Fatalf("slept %v, want %v", slept, tt.wantSleep)
				}
			}
		})
	}
}

func TestDoCustomRetryable(t *testing.T) {
	p := Policy{MaxAttempts: 5, Sleep: func(context.Context, time.Duration) error { return nil },
		Retryable: func(err error) bool { return !errors.Is(err, errBoom) }}
	calls := 0
	_ = p.Do(context.Background(), func(context.Context) error { calls++; return errBoom })
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestDoContextCancelledDuringSleep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p := Policy{MaxAttempts: 5, BaseDelay: time.Hour}
	calls := 0
	err := p.Do(ctx, func(context.Context) error {
		calls++
		cancel()
		return errBoom
	})
	if !errors.Is(err, context.Canceled) || !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want canceled joined with boom", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestBudget(t *testing.T) {
	b := NewBudget(4, 1) // retries allowed while tokens > 2
	p := Policy{MaxAttempts: 10, Budget: b, Sleep: func(context.Context, time.Duration) error { return nil }}
	calls := 0
	err := p.Do(context.Background(), func(context.Context) error { calls++; return errBoom })
	if !errors.Is(err, ErrBudgetExhausted) || !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want budget exhausted", err)
	}
	// tokens: 4 -> 3 (retry ok) -> 2 (refused)
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
	// Successes refill the budget.
	for i := 0; i < 2; i++ {
		_ = p.Do(context.Background(), func(context.Context) error { return nil })
	}
	if got := b.Tokens(); got != 4 {
		t.Fatalf("tokens = %v, want 4", got)
	}
}

func TestBudgetConcurrent(t *testing.T) {
	b := NewBudget(100, 0.5)
	p := Policy{MaxAttempts: 3, Budget: b, Sleep: func(context.Context, time.Duration) error { return nil }}
	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func(i int) {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 50; j++ {
				_ = p.Do(context.Background(), func(context.Context) error {
					if (i+j)%2 == 0 {
						return errBoom
					}
					return nil
				})
			}
		}(i)
	}
	for i := 0; i < 8; i++ {
		<-done
	}
	if tk := b.Tokens(); tk < 0 || tk > 100 {
		t.Fatalf("tokens out of range: %v", tk)
	}
}

func TestPermanentNil(t *testing.T) {
	if Permanent(nil) != nil {
		t.Fatal("Permanent(nil) should be nil")
	}
}
