// Command example simulates a flaky dependency and shows retries with
// exponential backoff, jitter, and a shared retry budget.
package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	retrybackoff "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/retry-backoff"
)

var errUnavailable = errors.New("503 service unavailable")

func main() {
	rng := rand.New(rand.NewSource(42)) // deterministic jitter
	var virtual time.Duration           // simulated clock: we "sleep" instantly

	policy := retrybackoff.Policy{
		MaxAttempts: 5,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    2 * time.Second,
		Jitter:      retrybackoff.FullJitter,
		Rand:        rng.Float64,
		Sleep: func(_ context.Context, d time.Duration) error {
			fmt.Printf("    backing off %v\n", d.Round(time.Millisecond))
			virtual += d
			return nil
		},
	}

	fmt.Println("== 1. Transient failure: dependency recovers on attempt 3")
	calls := 0
	err := policy.Do(context.Background(), func(context.Context) error {
		calls++
		if calls < 3 {
			fmt.Printf("  attempt %d: %v\n", calls, errUnavailable)
			return errUnavailable
		}
		fmt.Printf("  attempt %d: ok\n", calls)
		return nil
	})
	fmt.Printf("  result=%v after %v of (simulated) backoff\n\n", err, virtual.Round(time.Millisecond))

	fmt.Println("== 2. Permanent failure: 400 Bad Request is never retried")
	err = policy.Do(context.Background(), func(context.Context) error {
		fmt.Println("  attempt 1: 400 bad request")
		return retrybackoff.Permanent(errors.New("400 bad request"))
	})
	fmt.Printf("  result=%v\n\n", err)

	fmt.Println("== 3. Outage: a shared retry budget stops a retry storm")
	policy.Budget = retrybackoff.NewBudget(6, 0.1)
	policy.Sleep = func(context.Context, time.Duration) error { return nil }
	for req := 1; req <= 4; req++ {
		attempts := 0
		err := policy.Do(context.Background(), func(context.Context) error {
			attempts++
			return errUnavailable
		})
		fmt.Printf("  request %d: %d attempt(s), budget tokens left=%.1f, budget exhausted=%v\n",
			req, attempts, policy.Budget.Tokens(), errors.Is(err, retrybackoff.ErrBudgetExhausted))
	}
	fmt.Println("  -> once the budget drains, each request makes a single attempt instead of 5")
}
