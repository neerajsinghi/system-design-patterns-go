// Command example drives a circuit breaker through closed -> open ->
// half-open -> closed using a simulated clock and a dependency that goes down
// and comes back.
package main

import (
	"errors"
	"fmt"
	"time"

	circuitbreaker "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/circuit-breaker"
)

func main() {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	now := start
	clock := func() time.Time { return now }

	b := circuitbreaker.New(circuitbreaker.Settings{
		FailureThreshold: 3,
		OpenTimeout:      5 * time.Second,
		Now:              clock,
		OnStateChange: func(from, to circuitbreaker.State) {
			fmt.Printf("           ** state %s -> %s\n", from, to)
		},
	})

	healthy := true
	payments := func() error {
		if !healthy {
			return errors.New("payments: connection refused")
		}
		return nil
	}

	call := func(label string) {
		err := b.Execute(payments)
		result := "ok"
		switch {
		case errors.Is(err, circuitbreaker.ErrOpen):
			result = "rejected fast (circuit open, dependency not called)"
		case err != nil:
			result = "failed: " + err.Error()
		}
		fmt.Printf("t=%2ds %-14s %s\n", int(now.Sub(start).Seconds()), label, result)
	}

	call("request")
	fmt.Println("        -- payments service goes down --")
	healthy = false
	for i := 0; i < 4; i++ {
		call("request")
	}
	now = now.Add(2 * time.Second)
	call("request")

	now = now.Add(3 * time.Second) // OpenTimeout elapsed
	call("probe")                  // still down: half-open -> open again

	fmt.Println("        -- payments service recovers --")
	healthy = true
	now = now.Add(5 * time.Second)
	call("probe")
	call("request")
	fmt.Printf("final state: %s\n", b.State())
}
