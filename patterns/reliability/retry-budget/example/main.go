// Command example simulates 1000 clients sending one-time-password (OTP)
// requests to an SMS provider that degrades and then fails. It compares plain
// 3-attempt retries with the same retries behind a 10% retry budget, then
// shows how retries multiply across three layers of services.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"time"

	retrybudget "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/retry-budget"
)

const (
	clients     = 1000
	attempts    = 3
	ratio       = 0.1
	floorPerSec = 0.01 // each client may always retry about once every 100s
	maxBalance  = 2    // at most 2 banked retries per client
)

type phase struct {
	name    string
	seconds int
	failP   float64 // probability that one call to the provider fails
}

var phases = []phase{
	{"healthy", 60, 0.02},
	{"degraded", 60, 0.50},
	{"outage", 60, 0.90},
	{"recovered", 60, 0.02},
}

var errSMS = errors.New("sms provider: 503")

type result struct {
	requests, calls, ok, denied int
}

// simulate runs every phase. Each client sends one OTP request per simulated
// second. With budgets, each client has its own Budget, as a client library
// instance would.
func simulate(withBudget bool) []result {
	rng := rand.New(rand.NewPCG(7, 42)) // same failures for both runs
	now := time.Unix(0, 0)
	clock := func() time.Time { return now }

	budgets := make([]*retrybudget.Budget, clients)
	for i := range budgets {
		b, err := retrybudget.New(ratio,
			retrybudget.WithMaxBalance(maxBalance),
			retrybudget.WithMinRetriesPerSecond(floorPerSec),
			retrybudget.WithClock(clock))
		if err != nil {
			log.Fatal(err)
		}
		budgets[i] = b
	}

	ctx := context.Background()
	out := make([]result, len(phases))
	for p, ph := range phases {
		r := &out[p]
		send := func(context.Context) error {
			r.calls++
			if rng.Float64() < ph.failP {
				return errSMS
			}
			return nil
		}
		for range ph.seconds {
			for c := range clients {
				r.requests++
				var err error
				if withBudget {
					err = budgets[c].Do(ctx, attempts, send)
				} else {
					for range attempts {
						if err = send(ctx); err == nil {
							break
						}
					}
				}
				switch {
				case err == nil:
					r.ok++
				case errors.Is(err, retrybudget.ErrBudgetExhausted):
					r.denied++
				}
			}
			now = now.Add(time.Second)
		}
	}
	return out
}

func report(title string, rs []result) {
	fmt.Println(title)
	for i, r := range rs {
		fmt.Printf("   %-9s (%2.0f%% of calls fail)  provider calls %6d for %6d requests = %.2fx load   OTPs sent %5.1f%%",
			phases[i].name, phases[i].failP*100, r.calls, r.requests,
			float64(r.calls)/float64(r.requests), 100*float64(r.ok)/float64(r.requests))
		if r.denied > 0 {
			fmt.Printf("   retries denied %d", r.denied)
		}
		fmt.Println()
	}
	fmt.Println()
}

// layered sends n user requests through gateway -> OTP service -> SMS client
// -> provider, where the provider is completely down. Every layer makes up to
// 3 attempts; with budgets, each layer also has its own 10% budget.
func layered(n int, withBudget bool) (callsPerLayer [3]int) {
	var call func(layer int) func(context.Context) error
	budgets := make([]*retrybudget.Budget, 3)
	for i := range budgets {
		b, err := retrybudget.New(ratio) // default cap of 10, no floor
		if err != nil {
			log.Fatal(err)
		}
		budgets[i] = b
	}
	call = func(layer int) func(context.Context) error {
		return func(ctx context.Context) error {
			callsPerLayer[layer]++
			if layer == 2 {
				return errSMS // the provider itself
			}
			next := call(layer + 1)
			if withBudget {
				return budgets[layer+1].Do(ctx, attempts, next)
			}
			var err error
			for range attempts {
				if err = next(ctx); err == nil {
					return nil
				}
			}
			return err
		}
	}
	ctx := context.Background()
	for range n {
		top := call(0)
		if withBudget {
			_ = budgets[0].Do(ctx, attempts, top)
		} else {
			for range attempts {
				if top(ctx) == nil {
					break
				}
			}
		}
	}
	return callsPerLayer
}

func main() {
	fmt.Printf("%d clients, 1 OTP request per client per second, %d attempts per request\n", clients, attempts)
	fmt.Printf("budget: %.0f%% of requests, cap %d tokens, floor %.2f retries/s per client\n\n", ratio*100, maxBalance, floorPerSec)

	report("1) Plain retries: the worse the provider gets, the more load it receives", simulate(false))
	report("2) Same retries behind a 10% retry budget: load stays near 1.1x, sporadic failures still retried", simulate(true))

	const users = 1000
	plain := layered(users, false)
	budget := layered(users, true)
	fmt.Printf("3) Three layers (gateway -> OTP service -> SMS client -> provider), provider down, %d user requests\n", users)
	names := []string{"OTP service", "SMS client", "provider"}
	for i := range 3 {
		fmt.Printf("   calls into %-11s  plain %6d (%4.1fx)   with budgets %5d (%.2fx)\n",
			names[i], plain[i], float64(plain[i])/users, budget[i], float64(budget[i])/users)
	}
}
