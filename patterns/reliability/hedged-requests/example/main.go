// Command example sends 300 concurrent reads to a replicated store where 4% of
// replica responses hit a slow path (GC pause, cold cache). It compares p50
// and p99 latency without hedging and with a hedge fired at the observed p95.
// Real (but short) sleeps are used, so the numbers vary slightly per run.
package main

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	hedged "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/hedged-requests"
)

const (
	requests = 300
	fast     = 3 * time.Millisecond
	slow     = 80 * time.Millisecond
)

func main() {
	// Deterministic latency for each (request, attempt): attempt n of request
	// i is served by a different replica, so its slowness is independent.
	rng := rand.New(rand.NewSource(7))
	latency := make([][2]time.Duration, requests)
	for i := range latency {
		for a := range latency[i] {
			latency[i][a] = fast
			if rng.Float64() < 0.04 {
				latency[i][a] = slow
			}
		}
	}

	replica := func(i int) func(ctx context.Context, attempt int) (int, error) {
		return func(ctx context.Context, attempt int) (int, error) {
			select {
			case <-time.After(latency[i][attempt]):
				return attempt, nil
			case <-ctx.Done(): // a losing hedge stops work as soon as it is cancelled
				return 0, ctx.Err()
			}
		}
	}

	run := func(cfg hedged.Config, tracker *hedged.LatencyTracker) (p50, p99 time.Duration, hedges int64) {
		var wg sync.WaitGroup
		var extra atomic.Int64
		durations := make([]time.Duration, requests)
		for i := 0; i < requests; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				start := time.Now()
				fn := replica(i)
				_, _, err := hedged.Do(context.Background(), cfg, func(ctx context.Context, a int) (int, error) {
					if a > 0 {
						extra.Add(1)
					}
					return fn(ctx, a)
				})
				if err != nil {
					fmt.Println("unexpected error:", err)
				}
				durations[i] = time.Since(start)
				if tracker != nil {
					tracker.Observe(durations[i])
				}
			}(i)
		}
		wg.Wait()
		sort.Slice(durations, func(a, b int) bool { return durations[a] < durations[b] })
		return durations[requests/2], durations[requests*99/100], extra.Load()
	}

	tracker := hedged.NewLatencyTracker(requests)
	p50, p99, _ := run(hedged.Config{MaxAttempts: 1}, tracker)
	fmt.Printf("no hedging:       p50=%-4v p99=%-4v  extra requests: 0\n", p50.Round(time.Millisecond), p99.Round(time.Millisecond))

	delay := tracker.Quantile(0.95, 10*time.Millisecond) + time.Millisecond // small margin over p95
	hp50, hp99, hedges := run(hedged.Config{MaxAttempts: 2, Delay: delay}, nil)
	fmt.Printf("hedge after p95:  p50=%-4v p99=%-4v  extra requests: %d (%.0f%%), hedge delay ~%v\n",
		hp50.Round(time.Millisecond), hp99.Round(time.Millisecond), hedges,
		100*float64(hedges)/requests, delay.Round(time.Millisecond))
	fmt.Println("\nA few percent more requests turn an 80ms tail into roughly delay + 3ms.")
}
