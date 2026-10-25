// Command example shows a token bucket absorbing a burst, throttling the
// excess, refilling over (simulated) time, and limiting clients independently.
package main

import (
	"fmt"
	"strings"
	"time"

	tokenbucket "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/rate-limiter-token-bucket"
)

func main() {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	// 5 requests/second sustained, bursts of up to 10.
	l := tokenbucket.New(5, 10, clock)

	fmt.Println("== A client fires 15 requests at once (burst=10, rate=5/s)")
	var marks []string
	for i := 0; i < 15; i++ {
		if l.Allow() {
			marks = append(marks, "ok")
		} else {
			marks = append(marks, "429")
		}
	}
	fmt.Println("  ", strings.Join(marks, " "))

	fmt.Println("== 600ms later the bucket has earned 3 tokens")
	now = now.Add(600 * time.Millisecond)
	fmt.Printf("   tokens=%.1f\n", l.Tokens())
	for i := 1; i <= 4; i++ {
		fmt.Printf("   request %d allowed=%v\n", i, l.Allow())
	}

	fmt.Println("== Steady 5 req/s (one every 200ms) is always allowed")
	ok := 0
	for i := 0; i < 10; i++ {
		now = now.Add(200 * time.Millisecond)
		if l.Allow() {
			ok++
		}
	}
	fmt.Printf("   %d/10 allowed\n", ok)

	fmt.Println("== Per-client buckets: a noisy client cannot starve a quiet one")
	keyed := tokenbucket.NewKeyed(1, 3, clock)
	noisy, quiet := 0, 0
	for i := 0; i < 20; i++ {
		if keyed.Allow("noisy") {
			noisy++
		}
	}
	for i := 0; i < 2; i++ {
		if keyed.Allow("quiet") {
			quiet++
		}
	}
	fmt.Printf("   noisy: %d/20 allowed, quiet: %d/2 allowed\n", noisy, quiet)
}
