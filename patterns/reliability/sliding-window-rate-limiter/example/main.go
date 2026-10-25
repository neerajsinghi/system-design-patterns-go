// Command example compares a naive fixed window with the sliding log and the
// sliding window counter on the classic "burst at the window boundary" attack.
package main

import (
	"fmt"
	"time"

	sw "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/sliding-window-rate-limiter"
)

// fixedWindow is the naive limiter shown only for comparison.
type fixedWindow struct {
	limit int
	start time.Time
	count int
	now   func() time.Time
}

func (f *fixedWindow) Allow() bool {
	ws := f.now().Truncate(time.Minute)
	if !ws.Equal(f.start) {
		f.start, f.count = ws, 0
	}
	if f.count >= f.limit {
		return false
	}
	f.count++
	return true
}

func main() {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	now := base
	clock := func() time.Time { return now }

	const limit = 10 // 10 requests per minute
	limiters := []struct {
		name string
		l    interface{ Allow() bool }
	}{
		{"fixed window", &fixedWindow{limit: limit, now: clock}},
		{"sliding log", sw.NewLog(limit, time.Minute, clock)},
		{"sliding counter", sw.NewCounter(limit, time.Minute, clock)},
	}

	fmt.Printf("Quota: %d requests per minute.\n", limit)
	fmt.Println("Attack: 10 requests at 12:00:59, then 10 more at 12:01:01 (20 within 2 seconds).")
	fmt.Println()
	for _, lim := range limiters {
		now = base.Add(59 * time.Second)
		first := 0
		for i := 0; i < 10; i++ {
			if lim.l.Allow() {
				first++
			}
		}
		now = base.Add(61 * time.Second)
		second := 0
		for i := 0; i < 10; i++ {
			if lim.l.Allow() {
				second++
			}
		}
		fmt.Printf("  %-16s allowed %2d + %2d = %2d in 2s\n", lim.name, first, second, first+second)
	}
	fmt.Println()
	fmt.Println("The fixed window resets at 12:01:00 and lets the burst double the quota.")
	fmt.Println("Both sliding variants still see the earlier 10 requests and hold the line.")

	log := sw.NewLog(limit, time.Minute, clock)
	now = base
	for i := 0; i < limit; i++ {
		log.Allow()
	}
	now = base.Add(45 * time.Second)
	fmt.Printf("\nSliding log at +45s after a full burst: Retry-After = %v\n", log.RetryAfter())
}
