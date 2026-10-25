// Command example feeds a bursty stream of outbound SMS sends through a leaky
// bucket that a (hypothetical) provider requires to be at most 4/s.
package main

import (
	"errors"
	"fmt"
	"time"

	leakybucket "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/rate-limiter-leaky-bucket"
)

func main() {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := start
	l, err := leakybucket.New(4, 4, func() time.Time { return now }) // 4/s, 4 may wait
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("Leaky bucket: one release every %v, queue capacity 4\n\n", l.Interval())

	send := func(id int) {
		d, err := l.Reserve()
		at := now.Add(d).Sub(start)
		switch {
		case errors.Is(err, leakybucket.ErrOverflow):
			fmt.Printf("t=%4dms sms-%02d overflow: queue full, rejected\n", now.Sub(start).Milliseconds(), id)
		default:
			fmt.Printf("t=%4dms sms-%02d queued=%d, goes out at t=%4dms\n",
				now.Sub(start).Milliseconds(), id, l.Queued(), at.Milliseconds())
		}
	}

	fmt.Println("== burst of 7 sends arrives at once")
	for i := 1; i <= 7; i++ {
		send(i)
	}

	fmt.Println("\n== 600ms later, 2 more arrive; the queue has partly drained")
	now = now.Add(600 * time.Millisecond)
	send(8)
	send(9)

	fmt.Println("\nOutput is evenly spaced regardless of input burstiness.")
}
