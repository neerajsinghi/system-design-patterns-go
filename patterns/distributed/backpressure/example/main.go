// Command example compares blocking, dropping and signalling producers
// against the same small queue.
package main

import (
	"context"
	"fmt"
	"time"

	bp "github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/backpressure"
)

func main() {
	fmt.Println("1) Dropping: burst of 10 into a queue of 4 with no consumer")
	q1, _ := bp.New[int](4, 3)
	for i := 0; i < 10; i++ {
		q1.TryPut(i)
	}
	fmt.Printf("   queued=%d dropped=%d (producer never waited, data lost)\n\n", q1.Len(), q1.Dropped())

	fmt.Println("2) Signalling: producer reacts to Offer's answer")
	q2, _ := bp.New[int](4, 3)
	delay := time.Duration(0)
	for i := 0; i < 6; i++ {
		s := q2.Offer(i)
		switch s {
		case bp.SlowDown:
			delay = max(2*delay, time.Millisecond)
		case bp.Rejected:
			delay = max(4*delay, time.Millisecond)
		}
		fmt.Printf("   offer item %d -> %-9s len=%d, producer delay now %v\n", i, s, q2.Len(), delay)
	}
	fmt.Println("   (a server would map Rejected to 429/503 with Retry-After)")
	fmt.Println()

	fmt.Println("3) Blocking: fast producer, consumer takes 2ms per item")
	q3, _ := bp.New[int](4, 3)
	ctx := context.Background()
	consumed := make(chan int)
	go func() {
		n := 0
		for {
			if _, err := q3.Get(ctx); err != nil {
				consumed <- n
				return
			}
			time.Sleep(2 * time.Millisecond)
			n++
		}
	}()
	start := time.Now()
	for i := 0; i < 20; i++ {
		_ = q3.Put(ctx, i)
	}
	waited := time.Since(start)
	q3.Close()
	n := <-consumed
	fmt.Printf("   consumer processed %d/20, producer was slowed to the consumer's pace (spent ~%dms blocked)\n",
		n, waited.Round(10*time.Millisecond).Milliseconds())
}
