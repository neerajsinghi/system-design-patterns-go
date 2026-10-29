// Command example compares blocking, dropping and signalling producers
// against the same small queue, then fans one publisher out to three
// subscribers (one of them slow) through per-connection send queues under
// each overflow policy.
package main

import (
	"context"
	"errors"
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

	fmt.Println()
	fanOut()
}

// fanOut publishes 20 messages to three subscribers, each with its own send
// queue of 3. The two fast subscribers drain after every message; the slow
// one reads a single message after every 4th publish. Everything runs in one
// goroutine so the output is the same on every run.
func fanOut() {
	const msgs, capacity, blockTimeout = 20, 3, 2 * time.Millisecond
	fmt.Printf("4) Fan-out: 1 publisher -> 3 subscribers, send queue of %d each, %d messages\n", capacity, msgs)
	fmt.Println("   fast-a and fast-b drain after every message; slow reads 1 message per 4 published")
	for _, policy := range []bp.OverflowPolicy{bp.Block, bp.DropOldest, bp.DropNewest, bp.Disconnect} {
		names := []string{"fast-a", "fast-b", "slow"}
		queues := make([]*bp.SendQueue[int], len(names))
		got := make([][]int, len(names))
		for i := range queues {
			queues[i], _ = bp.NewSendQueue[int](capacity, policy, bp.WithBlockTimeout(blockTimeout))
		}
		timeouts := 0
		for m := 1; m <= msgs; m++ {
			for _, q := range queues {
				if err := q.Push(m); errors.Is(err, bp.ErrSendTimeout) {
					timeouts++
				}
			}
			for i, q := range queues {
				if names[i] == "slow" && m%4 != 0 {
					continue
				}
				for {
					v, ok := q.TryPop()
					if !ok {
						break
					}
					got[i] = append(got[i], v)
					if names[i] == "slow" {
						break // slow reads one at a time
					}
				}
			}
		}
		// After the burst the slow subscriber catches up on whatever is still queued.
		for {
			v, ok := queues[2].TryPop()
			if !ok {
				break
			}
			got[2] = append(got[2], v)
		}
		st := queues[2].Stats()
		fmt.Printf("   %-11s fast-a %d/%d, fast-b %d/%d | slow got %d %v, dropped %d, disconnected %v",
			policy, len(got[0]), msgs, len(got[1]), msgs, len(got[2]), got[2], st.Dropped, st.Disconnected)
		if policy == bp.Block {
			fmt.Printf(" | publisher hit the %v deadline %d times (~%v stalled)", blockTimeout, timeouts, time.Duration(timeouts)*blockTimeout)
		}
		fmt.Println()
	}
	fmt.Println("   block: same loss as drop-newest here, because in this one-goroutine run the slow reader cannot")
	fmt.Println("          drain while the publisher waits; and each stall also delayed fast-a and fast-b's next message")
	fmt.Println("   drop-oldest: slow ends up with the newest messages (18 19 20); drop-newest: it keeps its old backlog")
	fmt.Println("   disconnect: slow is cut off when message 4 overflows and must reconnect and resync; nobody else waits")
}
