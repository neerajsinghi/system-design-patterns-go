// Command example walks through a crash, a redelivery, a stale ack, a poison
// message landing in the DLQ, and then a pool of competing workers.
package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	cc "github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/competing-consumers"
)

func main() {
	clk := cc.NewManualClock(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	q, _ := cc.NewQueue(cc.Config{VisibilityTimeout: 30 * time.Second, MaxReceives: 3, Clock: clk})

	q.Enqueue("resize image 42")
	d1, _ := q.Receive()
	fmt.Printf("worker-A receives %s (%q), attempt %d, then crashes\n", d1.MessageID, d1.Body, d1.Attempt)
	if _, ok := q.Receive(); !ok {
		fmt.Println("worker-B polls: nothing visible (message is in flight)")
	}
	clk.Advance(31 * time.Second)
	d2, _ := q.Receive()
	fmt.Printf("+31s visibility timeout expired: worker-B receives %s, attempt %d\n", d2.MessageID, d2.Attempt)
	fmt.Printf("worker-A wakes up and acks with its old receipt: %v\n", q.Ack(d1.Receipt))
	fmt.Printf("worker-B acks: err=%v, queue length=%d\n", q.Ack(d2.Receipt), q.Len())

	fmt.Println()
	q.Enqueue("malformed payload")
	for {
		d, ok := q.Receive()
		if !ok {
			break
		}
		fmt.Printf("attempt %d on %q fails -> nack\n", d.Attempt, d.Body)
		_ = q.Nack(d.Receipt)
	}
	for _, dl := range q.DeadLetters() {
		fmt.Printf("moved to DLQ after %d receives: %s %q\n", dl.Receives, dl.MessageID, dl.Body)
	}

	fmt.Println()
	work, _ := cc.NewQueue(cc.Config{VisibilityTimeout: time.Minute, MaxReceives: 3})
	const jobs = 30
	for i := 0; i < jobs; i++ {
		work.Enqueue(fmt.Sprintf("job-%d", i))
	}
	ctx, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	done, failedOnce := 0, map[string]bool{}
	wait := cc.RunWorkers(ctx, work, 3, time.Millisecond, func(_ int, d cc.Delivery) error {
		mu.Lock()
		defer mu.Unlock()
		if d.Body == "job-7" && !failedOnce[d.Body] { // transient failure
			failedOnce[d.Body] = true
			return errors.New("transient")
		}
		done++
		if done == jobs {
			cancel()
		}
		return nil
	})
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		cancel()
	}
	wait()
	keys := make([]string, 0, len(failedOnce))
	for k := range failedOnce {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Printf("3 competing workers finished %d/%d jobs; retried after transient failure: %v; left in queue: %d\n",
		done, jobs, keys, work.Len())
}
