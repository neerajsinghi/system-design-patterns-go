// Command example shows a slow "recommendations" dependency filling its own
// bulkhead while the "checkout" dependency keeps serving.
package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/bulkhead"
)

func main() {
	group := bulkhead.NewGroup(
		bulkhead.Config{MaxConcurrent: 2, MaxWaiting: 0},
		map[string]bulkhead.Config{"recommendations": {MaxConcurrent: 3, MaxWaiting: 1}},
	)

	// recommendations is hung: each call blocks until we unblock it.
	unblock := make(chan struct{})
	slowRecs := func(ctx context.Context) error {
		select {
		case <-unblock:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	recs := group.Get("recommendations")
	var wg sync.WaitGroup
	results := make([]string, 6)
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := recs.Execute(ctx, slowRecs)
			switch {
			case err == nil:
				results[i] = "served"
			case errors.Is(err, bulkhead.ErrFull):
				results[i] = "rejected by bulkhead"
			default:
				results[i] = err.Error()
			}
		}(i)
	}

	// Wait until the slow dependency has filled its slots and queue.
	for {
		s := recs.Stats()
		if s.InFlight == 3 && s.Waiting == 1 && s.Rejected == 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	s := recs.Stats()
	fmt.Printf("recommendations is hung: in-flight=%d/%d waiting=%d rejected=%d\n",
		s.InFlight, s.Capacity, s.Waiting, s.Rejected)

	fmt.Println("meanwhile, checkout calls run in their own bulkhead:")
	for i := 1; i <= 3; i++ {
		err := group.Get("checkout").Execute(context.Background(), func(context.Context) error { return nil })
		fmt.Printf("  checkout #%d -> err=%v\n", i, err)
	}

	fmt.Println("recommendations recovers; queued and in-flight calls complete")
	close(unblock)
	wg.Wait()
	counts := map[string]int{}
	for _, r := range results {
		counts[r]++
	}
	fmt.Printf("  recommendations: %d served (3 in-flight + 1 queued), %d rejected immediately\n",
		counts["served"], counts["rejected by bulkhead"])
}
