// Command example counts "likes" on a viral post. 20 concurrent writers
// increment a counter stored in rows that each take 2ms to update. More
// shards mean more parallel row writes. Reads get more expensive.
package main

import (
	"fmt"
	"sync"
	"time"

	sc "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/sharded-counters"
)

func main() {
	const writers, perWriter = 20, 5
	fmt.Printf("%d writers x %d likes, each row write costs 2ms\n\n", writers, perWriter)
	fmt.Printf("%-8s %-12s %-14s %s\n", "shards", "write time", "likes/sec", "value (rows read)")

	for _, shards := range []int{1, 4, 20} {
		store := sc.NewMemStore(2 * time.Millisecond)
		counter, err := sc.NewDistributed(store, "likes:post-42", shards, nil)
		if err != nil {
			fmt.Println(err)
			return
		}
		start := time.Now()
		var wg sync.WaitGroup
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < perWriter; i++ {
					_ = counter.Add(1)
				}
			}()
		}
		wg.Wait()
		elapsed := time.Since(start)
		v, err := counter.Value()
		if err != nil {
			fmt.Println(err)
			return
		}
		rate := float64(writers*perWriter) / elapsed.Seconds()
		fmt.Printf("%-8d %-12s %-14.0f %d (%d)\n", shards, elapsed.Round(time.Millisecond), rate, v, shards)
	}

	fmt.Println("\nIn-process flavour: cache-line-padded shards, 8 goroutines x 100k increments")
	c := sc.New(0)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100_000; i++ {
				c.Inc()
			}
		}()
	}
	wg.Wait()
	fmt.Printf("  value = %d across %d shards (exact once writers stop)\n", c.Value(), c.Shards())
}
