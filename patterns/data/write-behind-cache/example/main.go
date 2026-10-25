// Command example simulates a page-view counter behind a write-behind cache:
// thousands of writes collapse into a handful of batched store writes, and a
// crash shows exactly what the pattern risks.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	writebehindcache "github.com/neerajsinghi/system-design-patterns-go/patterns/data/write-behind-cache"
)

func main() {
	ctx := context.Background()
	store := writebehindcache.NewMemStore[int]()
	tick := make(chan time.Time) // we drive the flush timer by hand
	cache, err := writebehindcache.New[int](store, writebehindcache.Config{MaxBatch: 50, Tick: tick})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("1) 10,000 page views across 20 pages (each view rewrites the page's counter)")
	counts := map[string]int{}
	for i := range 10_000 {
		page := fmt.Sprintf("/page/%d", i%20)
		counts[page]++
		_ = cache.Set(page, counts[page])
	}
	fmt.Printf("   dirty keys in memory: %d, keys in store: %d\n", cache.Dirty(), store.Len())

	fmt.Println("2) The flush timer fires")
	tick <- time.Time{}
	for cache.Stats().Batches == 0 { // wait for the background flush to land
		time.Sleep(time.Millisecond)
	}
	s := cache.Stats()
	fmt.Printf("   %d writes -> %d coalesced -> %d keys in %d batch(es)\n", s.Writes, s.Coalesced, s.Flushed, s.Batches)
	v, _ := store.Get("/page/0")
	fmt.Printf("   store /page/0 = %d\n", v)

	fmt.Println("3) The store goes down during the next flush: keys are re-queued")
	_ = cache.Set("/page/0", v+1)
	store.FailNext(1)
	err = cache.Flush(ctx)
	fmt.Printf("   flush error: %v; still dirty: %d\n", err, cache.Dirty())
	_ = cache.Flush(ctx)
	v, _ = store.Get("/page/0")
	fmt.Printf("   retry succeeded; store /page/0 = %d\n", v)

	fmt.Println("4) More writes, then the process crashes before the next flush")
	for i := range 7 {
		_ = cache.Set(fmt.Sprintf("/new/%d", i), 1)
	}
	lost := cache.Crash()
	fmt.Printf("   acknowledged-but-lost writes: %d (the durability caveat)\n", lost)
}
