// Command example shows a write-through cache: every write lands in the store
// before the cache, a failed store write never pollutes the cache, and a cold
// cache after a restart refills from the store.
package main

import (
	"context"
	"fmt"
	"log"

	writethroughcache "github.com/neerajsinghi/system-design-patterns-go/patterns/data/write-through-cache"
)

func main() {
	ctx := context.Background()
	store := writethroughcache.NewMemStore[int]()
	cache, err := writethroughcache.New[int](store)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("1) Set balance:alice=100")
	_ = cache.Set(ctx, "balance:alice", 100)
	sv, _, _ := store.Get(ctx, "balance:alice")
	cv, _ := cache.Get(ctx, "balance:alice")
	fmt.Printf("   store=%d cache=%d (same, because the store was written first)\n", sv, cv)

	fmt.Println("2) The store rejects the next write (disk full, constraint, ...)")
	store.FailNextPuts(1)
	err = cache.Set(ctx, "balance:alice", 999)
	sv, _, _ = store.Get(ctx, "balance:alice")
	cv, _ = cache.Get(ctx, "balance:alice")
	fmt.Printf("   Set error: %v\n   store=%d cache=%d (cache did not accept the failed write)\n", err, sv, cv)

	fmt.Println("3) 1000 reads")
	for range 1000 {
		_, _ = cache.Get(ctx, "balance:alice")
	}
	s := cache.Stats()
	fmt.Printf("   hits=%d store reads=%d store writes=%d\n", s.Hits, s.StoreReads, s.StoreWrites)

	fmt.Println("4) Process restarts: new, empty cache over the same store")
	cache, _ = writethroughcache.New[int](store)
	cv, _ = cache.Get(ctx, "balance:alice")
	cv2, _ := cache.Get(ctx, "balance:alice")
	s = cache.Stats()
	fmt.Printf("   first read %d (miss, from store), second read %d (hit); misses=%d hits=%d\n", cv, cv2, s.Misses, s.Hits)
}
