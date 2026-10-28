// Command example replays the same access traces against FIFO, LRU, LFU and
// W-TinyLFU caches of equal size and prints each policy's hit ratio.
package main

import (
	"fmt"
	"log"
	"math/rand/v2"
	"strings"

	cacheeviction "github.com/neerajsinghi/system-design-patterns-go/patterns/data/cache-eviction"
)

const (
	capacity = 500
	keys     = 10_000
	requests = 100_000
)

// trace draws keys from a Zipf distribution (a few keys get most traffic).
// If scanLen > 0 it splices in a batch-job scan of one-off keys every
// scanEvery requests. If shift is true, the popular keys change halfway
// through, as when yesterday's trending items cool off.
func trace(scanEvery, scanLen int, shift bool) []string {
	r := rand.New(rand.NewPCG(7, 7))
	z := rand.NewZipf(r, 1.1, 1, keys-1)
	out := make([]string, 0, requests)
	scanID := 0
	for i := range requests {
		k := z.Uint64()
		if shift && i >= requests/2 {
			k = (k + keys/2) % keys // a different set of items is now popular
		}
		out = append(out, fmt.Sprintf("item:%d", k))
		if scanLen > 0 && i%scanEvery == 0 {
			for range scanLen {
				out = append(out, fmt.Sprintf("export:%d", scanID))
				scanID++
			}
		}
	}
	return out
}

func policies() []cacheeviction.Policy[string, int] {
	fifo, err := cacheeviction.NewFIFO[string, int](capacity)
	if err != nil {
		log.Fatal(err)
	}
	lru, _ := cacheeviction.NewLRU[string, int](capacity)
	lfu, _ := cacheeviction.NewLFU[string, int](capacity)
	tiny, _ := cacheeviction.NewTinyLFU[string, int](capacity)
	return []cacheeviction.Policy[string, int]{fifo, lru, lfu, tiny}
}

func run(title string, tr []string) {
	fmt.Printf("%s\n   (%d accesses over %d items, cache holds %d)\n", title, len(tr), keys, capacity)
	for _, p := range policies() {
		hits, misses := cacheeviction.Replay(p, tr, func(string) int { return 0 })
		ratio := float64(hits) / float64(hits+misses)
		fmt.Printf("   %-10s hit ratio %5.1f%%  %s\n", p.Name(), 100*ratio, strings.Repeat("█", int(ratio*40)))
	}
}

func main() {
	run("1) Skewed traffic: a few product pages get most of the reads", trace(0, 0, false))
	fmt.Println("   Popularity never changes, so counting frequency (LFU) is close to optimal.")
	fmt.Println()
	run("2) Same traffic, plus an export job reading 400 one-off keys every 1,000 requests", trace(1_000, 400, false))
	fmt.Println("   LRU and FIFO let every scanned key push out a hot one; LFU and W-TinyLFU resist the scan.")
	fmt.Println()
	run("3) Scans, and halfway through a different set of items becomes popular", trace(1_000, 400, true))
	fmt.Println("   LFU's old favourites keep high counts and squat in the cache after they go cold.")
	fmt.Println("   W-TinyLFU's sketch halves its counts periodically, so it forgets the old hits and adapts.")
}
