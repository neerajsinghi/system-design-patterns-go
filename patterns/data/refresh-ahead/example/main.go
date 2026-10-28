// Command example warms 1,000 keys at the same instant (as after a deploy),
// then reads every key once a second for three minutes under three cache
// configurations, and shows how many database loads land in each second and
// how many reads had to wait for the database.
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	refreshahead "github.com/neerajsinghi/system-design-patterns-go/patterns/data/refresh-ahead"
)

const (
	keys    = 1_000
	ttl     = 60 * time.Second
	seconds = 180
)

type result struct {
	perSecond [seconds]int
	stats     refreshahead.Stats
}

func simulate(opts ...refreshahead.Option) result {
	var mu sync.Mutex
	now := time.Unix(0, 0)
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }

	var res result
	load := func(context.Context, int) (string, error) {
		res.perSecond[clock().Unix()]++
		return "row", nil
	}
	r := rand.New(rand.NewPCG(1, 2))
	opts = append([]refreshahead.Option{refreshahead.WithClock(clock), refreshahead.WithRand(r.Float64)}, opts...)
	cache, err := refreshahead.New(load, ttl, opts...)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	for range seconds {
		for k := range keys {
			_, _ = cache.Get(ctx, k)
		}
		mu.Lock()
		now = now.Add(time.Second)
		mu.Unlock()
	}
	res.stats = cache.Stats()
	return res
}

func report(title string, res result) {
	fmt.Println(title)
	// One bar per 10s bucket, scaled to the peak second within it.
	peak := 0
	for sec := 10; sec < seconds; sec += 10 {
		bucketPeak := 0
		for s := sec; s < sec+10; s++ {
			bucketPeak = max(bucketPeak, res.perSecond[s])
		}
		peak = max(peak, bucketPeak)
		fmt.Printf("   t=%3ds..%3ds  peak %4d loads/s  %s\n", sec, sec+9, bucketPeak, strings.Repeat("█", bucketPeak/25))
	}
	waited := res.stats.Misses - keys // the first read of every key always waits
	fmt.Printf("   => worst second: %d DB loads | reads that waited on the DB after warm-up: %d | total loads: %d\n\n",
		peak, waited, res.stats.Loads)
}

func main() {
	fmt.Printf("%d keys warmed at t=0, TTL %v, every key read once per second\n\n", keys, ttl)

	report("1) Fixed TTL: everything warmed together expires together", simulate())

	report("2) TTL jitter (up to 10% shorter): expiries spread over a 6s window, then drift apart",
		simulate(refreshahead.WithJitter(0.1)))

	report("3) Jitter + XFetch early refresh (beta 1, each load costs 5s of DB time)",
		simulate(refreshahead.WithJitter(0.1), refreshahead.WithEarlyRefresh(1),
			refreshahead.WithLoadCost(func(int) time.Duration { return 5 * time.Second })))

	fmt.Println("Jitter flattens the spike. Early refresh means a hot key is reloaded by one")
	fmt.Println("reader before it expires, so readers almost never wait on the database. The")
	fmt.Println("price is more total loads: entries are refreshed before their TTL is used up.")
}
