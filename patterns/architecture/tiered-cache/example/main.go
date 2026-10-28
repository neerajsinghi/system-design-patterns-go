// Command example simulates a streaming service's CDN at the moment a new
// season goes live: 5 city edges with 2,000 viewers each request episode 1 at
// once. It compares origin load with no shield, with an origin shield, and
// with the episode pre-warmed, then shows per-tier hit ratios for an hour of
// long-tail catalogue traffic.
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	tieredcache "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/tiered-cache"
)

var cities = []string{"mumbai", "delhi", "bengaluru", "chennai", "kolkata"}

type origin struct{ calls atomic.Int64 }

func (o *origin) fetch(_ context.Context, key string) (string, error) {
	o.calls.Add(1)
	time.Sleep(20 * time.Millisecond) // the origin is slow under load
	return "manifest for " + key, nil
}

func newEdges(parent tieredcache.Fetcher[string], ttl time.Duration, opts ...tieredcache.Option) []*tieredcache.Tier[string] {
	edges := make([]*tieredcache.Tier[string], len(cities))
	for i, c := range cities {
		e, err := tieredcache.NewTier("edge-"+c, ttl, parent, opts...)
		if err != nil {
			log.Fatal(err)
		}
		edges[i] = e
	}
	return edges
}

func launch(title string, shielded, prewarm bool) {
	o := &origin{}
	ctx := context.Background()
	parent := tieredcache.Fetcher[string](o.fetch)
	var shield *tieredcache.Tier[string]
	if shielded {
		shield, _ = tieredcache.NewTier("shield", time.Hour, o.fetch)
		parent = shield.Fetch
	}
	edges := newEdges(parent, 10*time.Minute)

	if prewarm {
		all := edges
		if shield != nil {
			all = append([]*tieredcache.Tier[string]{shield}, edges...)
		}
		if err := tieredcache.Prewarm(ctx, o.fetch, []string{"s2e1"}, all...); err != nil {
			log.Fatal(err)
		}
		o.calls.Store(0) // count only launch traffic
	}

	start := time.Now()
	var wg sync.WaitGroup
	for _, e := range edges {
		for range 2_000 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := e.Fetch(ctx, "s2e1"); err != nil {
					log.Fatal(err)
				}
			}()
		}
	}
	wg.Wait()

	var edgeFetches uint64
	for _, e := range edges {
		edgeFetches += e.Stats().Fetches
	}
	fmt.Println(title)
	fmt.Printf("   10,000 viewers -> %d requests left the edges -> %d reached the origin (%v)\n",
		edgeFetches, o.calls.Load(), time.Since(start).Round(5*time.Millisecond))
}

func longTail() {
	var mu sync.Mutex
	now := time.Unix(0, 0)
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }

	var originCalls int
	fetch := func(_ context.Context, key string) (string, error) { originCalls++; return key, nil }
	shield, _ := tieredcache.NewTier("shield", time.Hour, fetch, tieredcache.WithClock(clock))
	edges := newEdges(shield.Fetch, 5*time.Minute, tieredcache.WithClock(clock))

	r := rand.New(rand.NewPCG(9, 9))
	z := rand.NewZipf(r, 1.2, 1, 1_999) // 2,000 titles, a few very popular
	ctx := context.Background()
	const requests = 30_000
	for range requests {
		edge := edges[r.IntN(len(edges))]
		if _, err := edge.Fetch(ctx, fmt.Sprintf("title-%d", z.Uint64())); err != nil {
			log.Fatal(err)
		}
		mu.Lock()
		now = now.Add(time.Hour / requests)
		mu.Unlock()
	}

	var edgeHits, edgeMisses uint64
	for _, e := range edges {
		edgeHits += e.Stats().Hits
		edgeMisses += e.Stats().Misses
	}
	edgeRatio := float64(edgeHits) / float64(edgeHits+edgeMisses)
	fmt.Println("4) One hour of catalogue browsing (2,000 titles, skewed popularity), edge TTL 5m, shield TTL 1h")
	fmt.Printf("   edges  hit ratio %5.1f%%  (%d misses sent to the shield)\n", 100*edgeRatio, edgeMisses)
	fmt.Printf("   shield hit ratio %5.1f%%  (%d misses sent to the origin)\n", 100*shield.Stats().HitRatio(), originCalls)
	fmt.Printf("   the origin served %.1f%% of %d requests\n", 100*float64(originCalls)/requests, requests)
	fmt.Println("   Each edge only sees its own city's traffic; the shield sees all of it, so it")
	fmt.Println("   catches the long tail that individual edges keep missing.")
}

func main() {
	launch("1) No shield: every edge's miss goes to the origin", false, false)
	launch("2) Origin shield: edges miss into one shared tier, which fetches once", true, false)
	launch("3) Pre-warmed before release: nobody misses at all", true, true)
	fmt.Println()
	longTail()
}
