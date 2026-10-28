// Command example simulates cache-aside in front of a slow "database":
// a thundering herd on a cold key, TTL expiry, invalidation on write, and
// negative caching of a key that doesn't exist.
package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	cacheaside "github.com/neerajsinghi/system-design-patterns-go/patterns/data/cache-aside"
)

type db struct {
	mu      sync.Mutex
	rows    map[string]string
	queries atomic.Int64
}

func (d *db) load(_ context.Context, key string) (string, error) {
	d.queries.Add(1)
	time.Sleep(20 * time.Millisecond) // pretend this is an expensive query
	d.mu.Lock()
	defer d.mu.Unlock()
	v, ok := d.rows[key]
	if !ok {
		return "", cacheaside.ErrNotFound
	}
	return v, nil
}

func main() {
	store := &db{rows: map[string]string{"user:1": "Ada"}}
	now := time.Unix(0, 0)
	var clockMu sync.Mutex
	clock := func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return now }
	advance := func(d time.Duration) { clockMu.Lock(); now = now.Add(d); clockMu.Unlock() }

	cache, err := cacheaside.New(store.load, 5*time.Minute, cacheaside.WithClock(clock),
		cacheaside.WithNegativeTTL(30*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()

	fmt.Println("1) 100 concurrent requests hit a cold key")
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = cache.Get(ctx, "user:1") }()
	}
	wg.Wait()
	s := cache.Stats()
	fmt.Printf("   db queries: %d (shared in-flight waiters: %d)\n", store.queries.Load(), s.Shared)

	fmt.Println("2) 1000 more reads while the entry is fresh")
	for range 1000 {
		_, _ = cache.Get(ctx, "user:1")
	}
	fmt.Printf("   db queries: %d, hits: %d\n", store.queries.Load(), cache.Stats().Hits)

	fmt.Println("3) write Ada -> Grace: update the DB, then invalidate")
	store.mu.Lock()
	store.rows["user:1"] = "Grace"
	store.mu.Unlock()
	cache.Invalidate("user:1")
	v, _ := cache.Get(ctx, "user:1")
	fmt.Printf("   read after invalidate: %q (db queries: %d)\n", v, store.queries.Load())

	fmt.Println("4) six minutes pass: the TTL expires and the next read reloads")
	advance(6 * time.Minute)
	v, _ = cache.Get(ctx, "user:1")
	fmt.Printf("   read after expiry: %q (db queries: %d)\n", v, store.queries.Load())

	fmt.Println("5) a scraper requests user:404, which doesn't exist, 500 times")
	before := store.queries.Load()
	for range 500 {
		_, _ = cache.Get(ctx, "user:404")
	}
	fmt.Printf("   db queries for it: %d (the \"not found\" is cached for 30s; negative hits: %d)\n",
		store.queries.Load()-before, cache.Stats().NegativeHits)
}
