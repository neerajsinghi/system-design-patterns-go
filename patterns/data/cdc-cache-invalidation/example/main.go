// Command example compares application-driven cache invalidation ("update the
// database, then delete the key") with invalidation driven by the database's
// change log. It replays the delete-then-stale-set race step by step, lets a
// batch job write behind the application's back, and measures how a lagging
// invalidator turns its lag into a stale-read window.
package main

import (
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"sync"
	"time"

	cdc "github.com/neerajsinghi/system-design-patterns-go/patterns/data/cdc-cache-invalidation"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func newCache(ttl time.Duration, opts ...cdc.Option) *cdc.Cache {
	c, err := cdc.NewCache(ttl, opts...)
	if err != nil {
		log.Fatal(err)
	}
	return c
}

func newInvalidator(db *cdc.DB, c *cdc.Cache, opts ...cdc.Option) *cdc.Invalidator {
	inv, err := cdc.NewInvalidator(db, c, opts...)
	if err != nil {
		log.Fatal(err)
	}
	return inv
}

func write(db *cdc.DB, k, v string) cdc.Change {
	ch, err := db.Write(k, v)
	if err != nil {
		log.Fatal(err)
	}
	return ch
}

// race replays the delete-then-stale-set interleaving and returns how many
// seconds readers were served the old price.
func race(title string, versioned bool, opts ...cdc.Option) {
	const ttl = 60 * time.Second
	clk := &clock{t: time.Unix(0, 0)}
	db := cdc.NewDB(cdc.WithClock(clk.Now))
	write(db, "price:42", "$100")
	c := newCache(ttl, append(opts, cdc.WithClock(clk.Now))...)

	fmt.Println(title)
	var read cdc.Row
	var s cdc.Scheduler
	s.Add("reader", "cache.Get(price:42)", func() string {
		if _, ok := c.Get("price:42"); !ok {
			return "miss"
		}
		return "hit"
	}).Add("reader", "db.Read(price:42)", func() string {
		read, _ = db.Read("price:42")
		return fmt.Sprintf("%s @v%d (then the reader stalls: GC pause)", read.Value, read.Version)
	}).Add("writer", "db.Write(price:42, $120)", func() string {
		ch := write(db, "price:42", "$120")
		return fmt.Sprintf("committed v%d", ch.Version)
	}).Add("writer", "cache.Invalidate(price:42, v2)", func() string {
		c.Invalidate("price:42", 2)
		if versioned {
			return "key replaced by tombstone v2"
		}
		return "key deleted"
	}).Add("reader", "cache.Set(price:42, $100, v1)", func() string {
		if c.Set("price:42", read.Value, read.Version) {
			return "ACCEPTED: old price is now cached"
		}
		return "REJECTED: tombstone v2 is newer"
	})
	if err := s.Run(os.Stdout); err != nil {
		log.Fatal(err)
	}

	stale := 0
	for range 120 {
		if r, _ := c.ReadThrough(db, "price:42"); r.Value == "$100" {
			stale++
		}
		clk.Advance(time.Second)
	}
	fmt.Printf("   => reads once a second for 2 minutes: %d of 120 served the old $100 price\n\n", stale)
}

// batchJob warms the cache, then has the application update a few rows (with
// its own invalidation) and a batch job update many rows straight in the DB.
func batchJob(title string, useCDC bool, mode cdc.Mode) {
	const rows, appUpdates, batchUpdates = 5_000, 50, 1_000
	db := cdc.NewDB()
	for i := range rows {
		write(db, fmt.Sprintf("sku:%d", i), "price-v1")
	}
	c := newCache(time.Hour)
	inv := newInvalidator(db, c, cdc.WithMode(mode))
	inv.CatchUp() // start the invalidator at the end of the initial load
	for i := range rows {
		c.ReadThrough(db, fmt.Sprintf("sku:%d", i))
	}

	for i := range appUpdates { // the application path: write, then invalidate
		ch := write(db, fmt.Sprintf("sku:%d", i), "price-app")
		c.Invalidate(ch.Key, ch.Version)
	}
	for i := 1_000; i < 1_000+batchUpdates; i++ { // nightly repricing job, no cache code
		write(db, fmt.Sprintf("sku:%d", i), "price-batch")
	}
	applied := 0
	if useCDC {
		applied = inv.CatchUp()
	}

	stale := 0
	for i := range rows {
		k := fmt.Sprintf("sku:%d", i)
		want, _ := db.Read(k)
		if got, ok := c.Get(k); ok && got != want {
			stale++
		}
	}
	before := c.Stats().Misses
	for i := range rows {
		c.ReadThrough(db, fmt.Sprintf("sku:%d", i))
	}
	refill := c.Stats().Misses - before
	fmt.Printf("   %-38s changes applied by CDC: %4d | stale cached keys: %4d | DB reads to refill: %4d\n",
		title, applied, stale, refill)
}

type lagResult struct {
	maxLag, maxStale time.Duration
	staleReads       int
	finalBacklog     uint64
}

// lagRun writes one row every 10ms and reads one random key every 1ms for 10
// simulated seconds, while the invalidator polls every pollEvery.
func lagRun(pollEvery time.Duration, batch int) lagResult {
	const keys = 200
	clk := &clock{t: time.Unix(0, 0)}
	db := cdc.NewDB(cdc.WithClock(clk.Now))
	c := newCache(time.Hour, cdc.WithClock(clk.Now))
	inv := newInvalidator(db, c, cdc.WithClock(clk.Now), cdc.WithBatchSize(batch))
	r := rand.New(rand.NewPCG(7, 7))

	commits := make(map[string][]cdc.Change)
	for k := range keys {
		key := fmt.Sprint(k)
		commits[key] = append(commits[key], write(db, key, "v"))
	}
	inv.CatchUp()

	var res lagResult
	start := clk.Now()
	for ms := 1; ms <= 10_000; ms++ {
		clk.Advance(time.Millisecond)
		if ms%10 == 0 {
			key := fmt.Sprint(r.IntN(keys))
			commits[key] = append(commits[key], write(db, key, fmt.Sprint(ms)))
		}
		key := fmt.Sprint(r.IntN(keys))
		got, _ := c.ReadThrough(db, key)
		for _, ch := range commits[key] { // first commit the served value is missing
			if ch.Version > got.Version {
				res.staleReads++
				res.maxStale = max(res.maxStale, clk.Now().Sub(ch.CommittedAt))
				break
			}
		}
		if clk.Now().Sub(start)%pollEvery == 0 {
			res.maxLag = max(res.maxLag, inv.Lag().Time) // the lag peaks just before a poll
			inv.Poll()
		}
	}
	res.finalBacklog = inv.Lag().Events
	return res
}

func main() {
	fmt.Println("1) The delete-then-stale-set race, replayed step by step (TTL 60s)")
	fmt.Println()
	race("   a) plain cache-aside: Set always overwrites", false, cdc.WithoutVersionCheck())
	race("   b) versioned sets + invalidation tombstones", true)

	fmt.Println("2) 5,000 cached SKUs; the app updates 50 (and invalidates), a batch job reprices 1,000 directly")
	batchJob("app-driven invalidation only", false, cdc.ModeDelete)
	batchJob("CDC invalidator, delete mode", true, cdc.ModeDelete)
	batchJob("CDC invalidator, update mode", true, cdc.ModeUpdate)
	fmt.Println("   => only the change log sees the batch job. Delete mode pays a DB read per changed key;")
	fmt.Println("      update mode writes the new value in place and the next pass is all hits.")
	fmt.Println()

	fmt.Println("3) Invalidator lag is the stale window: 100 writes/s, 1,000 reads/s, 200 keys, 10s")
	fmt.Printf("   %-28s %12s %15s %12s %14s\n", "invalidator", "max lag", "oldest stale", "stale reads", "backlog at end")
	for _, cfg := range []struct {
		name  string
		poll  time.Duration
		batch int
	}{
		{"poll 100ms, batch 50", 100 * time.Millisecond, 50},
		{"poll 1s, batch 500", time.Second, 500},
		{"poll 100ms, batch 5 (slow)", 100 * time.Millisecond, 5},
	} {
		res := lagRun(cfg.poll, cfg.batch)
		fmt.Printf("   %-28s %12v %15v %12d %14d\n", cfg.name, res.maxLag, res.maxStale, res.staleReads, res.finalBacklog)
	}
	fmt.Println("   => the oldest stale read never exceeds the lag metric; alert on lag and you have bounded staleness.")
	fmt.Println("      A consumer slower than the write rate (batch 5 = 50 changes/s) falls further behind every second.")
}
