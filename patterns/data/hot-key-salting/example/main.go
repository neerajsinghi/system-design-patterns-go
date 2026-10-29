// Command example simulates a like-counter table during a celebrity post. An
// 8-partition table accepts 1,000 writes per second per partition (8,000/s in
// total). The workload is 4,000 writes/s: 2,500/s for one viral post and
// 1,500/s spread over 300 ordinary posts. It runs the same 10 simulated
// seconds three ways (no salting, every key salted with K=8, adaptive salting)
// and prints per-partition load, throttling and the read cost of each.
package main

import (
	"fmt"
	"log"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	hks "github.com/neerajsinghi/system-design-patterns-go/patterns/data/hot-key-salting"
)

const (
	partitions = 8
	capacity   = 1000 // writes per second per partition
	seconds    = 10
	hotPerSec  = 2500
	coldPerSec = 1500
	coldKeys   = 300
	shards     = 8
	hotKey     = "post:celebrity"
)

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *clock) add(d time.Duration) { c.mu.Lock(); c.now = c.now.Add(d); c.mu.Unlock() }

type result struct {
	parts                    []hks.PartitionStats
	stats                    hks.Stats
	hotAttempts, hotRejected int
	hotLikes, accepted       int64
	readTotal                int64
	hotFanout, coldFanout    int
	coldLookups              uint64
	hotKeys                  []string
	suffixParts              []int
}

func run(opts ...hks.Option) result {
	clk := &clock{now: time.Unix(1_700_000_000, 0)}
	table, err := hks.NewTable[int64](partitions, capacity, clk.Now)
	if err != nil {
		log.Fatal(err)
	}
	r := rand.New(rand.NewPCG(7, 11))
	opts = append([]hks.Option{hks.WithClock(clk.Now), hks.WithShards(shards), hks.WithPicker(r.IntN)}, opts...)
	s, err := hks.New(table, hks.Sum, opts...)
	if err != nil {
		log.Fatal(err)
	}

	var res result
	ops := make([]string, 0, hotPerSec+coldPerSec)
	step := time.Second / time.Duration(hotPerSec+coldPerSec)
	for range seconds {
		ops = ops[:0]
		for range hotPerSec {
			ops = append(ops, hotKey)
		}
		for range coldPerSec {
			ops = append(ops, fmt.Sprintf("post:%d", r.IntN(coldKeys)))
		}
		r.Shuffle(len(ops), func(i, j int) { ops[i], ops[j] = ops[j], ops[i] })
		for _, key := range ops {
			err := s.Write(key, 1)
			if key == hotKey {
				res.hotAttempts++
			}
			switch {
			case err == nil:
				res.accepted++
			case key == hotKey:
				res.hotRejected++
			}
			clk.add(step)
		}
	}

	// Read every post once: the sum must equal the accepted writes exactly.
	res.hotLikes, _, _ = s.Read(hotKey)
	res.readTotal = res.hotLikes
	before := s.Stats().Lookups
	for i := range coldKeys {
		v, _, _ := s.Read(fmt.Sprintf("post:%d", i))
		res.readTotal += v
	}
	res.coldLookups = s.Stats().Lookups - before
	res.hotFanout, res.coldFanout = s.Fanout(hotKey), s.Fanout("post:0")
	res.parts, res.stats, res.hotKeys = table.Stats(), s.Stats(), s.HotKeys()
	for _, k := range s.PhysicalKeys(hotKey) {
		res.suffixParts = append(res.suffixParts, table.PartitionFor(k))
	}
	return res
}

func show(title string, res result) {
	fmt.Println(title)
	var top uint64
	for _, p := range res.parts {
		top = max(top, p.Writes+p.Throttled)
	}
	for i, p := range res.parts {
		bar := strings.Repeat("█", int(p.Writes*30/top)) + strings.Repeat("░", int(p.Throttled*30/top))
		fmt.Printf("   partition %d: %6d writes (%4d/s) %6d throttled  %s\n", i, p.Writes, p.Writes/seconds, p.Throttled, bar)
	}
	st := res.stats
	fmt.Printf("   throttled: %d of %d writes (%.1f%%); hot post lost %d of %d likes (%.1f%%)\n",
		st.Throttled, st.Writes, pct(st.Throttled, st.Writes), res.hotRejected, res.hotAttempts,
		pct(uint64(res.hotRejected), uint64(res.hotAttempts)))
	fmt.Printf("   read cost: hot post %d lookup(s), ordinary post %d lookup(s); reading all %d posts took %d lookups\n",
		res.hotFanout, res.coldFanout, coldKeys+1, uint64(res.hotFanout)+res.coldLookups)
	check := "matches"
	if res.readTotal != res.accepted {
		check = "DOES NOT match"
	}
	fmt.Printf("   merged reads total %d likes, which %s the %d accepted writes\n", res.readTotal, check, res.accepted)
}

func pct(a, b uint64) float64 {
	if b == 0 {
		return 0
	}
	return 100 * float64(a) / float64(b)
}

func main() {
	fmt.Printf("%d partitions x %d writes/s = %d writes/s of table capacity\n", partitions, capacity, partitions*capacity)
	fmt.Printf("workload: %d writes/s to %q + %d writes/s over %d other posts, for %ds\n\n",
		hotPerSec, hotKey, coldPerSec, coldKeys, seconds)

	show("1) No salting: the viral post hashes to one partition, which can take only 1000 writes/s",
		run(hks.WithMode(hks.Unsalted)))
	fmt.Println()

	all := run(hks.WithMode(hks.SaltAll))
	show(fmt.Sprintf("2) Salt every key with K=%d: writes spread out, but every read fans out to %d lookups", shards, shards), all)
	fmt.Printf("   the hot post's %d suffixes hash to partitions %v: suffixes are hashed like any key, so two\n", shards, all.suffixParts)
	fmt.Println("   can share a partition and one can get none. That is why the hot partition still runs hotter.")
	fmt.Println()

	res := run(hks.WithMode(hks.Adaptive), hks.WithDetector(capacity/2, time.Second))
	show(fmt.Sprintf("3) Adaptive: salt a key only after %d writes within 1s", capacity/2), res)
	fmt.Printf("   detector promoted %d key(s): %v. Its first writes went to the unsalted key, which is suffix 0,\n", res.stats.Promotions, res.hotKeys)
	fmt.Println("   so reads after promotion still include them. The few throttled writes are the detection lag:")
	fmt.Println("   in the first second the key's first 500 writes all hit one partition before it was salted.")
}
