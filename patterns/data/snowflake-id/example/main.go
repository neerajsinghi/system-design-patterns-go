// Command example walks through Snowflake IDs: four nodes generating at once
// with no collisions, decoding an ID, a burst that overflows the 12-bit
// sequence, a clock that steps backwards, how well IDs sort across nodes with
// skewed clocks, and how much coordination a central counter needs instead.
//
// Every clock is simulated and advances a fixed amount each time it is read,
// so the numbers are the same on every run.
package main

import (
	"cmp"
	"errors"
	"fmt"
	"log"
	"slices"
	"sync"
	"time"

	snowflakeid "github.com/neerajsinghi/system-design-patterns-go/patterns/data/snowflake-id"
)

// pollClock moves forward by step on every read. A generator waiting for the
// next millisecond keeps reading it, so it always gets there.
type pollClock struct {
	mu    sync.Mutex
	t     time.Time
	step  time.Duration
	reads int
}

func (c *pollClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.t
	c.t = c.t.Add(c.step)
	c.reads++
	return t
}

func (c *pollClock) set(t time.Time) { c.mu.Lock(); c.t = t; c.mu.Unlock() }
func (c *pollClock) readCount() int  { c.mu.Lock(); defer c.mu.Unlock(); return c.reads }

var start = time.Date(2026, time.October, 28, 12, 0, 0, 0, time.UTC)

func must(g *snowflakeid.Generator, err error) *snowflakeid.Generator {
	if err != nil {
		log.Fatal(err)
	}
	return g
}

func main() {
	layout()
	concurrentNodes()
	decode()
	overflowBurst()
	clockBackwards()
	orderingAcrossNodes()
	centralCounter()
}

func layout() {
	g := must(snowflakeid.New(0))
	fmt.Println("1) Layout: 1 sign bit | 41 bits ms since epoch | 10 bits node | 12 bits sequence")
	fmt.Printf("   up to %d nodes x %d IDs per ms = %.1f million IDs/s per node\n",
		snowflakeid.MaxNode+1, snowflakeid.MaxSequence+1, float64(snowflakeid.MaxSequence+1)*1000/1e6)
	fmt.Printf("   2^41 ms = %.1f years: epoch %s runs out on %s\n",
		snowflakeid.Lifetime.Hours()/24/365.25, g.Epoch().Format("2006-01-02"), g.EndOfLife().Format("2006-01-02 15:04 MST"))
	twitter := time.UnixMilli(1288834974657).UTC()
	fmt.Printf("   (Twitter's epoch %s ends %s; an epoch of 1970 would already end in %s)\n\n",
		twitter.Format("2006-01-02"), twitter.Add(snowflakeid.Lifetime).Format("2006-01-02"),
		time.Unix(0, 0).UTC().Add(snowflakeid.Lifetime).Format("2006"))
}

func concurrentNodes() {
	const nodes, perNode = 4, 50000
	fmt.Printf("2) %d nodes generate %d IDs each at the same time, with no shared state\n", nodes, perNode)
	results := make([][]snowflakeid.ID, nodes)
	var wg sync.WaitGroup
	for n := range nodes {
		// All node clocks show the same time; each read moves it 10µs,
		// so a node makes ~100 IDs per millisecond.
		clk := &pollClock{t: start, step: 10 * time.Microsecond}
		g := must(snowflakeid.New(int64(n), snowflakeid.WithClock(clk.Now)))
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perNode {
				id, err := g.Next()
				if err != nil {
					log.Fatal(err)
				}
				results[n] = append(results[n], id)
			}
		}()
	}
	wg.Wait()

	all := slices.Concat(results...)
	nodesPerMs := map[int64]map[int64]bool{}
	for _, id := range all {
		if nodesPerMs[id.Millis()] == nil {
			nodesPerMs[id.Millis()] = map[int64]bool{}
		}
		nodesPerMs[id.Millis()][id.Node()] = true
	}
	shared := 0
	for _, ns := range nodesPerMs {
		if len(ns) == nodes {
			shared++
		}
	}
	slices.Sort(all)
	total := len(all)
	collisions := total - len(slices.Compact(all))
	fmt.Printf("   %d IDs, %d collisions; %d of %d milliseconds had IDs from all %d nodes\n",
		total, collisions, shared, len(nodesPerMs), nodes)
	fmt.Println("   same millisecond on every node, still unique: the node bits keep them apart")
	fmt.Println()
}

func decode() {
	clk := &pollClock{t: start.Add(1234 * time.Millisecond), step: 0}
	g := must(snowflakeid.New(42, snowflakeid.WithClock(clk.Now)))
	var id snowflakeid.ID
	for range 8 {
		id, _ = g.Next()
	}
	parsed, err := snowflakeid.Parse(id.String())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("3) Decoding one ID (the 8th ID node 42 made in that millisecond)")
	fmt.Printf("   id     %s  (binary %064b)\n", id, uint64(id))
	fmt.Printf("   millis %d since epoch -> %s\n", parsed.Millis(), parsed.Time().Format("2006-01-02 15:04:05.000 MST"))
	fmt.Printf("   node   %d\n   seq    %d\n\n", parsed.Node(), parsed.Seq())
}

func overflowBurst() {
	const burst = 10000
	// 100ns per read = 10,000 reads per ms: far more requests in one ms than
	// the 4,096 sequence numbers available.
	clk := &pollClock{t: start, step: 100 * time.Nanosecond}
	g := must(snowflakeid.New(7, snowflakeid.WithClock(clk.Now)))
	perMs := map[int64]int{}
	var ms []int64
	for range burst {
		id, err := g.Next()
		if err != nil {
			log.Fatal(err)
		}
		if perMs[id.Millis()] == 0 {
			ms = append(ms, id.Millis())
		}
		perMs[id.Millis()]++
	}
	fmt.Printf("4) Burst: %d IDs requested from one node, faster than 4,096 per ms\n", burst)
	for i, m := range ms {
		fmt.Printf("   ms +%d: %d IDs\n", i, perMs[m])
	}
	s := g.Stats()
	fmt.Printf("   sequence ran out %d times; each time Next waited for the next ms (%d clock reads for %d IDs)\n\n",
		s.OverflowWaits, clk.readCount(), burst)
}

func clockBackwards() {
	fmt.Println("5) The clock steps backwards (tolerance 10ms)")
	clk := &pollClock{t: start, step: 250 * time.Microsecond}
	g := must(snowflakeid.New(3, snowflakeid.WithClock(clk.Now), snowflakeid.WithClockTolerance(10*time.Millisecond)))
	var last snowflakeid.ID
	for range 20 {
		last, _ = g.Next()
	}
	lastAt := last.TimeFrom(g.Epoch())

	clk.set(lastAt.Add(-4 * time.Millisecond))
	before := clk.readCount()
	id, err := g.Next()
	fmt.Printf("   NTP steps the clock back 4ms: Next waited %d clock reads (~%v) and returned %s, err=%v, later than the previous ID: %v\n",
		clk.readCount()-before, time.Duration(clk.readCount()-before)*clk.step, id, err, id > last)
	last = id

	clk.set(lastAt.Add(-2 * time.Second))
	_, err = g.Next()
	fmt.Printf("   VM restored, clock 2s behind: err=%q (is ErrClockMovedBackwards: %v)\n",
		err, errors.Is(err, snowflakeid.ErrClockMovedBackwards))

	clk.set(lastAt.Add(5 * time.Millisecond))
	id, err = g.Next()
	fmt.Printf("   clock caught up: %s, err=%v, later than every earlier ID: %v\n", id, err, id > last)
	s := g.Stats()
	fmt.Printf("   stats: %d generated, %d regressions waited out, %d refused\n\n",
		s.Generated, s.BackwardsWaits, s.BackwardsErrors)
}

// orderingAcrossNodes issues events in a known true order over four nodes
// whose clocks are off by the given skews, then sorts by ID and measures how
// far the ID order strays from the true order.
func orderingAcrossNodes() {
	const events = 4000
	fmt.Printf("6) Do IDs sort by creation time across nodes? %d events, one every 0.5ms, spread over 4 nodes\n", events)
	for _, skew := range [][]time.Duration{
		{0, 0, 0, 0},
		{0, time.Millisecond, -2 * time.Millisecond, 3 * time.Millisecond},
	} {
		trueNow := start
		gens := make([]*snowflakeid.Generator, len(skew))
		for n := range skew {
			off := skew[n]
			gens[n] = must(snowflakeid.New(int64(n), snowflakeid.WithClock(func() time.Time { return trueNow.Add(off) })))
		}
		type event struct {
			id   snowflakeid.ID
			when time.Duration
		}
		var evs []event
		for i := range events {
			when := time.Duration(i) * 500 * time.Microsecond
			trueNow = start.Add(when)
			// Visit nodes 0,3,2,1 so node order inside a millisecond is not
			// accidentally the same as creation order.
			id, err := gens[(i*3)%len(gens)].Next()
			if err != nil {
				log.Fatal(err)
			}
			evs = append(evs, event{id, when})
		}
		slices.SortFunc(evs, func(a, b event) int { return cmp.Compare(a.id, b.id) })
		// An event's displacement is how much earlier it was created than
		// the latest event sorted before it.
		var latest, worst time.Duration
		displaced, over1ms := 0, 0
		for _, e := range evs {
			if d := latest - e.when; d > 0 {
				displaced++
				if d > time.Millisecond {
					over1ms++
				}
				worst = max(worst, d)
			}
			latest = max(latest, e.when)
		}
		spread := slices.Max(skew) - slices.Min(skew)
		fmt.Printf("   skews %-18v (spread %v): %4d events sort after a later one, %4d by more than 1ms, worst %v\n",
			fmt.Sprint(skew), spread, displaced, over1ms, worst)
	}
	fmt.Println("   equal clocks: disorder only inside one millisecond, where node bits decide (< 1ms)")
	fmt.Println("   skewed clocks: an ID is ordered only to within the skew spread, never better than the clocks")
	fmt.Println()
}

// counterService is a single central sequence, like a database sequence or a
// Redis INCR that every node calls for every ID.
type counterService struct {
	next  int64
	calls int
	down  bool
}

func (c *counterService) Next() (int64, error) {
	c.calls++
	if c.down {
		return 0, errors.New("counter service unavailable")
	}
	c.next++
	return c.next, nil
}

func centralCounter() {
	const nodes, perNode = 4, 3000
	fmt.Printf("7) Coordination: %d nodes x %d IDs, the shared service is down for the middle third\n", nodes, perNode)

	central := &counterService{}
	var centralOK, centralFailed int
	for i := range perNode {
		central.down = i >= perNode/3 && i < 2*perNode/3
		for range nodes {
			if _, err := central.Next(); err != nil {
				centralFailed++
			} else {
				centralOK++
			}
		}
	}

	// Snowflake talks to a coordinator (config, ZooKeeper, etcd) once per
	// node at start-up to get a node ID, then never again.
	coordinator := &counterService{}
	clk := &pollClock{t: start, step: 10 * time.Microsecond}
	gens := make([]*snowflakeid.Generator, nodes)
	for n := range nodes {
		nodeID, err := coordinator.Next()
		if err != nil {
			log.Fatal(err)
		}
		gens[n] = must(snowflakeid.New(nodeID-1, snowflakeid.WithClock(clk.Now)))
	}
	var sfOK, sfFailed int
	for i := range perNode {
		coordinator.down = i >= perNode/3 && i < 2*perNode/3
		for _, g := range gens {
			if _, err := g.Next(); err != nil {
				sfFailed++
			} else {
				sfOK++
			}
		}
	}
	fmt.Printf("   central counter: %5d calls to the shared service, %5d IDs issued, %5d failed\n",
		central.calls, centralOK, centralFailed)
	fmt.Printf("   snowflake:       %5d calls to the shared service, %5d IDs issued, %5d failed\n",
		coordinator.calls, sfOK, sfFailed)
	fmt.Println("   the counter gives perfect order at the price of a round trip per ID and a single point of failure;")
	fmt.Println("   snowflake needs the coordinator only to hand out node IDs (a node restarting during the outage would wait)")
}
