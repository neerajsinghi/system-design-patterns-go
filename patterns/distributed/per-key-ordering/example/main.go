// Command example simulates an order-events stream (created, paid, packed,
// shipped) and counts how often a consumer applies an order's events out of
// sequence. It compares a single consumer, a round-robin worker pool and
// key-partitioned workers, then lets the network reorder delivery and repairs
// it with a resequencer, and finally loses some events to show the gap
// timeout. Time is simulated, so the run is instant and repeatable.
package main

import (
	"fmt"
	"log"
	"math/rand/v2"
	"sort"
	"time"

	perkeyordering "github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/per-key-ordering"
)

const (
	orders     = 300
	workers    = 8
	orderEvery = 3 * time.Millisecond  // a new order starts every 3ms
	maxNetwork = 40 * time.Millisecond // step 3: delivery delay is 0-40ms per event
	gapTimeout = 100 * time.Millisecond
	tickEvery  = 5 * time.Millisecond
)

type event struct {
	perkeyordering.Event
	produced time.Duration // when the producer emitted it
	service  time.Duration // how long a worker takes to handle it
}

// generate builds the stream in producer order. Each order's events are 1-6ms
// apart, so events of neighbouring orders interleave.
func generate(rng *rand.Rand) []event {
	var evs []event
	for i := range orders {
		key := fmt.Sprintf("order-%04d", i)
		types := []string{"created", "paid", "shipped"}
		if rng.IntN(2) == 0 {
			types = []string{"created", "paid", "packed", "shipped"}
		}
		t := time.Duration(i) * orderEvery
		for s, typ := range types {
			if s > 0 {
				t += time.Duration(1+rng.IntN(6)) * time.Millisecond
			}
			evs = append(evs, event{
				Event:    perkeyordering.Event{Key: key, Seq: uint64(s + 1), Type: typ},
				produced: t,
				service:  time.Duration(1+rng.IntN(9)) * time.Millisecond,
			})
		}
	}
	sort.SliceStable(evs, func(a, b int) bool { return evs[a].produced < evs[b].produced })
	return evs
}

// violations counts events applied after a later event of the same order,
// and how many orders were affected; it also returns one example.
func violations(applied []perkeyordering.Event) (count, ordersHit int, example string) {
	maxSeq := map[string]uint64{}
	maxType := map[string]string{}
	hit := map[string]bool{}
	for _, e := range applied {
		if e.Seq < maxSeq[e.Key] {
			count++
			if !hit[e.Key] && example == "" {
				example = fmt.Sprintf("%s applied %q before %q", e.Key, maxType[e.Key], e.Type)
			}
			hit[e.Key] = true
			continue
		}
		maxSeq[e.Key], maxType[e.Key] = e.Seq, e.Type
	}
	return count, len(hit), example
}

// runWorkers simulates workers that each handle their own FIFO queue one
// event at a time. assign picks the worker for the k-th event (in arrival
// order). It returns the events in the order they finish (i.e. are applied)
// and the time the last one finishes.
func runWorkers(evs []event, arrival func(event) time.Duration, n int, assign func(k int, e event) int) ([]perkeyordering.Event, time.Duration) {
	type done struct {
		at time.Duration
		k  int
	}
	order := make([]int, len(evs))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool { return arrival(evs[order[a]]) < arrival(evs[order[b]]) })
	free := make([]time.Duration, n)
	finished := make([]done, 0, len(evs))
	for k, i := range order {
		w := assign(k, evs[i])
		start := max(free[w], arrival(evs[i]))
		free[w] = start + evs[i].service
		finished = append(finished, done{free[w], i})
	}
	sort.SliceStable(finished, func(a, b int) bool { return finished[a].at < finished[b].at })
	out := make([]perkeyordering.Event, len(finished))
	for j, d := range finished {
		out[j] = evs[d.k].Event
	}
	return out, finished[len(finished)-1].at
}

type resequenced struct {
	released            []perkeyordering.Event
	gaps                []perkeyordering.Gap
	held, maxHeldPerKey int
	maxWait             time.Duration // longest time an event sat in the buffer
	maxWaitUnaffected   time.Duration // same, only for orders that lost nothing
	stats               perkeyordering.Stats
}

// resequence feeds deliveries (in arrival order) through a Resequencer on a
// simulated clock, ticking every tickEvery.
func resequence(evs []event, arrival map[int]time.Duration, affected map[string]bool) resequenced {
	var now time.Duration
	base := time.Unix(0, 0)
	rs, err := perkeyordering.NewResequencer(gapTimeout,
		perkeyordering.WithClock(func() time.Time { return base.Add(now) }),
		perkeyordering.WithMaxBuffer(8))
	if err != nil {
		log.Fatal(err)
	}
	idx := make([]int, 0, len(arrival))
	for i := range arrival {
		idx = append(idx, i)
	}
	sort.Slice(idx, func(a, b int) bool {
		if arrival[idx[a]] != arrival[idx[b]] {
			return arrival[idx[a]] < arrival[idx[b]]
		}
		return idx[a] < idx[b]
	})
	arrivedAt := map[perkeyordering.Event]time.Duration{}
	var r resequenced
	collect := func(items []perkeyordering.Item) {
		for _, it := range items {
			if it.Gap != nil {
				r.gaps = append(r.gaps, *it.Gap)
				continue
			}
			r.released = append(r.released, it.Event)
			w := now - arrivedAt[it.Event]
			r.maxWait = max(r.maxWait, w)
			if !affected[it.Event.Key] {
				r.maxWaitUnaffected = max(r.maxWaitUnaffected, w)
			}
		}
		if b := rs.Stats().Buffered; b > r.held {
			r.held = b
		}
	}
	nextTick := tickEvery
	for _, i := range idx {
		for nextTick <= arrival[i] {
			now = nextTick
			collect(rs.Tick())
			nextTick += tickEvery
		}
		now = arrival[i]
		arrivedAt[evs[i].Event] = now
		items, err := rs.Offer(evs[i].Event)
		if err != nil {
			log.Fatal(err)
		}
		collect(items)
	}
	for rs.Stats().Buffered > 0 {
		now = nextTick
		collect(rs.Tick())
		nextTick += tickEvery
	}
	r.stats = rs.Stats()
	return r
}

func main() {
	rng := rand.New(rand.NewPCG(42, 2026))
	evs := generate(rng)
	produced := func(e event) time.Duration { return e.produced }
	fmt.Printf("%d orders, %d events (3-4 per order), a new order every %v, %d workers, 1-9ms per event\n\n",
		orders, len(evs), orderEvery, workers)

	fmt.Println("1) Delivery is in order; how the consumer side is parallelised decides what it sees")
	fmt.Println("   consumer                    violations  orders hit  all done at")
	single, singleEnd := runWorkers(evs, produced, 1, func(int, event) int { return 0 })
	rr, rrEnd := runWorkers(evs, produced, workers, func(k int, _ event) int { return k % workers })
	byKey, keyEnd := runWorkers(evs, produced, workers, func(_ int, e event) int {
		return perkeyordering.PartitionFor(e.Key, workers)
	})
	var rrExample string
	for _, row := range []struct {
		name    string
		applied []perkeyordering.Event
		end     time.Duration
	}{
		{"single consumer (global)", single, singleEnd},
		{"round-robin pool of 8", rr, rrEnd},
		{"8 key partitions", byKey, keyEnd},
	} {
		v, hit, ex := violations(row.applied)
		if rrExample == "" {
			rrExample = ex
		}
		fmt.Printf("   %-26s  %10d  %10d  %9v\n", row.name, v, hit, row.end.Round(time.Millisecond))
	}
	fmt.Printf("   e.g. round-robin: %s\n", rrExample)
	fmt.Println("   key partitions keep order and finish almost as fast as round-robin; one global consumer is ~5x slower")
	perPartition := make([]int, workers)
	for _, e := range evs {
		perPartition[perkeyordering.PartitionFor(e.Key, workers)]++
	}
	fmt.Printf("   events per key partition: %v (close to even; one hot key would pile onto a single partition)\n\n", perPartition)

	fmt.Println("2) The same stream through the real goroutine Dispatcher (8 partitions)")
	last := make([]map[string]uint64, workers)
	bad := make([]int, workers)
	for p := range last {
		last[p] = map[string]uint64{}
	}
	d, err := perkeyordering.NewDispatcher(workers, func(p int, e perkeyordering.Event) {
		if e.Seq < last[p][e.Key] {
			bad[p]++
		}
		last[p][e.Key] = e.Seq
	})
	if err != nil {
		log.Fatal(err)
	}
	for _, e := range evs {
		if err := d.Submit(e.Event); err != nil {
			log.Fatal(err)
		}
	}
	d.Close()
	total := 0
	for _, b := range bad {
		total += b
	}
	fmt.Printf("   violations: %d (each order lives on one partition, handled by one goroutine)\n\n", total)

	fmt.Printf("3) Now the network reorders delivery (each event delayed 0-%v); partitioning alone can't fix that\n", maxNetwork)
	arrival := make(map[int]time.Duration, len(evs))
	for i, e := range evs {
		arrival[i] = e.produced + time.Duration(rng.Int64N(int64(maxNetwork)))
	}
	byArrival := make([]event, len(evs))
	copy(byArrival, evs)
	for i := range byArrival {
		byArrival[i].produced = arrival[i] // reuse runWorkers with arrival as the start time
	}
	reordered, _ := runWorkers(byArrival, produced, workers, func(_ int, e event) int {
		return perkeyordering.PartitionFor(e.Key, workers)
	})
	v, hit, ex := violations(reordered)
	fmt.Printf("   8 key partitions, applied in arrival order: %d violations in %d orders (e.g. %s)\n", v, hit, ex)
	r := resequence(evs, arrival, nil)
	v, hit, _ = violations(r.released)
	fmt.Printf("   + resequencer (per-key seq numbers):         %d violations in %d orders\n", v, hit)
	fmt.Printf("   cost: up to %d events held at once, an event waited at most %v for its predecessor, %d gaps\n\n",
		r.held, r.maxWait.Round(100*time.Microsecond), len(r.gaps))

	fmt.Printf("4) Some events never arrive; the resequencer waits %v per gap, then skips and reports it\n", gapTimeout)
	// Lose the "paid" event of four orders, and deliver a fifth one 300ms late.
	lossy := make(map[int]time.Duration, len(arrival))
	for i, t := range arrival {
		lossy[i] = t
	}
	affected := map[string]bool{}
	var lateKey string
	picked := 0
	for i, e := range evs {
		if e.Type != "paid" || rng.IntN(40) != 0 || picked == 5 {
			continue
		}
		affected[e.Key] = true
		if picked == 4 {
			lossy[i] = arrival[i] + 300*time.Millisecond
			lateKey = e.Key
		} else {
			delete(lossy, i)
		}
		picked++
	}
	r = resequence(evs, lossy, affected)
	for _, g := range r.gaps {
		note := "lost"
		if g.Key == lateKey {
			note = "arrives 300ms late, after the skip"
		}
		fmt.Printf("   gap: %s seq %d (paid) skipped after waiting %v  [%s]\n", g.Key, g.First, g.Waited.Round(100*time.Microsecond), note)
	}
	v, _, _ = violations(r.released)
	fmt.Printf("   released %d events, %d violations; the late %q was ignored (duplicates counter: %d)\n",
		r.stats.Released, v, "paid", r.stats.Duplicates)
	fmt.Printf("   longest buffer wait: %v for the %d orders with a gap, %v for the other %d orders\n",
		r.maxWait.Round(100*time.Microsecond), len(affected), r.maxWaitUnaffected.Round(100*time.Microsecond), orders-len(affected))
	fmt.Printf("   (waits run a little past %v because expiry is checked on a %v tick; a gap only holds back its own order)\n", gapTimeout, tickEvery)
}
