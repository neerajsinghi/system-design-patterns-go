// Package outoforder simulates the "order events arrive out of sequence"
// incident. The order service publishes OrderCreated, PaymentCaptured and
// OrderShipped for every order. The fulfilment service consumes them with a
// pool of workers, and each order follows a strict state machine:
// created, then paid, then shipped. A notification step emails the customer
// when OrderShipped is handled.
//
// Two things scramble the order. The topic spreads events over partitions
// round-robin, so an order's three events land on different workers and
// whichever worker is quicker wins. And when a publish from the primary
// region times out, a second region retries it, so that event reaches the
// broker tens of milliseconds after the events that followed it.
//
// Broken applies events in whatever order the worker pool finishes them, and
// ignores sequence numbers. Fixed partitions by order ID (one worker owns an
// order) and runs a resequencer on each partition that uses the producer's
// per-order sequence numbers to undo upstream reordering, with a gap timeout
// so a lost event can't stall an order forever.
package outoforder

import (
	"fmt"
	"io"
	"math/rand/v2"
	"sort"
	"strings"
	"time"

	perkeyordering "github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/per-key-ordering"
)

const (
	Orders        = 300
	Workers       = 8                     // consumer workers (partitions)
	OrderEvery    = 3 * time.Millisecond  // a new order starts every 3ms
	RetryPercent  = 15                    // % of publishes that time out and are retried from the second region
	MinRetryDelay = 20 * time.Millisecond // how late a retried event reaches the broker
	MaxRetryDelay = 60 * time.Millisecond
	GapTimeout    = 100 * time.Millisecond // how long the resequencer waits for a missing event
	TickEvery     = 5 * time.Millisecond   // how often the consumer checks for expired gaps
	RepairCost    = 10 * time.Millisecond  // reading a skipped event's state back from the order service
	// LostOrder is the order whose PaymentCaptured is never delivered in the
	// cost run.
	LostOrder = "order-0150"
)

// The events of an order, in sequence. An event's Seq is its index + 1.
var eventTypes = []string{"OrderCreated", "PaymentCaptured", "OrderShipped"}

// States an order moves through. Event i moves an order from state i to i+1.
var stateNames = []string{"new", "created", "paid", "shipped"}

const (
	statePaid    = 2
	stateShipped = 3
)

// Report is what a run measured.
type Report struct {
	Name       string
	Partitions int
	Events     int
	// Retried is how many events were re-published from the second region.
	Retried int
	// UpstreamReorders counts events that reached the broker after a later
	// event of the same order. The traffic is identical for every run.
	UpstreamReorders int

	// InvalidTransitions counts events the state machine rejected (for
	// example OrderShipped for an order that is only created).
	InvalidTransitions int
	// UnpaidShippedEmails counts "your order has shipped" emails sent while
	// the order's payment had not been applied yet.
	UnpaidShippedEmails int
	// WrongFinalStates counts orders that did not end as shipped.
	WrongFinalStates int
	FinalStates      map[string]int
	FirstInvalid     string

	// Finish is when the last event was applied; MaxLag is the longest
	// time between an event reaching the broker and being applied.
	Finish, MaxLag time.Duration

	// Resequencer figures (Fixed only).
	Held                          int // events the resequencer had to hold for a predecessor
	MeanHoldWait, MaxHoldWait     time.Duration
	Gaps, Repairs                 int
	PartitionOnlyInvalid          int     // invalid transitions with key partitioning but no resequencer
	Global                        *Report // the fixed design on a single partition (one global consumer)
	LostEvent                     *LostEventCost
	maxHoldWaitExcluding          time.Duration // max hold wait over orders other than LostOrder
	lostGapWait, lostShippedDelay time.Duration
}

// LostEventCost is the fix's cost when an event never arrives: the order
// waits for the gap timeout, everyone else carries on.
type LostEventCost struct {
	Order string
	// GapWait is how long the resequencer held the order before skipping
	// the missing PaymentCaptured.
	GapWait time.Duration
	// ShippedDelay is how long the order's OrderShipped took from the broker
	// to being applied.
	ShippedDelay time.Duration
	// OthersMaxHoldWait is the longest hold of any other order in the same
	// run, and NormalMaxHoldWait the longest hold in the run with no loss.
	OthersMaxHoldWait, NormalMaxHoldWait time.Duration
	InvalidTransitions, WrongFinalStates int
	Repairs                              int
}

// Print writes the results.
func (r Report) Print(w io.Writer) {
	fmt.Fprintf(w, "%s\n", r.Name)
	fmt.Fprintf(w, "%d orders × 3 events (%d events), a new order every %v, %d workers, 1-9ms per event\n",
		Orders, r.Events, OrderEvery, r.Partitions)
	fmt.Fprintf(w, "%d events (%d%%) retried from the second region, %v-%v late; %d reached the broker after a later event of their order\n\n",
		r.Retried, RetryPercent, MinRetryDelay, MaxRetryDelay, r.UpstreamReorders)

	fmt.Fprintf(w, "  invalid state transitions:        %d\n", r.InvalidTransitions)
	if r.FirstInvalid != "" {
		fmt.Fprintf(w, "    first one: %s\n", r.FirstInvalid)
	}
	fmt.Fprintf(w, "  'shipped' emails for unpaid orders: %d\n", r.UnpaidShippedEmails)
	fmt.Fprintf(w, "  orders not in their final state:  %d of %d  (%s)\n", r.WrongFinalStates, Orders, r.finalStates())
	fmt.Fprintf(w, "  last event applied at %v; longest broker-to-applied lag %v\n",
		r.Finish.Round(time.Millisecond), r.MaxLag.Round(time.Millisecond))

	if r.Global != nil {
		fmt.Fprintf(w, "\n  parallelism kept: the same design on one global consumer is also correct (%d invalid), but finishes at %v with a %v lag\n",
			r.Global.InvalidTransitions, r.Global.Finish.Round(time.Millisecond), r.Global.MaxLag.Round(time.Millisecond))
		fmt.Fprintf(w, "  key partitioning without the resequencer: %d invalid transitions, all from the second-region retries\n", r.PartitionOnlyInvalid)
		fmt.Fprintf(w, "  resequencer: held %d events for a predecessor, mean %v, max %v; gaps %d\n",
			r.Held, r.MeanHoldWait.Round(100*time.Microsecond), r.MaxHoldWait.Round(100*time.Microsecond), r.Gaps)
	}
	if c := r.LostEvent; c != nil {
		fmt.Fprintf(w, "\n  cost run: %s's PaymentCaptured is never delivered\n", c.Order)
		fmt.Fprintf(w, "    the order waited %v (gap timeout %v, checked every %v), then the gap was skipped and paid was read back from the order service\n",
			c.GapWait.Round(100*time.Microsecond), GapTimeout, TickEvery)
		fmt.Fprintf(w, "    its OrderShipped was applied %v after reaching the broker; invalid %d, wrong final states %d, repairs %d\n",
			c.ShippedDelay.Round(100*time.Microsecond), c.InvalidTransitions, c.WrongFinalStates, c.Repairs)
		fmt.Fprintf(w, "    every other order: longest hold %v (with no event lost: %v), so the gap held back only its own order\n",
			c.OthersMaxHoldWait.Round(100*time.Microsecond), c.NormalMaxHoldWait.Round(100*time.Microsecond))
	}
}

func (r Report) finalStates() string {
	var parts []string
	for i := len(stateNames) - 1; i >= 0; i-- {
		if n := r.FinalStates[stateNames[i]]; n > 0 {
			parts = append(parts, fmt.Sprintf("%s %d", stateNames[i], n))
		}
	}
	return strings.Join(parts, ", ")
}

// delivery is one event as it reaches the broker.
type delivery struct {
	ev       perkeyordering.Event
	produced time.Duration // when the order service published it
	arrival  time.Duration // when it reached the broker
	retried  bool
	service  time.Duration // how long a worker takes to handle it
}

// traffic returns the seeded event stream in broker-arrival order. Every run
// calls it and gets identical events, timings and retries.
func traffic() []delivery {
	rng := rand.New(rand.NewPCG(2026, 1028))
	var ds []delivery
	for i := range Orders {
		key := fmt.Sprintf("order-%04d", i)
		t := time.Duration(i) * OrderEvery
		for s, typ := range eventTypes {
			if s > 0 {
				t += time.Duration(2+rng.IntN(7)) * time.Millisecond // 2-8ms after the previous event
			}
			d := delivery{
				ev:       perkeyordering.Event{Key: key, Seq: uint64(s + 1), Type: typ},
				produced: t,
				arrival:  t,
				service:  time.Duration(1+rng.IntN(9)) * time.Millisecond,
			}
			if rng.IntN(100) < RetryPercent {
				d.retried = true
				d.arrival += MinRetryDelay + time.Duration(rng.Int64N(int64(MaxRetryDelay-MinRetryDelay)))
			}
			ds = append(ds, d)
		}
	}
	sort.SliceStable(ds, func(a, b int) bool { return ds[a].arrival < ds[b].arrival })
	return ds
}

// work is one thing a partition's worker does: apply an event, or repair a
// skipped gap by reading the order's state from the order service.
type work struct {
	partition int
	ready     time.Duration // when it was handed to the worker
	service   time.Duration
	finish    time.Duration
	arrival   time.Duration // when the event reached the broker (events only)
	ev        perkeyordering.Event
	gap       *perkeyordering.Gap
}

// process runs every partition's worker over its FIFO queue, one item at a
// time, and returns the items in the order they finish (the order their
// effects are applied) plus the time the last one finishes.
func process(items []work, partitions int) ([]work, time.Duration) {
	free := make([]time.Duration, partitions)
	for i := range items {
		it := &items[i]
		start := max(free[it.partition], it.ready)
		it.finish = start + it.service
		free[it.partition] = it.finish
	}
	sort.SliceStable(items, func(a, b int) bool { return items[a].finish < items[b].finish })
	end := time.Duration(0)
	for _, f := range free {
		end = max(end, f)
	}
	return items, end
}

// consumer is the fulfilment service's state machine and notification step.
// Both designs use exactly this code.
type consumer struct {
	state                         map[string]int
	invalid, unpaidEmails, repair int
	firstInvalid                  string
}

func newConsumer() *consumer { return &consumer{state: make(map[string]int)} }

func (c *consumer) apply(e perkeyordering.Event) {
	cur := c.state[e.Key]
	if e.Type == "OrderShipped" && cur != statePaid {
		// The notification step emails on OrderShipped as soon as it
		// handles it.
		c.unpaidEmails++
	}
	if int(e.Seq) != cur+1 {
		c.invalid++
		if c.firstInvalid == "" {
			c.firstInvalid = fmt.Sprintf("%s: %s rejected, order is still %q", e.Key, e.Type, stateNames[cur])
		}
		return // rejected and dropped
	}
	c.state[e.Key] = cur + 1
}

// fill copies the consumer's outcome into r.
func (c *consumer) fill(r *Report, applied []work, end time.Duration) {
	r.InvalidTransitions, r.UnpaidShippedEmails, r.Repairs = c.invalid, c.unpaidEmails, c.repair
	r.FirstInvalid = c.firstInvalid
	r.FinalStates = map[string]int{}
	for i := range Orders {
		s := c.state[fmt.Sprintf("order-%04d", i)]
		r.FinalStates[stateNames[s]]++
		if s != stateShipped {
			r.WrongFinalStates++
		}
	}
	r.Finish = end
	for _, it := range applied {
		if it.gap == nil {
			r.MaxLag = max(r.MaxLag, it.finish-it.arrival)
		}
	}
}

func newReport(name string, partitions int, ds []delivery) Report {
	r := Report{Name: name, Partitions: partitions, Events: len(ds)}
	maxSeq := map[string]uint64{}
	for _, d := range ds {
		if d.retried {
			r.Retried++
		}
		if d.ev.Seq < maxSeq[d.ev.Key] {
			r.UpstreamReorders++
		}
		maxSeq[d.ev.Key] = max(maxSeq[d.ev.Key], d.ev.Seq)
	}
	return r
}

// applyAsTheyCome hands every event to a worker as soon as it reaches the
// broker and applies it when the worker finishes. assign picks the worker
// for the k-th arrival.
func applyAsTheyCome(name string, assign func(k int, e perkeyordering.Event) int) Report {
	ds := traffic()
	r := newReport(name, Workers, ds)
	items := make([]work, len(ds))
	for k, d := range ds {
		items[k] = work{partition: assign(k, d.ev), ready: d.arrival, service: d.service, arrival: d.arrival, ev: d.ev}
	}
	applied, end := process(items, Workers)
	c := newConsumer()
	for _, it := range applied {
		c.apply(it.ev)
	}
	c.fill(&r, applied, end)
	return r
}

// Broken: the topic spreads events round-robin over 8 partitions, each with
// its own worker, and the consumer applies events in whatever order they
// finish. Sequence numbers are ignored.
func Broken() Report {
	return applyAsTheyCome("BROKEN: round-robin partitions + worker pool, events applied as they come",
		func(k int, _ perkeyordering.Event) int { return k % Workers })
}

// resequenced runs the fixed design on the given number of partitions. If
// lose is non-empty, that order's PaymentCaptured is never delivered.
func resequenced(name string, partitions int, lose string) Report {
	all := traffic()
	r := newReport(name, partitions, all)
	ds := all[:0:0]
	for _, d := range all {
		if d.ev.Key == lose && d.ev.Type == "PaymentCaptured" {
			continue
		}
		ds = append(ds, d)
	}

	var now time.Duration
	base := time.Unix(0, 0)
	rs, err := perkeyordering.NewResequencer(GapTimeout,
		perkeyordering.WithClock(func() time.Time { return base.Add(now) }),
		perkeyordering.WithMaxBuffer(8))
	if err != nil {
		panic(err)
	}
	type info struct{ arrival, service time.Duration }
	arrived := make(map[perkeyordering.Event]info, len(ds))
	var items []work
	var totalHold time.Duration
	collect := func(out []perkeyordering.Item) {
		for _, it := range out {
			if it.Gap != nil {
				g := *it.Gap
				r.Gaps++
				if g.Key == lose {
					r.lostGapWait = g.Waited
				}
				items = append(items, work{partition: perkeyordering.PartitionFor(g.Key, partitions), ready: now, service: RepairCost, gap: &g})
				continue
			}
			in := arrived[it.Event]
			if hold := now - in.arrival; hold > 0 {
				r.Held++
				totalHold += hold
				r.MaxHoldWait = max(r.MaxHoldWait, hold)
				if it.Event.Key != lose {
					r.maxHoldWaitExcluding = max(r.maxHoldWaitExcluding, hold)
				}
			}
			items = append(items, work{partition: perkeyordering.PartitionFor(it.Event.Key, partitions),
				ready: now, service: in.service, arrival: in.arrival, ev: it.Event})
		}
	}
	nextTick := TickEvery
	for _, d := range ds {
		for nextTick <= d.arrival {
			now = nextTick
			collect(rs.Tick())
			nextTick += TickEvery
		}
		now = d.arrival
		arrived[d.ev] = info{d.arrival, d.service}
		out, err := rs.Offer(d.ev)
		if err != nil {
			panic(err)
		}
		collect(out)
	}
	for rs.Stats().Buffered > 0 {
		now = nextTick
		collect(rs.Tick())
		nextTick += TickEvery
	}
	if r.Held > 0 {
		r.MeanHoldWait = totalHold / time.Duration(r.Held)
	}

	applied, end := process(items, partitions)
	c := newConsumer()
	for _, it := range applied {
		if it.gap != nil {
			// The events in the hole are gone from the stream, so read
			// what they did back from the order service (the source of
			// truth) and apply that before carrying on.
			for s := it.gap.First; s <= it.gap.Last; s++ {
				c.apply(perkeyordering.Event{Key: it.gap.Key, Seq: s, Type: eventTypes[s-1]})
				c.repair++
			}
			continue
		}
		c.apply(it.ev)
		if it.ev.Key == lose && it.ev.Type == "OrderShipped" {
			r.lostShippedDelay = it.finish - it.arrival
		}
	}
	c.fill(&r, applied, end)
	return r
}

// Fixed: events are partitioned by order ID, so one worker owns each order,
// and each partition puts a resequencer in front of its worker that releases
// an order's events strictly by their producer-assigned sequence number. A
// missing number is waited for at most GapTimeout, then skipped and repaired
// from the order service. The report also runs the same design on one
// partition (a single global consumer), key partitioning without the
// resequencer, and the same traffic with one event lost.
func Fixed() Report {
	r := resequenced("FIXED: partition by order ID + per-order sequence numbers + resequencer (100ms gap timeout)", Workers, "")
	g := resequenced("single global consumer + resequencer", 1, "")
	r.Global = &g
	r.PartitionOnlyInvalid = applyAsTheyCome("", func(_ int, e perkeyordering.Event) int {
		return perkeyordering.PartitionFor(e.Key, Workers)
	}).InvalidTransitions
	lost := resequenced("", Workers, LostOrder)
	r.LostEvent = &LostEventCost{
		Order:              LostOrder,
		GapWait:            lost.lostGapWait,
		ShippedDelay:       lost.lostShippedDelay,
		OthersMaxHoldWait:  lost.maxHoldWaitExcluding,
		NormalMaxHoldWait:  r.MaxHoldWait,
		InvalidTransitions: lost.InvalidTransitions,
		WrongFinalStates:   lost.WrongFinalStates,
		Repairs:            lost.Repairs,
	}
	return r
}
