// Package seatdoublebooking simulates the "ticketing site sells the same seat
// twice" incident. A popular show goes on sale and shoppers pile onto the
// best seats. The booking service reads the seat ("free"), takes the
// shopper's payment, and then writes "booked". When two shoppers read the
// seat before either writes, both see it free, both pay, and both get a
// ticket. Two other bugs ride along: a seat picked by a shopper who then
// abandons checkout stays locked for the rest of the sale, and the payment
// gateway's retried webhook makes the booking service capture the payment
// a second time.
//
// Broken locks seats with a read-then-write and no expiry, books with a
// blind write, and captures payment on every webhook delivery.
// Fixed takes a time-limited hold when a seat is picked
// (patterns/data/reservation-hold), makes the final booking a
// compare-and-set on the seat's version (patterns/data/optimistic-locking),
// and handles the payment webhook through an idempotency key
// (patterns/reliability/idempotency-key).
//
// The race is modelled explicitly rather than left to goroutine scheduling:
// shoppers arrive in rounds, and in every round all of them read the seat
// map before any of them writes. Traffic comes from a seeded generator, and
// time is a simulated clock, so every run prints the same numbers.
package seatdoublebooking

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"sort"
	"strings"
	"time"

	optimisticlocking "github.com/neerajsinghi/system-design-patterns-go/patterns/data/optimistic-locking"
	reservationhold "github.com/neerajsinghi/system-design-patterns-go/patterns/data/reservation-hold"
	idempotencykey "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/idempotency-key"
)

const (
	Rows             = 6
	SeatsPerRow      = 8
	Seats            = Rows * SeatsPerRow
	Rounds           = 20 // one round of arrivals per simulated minute
	ShoppersPerRound = 6
	Shoppers         = Rounds * ShoppersPerRound
	Preferences      = 3    // seats each shopper would accept, best first
	AbandonRate      = 0.2  // shoppers who pick a seat and never pay
	SlowPayRate      = 0.1  // payers who take 6-9 minutes instead of 1-3
	WebhookRetryRate = 0.25 // payments whose webhook the gateway delivers twice
	HoldTTL          = 5 * time.Minute
	WebhookRetry     = 30 * time.Second // gateway retries when it didn't see our 200 in time
	// FailoverAt is when, in the fixed design only, the hold store fails over
	// and loses every active hold (an async replica promoted without the
	// latest writes). It is there to show why the booking write needs its
	// own guard even though seats are held.
	FailoverAt = 12 * time.Minute
	End        = 45 * time.Minute
	seed1      = 7
	seed2      = 11
)

// Report is what a run measured.
type Report struct {
	Name string
	// SeatsSold is how many distinct seats have at least one ticket.
	SeatsSold int
	// TicketsIssued counts every ticket the service sent, duplicates included.
	TicketsIssued int
	// DoubleBooked lists seats ticketed to more than one customer, as
	// "A1: c003, c004".
	DoubleBooked []string
	// ChargedTwice counts customers whose payment was captured more than once.
	ChargedTwice int
	// SeatsStuck counts seats that, at the end of the sale, are neither sold
	// nor available to buy.
	SeatsStuck int
	// Abandoned counts shoppers who claimed a seat and never paid;
	// AbandonedResold counts their seats later sold to someone who arrived
	// after them.
	Abandoned, AbandonedResold int
	// SeatTaken counts shoppers who saw a free seat but lost the race for it
	// and were told so before paying.
	SeatTaken int
	// NoSeatShown counts shoppers for whom none of their seats looked free.
	NoSeatShown int
	// RefundsLate are payments returned because the hold expired before the
	// payment finished; RefundsLostRace are payments returned because the
	// booking compare-and-set found the seat already sold.
	RefundsLate, RefundsLostRace int
	// WebhookRetries counts duplicate webhook deliveries (same in both runs);
	// RetriesReplayed counts those answered from the idempotency store.
	WebhookRetries, RetriesReplayed int
	// HoldsReaped counts holds returned to sale by the reaper; HoldsLost
	// counts holds wiped by the hold-store failover.
	HoldsReaped, HoldsLost int
}

// Refunds is the total number of payments returned.
func (r Report) Refunds() int { return r.RefundsLate + r.RefundsLostRace }

// Print writes the summary.
func (r Report) Print(w io.Writer) {
	fmt.Fprintf(w, "%s\n", r.Name)
	fmt.Fprintf(w, "%d seats, %d shoppers in %d rounds of %d (each round, everyone reads the seat map before anyone writes)\n",
		Seats, Shoppers, Rounds, ShoppersPerRound)
	fmt.Fprintf(w, "%.0f%% abandon at checkout, %.0f%% pay slowly (6-9 min, hold TTL %v), %.0f%% of webhooks delivered twice\n\n",
		AbandonRate*100, SlowPayRate*100, HoldTTL, WebhookRetryRate*100)
	fmt.Fprintf(w, "  seats sold:                              %d of %d\n", r.SeatsSold, Seats)
	fmt.Fprintf(w, "  tickets issued:                          %d\n", r.TicketsIssued)
	fmt.Fprintf(w, "  seats double-booked:                     %d\n", len(r.DoubleBooked))
	for i, d := range r.DoubleBooked {
		if i == 5 {
			fmt.Fprintf(w, "      ... and %d more\n", len(r.DoubleBooked)-5)
			break
		}
		fmt.Fprintf(w, "      %s\n", d)
	}
	fmt.Fprintf(w, "  customers charged twice:                 %d (of %d duplicate webhooks)\n", r.ChargedTwice, r.WebhookRetries)
	fmt.Fprintf(w, "  seats stuck at the end of the sale:      %d\n", r.SeatsStuck)
	fmt.Fprintf(w, "  abandoned checkouts:                     %d (their seats later sold to someone else: %d)\n", r.Abandoned, r.AbandonedResold)
	fmt.Fprintf(w, "  shoppers told \"seat taken\" before paying: %d\n", r.SeatTaken)
	fmt.Fprintf(w, "  shoppers who found none of their seats:  %d\n", r.NoSeatShown)
	fmt.Fprintf(w, "  refunds:                                 %d (hold expired mid-payment: %d, lost the final compare-and-set: %d)\n",
		r.Refunds(), r.RefundsLate, r.RefundsLostRace)
	if r.RetriesReplayed > 0 || r.HoldsReaped > 0 || r.HoldsLost > 0 {
		fmt.Fprintf(w, "  duplicate webhooks replayed from the idempotency store: %d\n", r.RetriesReplayed)
		fmt.Fprintf(w, "  expired holds returned to sale by the reaper:           %d\n", r.HoldsReaped)
		fmt.Fprintf(w, "  active holds wiped by the hold-store failover (t=%v): %d\n", FailoverAt, r.HoldsLost)
	}
}

// shopper is one customer's script; both designs replay the same scripts.
type shopper struct {
	id       string
	round    int
	prefs    []string
	abandons bool
	payTime  time.Duration
	retried  bool
	// Set during a run.
	seat string
}

func seatName(i int) string { return fmt.Sprintf("%c%d", 'A'+i/SeatsPerRow, i%SeatsPerRow+1) }

// traffic generates the shoppers. Preferences lean heavily towards the front
// rows, which is where the races happen.
func traffic() []*shopper {
	rng := rand.New(rand.NewPCG(seed1, seed2))
	out := make([]*shopper, 0, Shoppers)
	for i := range Shoppers {
		c := &shopper{id: fmt.Sprintf("c%03d", i+1), round: i / ShoppersPerRound}
		seen := map[int]bool{}
		for len(c.prefs) < Preferences {
			f := rng.Float64()
			idx := int(f * f * Seats)
			if !seen[idx] {
				seen[idx] = true
				c.prefs = append(c.prefs, seatName(idx))
			}
		}
		c.abandons = rng.Float64() < AbandonRate
		if rng.Float64() < SlowPayRate {
			c.payTime = 6*time.Minute + time.Duration(rng.IntN(180))*time.Second
		} else {
			c.payTime = time.Minute + time.Duration(rng.IntN(120))*time.Second
		}
		c.retried = rng.Float64() < WebhookRetryRate
		out = append(out, c)
	}
	return out
}

// Seat records live in a versioned store in both designs; only how they are
// written differs.
type seatStatus int

const (
	free seatStatus = iota
	locked
	booked
)

type seatRec struct {
	Status seatStatus
	Holder string
}

// design is the part that differs between Broken and Fixed.
type design interface {
	// browse is the read phase: which of the shopper's seats look free.
	browse(c *shopper) []string
	// claim is the write phase: try to take one of the candidates.
	claim(c *shopper, candidates []string) (string, bool)
	// webhook handles a "payment succeeded" delivery from the gateway.
	webhook(c *shopper)
	tick()
	failover()
	// stuck reports, at the end, seats neither sold nor available.
	stuck() int
}

// sim holds what both designs share: the clock, the store, and the books
// (captures, tickets, refunds).
type sim struct {
	now      time.Duration
	store    *optimisticlocking.Store[string, seatRec]
	charges  map[string]int
	tickets  map[string][]*shopper
	refunded map[string]bool
	r        *Report
}

func newSim(name string) *sim {
	return &sim{
		store:    optimisticlocking.NewStore[string, seatRec](),
		charges:  map[string]int{},
		tickets:  map[string][]*shopper{},
		refunded: map[string]bool{},
		r:        &Report{Name: name},
	}
}

func (s *sim) clock() time.Time { return time.Unix(0, 0).Add(s.now) }

func (s *sim) capture(c *shopper) { s.charges[c.id]++ }

func (s *sim) issue(seat string, c *shopper) {
	s.tickets[seat] = append(s.tickets[seat], c)
	s.r.TicketsIssued++
}

// refund returns the shopper's payment. Here that means the authorisation is
// released and never captured.
func (s *sim) refund(c *shopper, late bool) {
	if s.refunded[c.id] {
		return
	}
	s.refunded[c.id] = true
	if late {
		s.r.RefundsLate++
	} else {
		s.r.RefundsLostRace++
	}
}

type kind int

// Events at the same instant run in this order.
const (
	evFailover kind = iota
	evReap
	evWebhook
	evRound
)

type event struct {
	at    time.Duration
	kind  kind
	seq   int
	round int
	c     *shopper
}

type queue []event

func (q queue) Len() int { return len(q) }
func (q queue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	if q[i].kind != q[j].kind {
		return q[i].kind < q[j].kind
	}
	return q[i].seq < q[j].seq
}
func (q queue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *queue) Push(x any)   { *q = append(*q, x.(event)) }
func (q *queue) Pop() any {
	old := *q
	e := old[len(old)-1]
	*q = old[:len(old)-1]
	return e
}

func (s *sim) run(d design, withHoldStore bool) Report {
	shoppers := traffic()
	q := &queue{}
	seq := 0
	push := func(e event) { seq++; e.seq = seq; heap.Push(q, e) }
	for r := range Rounds {
		push(event{at: time.Duration(r) * time.Minute, kind: evRound, round: r})
	}
	if withHoldStore {
		for t := time.Minute; t <= End; t += time.Minute {
			push(event{at: t, kind: evReap})
		}
		push(event{at: FailoverAt, kind: evFailover})
	}

	type abandon struct {
		seat  string
		round int
	}
	var abandons []abandon
	for q.Len() > 0 {
		e := heap.Pop(q).(event)
		s.now = e.at
		switch e.kind {
		case evRound:
			group := shoppers[e.round*ShoppersPerRound : (e.round+1)*ShoppersPerRound]
			// Read phase: everyone in the round looks at the seat map first.
			looks := make([][]string, len(group))
			for i, c := range group {
				looks[i] = d.browse(c)
			}
			// Write phase: then each of them, in arrival order, claims a seat.
			for i, c := range group {
				if len(looks[i]) == 0 {
					s.r.NoSeatShown++
					continue
				}
				seat, ok := d.claim(c, looks[i])
				if !ok {
					s.r.SeatTaken++
					continue
				}
				c.seat = seat
				if c.abandons {
					s.r.Abandoned++
					abandons = append(abandons, abandon{seat, c.round})
					continue
				}
				push(event{at: e.at + c.payTime, kind: evWebhook, c: c})
				if c.retried {
					push(event{at: e.at + c.payTime + WebhookRetry, kind: evWebhook, c: c})
					s.r.WebhookRetries++
				}
			}
		case evWebhook:
			d.webhook(e.c)
		case evReap:
			d.tick()
		case evFailover:
			d.failover()
		}
	}
	s.now = End
	d.tick()

	seats := make([]string, 0, len(s.tickets))
	for seat := range s.tickets {
		seats = append(seats, seat)
	}
	sort.Strings(seats)
	for _, seat := range seats {
		owners := []string{}
		seen := map[string]bool{}
		for _, c := range s.tickets[seat] {
			if !seen[c.id] {
				seen[c.id] = true
				owners = append(owners, c.id)
			}
		}
		if len(owners) > 1 {
			s.r.DoubleBooked = append(s.r.DoubleBooked, seat+": "+strings.Join(owners, ", "))
		}
	}
	s.r.SeatsSold = len(seats)
	for _, n := range s.charges {
		if n > 1 {
			s.r.ChargedTwice++
		}
	}
	for _, a := range abandons {
		for _, c := range s.tickets[a.seat] {
			if c.round > a.round {
				s.r.AbandonedResold++
				break
			}
		}
	}
	s.r.SeatsStuck = d.stuck()
	return *s.r
}

// broken: "if seat is free, mark it locked" as two separate steps, a lock
// with no expiry, a blind "booked" write when payment arrives, and a webhook
// handler that captures on every delivery.
type broken struct{ *sim }

func (b broken) browse(c *shopper) []string {
	var out []string
	for _, seat := range c.prefs {
		if b.store.Get(seat).Value.Status == free {
			out = append(out, seat)
		}
	}
	return out
}

func (b broken) claim(c *shopper, candidates []string) (string, bool) {
	// The check happened during browse; the write doesn't repeat it.
	b.store.Put(candidates[0], seatRec{Status: locked, Holder: c.id})
	return candidates[0], true
}

func (b broken) webhook(c *shopper) {
	b.capture(c)
	b.store.Put(c.seat, seatRec{Status: booked, Holder: c.id})
	b.issue(c.seat, c)
}

func (broken) tick()     {}
func (broken) failover() {}

func (b broken) stuck() int {
	n := 0
	for i := range Seats {
		if b.store.Get(seatName(i)).Value.Status == locked {
			n++
		}
	}
	return n
}

// Broken runs the broken design.
func Broken() Report {
	s := newSim("BROKEN: read-then-write seat lock with no expiry, blind booking write, webhook captures on every delivery")
	return s.run(broken{s}, false)
}

// fixed: a TTL hold per seat, a compare-and-set booking write, and an
// idempotent webhook handler.
type fixed struct {
	*sim
	inv   *reservationhold.Inventory
	idem  *idempotencykey.Store
	holds map[string]holdRef // customer -> hold
}

type holdRef struct {
	id      string
	inv     *reservationhold.Inventory // which hold store issued it
	version uint64                     // seat version read when the hold was taken
}

func newInventory(f *fixed) *reservationhold.Inventory {
	stock := make(map[string]int, Seats)
	for i := range Seats {
		seat := seatName(i)
		stock[seat] = 1
		if f.store.Get(seat).Value.Status == booked {
			stock[seat] = 0
		}
	}
	inv, err := reservationhold.New(stock, reservationhold.WithClock(f.clock))
	if err != nil {
		panic(err)
	}
	return inv
}

func (f *fixed) browse(c *shopper) []string {
	var out []string
	for _, seat := range c.prefs {
		if f.store.Get(seat).Value.Status != booked && f.inv.Available(seat) > 0 {
			out = append(out, seat)
		}
	}
	return out
}

func (f *fixed) claim(c *shopper, candidates []string) (string, bool) {
	for _, seat := range candidates {
		h, err := f.inv.Hold([]string{seat}, c.id, HoldTTL)
		if errors.Is(err, reservationhold.ErrUnavailable) {
			continue // someone earlier in this round got it
		}
		if err != nil {
			panic(err)
		}
		f.holds[c.id] = holdRef{id: h.ID, inv: f.inv, version: f.store.Get(seat).Version}
		return seat, true
	}
	return "", false
}

func (f *fixed) webhook(c *shopper) {
	key := "payment-" + c.id
	fp := idempotencykey.Fingerprint([]byte(c.id), []byte(c.seat))
	_, outcome, err := f.idem.Do(context.Background(), key, fp, func(context.Context) (idempotencykey.Response, error) {
		ref := f.holds[c.id]
		if ref.inv == f.inv {
			if _, err := f.inv.Confirm(ref.id); err != nil {
				// Expired (the shopper paid after the deadline) or released:
				// the seat may already be someone else's.
				f.refund(c, true)
				return idempotencykey.Response{Status: 409, Body: []byte("hold expired, refunded")}, nil
			}
		} else {
			// The hold store lost this hold in the failover. The seat store is
			// the source of truth, so go straight to the guarded write.
		}
		_, err := f.store.CompareAndSet(c.seat, ref.version, seatRec{Status: booked, Holder: c.id})
		if errors.Is(err, optimisticlocking.ErrConflict) {
			f.refund(c, false)
			return idempotencykey.Response{Status: 409, Body: []byte("seat taken, refunded")}, nil
		}
		f.capture(c)
		f.issue(c.seat, c)
		return idempotencykey.Response{Status: 200, Body: []byte("booked " + c.seat)}, nil
	})
	if err != nil {
		panic(err)
	}
	if outcome == idempotencykey.Replayed {
		f.r.RetriesReplayed++
	}
}

func (f *fixed) tick() { f.r.HoldsReaped += len(f.inv.Reap()) }

// failover replaces the hold store with an empty one rebuilt from the seat
// store: sold seats stay sold, every active hold is gone.
func (f *fixed) failover() {
	f.tick()
	f.r.HoldsLost = f.inv.Totals().Held
	f.inv = newInventory(f)
}

func (f *fixed) stuck() int {
	n := 0
	for i := range Seats {
		seat := seatName(i)
		if f.store.Get(seat).Value.Status != booked && f.inv.Available(seat) == 0 {
			n++
		}
	}
	return n
}

// Fixed runs the fixed design.
func Fixed() Report {
	s := newSim("FIXED: TTL reservation hold + compare-and-set booking + idempotent payment webhook")
	f := &fixed{sim: s, holds: map[string]holdRef{}}
	f.idem = idempotencykey.NewStore(24*time.Hour, s.clock)
	f.inv = newInventory(f)
	return s.run(f, true)
}
