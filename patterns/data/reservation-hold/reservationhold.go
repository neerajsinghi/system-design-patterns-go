// Package reservationhold implements time-limited reservation holds: the
// "your seats are held for 10:00" timer that ticketing, airline and
// e-commerce checkouts put between "add to cart" and "payment succeeded".
//
// A checkout has to take inventory out of circulation while the customer
// pays, or two customers can pay for the same seat. Taking it out is the easy
// half. The hard half is giving it back. Shoppers close the tab, payment pages
// time out and phones lose signal, so if a hold lives until someone explicitly
// releases it, abandoned carts lock seats forever: the event ends with empty
// seats while real buyers were told it was sold out. The opposite mistake is
// just as bad: if seats are returned on a timer but a payment that finishes
// after the timer is still accepted, the seat may already belong to someone
// else, and the venue is oversold.
//
// This package gives every hold a deadline. Hold takes units out of the
// available pool atomically (all items or none) and stamps an expiry. Confirm
// turns the hold into a sale only if the deadline has not passed, and is
// idempotent so a retried payment callback cannot double-sell. Expired holds
// go back to the pool in two ways: lazily, whenever a Hold, Confirm, Release
// or Available call touches an item with an overdue hold, and in bulk by
// Reap, which a caller runs on a ticker to keep counts accurate and to notify
// holders whose time ran out.
package reservationhold

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

var (
	// ErrUnknownItem is returned when a hold names an item the inventory does
	// not stock.
	ErrUnknownItem = errors.New("reservationhold: unknown item")
	// ErrUnavailable is returned when there are not enough free units to
	// satisfy a hold. Nothing is held when it is returned.
	ErrUnavailable = errors.New("reservationhold: not enough units available")
	// ErrHoldNotFound is returned for an ID that was never issued.
	ErrHoldNotFound = errors.New("reservationhold: hold not found")
	// ErrHoldExpired is returned by Confirm when the hold's deadline has
	// passed. Its units have gone (or are going) back to the pool, so the
	// payment must be refunded rather than turned into a sale.
	ErrHoldExpired = errors.New("reservationhold: hold expired")
	// ErrHoldReleased is returned by Confirm for a hold that was released.
	ErrHoldReleased = errors.New("reservationhold: hold was released")
	// ErrHoldConfirmed is returned by Release for a hold that is already a
	// sale; a sale is undone by a refund flow, not by releasing the hold.
	ErrHoldConfirmed = errors.New("reservationhold: hold already confirmed")
)

// Status is where a hold is in its life.
type Status int

const (
	// Active holds have units reserved and can still be confirmed.
	Active Status = iota
	// Confirmed holds are sales.
	Confirmed
	// Released holds were given back by the holder.
	Released
	// Expired holds ran out of time and their units were returned.
	Expired
)

func (s Status) String() string {
	switch s {
	case Active:
		return "active"
	case Confirmed:
		return "confirmed"
	case Released:
		return "released"
	case Expired:
		return "expired"
	}
	return fmt.Sprintf("Status(%d)", int(s))
}

// Hold is a snapshot of a reservation.
type Hold struct {
	ID        string
	Holder    string
	Items     map[string]int // item ID -> units held
	ExpiresAt time.Time
	Status    Status
}

// Units is the total number of units in the hold.
func (h Hold) Units() int {
	n := 0
	for _, q := range h.Items {
		n += q
	}
	return n
}

// Stats counts what happened to holds.
type Stats struct {
	Holds         uint64 // holds granted
	Rejected      uint64 // holds refused with ErrUnavailable
	Confirmed     uint64 // holds turned into sales (repeat confirms not counted)
	Released      uint64 // holds given back by the holder
	ExpiredLazy   uint64 // expired holds returned because a call touched them
	ExpiredReaped uint64 // expired holds returned by Reap
	LateConfirms  uint64 // Confirm calls refused with ErrHoldExpired
}

// Totals is the state of every unit in the inventory. Available + Held +
// Sold always equals the stock the inventory was created with.
type Totals struct {
	Available, Held, Sold int
}

type hold struct {
	id        string
	holder    string
	items     map[string]int
	expiresAt time.Time
	status    Status
}

func (h *hold) snapshot() Hold {
	items := make(map[string]int, len(h.items))
	for k, v := range h.items {
		items[k] = v
	}
	return Hold{ID: h.id, Holder: h.holder, Items: items, ExpiresAt: h.expiresAt, Status: h.status}
}

type item struct {
	available, held, sold int
	active                map[string]*hold // active holds that include this item
}

// Inventory is a concurrency-safe pool of items with time-limited holds.
type Inventory struct {
	now func() time.Time

	mu    sync.Mutex
	items map[string]*item
	holds map[string]*hold
	seq   uint64
	stats Stats
}

// Option configures an Inventory.
type Option func(*Inventory)

// WithClock injects a clock, which makes expiry testable.
func WithClock(now func() time.Time) Option {
	return func(inv *Inventory) {
		if now != nil {
			inv.now = now
		}
	}
}

// New creates an inventory. stock maps item IDs to unit counts: 1 for a
// numbered seat, N for general admission or a product's stock level.
func New(stock map[string]int, opts ...Option) (*Inventory, error) {
	if len(stock) == 0 {
		return nil, errors.New("reservationhold: stock must not be empty")
	}
	inv := &Inventory{
		now:   time.Now,
		items: make(map[string]*item, len(stock)),
		holds: make(map[string]*hold),
	}
	for id, n := range stock {
		if id == "" {
			return nil, errors.New("reservationhold: item ID must not be empty")
		}
		if n < 0 {
			return nil, fmt.Errorf("reservationhold: stock for %q must not be negative", id)
		}
		inv.items[id] = &item{available: n, active: make(map[string]*hold)}
	}
	for _, opt := range opts {
		opt(inv)
	}
	return inv, nil
}

// Hold reserves one unit per entry in items (repeat an ID to hold several
// units of it) for holder, until now+ttl. It is all-or-nothing: if any item
// is short, nothing is held and the error wraps ErrUnavailable.
func (inv *Inventory) Hold(items []string, holder string, ttl time.Duration) (Hold, error) {
	if len(items) == 0 {
		return Hold{}, errors.New("reservationhold: hold needs at least one item")
	}
	if holder == "" {
		return Hold{}, errors.New("reservationhold: holder must not be empty")
	}
	if ttl <= 0 {
		return Hold{}, errors.New("reservationhold: ttl must be positive")
	}
	want := make(map[string]int, len(items))
	for _, id := range items {
		want[id]++
	}

	inv.mu.Lock()
	defer inv.mu.Unlock()
	now := inv.now()
	for id := range want {
		if _, ok := inv.items[id]; !ok {
			return Hold{}, fmt.Errorf("%w: %q", ErrUnknownItem, id)
		}
	}
	// Check every item before touching any, so a short item leaves the
	// others untouched. Overdue holds on these items are returned first.
	for id, n := range want {
		it := inv.items[id]
		inv.expireItemLocked(it, now)
		if it.available < n {
			inv.stats.Rejected++
			return Hold{}, fmt.Errorf("%w: %q has %d, need %d", ErrUnavailable, id, it.available, n)
		}
	}
	inv.seq++
	h := &hold{
		id:        fmt.Sprintf("h-%06d", inv.seq),
		holder:    holder,
		items:     want,
		expiresAt: now.Add(ttl),
		status:    Active,
	}
	for id, n := range want {
		it := inv.items[id]
		it.available -= n
		it.held += n
		it.active[h.id] = h
	}
	inv.holds[h.id] = h
	inv.stats.Holds++
	return h.snapshot(), nil
}

// Confirm turns an active, unexpired hold into a sale. Confirming a hold that
// is already confirmed succeeds again and returns the same hold, so a retried
// payment callback is safe. After the deadline it fails with ErrHoldExpired,
// even if no one has reaped the hold yet: the deadline, not the reaper,
// decides.
func (inv *Inventory) Confirm(holdID string) (Hold, error) {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	h, ok := inv.holds[holdID]
	if !ok {
		return Hold{}, ErrHoldNotFound
	}
	if h.status == Active && !inv.now().Before(h.expiresAt) {
		inv.finishLocked(h, Expired)
		inv.stats.ExpiredLazy++
	}
	switch h.status {
	case Confirmed:
		return h.snapshot(), nil
	case Released:
		return h.snapshot(), ErrHoldReleased
	case Expired:
		inv.stats.LateConfirms++
		return h.snapshot(), ErrHoldExpired
	}
	for id, n := range h.items {
		it := inv.items[id]
		it.held -= n
		it.sold += n
		delete(it.active, h.id)
	}
	h.status = Confirmed
	inv.stats.Confirmed++
	return h.snapshot(), nil
}

// Release gives an active hold's units back. Releasing a hold that is
// already released or expired is a no-op; releasing a sale is ErrHoldConfirmed.
func (inv *Inventory) Release(holdID string) error {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	h, ok := inv.holds[holdID]
	if !ok {
		return ErrHoldNotFound
	}
	switch h.status {
	case Confirmed:
		return ErrHoldConfirmed
	case Released, Expired:
		return nil
	}
	if !inv.now().Before(h.expiresAt) {
		inv.finishLocked(h, Expired)
		inv.stats.ExpiredLazy++
		return nil
	}
	inv.finishLocked(h, Released)
	inv.stats.Released++
	return nil
}

// Reap returns every overdue hold's units to the pool and reports those
// holds, ordered by expiry then ID. Run it periodically so that totals,
// dashboards and "seats are back" notifications don't wait for a shopper to
// touch the item.
func (inv *Inventory) Reap() []Hold {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	now := inv.now()
	var out []Hold
	for _, h := range inv.holds {
		if h.status == Active && !now.Before(h.expiresAt) {
			inv.finishLocked(h, Expired)
			inv.stats.ExpiredReaped++
			out = append(out, h.snapshot())
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].ExpiresAt.Equal(out[j].ExpiresAt) {
			return out[i].ExpiresAt.Before(out[j].ExpiresAt)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// Available reports free units of an item, first returning any overdue
// holds on it. Unknown items report 0.
func (inv *Inventory) Available(itemID string) int {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	it, ok := inv.items[itemID]
	if !ok {
		return 0
	}
	inv.expireItemLocked(it, inv.now())
	return it.available
}

// Get returns a snapshot of a hold. It does not expire anything, so an
// overdue hold that nobody has touched still reports Active.
func (inv *Inventory) Get(holdID string) (Hold, bool) {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	h, ok := inv.holds[holdID]
	if !ok {
		return Hold{}, false
	}
	return h.snapshot(), true
}

// Totals sums units across all items as currently recorded. It does not
// expire anything, so overdue holds count as Held until something touches
// them or Reap runs.
func (inv *Inventory) Totals() Totals {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	var t Totals
	for _, it := range inv.items {
		t.Available += it.available
		t.Held += it.held
		t.Sold += it.sold
	}
	return t
}

// Stats returns a snapshot of the counters.
func (inv *Inventory) Stats() Stats {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	return inv.stats
}

// expireItemLocked returns overdue holds that include it.
func (inv *Inventory) expireItemLocked(it *item, now time.Time) {
	for _, h := range it.active {
		if !now.Before(h.expiresAt) {
			inv.finishLocked(h, Expired)
			inv.stats.ExpiredLazy++
		}
	}
}

// finishLocked moves an active hold's units back to available.
func (inv *Inventory) finishLocked(h *hold, to Status) {
	for id, n := range h.items {
		it := inv.items[id]
		it.held -= n
		it.available += n
		delete(it.active, h.id)
	}
	h.status = to
}
