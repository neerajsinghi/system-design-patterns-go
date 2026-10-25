// Package materializedview maintains a precomputed aggregate over a base
// table incrementally, from the table's change events, instead of
// recomputing it from scratch on every query.
//
// The base table emits a change-data-capture (CDC) style stream: every
// insert, update and delete, with the row images before and after and a
// log sequence number (LSN). The view applies each change as a delta:
// subtract the old row's contribution and add the new row's. Updating it
// costs O(1) per change, whatever the table size. LSNs make Apply
// idempotent (duplicates are skipped) and detect gaps (a missed change
// would silently corrupt the view, so it is an error).
package materializedview

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
)

// Order is a row in the base table. Amount is in cents.
type Order struct {
	ID       string
	Customer string
	Status   string // "placed", "shipped", "cancelled", ...
	Amount   int64
}

// Op is the kind of change.
type Op int

// Change operations.
const (
	Insert Op = iota + 1
	Update
	Delete
)

// Change is one CDC event. Before is nil for inserts and After is nil for
// deletes. Consumers must treat the row images as read-only.
type Change struct {
	LSN    uint64
	Op     Op
	Before *Order
	After  *Order
}

// Errors.
var (
	ErrExists   = errors.New("materializedview: row already exists")
	ErrNotFound = errors.New("materializedview: row not found")
	ErrGap      = errors.New("materializedview: change stream gap")
)

// Table is the base table. Every mutation is recorded in its change log.
type Table struct {
	mu   sync.RWMutex
	rows map[string]Order
	log  []Change
}

// NewTable returns an empty table.
func NewTable() *Table { return &Table{rows: make(map[string]Order)} }

func (t *Table) emit(op Op, before, after *Order) {
	t.log = append(t.log, Change{LSN: uint64(len(t.log)) + 1, Op: op, Before: before, After: after})
}

// Insert adds a new order.
func (t *Table) Insert(o Order) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.rows[o.ID]; ok {
		return fmt.Errorf("%w: %s", ErrExists, o.ID)
	}
	t.rows[o.ID] = o
	t.emit(Insert, nil, &o)
	return nil
}

// Update modifies an existing order through fn. The ID can't change.
func (t *Table) Update(id string, fn func(*Order)) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	before, ok := t.rows[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	after := before
	if fn != nil {
		fn(&after)
	}
	after.ID = id
	t.rows[id] = after
	t.emit(Update, &before, &after)
	return nil
}

// Delete removes an order.
func (t *Table) Delete(id string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	before, ok := t.rows[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	delete(t.rows, id)
	t.emit(Delete, &before, nil)
	return nil
}

// ChangesSince returns changes with LSN > lsn.
func (t *Table) ChangesSince(lsn uint64) []Change {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if lsn >= uint64(len(t.log)) {
		return nil
	}
	return slices.Clone(t.log[lsn:])
}

// Rows returns a copy of all rows (a full scan, which is what the view avoids).
func (t *Table) Rows() []Order {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]Order, 0, len(t.rows))
	for _, o := range t.rows {
		out = append(out, o)
	}
	return out
}

// CustomerStats is one row of the view.
type CustomerStats struct {
	Customer string
	Orders   int   // all orders, any status
	Revenue  int64 // sum of Amount over non-cancelled orders
}

// View is a materialized "revenue per customer" aggregate:
//
//	SELECT customer, COUNT(*), SUM(amount) FILTER (WHERE status <> 'cancelled')
//	FROM orders GROUP BY customer
type View struct {
	mu   sync.RWMutex
	lsn  uint64
	rows map[string]*CustomerStats
}

// NewView returns an empty view at LSN 0.
func NewView() *View { return &View{rows: make(map[string]*CustomerStats)} }

func (v *View) contribute(o *Order, sign int64) {
	if o == nil {
		return
	}
	r := v.rows[o.Customer]
	if r == nil {
		r = &CustomerStats{Customer: o.Customer}
		v.rows[o.Customer] = r
	}
	r.Orders += int(sign)
	if o.Status != "cancelled" {
		r.Revenue += sign * o.Amount
	}
	if r.Orders == 0 {
		delete(v.rows, o.Customer) // group became empty
	}
}

// Apply folds one change into the view. Changes at or below the view's LSN
// are ignored (at-least-once delivery is safe). A change beyond LSN+1 returns ErrGap.
func (v *View) Apply(c Change) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if c.LSN <= v.lsn {
		return nil
	}
	if c.LSN != v.lsn+1 {
		return fmt.Errorf("%w: at %d, got %d", ErrGap, v.lsn, c.LSN)
	}
	v.contribute(c.Before, -1) // retract the old row...
	v.contribute(c.After, +1)  // ...and assert the new one
	v.lsn = c.LSN
	return nil
}

// CatchUp applies every change from t since the view's LSN and returns how many were applied.
func (v *View) CatchUp(t *Table) (int, error) {
	n := 0
	for _, c := range t.ChangesSince(v.LSN()) {
		before := v.LSN()
		if err := v.Apply(c); err != nil {
			return n, err
		}
		if v.LSN() > before {
			n++
		}
	}
	return n, nil
}

// LSN is the last change applied.
func (v *View) LSN() uint64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.lsn
}

// Get returns one customer's aggregate.
func (v *View) Get(customer string) (CustomerStats, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	r, ok := v.rows[customer]
	if !ok {
		return CustomerStats{}, false
	}
	return *r, true
}

// All returns every row, sorted by customer.
func (v *View) All() []CustomerStats {
	v.mu.RLock()
	defer v.mu.RUnlock()
	out := make([]CustomerStats, 0, len(v.rows))
	for _, r := range v.rows {
		out = append(out, *r)
	}
	slices.SortFunc(out, func(a, b CustomerStats) int { return strings.Compare(a.Customer, b.Customer) })
	return out
}

// Recompute builds the same aggregate from scratch with a full scan. It is
// the slow path the view replaces, and a correctness oracle in tests.
func Recompute(rows []Order) []CustomerStats {
	v := NewView()
	for i := range rows {
		v.contribute(&rows[i], +1)
	}
	return v.All()
}
