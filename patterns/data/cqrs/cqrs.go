// Package cqrs demonstrates Command Query Responsibility Segregation with an
// inventory domain.
//
// The write side (Inventory) is the only place that enforces business rules.
// It accepts commands, validates them against its own minimal state, and
// records the outcome as events in an append-only log. The read side
// (StockView) is a separate, denormalised projection built from those events
// and shaped for the queries the UI needs. It never validates anything.
//
// The two sides are eventually consistent. The projection catches up by
// reading the log from its last position. Because Sync is explicit, tests
// and the example can show the lag deterministically.
package cqrs

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
)

// Event is something that happened on the write side.
type Event interface{ sku() string }

// StockAdded records stock received into the warehouse.
type StockAdded struct {
	SKU string
	Qty int
}

// StockReserved records stock allocated to an order.
type StockReserved struct {
	SKU     string
	Qty     int
	OrderID string
}

func (e StockAdded) sku() string    { return e.SKU }
func (e StockReserved) sku() string { return e.SKU }

// Record is an event with its position in the log (1-based).
type Record struct {
	Seq   uint64
	Event Event
}

// Log is an append-only, concurrency-safe event log shared by both sides.
type Log struct {
	mu      sync.RWMutex
	records []Record
}

func (l *Log) append(e Event) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	seq := uint64(len(l.records)) + 1
	l.records = append(l.records, Record{Seq: seq, Event: e})
	return seq
}

// Since returns the records after position seq.
func (l *Log) Since(seq uint64) []Record {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if seq >= uint64(len(l.records)) {
		return nil
	}
	return slices.Clone(l.records[seq:])
}

// Errors returned by commands.
var (
	ErrInvalidQty   = errors.New("cqrs: quantity must be positive")
	ErrInsufficient = errors.New("cqrs: insufficient stock")
	ErrUnknownSKU   = errors.New("cqrs: unknown sku")
)

// Inventory is the write model. It keeps only what it needs to validate
// commands: available quantity per SKU.
type Inventory struct {
	log       *Log
	mu        sync.Mutex
	available map[string]int
}

// NewInventory returns a write model that appends to log.
func NewInventory(log *Log) (*Inventory, error) {
	if log == nil {
		return nil, errors.New("cqrs: log must not be nil")
	}
	return &Inventory{log: log, available: make(map[string]int)}, nil
}

// AddStock is a command: receive qty units of sku.
func (inv *Inventory) AddStock(sku string, qty int) error {
	if qty <= 0 {
		return ErrInvalidQty
	}
	inv.mu.Lock()
	defer inv.mu.Unlock()
	inv.available[sku] += qty
	inv.log.append(StockAdded{SKU: sku, Qty: qty})
	return nil
}

// Reserve is a command: allocate qty units of sku to an order, refusing to
// oversell. Validation and the append happen under one lock, so two
// concurrent reservations can't both take the last unit.
func (inv *Inventory) Reserve(sku string, qty int, orderID string) error {
	if qty <= 0 {
		return ErrInvalidQty
	}
	inv.mu.Lock()
	defer inv.mu.Unlock()
	have, ok := inv.available[sku]
	if !ok {
		return ErrUnknownSKU
	}
	if have < qty {
		return fmt.Errorf("%w: %s has %d, want %d", ErrInsufficient, sku, have, qty)
	}
	inv.available[sku] = have - qty
	inv.log.append(StockReserved{SKU: sku, Qty: qty, OrderID: orderID})
	return nil
}

// Item is one row of the read model, denormalised for display.
type Item struct {
	SKU       string
	Available int
	Reserved  int
	Orders    int
}

// StockView is the read model: a projection of the log optimised for queries.
type StockView struct {
	mu       sync.RWMutex
	position uint64
	items    map[string]*Item
}

// NewStockView returns an empty projection at position 0.
func NewStockView() *StockView { return &StockView{items: make(map[string]*Item)} }

// Sync applies every record after the view's position and returns how many
// were applied. Applying is idempotent with respect to position: replaying
// records the view has already seen is a no-op.
func (v *StockView) Sync(log *Log) int {
	if log == nil {
		return 0
	}
	v.mu.RLock()
	from := v.position
	v.mu.RUnlock()
	recs := log.Since(from)

	v.mu.Lock()
	defer v.mu.Unlock()
	applied := 0
	for _, r := range recs {
		if r.Seq <= v.position {
			continue // a concurrent Sync got here first
		}
		it := v.items[r.Event.sku()]
		if it == nil {
			it = &Item{SKU: r.Event.sku()}
			v.items[it.SKU] = it
		}
		switch e := r.Event.(type) {
		case StockAdded:
			it.Available += e.Qty
		case StockReserved:
			it.Available -= e.Qty
			it.Reserved += e.Qty
			it.Orders++
		}
		v.position = r.Seq
		applied++
	}
	return applied
}

// Position is the last log sequence the view has applied.
func (v *StockView) Position() uint64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.position
}

// Get returns one item as of the view's position.
func (v *StockView) Get(sku string) (Item, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	it, ok := v.items[sku]
	if !ok {
		return Item{}, false
	}
	return *it, true
}

// LowStock returns items with Available < threshold, lowest first, which is a
// query the write model has no reason to support efficiently.
func (v *StockView) LowStock(threshold int) []Item {
	v.mu.RLock()
	defer v.mu.RUnlock()
	var out []Item
	for _, it := range v.items {
		if it.Available < threshold {
			out = append(out, *it)
		}
	}
	slices.SortFunc(out, func(a, b Item) int {
		return cmp.Or(cmp.Compare(a.Available, b.Available), strings.Compare(a.SKU, b.SKU))
	})
	return out
}
