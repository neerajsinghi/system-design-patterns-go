// Package sagachoreography implements a choreographed saga: there is no
// central coordinator. Each service listens for events, performs its local
// transaction, and publishes the next event. Failures are published as
// events too, and the services that already did work react to them by
// running their own compensations.
//
// Flow modelled here (order -> payment -> inventory):
//
//	OrderCreated      -> Payment charges      -> PaymentCompleted | PaymentFailed
//	PaymentCompleted  -> Inventory reserves   -> InventoryReserved | InventoryFailed
//	InventoryReserved -> Order confirms       -> OrderConfirmed
//	InventoryFailed   -> Payment refunds      -> PaymentRefunded   (compensation)
//	PaymentFailed / PaymentRefunded -> Order cancels -> OrderCancelled
//
// The event bus is an in-process FIFO drained by RunUntilIdle, which keeps
// the simulation deterministic. Handlers are idempotent so the bus can
// deliver duplicates, as real at-least-once brokers do.
package sagachoreography

import (
	"fmt"
	"slices"
	"sync"
)

// Event types.
const (
	OrderCreated      = "OrderCreated"
	PaymentCompleted  = "PaymentCompleted"
	PaymentFailed     = "PaymentFailed"
	InventoryReserved = "InventoryReserved"
	InventoryFailed   = "InventoryFailed"
	PaymentRefunded   = "PaymentRefunded"
	OrderConfirmed    = "OrderConfirmed"
	OrderCancelled    = "OrderCancelled"
)

// Event is a domain event. Every event carries enough data for the next
// service to act without calling back to the previous one.
type Event struct {
	ID       int
	Type     string
	OrderID  string
	Customer string
	Item     string
	Amount   int
	Reason   string
}

// Bus is a deterministic in-process event bus.
type Bus struct {
	mu        sync.Mutex
	handlers  map[string][]func(Event)
	queue     []Event
	nextID    int
	history   []Event
	duplicate bool
}

// NewBus returns an empty bus. If duplicate is true every event is delivered
// twice, simulating an at-least-once broker.
func NewBus(duplicate bool) *Bus {
	return &Bus{handlers: make(map[string][]func(Event)), duplicate: duplicate}
}

// Subscribe registers h for events of type typ.
func (b *Bus) Subscribe(typ string, h func(Event)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers[typ] = append(b.handlers[typ], h)
}

// Publish enqueues e (assigning its ID). Delivery happens in RunUntilIdle.
func (b *Bus) Publish(e Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextID++
	e.ID = b.nextID
	b.queue = append(b.queue, e)
	b.history = append(b.history, e)
	if b.duplicate {
		b.queue = append(b.queue, e)
	}
}

// RunUntilIdle delivers queued events (including those published by
// handlers) until the queue is empty or maxDeliveries is reached, which
// guards against event loops. It returns the number of deliveries.
func (b *Bus) RunUntilIdle(maxDeliveries int) int {
	n := 0
	for n < maxDeliveries {
		b.mu.Lock()
		if len(b.queue) == 0 {
			b.mu.Unlock()
			return n
		}
		e := b.queue[0]
		b.queue = b.queue[1:]
		hs := slices.Clone(b.handlers[e.Type])
		b.mu.Unlock()
		for _, h := range hs { // handlers run without the lock so they can Publish
			h(e)
		}
		n++
	}
	return n
}

// History returns every event published so far, in order (without duplicates).
func (b *Bus) History() []Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]Event(nil), b.history...)
}

// OrderStatus is the state of an order in the order service.
type OrderStatus string

// Order states.
const (
	Pending   OrderStatus = "pending"
	Confirmed OrderStatus = "confirmed"
	Cancelled OrderStatus = "cancelled"
)

// OrderService owns orders.
type OrderService struct {
	mu     sync.Mutex
	bus    *Bus
	orders map[string]OrderStatus
}

// Place creates a pending order and starts the saga.
func (s *OrderService) Place(id, customer, item string, amount int) error {
	s.mu.Lock()
	if _, ok := s.orders[id]; ok {
		s.mu.Unlock()
		return fmt.Errorf("sagachoreography: order %q already exists", id)
	}
	if amount <= 0 {
		s.mu.Unlock()
		return fmt.Errorf("sagachoreography: amount must be positive")
	}
	s.orders[id] = Pending
	s.mu.Unlock()
	s.bus.Publish(Event{Type: OrderCreated, OrderID: id, Customer: customer, Item: item, Amount: amount})
	return nil
}

// Status returns an order's state.
func (s *OrderService) Status(id string) OrderStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.orders[id]
}

// transition moves a pending order to a final state exactly once.
func (s *OrderService) transition(e Event, to OrderStatus, typ string) {
	s.mu.Lock()
	if s.orders[e.OrderID] != Pending {
		s.mu.Unlock()
		return // duplicate or late event: already final
	}
	s.orders[e.OrderID] = to
	s.mu.Unlock()
	e.Type = typ
	s.bus.Publish(e)
}

// PaymentService owns customer balances.
type PaymentService struct {
	mu       sync.Mutex
	bus      *Bus
	balances map[string]int
	charged  map[string]int // orderID -> amount charged and not refunded
	done     map[string]bool
}

// Balance returns a customer's balance.
func (s *PaymentService) Balance(customer string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.balances[customer]
}

func (s *PaymentService) onOrderCreated(e Event) {
	s.mu.Lock()
	if s.done[e.OrderID] {
		s.mu.Unlock()
		return // duplicate delivery: already decided for this order
	}
	s.done[e.OrderID] = true
	out := e
	if s.balances[e.Customer] < e.Amount {
		out.Type, out.Reason = PaymentFailed, "insufficient funds"
	} else {
		s.balances[e.Customer] -= e.Amount
		s.charged[e.OrderID] = e.Amount
		out.Type = PaymentCompleted
	}
	s.mu.Unlock()
	s.bus.Publish(out)
}

// onInventoryFailed is the compensation: refund the charge.
func (s *PaymentService) onInventoryFailed(e Event) {
	s.mu.Lock()
	amt, ok := s.charged[e.OrderID]
	if !ok {
		s.mu.Unlock()
		return // already refunded (duplicate) or never charged
	}
	delete(s.charged, e.OrderID)
	s.balances[e.Customer] += amt
	s.mu.Unlock()
	e.Type = PaymentRefunded
	s.bus.Publish(e)
}

// InventoryService owns stock levels.
type InventoryService struct {
	mu    sync.Mutex
	bus   *Bus
	stock map[string]int
	done  map[string]bool
}

// Stock returns the units left of item.
func (s *InventoryService) Stock(item string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stock[item]
}

func (s *InventoryService) onPaymentCompleted(e Event) {
	s.mu.Lock()
	if s.done[e.OrderID] {
		s.mu.Unlock()
		return
	}
	s.done[e.OrderID] = true
	if s.stock[e.Item] > 0 {
		s.stock[e.Item]--
		e.Type = InventoryReserved
	} else {
		e.Type, e.Reason = InventoryFailed, "out of stock"
	}
	s.mu.Unlock()
	s.bus.Publish(e)
}

// System wires the three services to one bus.
type System struct {
	Bus       *Bus
	Orders    *OrderService
	Payments  *PaymentService
	Inventory *InventoryService
}

// NewSystem creates the services with initial balances and stock and
// subscribes them to the events they care about.
func NewSystem(balances, stock map[string]int, duplicateDelivery bool) *System {
	bus := NewBus(duplicateDelivery)
	cp := func(m map[string]int) map[string]int {
		out := make(map[string]int, len(m))
		for k, v := range m {
			out[k] = v
		}
		return out
	}
	orders := &OrderService{bus: bus, orders: map[string]OrderStatus{}}
	pay := &PaymentService{bus: bus, balances: cp(balances), charged: map[string]int{}, done: map[string]bool{}}
	inv := &InventoryService{bus: bus, stock: cp(stock), done: map[string]bool{}}

	bus.Subscribe(OrderCreated, pay.onOrderCreated)
	bus.Subscribe(PaymentCompleted, inv.onPaymentCompleted)
	bus.Subscribe(InventoryFailed, pay.onInventoryFailed)
	bus.Subscribe(InventoryReserved, func(e Event) { orders.transition(e, Confirmed, OrderConfirmed) })
	bus.Subscribe(PaymentFailed, func(e Event) { orders.transition(e, Cancelled, OrderCancelled) })
	bus.Subscribe(PaymentRefunded, func(e Event) { orders.transition(e, Cancelled, OrderCancelled) })
	return &System{Bus: bus, Orders: orders, Payments: pay, Inventory: inv}
}
