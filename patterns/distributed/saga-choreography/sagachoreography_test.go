package sagachoreography

import (
	"reflect"
	"testing"
)

func types(evs []Event) []string {
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = e.Type
	}
	return out
}

func TestSagaOutcomes(t *testing.T) {
	tests := []struct {
		name        string
		balance     int
		stock       int
		wantStatus  OrderStatus
		wantBalance int
		wantStock   int
		wantEvents  []string
	}{
		{
			name: "happy path", balance: 100, stock: 1,
			wantStatus: Confirmed, wantBalance: 70, wantStock: 0,
			wantEvents: []string{OrderCreated, PaymentCompleted, InventoryReserved, OrderConfirmed},
		},
		{
			name: "payment fails, nothing to compensate", balance: 10, stock: 1,
			wantStatus: Cancelled, wantBalance: 10, wantStock: 1,
			wantEvents: []string{OrderCreated, PaymentFailed, OrderCancelled},
		},
		{
			name: "out of stock triggers refund", balance: 100, stock: 0,
			wantStatus: Cancelled, wantBalance: 100, wantStock: 0,
			wantEvents: []string{OrderCreated, PaymentCompleted, InventoryFailed, PaymentRefunded, OrderCancelled},
		},
	}
	for _, tt := range tests {
		for _, dup := range []bool{false, true} {
			name := tt.name
			if dup {
				name += " (duplicate delivery)"
			}
			t.Run(name, func(t *testing.T) {
				s := NewSystem(map[string]int{"alice": tt.balance}, map[string]int{"book": tt.stock}, dup)
				if err := s.Orders.Place("o1", "alice", "book", 30); err != nil {
					t.Fatal(err)
				}
				s.Bus.RunUntilIdle(100)
				if got := s.Orders.Status("o1"); got != tt.wantStatus {
					t.Fatalf("status=%v want %v", got, tt.wantStatus)
				}
				if b := s.Payments.Balance("alice"); b != tt.wantBalance {
					t.Fatalf("balance=%d want %d", b, tt.wantBalance)
				}
				if st := s.Inventory.Stock("book"); st != tt.wantStock {
					t.Fatalf("stock=%d want %d", st, tt.wantStock)
				}
				if got := types(s.Bus.History()); !reflect.DeepEqual(got, tt.wantEvents) {
					t.Fatalf("events=%v\nwant %v", got, tt.wantEvents)
				}
			})
		}
	}
}

func TestConcurrentOrdersCompeteForStock(t *testing.T) {
	s := NewSystem(map[string]int{"alice": 100, "bob": 100}, map[string]int{"book": 1}, false)
	_ = s.Orders.Place("a", "alice", "book", 20)
	_ = s.Orders.Place("b", "bob", "book", 20)
	s.Bus.RunUntilIdle(100)
	if s.Orders.Status("a") != Confirmed || s.Orders.Status("b") != Cancelled {
		t.Fatalf("a=%v b=%v", s.Orders.Status("a"), s.Orders.Status("b"))
	}
	if s.Payments.Balance("alice") != 80 || s.Payments.Balance("bob") != 100 {
		t.Fatal("bob should have been refunded")
	}
}

func TestPlaceValidationAndLoopGuard(t *testing.T) {
	s := NewSystem(nil, nil, false)
	if err := s.Orders.Place("x", "c", "i", 0); err == nil {
		t.Fatal("zero amount must fail")
	}
	_ = s.Orders.Place("x", "c", "i", 5)
	if err := s.Orders.Place("x", "c", "i", 5); err == nil {
		t.Fatal("duplicate order id must fail")
	}
	// An event loop is bounded by maxDeliveries.
	b := NewBus(false)
	b.Subscribe("ping", func(e Event) { b.Publish(Event{Type: "ping"}) })
	b.Publish(Event{Type: "ping"})
	if n := b.RunUntilIdle(10); n != 10 {
		t.Fatalf("deliveries=%d", n)
	}
}
