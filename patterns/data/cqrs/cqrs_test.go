package cqrs

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func setup(t *testing.T) (*Log, *Inventory, *StockView) {
	t.Helper()
	log := &Log{}
	inv, err := NewInventory(log)
	if err != nil {
		t.Fatal(err)
	}
	return log, inv, NewStockView()
}

func TestNewInventoryRejectsNilLog(t *testing.T) {
	if _, err := NewInventory(nil); err == nil {
		t.Fatal("expected error")
	}
	if n := NewStockView().Sync(nil); n != 0 {
		t.Fatal("Sync(nil) should be a no-op")
	}
}

func TestCommandValidation(t *testing.T) {
	_, inv, _ := setup(t)
	_ = inv.AddStock("apple", 5)
	tests := []struct {
		name string
		run  func() error
		want error
	}{
		{"add zero", func() error { return inv.AddStock("apple", 0) }, ErrInvalidQty},
		{"reserve negative", func() error { return inv.Reserve("apple", -1, "o1") }, ErrInvalidQty},
		{"reserve unknown sku", func() error { return inv.Reserve("pear", 1, "o1") }, ErrUnknownSKU},
		{"oversell", func() error { return inv.Reserve("apple", 6, "o1") }, ErrInsufficient},
		{"valid reserve", func() error { return inv.Reserve("apple", 5, "o1") }, nil},
		{"now sold out", func() error { return inv.Reserve("apple", 1, "o2") }, ErrInsufficient},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.run(); !errors.Is(err, tt.want) {
				t.Fatalf("got %v, want %v", err, tt.want)
			}
		})
	}
}

func TestRejectedCommandsEmitNoEvents(t *testing.T) {
	log, inv, _ := setup(t)
	_ = inv.AddStock("a", 1)
	_ = inv.Reserve("a", 2, "o")
	_ = inv.AddStock("a", -3)
	if n := len(log.Since(0)); n != 1 {
		t.Fatalf("log has %d events, want 1", n)
	}
}

func TestProjectionIsEventuallyConsistent(t *testing.T) {
	log, inv, view := setup(t)
	_ = inv.AddStock("apple", 10)
	_ = inv.Reserve("apple", 3, "o1")

	if _, ok := view.Get("apple"); ok {
		t.Fatal("view should lag until Sync")
	}
	if n := view.Sync(log); n != 2 {
		t.Fatalf("applied %d, want 2", n)
	}
	got, _ := view.Get("apple")
	want := Item{SKU: "apple", Available: 7, Reserved: 3, Orders: 1}
	if got != want {
		t.Fatalf("got %+v want %+v", got, want)
	}
	if n := view.Sync(log); n != 0 || view.Position() != 2 {
		t.Fatalf("re-sync applied %d, position %d", n, view.Position())
	}
}

func TestRebuildFromScratchMatches(t *testing.T) {
	log, inv, live := setup(t)
	for i := range 50 {
		sku := fmt.Sprintf("sku-%d", i%7)
		_ = inv.AddStock(sku, i%5+1)
		_ = inv.Reserve(sku, i%3+1, fmt.Sprint(i))
		if i%10 == 0 {
			live.Sync(log) // incremental
		}
	}
	live.Sync(log)
	rebuilt := NewStockView()
	rebuilt.Sync(log) // full replay into a fresh read model
	for i := range 7 {
		sku := fmt.Sprintf("sku-%d", i)
		a, _ := live.Get(sku)
		b, _ := rebuilt.Get(sku)
		if a != b {
			t.Fatalf("%s: incremental %+v vs rebuilt %+v", sku, a, b)
		}
	}
}

func TestLowStockQuery(t *testing.T) {
	log, inv, view := setup(t)
	_ = inv.AddStock("a", 10)
	_ = inv.AddStock("b", 2)
	_ = inv.AddStock("c", 2)
	_ = inv.AddStock("d", 1)
	view.Sync(log)
	got := view.LowStock(3)
	want := []string{"d", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("got %+v", got)
	}
	for i := range want {
		if got[i].SKU != want[i] {
			t.Fatalf("order %v, want %v", got, want)
		}
	}
}

func TestConcurrentReservationsNeverOversell(t *testing.T) {
	log, inv, view := setup(t)
	_ = inv.AddStock("ticket", 100)
	var ok atomic.Int64
	var wg sync.WaitGroup
	for w := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 10 {
				if inv.Reserve("ticket", 1, fmt.Sprintf("%d-%d", w, i)) == nil {
					ok.Add(1)
				}
				view.Sync(log) // readers projecting concurrently
			}
		}()
	}
	wg.Wait()
	view.Sync(log)
	it, _ := view.Get("ticket")
	if ok.Load() != 100 || it.Available != 0 || it.Reserved != 100 {
		t.Fatalf("ok=%d view=%+v", ok.Load(), it)
	}
}
