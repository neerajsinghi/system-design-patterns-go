// Command example runs a small warehouse with CQRS: commands are validated by
// the write model, and a separate read model answers dashboard queries after
// it catches up with the event log.
package main

import (
	"fmt"
	"log"

	"github.com/neerajsinghi/system-design-patterns-go/patterns/data/cqrs"
)

func main() {
	events := &cqrs.Log{}
	inv, err := cqrs.NewInventory(events)
	if err != nil {
		log.Fatal(err)
	}
	dashboard := cqrs.NewStockView()

	fmt.Println("Commands (write side):")
	cmds := []struct {
		desc string
		run  func() error
	}{
		{"AddStock keyboard 20", func() error { return inv.AddStock("keyboard", 20) }},
		{"AddStock mouse 5", func() error { return inv.AddStock("mouse", 5) }},
		{"AddStock monitor 3", func() error { return inv.AddStock("monitor", 3) }},
		{"Reserve mouse 3 (order A)", func() error { return inv.Reserve("mouse", 3, "A") }},
		{"Reserve monitor 2 (order B)", func() error { return inv.Reserve("monitor", 2, "B") }},
		{"Reserve monitor 2 (order C)", func() error { return inv.Reserve("monitor", 2, "C") }},
		{"Reserve webcam 1 (order D)", func() error { return inv.Reserve("webcam", 1, "D") }},
	}
	for _, c := range cmds {
		if err := c.run(); err != nil {
			fmt.Printf("  %-28s REJECTED: %v\n", c.desc, err)
		} else {
			fmt.Printf("  %-28s ok\n", c.desc)
		}
	}

	fmt.Printf("\nEvent log has %d events; dashboard is at position %d (stale)\n",
		len(events.Since(0)), dashboard.Position())
	fmt.Printf("  dashboard.LowStock(3) before sync: %v\n", dashboard.LowStock(3))

	n := dashboard.Sync(events)
	fmt.Printf("\nProjector applies %d events -> position %d\n", n, dashboard.Position())
	fmt.Println("Query (read side): items with fewer than 3 available")
	for _, it := range dashboard.LowStock(3) {
		fmt.Printf("  %-8s available=%d reserved=%d orders=%d\n", it.SKU, it.Available, it.Reserved, it.Orders)
	}

	_ = inv.Reserve("keyboard", 18, "E")
	fmt.Println("\nA new reservation lands on the write side...")
	k, _ := dashboard.Get("keyboard")
	fmt.Printf("  dashboard says keyboard available=%d (eventually consistent)\n", k.Available)
	dashboard.Sync(events)
	k, _ = dashboard.Get("keyboard")
	fmt.Printf("  after sync: keyboard available=%d\n", k.Available)

	fresh := cqrs.NewStockView()
	fresh.Sync(events)
	fmt.Printf("\nA brand-new read model rebuilt from the log reaches position %d with the same data: %v\n",
		fresh.Position(), fresh.LowStock(3))
}
