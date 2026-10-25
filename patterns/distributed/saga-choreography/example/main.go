// Command example runs two orders through a choreographed saga. The second
// order is paid for but the item is out of stock, so the payment service
// compensates with a refund when it hears InventoryFailed.
package main

import (
	"fmt"

	saga "github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/saga-choreography"
)

func main() {
	// duplicate=true: the bus delivers every event twice, like an
	// at-least-once broker. The idempotent handlers make that harmless.
	s := saga.NewSystem(
		map[string]int{"alice": 100, "bob": 100},
		map[string]int{"keyboard": 1},
		true,
	)
	_ = s.Orders.Place("order-1", "alice", "keyboard", 60)
	_ = s.Orders.Place("order-2", "bob", "keyboard", 60)
	n := s.Bus.RunUntilIdle(1000)

	fmt.Println("event log (no central coordinator, each service reacts to the previous event):")
	for _, e := range s.Bus.History() {
		line := fmt.Sprintf("  #%-2d %-18s %s", e.ID, e.Type, e.OrderID)
		if e.Reason != "" {
			line += " (" + e.Reason + ")"
		}
		fmt.Println(line)
	}
	fmt.Printf("\n%d deliveries for %d events (every event delivered twice, applied once)\n", n, len(s.Bus.History()))
	fmt.Printf("order-1: %s, alice balance=%d\n", s.Orders.Status("order-1"), s.Payments.Balance("alice"))
	fmt.Printf("order-2: %s, bob balance=%d (refunded)\n", s.Orders.Status("order-2"), s.Payments.Balance("bob"))
	fmt.Printf("keyboard stock=%d\n", s.Inventory.Stock("keyboard"))
}
