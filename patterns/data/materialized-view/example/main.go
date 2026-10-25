// Command example keeps a "revenue per customer" dashboard up to date from an
// orders table's change stream, and compares the work done with a full
// recomputation.
package main

import (
	"fmt"
	"log"

	"github.com/neerajsinghi/system-design-patterns-go/patterns/data/materialized-view"
)

func dollars(c int64) string { return fmt.Sprintf("$%d.%02d", c/100, c%100) }

func printView(v *materializedview.View) {
	for _, r := range v.All() {
		fmt.Printf("    %-8s orders=%-5d revenue=%s\n", r.Customer, r.Orders, dollars(r.Revenue))
	}
}

func main() {
	orders := materializedview.NewTable()
	view := materializedview.NewView()
	must := func(err error) {
		if err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("Bulk load: 10,000 historical orders across 3 customers")
	customers := []string{"acme", "globex", "initech"}
	for i := range 10_000 {
		must(orders.Insert(materializedview.Order{
			ID: fmt.Sprintf("h%d", i), Customer: customers[i%3], Status: "shipped", Amount: int64(100 + i%900),
		}))
	}
	n, err := view.CatchUp(orders)
	must(err)
	fmt.Printf("  view applied %d changes (LSN %d)\n", n, view.LSN())
	printView(view)

	fmt.Println("\nLive traffic, as change events:")
	must(orders.Insert(materializedview.Order{ID: "n1", Customer: "acme", Status: "placed", Amount: 50_000}))
	fmt.Println("  INSERT n1 acme $500.00")
	must(orders.Update("h0", func(o *materializedview.Order) { o.Status = "cancelled" }))
	fmt.Println("  UPDATE h0 status=cancelled (retract its revenue)")
	must(orders.Update("n1", func(o *materializedview.Order) { o.Customer = "globex" }))
	fmt.Println("  UPDATE n1 customer acme -> globex (move between groups)")
	must(orders.Delete("h1"))
	fmt.Println("  DELETE h1")

	n, err = view.CatchUp(orders)
	must(err)
	fmt.Printf("\n  view applied only %d deltas (LSN %d)\n", n, view.LSN())
	printView(view)

	full := materializedview.Recompute(orders.Rows())
	match := len(full) == len(view.All())
	for i, r := range view.All() {
		match = match && r == full[i]
	}
	fmt.Printf("\nA full recompute scans %d rows to get the same answer: match=%v\n", len(orders.Rows()), match)

	fmt.Println("\nThe change stream redelivers an event (at-least-once delivery):")
	last := orders.ChangesSince(view.LSN() - 1)[0]
	must(view.Apply(last))
	fmt.Printf("  re-applied LSN %d: ignored, view still at LSN %d\n", last.LSN, view.LSN())

	must(orders.Insert(materializedview.Order{ID: "n2", Customer: "acme", Status: "placed", Amount: 1}))
	must(orders.Insert(materializedview.Order{ID: "n3", Customer: "acme", Status: "placed", Amount: 1}))
	skipped := orders.ChangesSince(view.LSN() + 1)[0]
	fmt.Printf("  a consumer that skips an event and delivers LSN %d gets: %v\n", skipped.LSN, view.Apply(skipped))
}
