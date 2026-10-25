// Command example books a trip (flight, hotel, payment) through an
// orchestrator. The card is declined on step 3, so the hotel and flight are
// cancelled in reverse order.
package main

import (
	"context"
	"errors"
	"fmt"

	saga "github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/saga-orchestration"
)

// inventory is a tiny in-process "service" with reservable seats/rooms.
type inventory struct {
	name     string
	free     int
	reserved map[string]bool
}

func (s *inventory) reserve(trip string) error {
	if s.reserved[trip] {
		return nil // idempotent
	}
	if s.free == 0 {
		return errors.New(s.name + " sold out")
	}
	s.free--
	s.reserved[trip] = true
	fmt.Printf("  %-7s reserved for %s (free=%d)\n", s.name, trip, s.free)
	return nil
}

func (s *inventory) cancel(trip string) error {
	if !s.reserved[trip] {
		return nil // idempotent: already cancelled
	}
	s.free++
	delete(s.reserved, trip)
	fmt.Printf("  %-7s cancelled for %s (free=%d)\n", s.name, trip, s.free)
	return nil
}

func book(o *saga.Orchestrator, flights, hotels *inventory, trip string, cardOK bool) {
	fmt.Printf("saga %s:\n", trip)
	steps := []saga.Step{
		{Name: "reserve-flight",
			Action:     func(context.Context) error { return flights.reserve(trip) },
			Compensate: func(context.Context) error { return flights.cancel(trip) }},
		{Name: "reserve-hotel",
			Action:     func(context.Context) error { return hotels.reserve(trip) },
			Compensate: func(context.Context) error { return hotels.cancel(trip) }},
		{Name: "charge-card",
			Action: func(context.Context) error {
				if !cardOK {
					fmt.Println("  payment declined")
					return errors.New("card declined")
				}
				fmt.Println("  payment captured")
				return nil
			}},
		{Name: "send-confirmation",
			Action: func(context.Context) error { fmt.Println("  confirmation email sent"); return nil }},
	}
	res := o.Run(context.Background(), steps)
	fmt.Printf("  => %s", res.Status)
	if res.FailedStep != "" {
		fmt.Printf(" (failed at %s: %v)", res.FailedStep, res.Err)
	}
	fmt.Println()
	for _, e := range res.Log {
		fmt.Printf("     log: %-17s %s\n", e.Step, e.Kind)
	}
}

func main() {
	flights := &inventory{name: "flight", free: 2, reserved: map[string]bool{}}
	hotels := &inventory{name: "hotel", free: 2, reserved: map[string]bool{}}
	o := &saga.Orchestrator{CompensationAttempts: 3}

	book(o, flights, hotels, "trip-1", true)
	fmt.Println()
	book(o, flights, hotels, "trip-2", false)
	fmt.Printf("\nfinal inventory: flight free=%d, hotel free=%d (trip-2 left nothing behind)\n", flights.free, hotels.free)
}
