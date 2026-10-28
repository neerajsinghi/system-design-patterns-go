package seatdoublebooking

import (
	"reflect"
	"testing"
)

// The invariant: every seat is sold at most once, no customer is charged
// twice, and seats abandoned at checkout come back to sale.
func TestBrokenViolatesInvariant(t *testing.T) {
	r := Broken()
	if len(r.DoubleBooked) == 0 {
		t.Error("broken run should sell at least one seat twice")
	}
	if r.TicketsIssued <= r.SeatsSold {
		t.Errorf("broken run issued %d tickets for %d seats, want more tickets than seats", r.TicketsIssued, r.SeatsSold)
	}
	if r.ChargedTwice == 0 {
		t.Error("broken run should charge a customer twice on a retried webhook")
	}
	if r.SeatsStuck == 0 || r.AbandonedResold != 0 {
		t.Errorf("broken run: %d seats stuck, %d abandoned seats resold; want stuck seats and none resold", r.SeatsStuck, r.AbandonedResold)
	}
}

func TestFixedHoldsInvariant(t *testing.T) {
	r := Fixed()
	if len(r.DoubleBooked) != 0 || r.TicketsIssued != r.SeatsSold {
		t.Errorf("fixed run double-booked %v (%d tickets for %d seats)", r.DoubleBooked, r.TicketsIssued, r.SeatsSold)
	}
	if r.ChargedTwice != 0 {
		t.Errorf("fixed run charged %d customers twice", r.ChargedTwice)
	}
	if r.WebhookRetries == 0 || r.RetriesReplayed != r.WebhookRetries {
		t.Errorf("fixed run replayed %d of %d duplicate webhooks, want all of them", r.RetriesReplayed, r.WebhookRetries)
	}
	if r.SeatsStuck != 0 {
		t.Errorf("fixed run left %d seats stuck", r.SeatsStuck)
	}
	if r.AbandonedResold == 0 || r.HoldsReaped == 0 {
		t.Errorf("fixed run: %d abandoned seats resold, %d holds reaped; want both > 0", r.AbandonedResold, r.HoldsReaped)
	}
	if r.SeatsSold <= Broken().SeatsSold {
		t.Error("returning abandoned seats to sale should sell more seats than the broken run")
	}
}

// Holds alone are not the correctness guarantee: when the hold store loses
// its holds, the compare-and-set on the seat version is what stops a second
// sale, and the loser is refunded instead of ticketed.
func TestCompareAndSetCatchesWhatHoldsMiss(t *testing.T) {
	r := Fixed()
	if r.HoldsLost == 0 || r.RefundsLostRace == 0 {
		t.Fatalf("fixed run: %d holds lost in failover, %d bookings refused by compare-and-set; want both > 0", r.HoldsLost, r.RefundsLostRace)
	}
}

// The cost of the fix: shoppers who lose a race are told "seat taken", and
// payers who finish after their hold expired are refunded. The broken design
// never says no; it sells the seat twice instead.
func TestFixCostsRefusalsAndRefunds(t *testing.T) {
	b, f := Broken(), Fixed()
	if b.SeatTaken != 0 || b.Refunds() != 0 {
		t.Errorf("broken run: %d told seat taken, %d refunds; want 0 and 0", b.SeatTaken, b.Refunds())
	}
	if f.SeatTaken == 0 || f.RefundsLate == 0 {
		t.Errorf("fixed run: %d told seat taken, %d late payers refunded; want both > 0", f.SeatTaken, f.RefundsLate)
	}
}

func TestDeterministic(t *testing.T) {
	if a, b := Broken(), Broken(); !reflect.DeepEqual(a, b) {
		t.Error("Broken is not deterministic")
	}
	if a, b := Fixed(), Fixed(); !reflect.DeepEqual(a, b) {
		t.Error("Fixed is not deterministic")
	}
}
