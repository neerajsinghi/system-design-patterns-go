package outoforder

import (
	"reflect"
	"testing"
)

// The invariant: the state machine never sees an invalid transition, no
// customer is told an unpaid order has shipped, and every order ends shipped.
func TestBrokenViolatesInvariant(t *testing.T) {
	r := Broken()
	if r.InvalidTransitions == 0 || r.UnpaidShippedEmails == 0 || r.WrongFinalStates == 0 {
		t.Fatalf("broken run should apply events out of order: invalid %d, unpaid emails %d, wrong final states %d",
			r.InvalidTransitions, r.UnpaidShippedEmails, r.WrongFinalStates)
	}
}

func TestFixedHoldsInvariant(t *testing.T) {
	r := Fixed()
	if r.InvalidTransitions != 0 || r.UnpaidShippedEmails != 0 || r.WrongFinalStates != 0 {
		t.Fatalf("fixed run: invalid %d, unpaid emails %d, wrong final states %d (%v)",
			r.InvalidTransitions, r.UnpaidShippedEmails, r.WrongFinalStates, r.FinalStates)
	}
	if r.FinalStates["shipped"] != Orders {
		t.Fatalf("fixed run: %d orders shipped, want %d", r.FinalStates["shipped"], Orders)
	}
	// No event was lost, and every retry lands well inside the gap timeout,
	// so nothing should have been skipped.
	if r.Gaps != 0 || r.Repairs != 0 {
		t.Fatalf("fixed run skipped %d gaps (%d repairs), want none", r.Gaps, r.Repairs)
	}
	if r.Held == 0 || r.MaxHoldWait >= GapTimeout {
		t.Fatalf("resequencer held %d events, max wait %v; want some held, all under %v", r.Held, r.MaxHoldWait, GapTimeout)
	}
}

// Both runs see the same upstream reordering, so the difference is the design.
func TestIdenticalTraffic(t *testing.T) {
	b, f := Broken(), Fixed()
	if b.Events != f.Events || b.Retried != f.Retried || b.UpstreamReorders != f.UpstreamReorders || b.UpstreamReorders == 0 {
		t.Fatalf("traffic differs or has no reordering: broken %d/%d/%d, fixed %d/%d/%d",
			b.Events, b.Retried, b.UpstreamReorders, f.Events, f.Retried, f.UpstreamReorders)
	}
}

// Key partitioning alone fixes the worker-pool race but not the retries from
// the second region; that's what the resequencer is for.
func TestPartitioningAloneIsNotEnough(t *testing.T) {
	r := Fixed()
	if r.PartitionOnlyInvalid == 0 || r.PartitionOnlyInvalid >= Broken().InvalidTransitions {
		t.Fatalf("partition-only invalid transitions %d; want fewer than broken but more than 0", r.PartitionOnlyInvalid)
	}
}

// Ordering per key keeps the parallelism: the fixed design finishes about as
// fast as the broken pool, while one global consumer falls seconds behind.
func TestKeyPartitioningKeepsParallelism(t *testing.T) {
	b, f := Broken(), Fixed()
	g := f.Global
	if g.InvalidTransitions != 0 || g.WrongFinalStates != 0 {
		t.Fatalf("global consumer should also be correct: invalid %d, wrong %d", g.InvalidTransitions, g.WrongFinalStates)
	}
	if f.Finish > b.Finish+b.Finish/10 {
		t.Fatalf("fixed finished at %v, more than 10%% after broken (%v)", f.Finish, b.Finish)
	}
	if g.Finish < 3*f.Finish {
		t.Fatalf("global consumer finished at %v, want at least 3x the partitioned %v", g.Finish, f.Finish)
	}
}

// The fix's cost: an order whose event is lost waits for the gap timeout and
// is repaired from the source of truth, and no other order is held longer.
func TestLostEventWaitsGapTimeoutOnlyForItsOrder(t *testing.T) {
	c := Fixed().LostEvent
	if c.GapWait < GapTimeout || c.GapWait > GapTimeout+TickEvery {
		t.Fatalf("gap wait %v, want between %v and %v", c.GapWait, GapTimeout, GapTimeout+TickEvery)
	}
	if c.ShippedDelay < GapTimeout+RepairCost {
		t.Fatalf("lost order's OrderShipped applied after %v, want at least %v", c.ShippedDelay, GapTimeout+RepairCost)
	}
	if c.Repairs != 1 || c.InvalidTransitions != 0 || c.WrongFinalStates != 0 {
		t.Fatalf("lost-event run: repairs %d, invalid %d, wrong final states %d; want 1, 0, 0",
			c.Repairs, c.InvalidTransitions, c.WrongFinalStates)
	}
	if c.OthersMaxHoldWait != c.NormalMaxHoldWait {
		t.Fatalf("other orders held up to %v, want the same as with no loss (%v)", c.OthersMaxHoldWait, c.NormalMaxHoldWait)
	}
}

func TestDeterministic(t *testing.T) {
	if a, b := Broken(), Broken(); !reflect.DeepEqual(a, b) {
		t.Fatal("broken run is not deterministic")
	}
	if a, b := Fixed(), Fixed(); !reflect.DeepEqual(a, b) {
		t.Fatal("fixed run is not deterministic")
	}
}
