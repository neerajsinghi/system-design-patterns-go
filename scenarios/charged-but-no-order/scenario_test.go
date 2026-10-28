package chargednoorder

import (
	"reflect"
	"sync"
	"testing"
)

// The invariant: for every checkout, either the order is confirmed and the
// customer was charged exactly once, or there is no order and the net
// charge is zero; and downstream's view of confirmed orders matches the
// database.
func TestBrokenViolatesInvariant(t *testing.T) {
	r := Broken()
	if r.Violations == 0 {
		t.Fatal("broken run should break the invariant")
	}
	for name, n := range map[string]int{
		"charged without an order": r.ChargedNoOrder,
		"double charges":           r.DoubleCharged,
		"orphan events":            r.OrphanEvents,
		"missing events":           r.MissingEvents,
		"orders stuck pending":     r.StuckPending,
		"inventory leaked":         r.InventoryLeaked,
	} {
		if n == 0 {
			t.Errorf("broken run: want some %s, got 0", name)
		}
	}
	if r.ClientRetries == 0 {
		t.Error("broken run: customers should have pressed Pay again")
	}
}

func TestFixedHoldsInvariant(t *testing.T) {
	r := Fixed()
	if r.Violations != 0 || r.ChargedNoOrder != 0 || r.DoubleCharged != 0 ||
		r.OrphanEvents != 0 || r.MissingEvents != 0 || r.EventsBeforeCommit != 0 {
		t.Fatalf("fixed run: %d violations (charged without order %d, double %d, orphan %d, missing %d, before commit %d)",
			r.Violations, r.ChargedNoOrder, r.DoubleCharged, r.OrphanEvents, r.MissingEvents, r.EventsBeforeCommit)
	}
	if r.StuckPending != 0 || r.InventoryLeaked != 0 {
		t.Errorf("fixed run left %d sagas unfinished and %d units reserved", r.StuckPending, r.InventoryLeaked)
	}
	// Every checkout either confirms or is cleanly declined/compensated.
	if r.Confirmed+r.Compensated != Checkouts {
		t.Errorf("fixed run: %d confirmed + %d compensated != %d checkouts", r.Confirmed, r.Compensated, Checkouts)
	}
}

// Each mechanism in the fix did real work.
func TestFixedMechanisms(t *testing.T) {
	r := Fixed()
	if r.SagasResumed != r.Faults.Crashes {
		t.Errorf("resumed %d sagas, want one per crash (%d)", r.SagasResumed, r.Faults.Crashes)
	}
	if r.PaymentReplays == 0 {
		t.Error("retried and resumed charges should be replayed from the idempotency store")
	}
	if r.ConfirmRetries != r.Faults.CommitFails {
		t.Errorf("retried %d confirms, want one per aborted commit (%d)", r.ConfirmRetries, r.Faults.CommitFails)
	}
	if r.RelayFailures == 0 {
		t.Error("the relay should have hit the broker outages and retried")
	}
	if r.Compensated != r.Faults.Declines+r.Faults.Outages {
		t.Errorf("compensated %d sagas, want declines + outages (%d)", r.Compensated, r.Faults.Declines+r.Faults.Outages)
	}
}

// The cost of the fix: customers in a payment outage see a charge and then
// a refund instead of an order; checkout is slower (saga writes, charge
// retries) and events reach downstream a relay pass after commit rather
// than immediately.
func TestFixCosts(t *testing.T) {
	b, f := Broken(), Fixed()
	if b.Refunds != 0 {
		t.Errorf("broken run issued %d refunds, want 0", b.Refunds)
	}
	if f.Refunds != f.Faults.Outages || f.Refunds == 0 {
		t.Errorf("fixed run issued %d refunds, want one per payment outage (%d)", f.Refunds, f.Faults.Outages)
	}
	if f.CleanLatency <= b.CleanLatency || f.MeanLatency <= b.MeanLatency || f.MaxLatency <= b.MaxLatency {
		t.Errorf("fixed checkout should be slower: clean %v vs %v, mean %v vs %v, max %v vs %v",
			f.CleanLatency, b.CleanLatency, f.MeanLatency, b.MeanLatency, f.MaxLatency, b.MaxLatency)
	}
	if f.MeanEventLag < RelayInterval || b.MaxEventLag != 0 {
		t.Errorf("event lag: fixed mean %v (want >= %v), broken max %v (want 0, it publishes inline)",
			f.MeanEventLag, RelayInterval, b.MaxEventLag)
	}
}

func TestSameTraffic(t *testing.T) {
	if b, f := Broken(), Fixed(); b.Faults != f.Faults {
		t.Fatalf("runs saw different faults: %+v vs %+v", b.Faults, f.Faults)
	}
}

// Runs share no state: running them concurrently gives the same reports.
func TestDeterministicAndIndependent(t *testing.T) {
	wantB, wantF := Broken(), Fixed()
	var wg sync.WaitGroup
	got := make([]Report, 6)
	for i := range got {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if i%2 == 0 {
				got[i] = Broken()
			} else {
				got[i] = Fixed()
			}
		}()
	}
	wg.Wait()
	for i, r := range got {
		want := wantF
		if i%2 == 0 {
			want = wantB
		}
		if !reflect.DeepEqual(r, want) {
			t.Errorf("run %d differs from a sequential run", i)
		}
	}
}
