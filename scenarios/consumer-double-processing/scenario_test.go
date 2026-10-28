package consumerdouble

import (
	"sync"
	"testing"
)

// The invariant: after the redrive, every event's side effect has been
// applied exactly once, and the group's lag is back to zero.
func TestBrokenViolatesInvariant(t *testing.T) {
	r := Broken()
	if r.DoubleApplied == 0 || r.PointsOverCredited == 0 {
		t.Fatalf("broken run should credit some events twice: %d double-applied, %d points", r.DoubleApplied, r.PointsOverCredited)
	}
	if r.ReplaysAfterDeploy == 0 || r.ReplaysAfterCrash == 0 {
		t.Fatalf("broken run should replay after both deploy kills and crashes: %d / %d", r.ReplaysAfterDeploy, r.ReplaysAfterCrash)
	}
	if r.PoisonCrashes < 2 || r.MaxStuck == 0 {
		t.Fatalf("poison event should crash-loop the consumer and stall its partition: %d crashes, %d stuck", r.PoisonCrashes, r.MaxStuck)
	}
	if r.MaxApplications < 3 {
		t.Fatalf("crash-loop laps should apply some event at least 3 times, worst was %dx", r.MaxApplications)
	}
}

func TestFixedHoldsInvariant(t *testing.T) {
	r := Fixed()
	if r.DoubleApplied != 0 || r.Lost != 0 || r.MaxApplications != 1 {
		t.Fatalf("fixed run: %d double-applied, %d lost, worst %dx; want exactly once", r.DoubleApplied, r.Lost, r.MaxApplications)
	}
	if r.FinalLag != 0 {
		t.Fatalf("fixed run ends with lag %d, want 0", r.FinalLag)
	}
	if r.Redeliveries == 0 || r.InboxSkipped != r.Redeliveries {
		t.Fatalf("every redelivery should be skipped by the inbox: %d redeliveries, %d skipped", r.Redeliveries, r.InboxSkipped)
	}
	if r.Crashes != 0 || r.MaxStuck != 0 {
		t.Fatalf("a failing event should not crash or stall the consumer: %d crashes, %d stuck", r.Crashes, r.MaxStuck)
	}
	// The poison event and the transient one go to the retry queue; only the
	// poison one exhausts its attempts and is redriven after the fix.
	if r.RetryQueued != 2 || r.DLQPeak != 1 || r.DLQFinal != 0 || r.Redriven != 1 {
		t.Fatalf("retry/DLQ: queued %d, DLQ peak %d, at end %d, redriven %d; want 2, 1, 0, 1",
			r.RetryQueued, r.DLQPeak, r.DLQFinal, r.Redriven)
	}
}

func TestSameTrafficAndDeploys(t *testing.T) {
	b, f := Broken(), Fixed()
	if b.Events != f.Events || b.Events == 0 {
		t.Fatalf("runs saw different traffic: %d vs %d events", b.Events, f.Events)
	}
	if b.Lost != 0 {
		t.Fatalf("commit-after-processing should not lose events, broken lost %d", b.Lost)
	}
	if b.FinalLag != 0 {
		t.Fatalf("after the parser fix the broken run should also catch up, final lag %d", b.FinalLag)
	}
	if f.MaxLag >= b.MaxLag {
		t.Fatalf("fixed max lag %d should be below broken %d", f.MaxLag, b.MaxLag)
	}
}

// The fix's costs: the inbox holds one row per processed event until it is
// pruned, and the poison event's effect still waits for the fix and redrive.
func TestFixCost(t *testing.T) {
	r := Fixed()
	if r.InboxRows != r.Events {
		t.Fatalf("inbox has %d rows, want one per event (%d)", r.InboxRows, r.Events)
	}
	if r.PoisonApplied < FixAt {
		t.Fatalf("poison event applied at t=%d, before the fix at t=%d", r.PoisonApplied, FixAt)
	}
	if b := Broken(); b.InboxRows != 0 {
		t.Fatalf("broken run has no inbox, yet %d rows", b.InboxRows)
	}
}

func TestDeterministicUnderConcurrency(t *testing.T) {
	want := [2]Report{Broken(), Fixed()}
	var wg sync.WaitGroup
	got := make([][2]Report, 4)
	for i := range got {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got[i] = [2]Report{Broken(), Fixed()}
		}()
	}
	wg.Wait()
	for i, g := range got {
		if g != want {
			t.Fatalf("run %d differs from the first run", i)
		}
	}
}
