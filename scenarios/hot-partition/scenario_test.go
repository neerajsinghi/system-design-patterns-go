package hotpartition

import (
	"testing"
	"time"

	hks "github.com/neerajsinghi/system-design-patterns-go/patterns/data/hot-key-salting"
)

// The invariant: once the finale poll has had one second to be detected, no
// partition is asked for more than its capacity, so no vote is throttled; and
// every poll reads back exactly the number of votes the table accepted.
func TestBrokenViolatesInvariant(t *testing.T) {
	r := Broken()
	if r.PeakLoad() <= Capacity || r.ThrottledFrom(VotingOpens+1) == 0 {
		t.Fatalf("broken run should keep throttling: peak %d, throttled after detection window %d", r.PeakLoad(), r.ThrottledFrom(VotingOpens+1))
	}
	if r.ColdThrottled == 0 {
		t.Fatal("broken run should also throttle votes for unrelated polls on the hot partition")
	}
}

func TestFixedHoldsInvariant(t *testing.T) {
	r := Fixed()
	if r.DetectedAt != VotingOpens {
		t.Fatalf("finale poll salted at t=%d, want t=%d", r.DetectedAt, VotingOpens)
	}
	for s := VotingOpens + 1; s < Duration; s++ {
		if r.BusiestLoad[s] > Capacity || r.Throttled[s] != 0 {
			t.Fatalf("t=%ds: busiest partition %d (capacity %d), throttled %d", s, r.BusiestLoad[s], Capacity, r.Throttled[s])
		}
	}
	if r.CountMismatches != 0 {
		t.Fatalf("%d polls read back a total different from their accepted votes", r.CountMismatches)
	}
	if r.Accepted+r.TotalThrottled() != uint64(Duration-VotingOpens)*FinalePerSec+Duration*ColdPerSec {
		t.Fatalf("accepted %d + throttled %d does not add up to the traffic sent", r.Accepted, r.TotalThrottled())
	}
}

// Scaling out does not help: one key still maps to one partition.
func TestScalingOutDoesNotHelp(t *testing.T) {
	so := Broken().ScaledOut
	if so == nil || so.Partitions != 2*Partitions {
		t.Fatal("broken report should include the scaled-out run")
	}
	if so.PeakLoad() <= Capacity || so.ThrottledFrom(VotingOpens+1) == 0 {
		t.Fatalf("scaled-out run: peak %d, throttled %d; doubling partitions should not fix a single hot key", so.PeakLoad(), so.TotalThrottled())
	}
}

// The cost of the fix: reading the salted poll scatter-gathers K physical
// keys, while cold polls still cost one lookup.
func TestSaltingCostsReadFanout(t *testing.T) {
	b, f := Broken(), Fixed()
	if b.FinaleReadLookups != 1 || b.ColdReadLookups != 1 {
		t.Fatalf("broken reads: finale %d, cold %d lookups, want 1 and 1", b.FinaleReadLookups, b.ColdReadLookups)
	}
	if f.FinaleReadLookups != Shards || f.ColdReadLookups != 1 {
		t.Fatalf("fixed reads: finale %d, cold %d lookups, want %d and 1", f.FinaleReadLookups, f.ColdReadLookups, Shards)
	}
}

// Detection lag is the only throttling left. Salting the poll up front (the
// "we know the final is tonight" case) removes it.
func TestPreSaltingRemovesDetectionLag(t *testing.T) {
	r := run("pre-salted", Partitions, hks.WithMode(hks.Adaptive), hks.WithDetector(Threshold, time.Second), hks.WithHotKeys(finale))
	if n := r.TotalThrottled(); n != 0 {
		t.Fatalf("pre-salted run throttled %d votes, want 0", n)
	}
	if f := Fixed(); f.Throttled[VotingOpens] == 0 {
		t.Fatal("adaptive run should pay some throttling in the detection second")
	}
}
