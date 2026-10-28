package hotshortlink

import "testing"

// The invariant: no cache node is pushed past its capacity, so no redirect is
// rejected, and an edited link is visible everywhere within the L1 TTL.
func TestBrokenViolatesInvariant(t *testing.T) {
	r := Broken()
	if r.PeakNodeOps() <= NodeCapacity || r.Rejected == 0 {
		t.Fatalf("broken run should overload the viral key's node: peak %d, rejected %d", r.PeakNodeOps(), r.Rejected)
	}
}

func TestFixedHoldsInvariant(t *testing.T) {
	r := Fixed()
	if r.PeakNodeOps() > NodeCapacity || r.Rejected != 0 {
		t.Fatalf("fixed run: peak %d ops/s (capacity %d), rejected %d", r.PeakNodeOps(), NodeCapacity, r.Rejected)
	}
	if r.StaleAfterEdit > L1TTL {
		t.Fatalf("old target served for %v after the edit, want at most the L1 TTL %v", r.StaleAfterEdit, L1TTL)
	}
}

// The trade-off made explicit: the broken run has no staleness, the fixed one does.
func TestL1CostsStaleness(t *testing.T) {
	if b, f := Broken().StaleAfterEdit, Fixed().StaleAfterEdit; b != 0 || f == 0 {
		t.Fatalf("staleness: broken %v (want 0), fixed %v (want > 0)", b, f)
	}
}
