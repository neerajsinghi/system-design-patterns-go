package releaseday

import "testing"

// The invariant: the origin is never asked for more than its capacity, and no
// viewer request fails, including during an origin outage.
func TestBrokenViolatesInvariant(t *testing.T) {
	r := Broken()
	if r.PeakOriginCalls() <= OriginCapacity || r.OriginCrashes == 0 || r.TotalViewerErrors() == 0 {
		t.Fatalf("broken run should overload the origin: peak %d, crashes %d, viewer errors %d",
			r.PeakOriginCalls(), r.OriginCrashes, r.TotalViewerErrors())
	}
}

func TestFixedHoldsInvariant(t *testing.T) {
	r := Fixed()
	if r.PeakOriginCalls() > OriginCapacity || r.OriginCrashes != 0 {
		t.Fatalf("fixed run: peak %d origin fetches/s (capacity %d), crashes %d", r.PeakOriginCalls(), OriginCapacity, r.OriginCrashes)
	}
	if n := r.TotalViewerErrors(); n != 0 {
		t.Fatalf("fixed run failed %d viewer requests", n)
	}
	if r.StaleServed == 0 {
		t.Fatal("fixed run should have ridden out the outage on stale content")
	}
}

func TestFixedLaunchNeedsNoOriginFetch(t *testing.T) {
	if r := Fixed(); r.OriginCalls[0] != 0 {
		t.Fatalf("pre-warmed launch second made %d origin fetches, want 0", r.OriginCalls[0])
	}
}
