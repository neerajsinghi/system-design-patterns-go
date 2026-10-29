package staleriderlocation

import (
	"reflect"
	"testing"
)

// The invariant: no shown position is older than MaxShownAge unless it is
// flagged stale, the dot never moves to an older fix after a newer one, and
// the p95 distance between the dot and the rider is at most MaxP95ErrMetres.
func TestBrokenViolatesInvariant(t *testing.T) {
	r := Broken()
	if r.StaleUnflaggedSeconds == 0 || r.BackwardJumps == 0 || r.ErrP95 <= MaxP95ErrMetres {
		t.Fatalf("broken run should show stale, backwards and far-off dots: unflagged stale %ds, backward jumps %d, p95 error %.0fm",
			r.StaleUnflaggedSeconds, r.BackwardJumps, r.ErrP95)
	}
	if r.OverwrittenByOlder == 0 || r.FlaggedSeconds != 0 {
		t.Fatalf("broken store overwrote %d newer fixes, flagged %ds; want >0 and 0", r.OverwrittenByOlder, r.FlaggedSeconds)
	}
}

func TestFixedHoldsInvariant(t *testing.T) {
	r := Fixed()
	if r.StaleUnflaggedSeconds != 0 || r.MaxUnflaggedAge > MaxShownAge {
		t.Fatalf("fixed run showed %ds of stale positions unflagged (oldest unflagged %v, limit %v)",
			r.StaleUnflaggedSeconds, r.MaxUnflaggedAge, MaxShownAge)
	}
	if r.BackwardJumps != 0 {
		t.Fatalf("fixed run jumped backwards %d times", r.BackwardJumps)
	}
	if r.ErrP95 > MaxP95ErrMetres {
		t.Fatalf("fixed run p95 display error %.0fm, want <= %.0fm", r.ErrP95, MaxP95ErrMetres)
	}
	// The riders who lost signal must have been flagged, by lease expiry.
	if r.StaleMarkers < 4 || r.FlaggedSeconds == 0 {
		t.Fatalf("fixed run sent %d stale markers and flagged %ds; want the silent riders flagged", r.StaleMarkers, r.FlaggedSeconds)
	}
	if r.RejectedOlder == 0 {
		t.Fatal("fixed run should have rejected late, older fixes")
	}
}

// After the gateway crash, the registry routes pushes to whichever node the
// customer reconnected to; only the few sent in the detection window are lost.
func TestRoutingFollowsReconnect(t *testing.T) {
	r := Fixed()
	if r.ReconnectedCustomer == 0 || r.RoutedToNewNode == 0 {
		t.Fatalf("reconnected %d customers, routed %d pushes to the new node; want both > 0", r.ReconnectedCustomer, r.RoutedToNewNode)
	}
	if limit := r.ReconnectedCustomer * NodeDeadAfter; r.LostToDeadNode > limit {
		t.Fatalf("%d pushes lost to the dead node, want at most %d (one per fix during detection)", r.LostToDeadNode, limit)
	}
}

// The fix's cost: one persistent connection per customer and a push per
// accepted fix, far more messages than polling. The one-slot drop-oldest
// queue throws away intermediate positions for slow links, which is fine
// because the invariant still holds.
func TestFixedCost(t *testing.T) {
	b, f := Broken(), Fixed()
	if f.PersistentConns != Riders || b.PersistentConns != 0 {
		t.Fatalf("persistent connections: fixed %d, broken %d; want %d and 0", f.PersistentConns, b.PersistentConns, Riders)
	}
	if f.FanOut < 0.98 || f.FanOut > 1 {
		t.Fatalf("fan-out %.2f pushes per accepted fix, want just under 1 (one watcher per rider, minus pushes lost to the crash)", f.FanOut)
	}
	if f.Delivered < 5*b.PollRequests {
		t.Fatalf("fixed wrote %d messages vs %d polls; want push to cost several times more messages", f.Delivered, b.PollRequests)
	}
	if f.Dropped == 0 || f.Pushes != f.Delivered+f.Dropped+f.Undelivered {
		t.Fatalf("pushes %d, delivered %d, dropped %d, lost in crashed queues %d: books don't balance or nothing dropped",
			f.Pushes, f.Delivered, f.Dropped, f.Undelivered)
	}
}

// Both runs see the same rider movement and network.
func TestIdenticalTraffic(t *testing.T) {
	b, f := Broken(), Fixed()
	if b.FixesSent != f.FixesSent || b.ReorderedFixes != f.ReorderedFixes || b.RedeliveredFixes != f.RedeliveredFixes ||
		b.LostToSignal != f.LostToSignal || b.CustomerSeconds != f.CustomerSeconds || b.RedeliveredFixes == 0 {
		t.Fatalf("traffic differs: broken %+v, fixed %+v", b, f)
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
