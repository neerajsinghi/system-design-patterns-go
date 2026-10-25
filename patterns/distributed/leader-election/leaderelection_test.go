package leaderelection

import (
	"errors"
	"testing"
	"time"
)

const ttl = 10 * time.Second

func cluster(t *testing.T, margin time.Duration, ids ...string) (*ManualClock, *LeaseStore, []*Node) {
	t.Helper()
	clk := NewManualClock(time.Unix(1000, 0))
	store := NewLeaseStore(clk)
	var nodes []*Node
	for _, id := range ids {
		n, err := NewNode(id, store, clk, ttl, margin)
		if err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, n)
	}
	return clk, store, nodes
}

func leaders(nodes []*Node) []string {
	var out []string
	for _, n := range nodes {
		if n.IsLeader() {
			out = append(out, n.ID)
		}
	}
	return out
}

func TestElectionBasics(t *testing.T) {
	clk, store, nodes := cluster(t, 0, "n1", "n2", "n3")
	for i, n := range nodes {
		err := n.Tick()
		if i == 0 && err != nil {
			t.Fatalf("first node should win: %v", err)
		}
		if i > 0 && !errors.Is(err, ErrHeld) {
			t.Fatalf("%s: want ErrHeld, got %v", n.ID, err)
		}
	}
	if got := leaders(nodes); len(got) != 1 || got[0] != "n1" || nodes[0].Term() != 1 {
		t.Fatalf("leaders=%v term=%d", got, nodes[0].Term())
	}

	clk.Advance(5 * time.Second)
	_ = nodes[0].Tick() // renewal keeps the term
	if l := store.Current(); l.Term != 1 || !l.Expires.Equal(clk.Now().Add(ttl)) {
		t.Fatalf("renewal: %+v", l)
	}
}

func TestFailoverScenarios(t *testing.T) {
	tests := []struct {
		name       string
		disrupt    func(store *LeaseStore, n1 *Node)
		advance    time.Duration
		wantLeader string
		wantTerm   uint64
	}{
		{"leader resigns: immediate failover", func(_ *LeaseStore, n1 *Node) { _ = n1.Resign() }, 0, "n2", 2},
		{"leader partitioned: failover after ttl", func(s *LeaseStore, _ *Node) { s.Partition("n1", true) }, ttl, "n2", 2},
		{"leader partitioned briefly: no failover", func(s *LeaseStore, _ *Node) { s.Partition("n1", true) }, ttl / 2, "n1", 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clk, store, nodes := cluster(t, 0, "n1", "n2")
			_ = nodes[0].Tick()
			tt.disrupt(store, nodes[0])
			clk.Advance(tt.advance)
			_ = nodes[1].Tick()
			l := store.Current()
			if l.Holder != tt.wantLeader || l.Term != tt.wantTerm {
				t.Fatalf("lease=%+v", l)
			}
			if got := leaders(nodes); len(got) != 1 || got[0] != tt.wantLeader {
				t.Fatalf("nodes believing they lead: %v", got)
			}
		})
	}
}

// TestAtMostOneLeader drives a long randomless schedule of ticks, partitions
// and heals and checks that no two nodes ever believe they lead at once.
func TestAtMostOneLeader(t *testing.T) {
	clk, store, nodes := cluster(t, time.Second, "n1", "n2", "n3")
	for step := 0; step < 300; step++ {
		victim := nodes[(step/20)%3].ID
		store.Partition(victim, step%20 < 12) // each node in turn is cut off for a while
		for _, n := range nodes {
			_ = n.Tick()
		}
		if got := leaders(nodes); len(got) > 1 {
			t.Fatalf("step %d: split brain %v", step, got)
		}
		clk.Advance(time.Second)
	}
	if store.Current().Term < 3 {
		t.Fatalf("expected several failovers, term=%d", store.Current().Term)
	}
}

func TestMarginStepsDownEarly(t *testing.T) {
	clk, _, nodes := cluster(t, 2*time.Second, "n1")
	_ = nodes[0].Tick()
	clk.Advance(ttl - 2*time.Second)
	if nodes[0].IsLeader() {
		t.Fatal("node should stop acting as leader margin before expiry")
	}
}

func TestStaleTermWriteRejected(t *testing.T) {
	clk, store, nodes := cluster(t, 0, "old", "new")
	r := &Resource{}
	_ = nodes[0].Tick()
	oldTerm := nodes[0].Term()
	if err := r.Write(oldTerm, "old: a"); err != nil {
		t.Fatal(err)
	}
	// "old" pauses (GC, VM migration) past its lease; "new" takes over.
	store.Partition("old", true)
	clk.Advance(ttl)
	_ = nodes[1].Tick()
	if err := r.Write(nodes[1].Term(), "new: b"); err != nil {
		t.Fatal(err)
	}
	// "old" wakes up and writes with the term it remembers.
	if err := r.Write(oldTerm, "old: c"); !errors.Is(err, ErrStaleTerm) {
		t.Fatalf("stale write should be rejected, got %v", err)
	}
	if got := r.Log(); len(got) != 2 {
		t.Fatalf("log=%v", got)
	}
}

func TestBadConfig(t *testing.T) {
	clk := NewManualClock(time.Unix(0, 0))
	s := NewLeaseStore(clk)
	for _, c := range []struct{ ttl, margin time.Duration }{{0, 0}, {time.Second, time.Second}, {time.Second, -1}} {
		if _, err := NewNode("x", s, clk, c.ttl, c.margin); !errors.Is(err, ErrBadTTL) {
			t.Errorf("NewNode(%v,%v) err=%v", c.ttl, c.margin, err)
		}
	}
	if _, err := s.TryAcquire("x", 0); !errors.Is(err, ErrBadTTL) {
		t.Error("zero ttl")
	}
}
