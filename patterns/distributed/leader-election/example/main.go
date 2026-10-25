// Command example simulates three nodes electing a leader, the leader being
// partitioned away, a new leader taking over with a higher term, and the old
// leader's late write being rejected.
package main

import (
	"fmt"
	"time"

	le "github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/leader-election"
)

func main() {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := le.NewManualClock(start)
	store := le.NewLeaseStore(clk)
	const ttl = 10 * time.Second
	var nodes []*le.Node
	for _, id := range []string{"node-a", "node-b", "node-c"} {
		n, _ := le.NewNode(id, store, clk, ttl, time.Second)
		nodes = append(nodes, n)
	}
	journal := &le.Resource{}

	tick := func() {
		for _, n := range nodes {
			_ = n.Tick()
		}
	}
	status := func(label string) {
		l := store.Current()
		leader := "none"
		for _, n := range nodes {
			if n.IsLeader() {
				leader = n.ID
			}
		}
		fmt.Printf("t=%2ds %-34s lease: holder=%s term=%d | acting leader: %s\n",
			int(clk.Now().Sub(start).Seconds()), label, l.Holder, l.Term, leader)
	}

	tick()
	status("all nodes campaign")
	a := nodes[0]
	oldTerm := a.Term()
	fmt.Printf("       node-a writes with term %d: %v\n", oldTerm, journal.Write(oldTerm, "a: set x=1"))

	for i := 0; i < 2; i++ {
		clk.Advance(3 * time.Second)
		tick()
	}
	status("node-a renews every 3s")

	store.Partition("node-a", true)
	fmt.Println("       --- node-a is partitioned from the lease store (and pauses) ---")
	for i := 0; i < 4; i++ {
		clk.Advance(3 * time.Second)
		tick()
		status("ticking (node-a cut off)")
	}
	b := nodes[1]
	fmt.Printf("       node-b writes with term %d: %v\n", b.Term(), journal.Write(b.Term(), "b: set x=2"))

	fmt.Printf("       node-a resumes and writes with old term %d: %v\n", oldTerm, journal.Write(oldTerm, "a: set x=3"))
	store.Partition("node-a", false)
	clk.Advance(3 * time.Second)
	tick()
	status("healed: node-a sees ErrHeld")
	fmt.Printf("journal: %v\n", journal.Log())
}
