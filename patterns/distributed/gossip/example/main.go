// Command example spreads a config value and membership through 50 nodes
// over a lossy network, then crashes a node and watches suspicion spread.
package main

import (
	"fmt"

	"github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/gossip"
)

func main() {
	const n = 50
	c, err := gossip.NewCluster(gossip.Config{Nodes: n, Fanout: 2, DropRate: 0.2, SuspectAfter: 8, Seed: 2026})
	if err != nil {
		fmt.Println(err)
		return
	}
	_ = c.Set("n42", "feature.dark-mode", "on")
	fmt.Printf("%d nodes, fanout 2, 20%% message loss; every node starts knowing only itself and seed n0\n", n)
	fmt.Println("n42 sets feature.dark-mode=on")

	for r := 1; r <= 30; r++ {
		c.Round()
		have := 0
		knowAll := 0
		for i := 0; i < n; i++ {
			id := fmt.Sprintf("n%d", i)
			if v, ok := c.Get(id, "feature.dark-mode"); ok && v.Value == "on" {
				have++
			}
			if c.Known(id) == n {
				knowAll++
			}
		}
		fmt.Printf("  round %2d: %2d/%d have the value, %2d/%d know every member\n", r, have, n, knowAll, n)
		if c.Converged("feature.dark-mode") {
			fmt.Printf("converged after %d rounds (log2(%d) ~ 5.6), %d exchanges\n", r, n, c.Messages())
			break
		}
	}

	fmt.Println("\nn7 crashes")
	_ = c.Crash("n7")
	for r := 1; r <= 15; r++ {
		c.Round()
		suspecting := 0
		for i := 0; i < n; i++ {
			if i == 7 {
				continue
			}
			for _, s := range c.Suspects(fmt.Sprintf("n%d", i)) {
				if s == "n7" {
					suspecting++
				}
			}
		}
		if suspecting > 0 || r%3 == 0 {
			fmt.Printf("  +%2d rounds: %2d/%d live nodes suspect n7\n", r, suspecting, n-1)
		}
		if suspecting == n-1 {
			break
		}
	}
}
