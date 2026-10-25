// Command example runs the same deterministic traffic through four
// balancers. One of the four backends is 10x slower, and the output shows
// how each strategy spreads requests and in-flight load.
package main

import (
	"fmt"
	"math/rand/v2"

	lb "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/load-balancing"
)

const ticks = 2000

// run sends one request per tick. A request to backend i lasts cost[i] ticks.
func run(bal lb.Balancer, bs []*lb.Backend, cost []int) (counts []int, peak []int64) {
	counts, peak = make([]int, len(bs)), make([]int64, len(bs))
	pending := map[int][]func(){}
	for tick := 0; tick < ticks; tick++ {
		for _, done := range pending[tick] {
			done()
		}
		delete(pending, tick)
		b, done, err := bal.Pick()
		if err != nil {
			fmt.Println("pick:", err)
			return
		}
		for i := range bs {
			if bs[i] == b {
				counts[i]++
				pending[tick+cost[i]] = append(pending[tick+cost[i]], done)
			}
			peak[i] = max(peak[i], bs[i].Active())
		}
	}
	return counts, peak
}

func main() {
	names := []string{"big", "small", "medium", "slow"}
	weights := []int{4, 1, 2, 1}
	cost := []int{1, 1, 1, 10} // "slow" takes 10 ticks per request
	fresh := func() []*lb.Backend {
		bs := make([]*lb.Backend, len(names))
		for i := range names {
			bs[i] = &lb.Backend{Name: names[i], Weight: weights[i]}
		}
		return bs
	}

	strategies := []struct {
		name string
		mk   func([]*lb.Backend) lb.Balancer
	}{
		{"round robin", func(bs []*lb.Backend) lb.Balancer { return lb.NewRoundRobin(bs...) }},
		{"weighted RR (4:1:2:1)", func(bs []*lb.Backend) lb.Balancer { return lb.NewWeightedRoundRobin(bs...) }},
		{"least connections", func(bs []*lb.Backend) lb.Balancer { return lb.NewLeastConnections(bs...) }},
		{"power of two choices", func(bs []*lb.Backend) lb.Balancer {
			return lb.NewPowerOfTwo(rand.New(rand.NewPCG(42, 42)), bs...)
		}},
	}

	fmt.Printf("%d requests, one per tick. Backend \"slow\" needs %d ticks per request, the rest need 1.\n\n", ticks, cost[3])
	fmt.Printf("%-22s", "strategy")
	for _, n := range names {
		fmt.Printf("%16s", n)
	}
	fmt.Println()
	for _, s := range strategies {
		bs := fresh()
		counts, peak := run(s.mk(bs), bs, cost)
		fmt.Printf("%-22s", s.name)
		for i := range bs {
			fmt.Printf("%16s", fmt.Sprintf("%d (peak %d)", counts[i], peak[i]))
		}
		fmt.Println()
	}
	fmt.Println("\nRound robin and weighted RR don't look at load, so requests queue on \"slow\" (peak in-flight > 1).")
	fmt.Println("Least connections and P2C watch in-flight counts and route around it.")
}
