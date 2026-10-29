// Command example runs the same deterministic traffic through five
// balancers. One of the four backends is 10x slower, and the output shows
// how each strategy spreads requests and in-flight load and what latency
// clients see. It then shows IP-hash client affinity and how many clients
// lose their backend when one is removed, for modulo vs rendezvous hashing.
package main

import (
	"fmt"
	"math/rand/v2"
	"time"

	lb "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/load-balancing"
)

const ticks = 2000

// tick is the simulated clock: one tick is one millisecond. The
// least-response-time balancer reads it to time requests.
var tick int

func simNow() time.Time { return time.Unix(0, 0).Add(time.Duration(tick) * time.Millisecond) }

// run sends one request per tick. A request to backend i lasts cost[i] ticks.
func run(bal lb.Balancer, bs []*lb.Backend, cost []int) (counts []int, peak []int64) {
	counts, peak = make([]int, len(bs)), make([]int64, len(bs))
	pending := map[int][]func(){}
	for tick = 0; tick < ticks; tick++ {
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
		{"least response time", func(bs []*lb.Backend) lb.Balancer {
			l, err := lb.NewLeastResponseTime(lb.LRTConfig{Now: simNow}, bs...)
			if err != nil {
				panic(err)
			}
			return l
		}},
	}

	fmt.Println("1. Routing around a slow backend")
	fmt.Printf("%d requests, one per tick. Backend \"slow\" needs %d ticks per request, the rest need 1.\n\n", ticks, cost[3])
	fmt.Printf("%-22s", "strategy")
	for _, n := range names {
		fmt.Printf("%16s", n)
	}
	fmt.Printf("%14s\n", "mean latency")
	for _, s := range strategies {
		bs := fresh()
		counts, peak := run(s.mk(bs), bs, cost)
		fmt.Printf("%-22s", s.name)
		total := 0
		for i := range bs {
			fmt.Printf("%16s", fmt.Sprintf("%d (peak %d)", counts[i], peak[i]))
			total += counts[i] * cost[i]
		}
		fmt.Printf("%11.2f ms\n", float64(total)/ticks)
	}
	fmt.Println("\nRound robin and weighted RR don't look at load, so requests queue on \"slow\" (peak in-flight > 1).")
	fmt.Println("Least connections and P2C watch in-flight counts and route around it, but an idle \"slow\" ties with")
	fmt.Println("an idle fast backend, so it still gets requests. Least response time also knows \"slow\" takes 10 ms")
	fmt.Println("and stops sending to it after its first sample. The flip side: at this load it never probes \"slow\"")
	fmt.Println("again, so it would not notice a recovery until the fast backends got busy.")
	fmt.Println("(Mean latency here is service time only. The simulation doesn't slow a backend as it queues up.)")

	affinity()
}

// affinity routes 1000 clients by source IP over five backends, checks that
// every client sticks, then removes one backend.
func affinity() {
	const clients = 1000
	ips := make([]string, clients)
	for i := range ips {
		ips[i] = fmt.Sprintf("203.0.%d.%d:%d", i/250, i%250+1, 40000+i) // ip:port, like RemoteAddr
	}
	fmt.Printf("\n2. Client affinity: %d clients (by source IP) over 5 backends\n", clients)
	modes := []struct {
		name string
		mode lb.HashMode
	}{{"modulo (ip_hash)", lb.HashModulo}, {"rendezvous", lb.HashRendezvous}}
	for _, m := range modes {
		bs := make([]*lb.Backend, 5)
		for i := range bs {
			bs[i] = &lb.Backend{Name: fmt.Sprintf("b%d", i)}
		}
		h, err := lb.NewIPHash(m.mode, bs...)
		if err != nil {
			panic(err)
		}
		route := func() (map[string]string, map[string]int) {
			where, per := map[string]string{}, map[string]int{}
			for _, ip := range ips {
				b, done, _ := h.PickFor(lb.ClientKey(ip))
				done()
				where[ip] = b.Name
				per[b.Name]++
			}
			return where, per
		}
		before, per := route()
		again, _ := route()
		sticky := 0
		for ip := range before {
			if again[ip] == before[ip] {
				sticky++
			}
		}
		h.Remove("b2")
		after, _ := route()
		moved, onB2 := 0, 0
		for ip, b := range before {
			if b == "b2" {
				onB2++
			}
			if after[ip] != b {
				moved++
			}
		}
		fmt.Printf("  %-17s clients per backend %v, same backend on 2nd request: %d/%d\n",
			m.name, []int{per["b0"], per["b1"], per["b2"], per["b3"], per["b4"]}, sticky, clients)
		fmt.Printf("  %-17s remove b2 (it had %d clients): %d clients moved (%.0f%%)\n", "", onB2, moved, 100*float64(moved)/clients)
	}
	fmt.Println("  Modulo hashing reshuffles most clients when n changes. Rendezvous (like a consistent-hash ring)")
	fmt.Println("  moves only b2's clients. Either way those clients lose any session state that lived on b2.")
}
