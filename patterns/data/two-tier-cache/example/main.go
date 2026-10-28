// Command example simulates a URL shortener whose one viral link gets 90% of
// the traffic. Four app servers share a three-node L2 cache. It compares the
// load on each L2 node with no protection, with hot-key replication, and with
// replication plus an in-process L1, then shows L1's staleness window.
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	twotier "github.com/neerajsinghi/system-design-patterns-go/patterns/data/two-tier-cache"
)

const (
	servers  = 4
	nodes    = 3
	requests = 40_000
	viral    = "abc123"
)

type world struct {
	mu   sync.Mutex
	now  time.Time
	urls map[string]string
}

func (w *world) clock() time.Time     { w.mu.Lock(); defer w.mu.Unlock(); return w.now }
func (w *world) tick(d time.Duration) { w.mu.Lock(); w.now = w.now.Add(d); w.mu.Unlock() }
func (w *world) set(k, v string)      { w.mu.Lock(); w.urls[k] = v; w.mu.Unlock() }
func (w *world) load(_ context.Context, k string) (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.urls[k], nil
}

func newWorld() *world {
	w := &world{now: time.Unix(0, 0), urls: map[string]string{viral: "https://example.com/final-over"}}
	for i := range 1000 {
		w.urls[fmt.Sprintf("link%d", i)] = fmt.Sprintf("https://example.com/%d", i)
	}
	return w
}

// run sends requests spread over 20 simulated seconds to random servers and
// returns the ops each L2 node served.
func run(l1TTL time.Duration, opts ...twotier.Option) []uint64 {
	w := newWorld()
	shared, err := twotier.NewShared[string](nodes, time.Hour, w.clock)
	if err != nil {
		log.Fatal(err)
	}
	opts = append([]twotier.Option{twotier.WithClock(w.clock)}, opts...)
	clients := make([]*twotier.Client[string], servers)
	for i := range clients {
		clients[i], _ = twotier.NewClient(shared, w.load, l1TTL, opts...)
	}
	r := rand.New(rand.NewPCG(3, 4))
	ctx := context.Background()
	for range requests {
		key := viral
		if r.IntN(10) == 0 {
			key = fmt.Sprintf("link%d", r.IntN(1000))
		}
		if _, err := clients[r.IntN(servers)].Get(ctx, key); err != nil {
			log.Fatal(err)
		}
		w.tick(500 * time.Microsecond)
	}
	return shared.Ops()
}

func show(title string, ops []uint64) {
	fmt.Println(title)
	var top uint64
	for _, n := range ops {
		top = max(top, n)
	}
	for i, n := range ops {
		fmt.Printf("   L2 node %d: %6d ops  %s\n", i, n, strings.Repeat("█", int(n*40/max(top, 1))))
	}
}

func main() {
	fmt.Printf("%d requests over 20s, 90%% for one viral link, %d app servers, %d L2 nodes\n\n", requests, servers, nodes)

	show("1) L2 only: the viral key lives on one node, which takes almost everything", run(0))
	fmt.Println()
	show("2) Hot-key replication: the viral key is stored on all 3 nodes, reads pick one at random",
		run(0, twotier.WithHotKeys(nodes, viral)))
	fmt.Println()
	show("3) Replication + 1s L1 on each app server: most reads never leave the process",
		run(time.Second, twotier.WithHotKeys(nodes, viral)))

	fmt.Println()
	fmt.Println("4) The price of L1: the link's target changes, written via server 0")
	w := newWorld()
	shared, _ := twotier.NewShared[string](nodes, time.Hour, w.clock)
	s0, _ := twotier.NewClient(shared, w.load, time.Second, twotier.WithClock(w.clock), twotier.WithHotKeys(nodes, viral))
	s1, _ := twotier.NewClient(shared, w.load, time.Second, twotier.WithClock(w.clock), twotier.WithHotKeys(nodes, viral))
	ctx := context.Background()
	s0.Get(ctx, viral)
	s1.Get(ctx, viral)
	w.set(viral, "https://example.com/highlights")
	s0.Invalidate(viral)
	for _, at := range []string{"+0s", "+0.5s", "+1.0s"} {
		v0, _ := s0.Get(ctx, viral)
		v1, _ := s1.Get(ctx, viral)
		fmt.Printf("   t=%-6s server 0 -> %-32s server 1 -> %s\n", at, v0, v1)
		w.tick(500 * time.Millisecond)
	}
	fmt.Println("   Server 1 served the old target until its 1s L1 entry expired.")
}
