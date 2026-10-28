// Package hotshortlink simulates the "URL shortener crashes during the
// cricket final" incident: one short link goes viral and receives 90% of all
// redirects. The shared cache cluster has plenty of total capacity, but the
// viral key lives on one node, which is driven far past its limit and starts
// rejecting requests for every key it owns, not just the viral one.
//
// Broken uses the shared cache only. Fixed adds the two-tier cache pattern:
// a 1-second in-process L1 cache on each app server and the viral key
// replicated across all cache nodes. Midway through, the link's target is
// edited, which measures what L1 costs in staleness.
package hotshortlink

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	twotier "github.com/neerajsinghi/system-design-patterns-go/patterns/data/two-tier-cache"
)

const (
	AppServers     = 4
	CacheNodes     = 3
	NodeCapacity   = 2_000 // ops per second one cache node can serve
	RequestsPerSec = 6_000
	ViralShare     = 0.9
	Duration       = 30 // simulated seconds
	EditAt         = 15 // the viral link's target is edited half-way through this second
	L1TTL          = time.Second
	viral          = "ipl-final"
)

// Report is what a run measured.
type Report struct {
	Name string
	// NodeOps[s][n] is how many operations cache node n received in second s.
	NodeOps [Duration][CacheNodes]uint64
	// Rejected are requests a node turned away because it was over capacity.
	Rejected uint64
	// StaleAfterEdit is how long, after the edit, any server kept redirecting
	// to the old target.
	StaleAfterEdit time.Duration
}

// PeakNodeOps returns the highest per-node, per-second load.
func (r Report) PeakNodeOps() uint64 {
	var peak uint64
	for _, sec := range r.NodeOps {
		for _, n := range sec {
			peak = max(peak, n)
		}
	}
	return peak
}

// Print writes per-node load and a summary.
func (r Report) Print(w io.Writer) {
	fmt.Fprintf(w, "%s\n", r.Name)
	fmt.Fprintf(w, "%d redirects/s (%.0f%% for one link), %d app servers, %d cache nodes of %d ops/s each\n\n",
		RequestsPerSec, ViralShare*100, AppServers, CacheNodes, NodeCapacity)
	fmt.Fprintln(w, "  average load per cache node:")
	for n := range CacheNodes {
		var sum uint64
		for s := range Duration {
			sum += r.NodeOps[s][n]
		}
		avg := sum / Duration
		over := ""
		if avg > NodeCapacity {
			over = fmt.Sprintf("  OVER CAPACITY (%.1fx)", float64(avg)/NodeCapacity)
		}
		fmt.Fprintf(w, "    node %d: %5d ops/s  %s%s\n", n, avg, strings.Repeat("█", int(min(avg, 6_000)/150)), over)
	}
	fmt.Fprintf(w, "\n  busiest node-second: %d ops (capacity %d)\n", r.PeakNodeOps(), NodeCapacity)
	fmt.Fprintf(w, "  requests rejected by overloaded nodes: %d\n", r.Rejected)
	fmt.Fprintf(w, "  after the link was edited at t=%d.5s, old target still served for: %v\n", EditAt, r.StaleAfterEdit)
}

func run(name string, l1 time.Duration, opts ...twotier.Option) Report {
	r := Report{Name: name}
	var mu sync.Mutex
	now := time.Unix(0, 0)
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	target := "https://example.com/live-score"
	load := func(_ context.Context, key string) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		if key == viral {
			return target, nil
		}
		return "https://example.com/" + key, nil
	}

	shared, err := twotier.NewShared[string](CacheNodes, time.Hour, clock)
	if err != nil {
		panic(err)
	}
	rng := rand.New(rand.NewPCG(5, 6))
	opts = append([]twotier.Option{twotier.WithClock(clock), twotier.WithPicker(rng.IntN)}, opts...)
	servers := make([]*twotier.Client[string], AppServers)
	for i := range servers {
		servers[i], err = twotier.NewClient(shared, load, l1, opts...)
		if err != nil {
			panic(err)
		}
	}

	ctx := context.Background()
	const newTarget = "https://example.com/highlights"
	var lastStale time.Time
	editTime := time.Unix(EditAt, 0).Add(time.Second / 2)
	for sec := range Duration {
		before := shared.Ops()
		for i := range RequestsPerSec {
			mu.Lock()
			now = time.Unix(int64(sec), 0).Add(time.Duration(i) * time.Second / RequestsPerSec)
			mu.Unlock()
			if sec == EditAt && i == RequestsPerSec/2 {
				mu.Lock()
				target = newTarget
				mu.Unlock()
				servers[0].Invalidate(viral) // the edit goes through server 0
			}
			key := viral
			if rng.Float64() >= ViralShare {
				key = fmt.Sprintf("link-%d", rng.IntN(5_000))
			}
			v, err := servers[rng.IntN(AppServers)].Get(ctx, key)
			if err != nil {
				panic(err)
			}
			if key == viral && !clock().Before(editTime) && v != newTarget {
				lastStale = clock()
			}
		}
		after := shared.Ops()
		for n := range CacheNodes {
			ops := after[n] - before[n]
			r.NodeOps[sec][n] = ops
			if ops > NodeCapacity {
				r.Rejected += ops - NodeCapacity
			}
		}
	}
	if !lastStale.IsZero() {
		r.StaleAfterEdit = lastStale.Sub(editTime).Round(10 * time.Millisecond)
	}
	return r
}

// Broken reads every redirect from the shared cache cluster. The viral key
// lives on one node.
func Broken() Report {
	return run("BROKEN: shared cache only; the viral key lives on one node", 0)
}

// Fixed adds a 1s in-process L1 on every app server and replicates the viral
// key on every cache node. In production the hot key would be found by
// hot-key detection; here it is named up front.
func Fixed() Report {
	return run("FIXED: two-tier cache (1s L1 per server + viral key replicated on all nodes)",
		L1TTL, twotier.WithHotKeys(CacheNodes, viral))
}
