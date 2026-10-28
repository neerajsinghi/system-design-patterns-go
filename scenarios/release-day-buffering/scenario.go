// Package releaseday simulates the "streaming service buffers after a new
// season release" incident. At launch, every CDN edge misses on the new
// episode at the same moment and all of them fetch it from the origin. The
// origin is pushed past its capacity and falls over; when it comes back,
// every edge still missing the episode retries in the same second and knocks
// it over again. Later, an unrelated origin outage turns every cache expiry
// into an error.
//
// Broken has edges fetching straight from the origin with plain TTLs.
// Fixed pre-warms the episode into every tier before release, puts an origin
// shield between the edges and the origin, and lets the shield serve stale
// content while the origin is down.
package releaseday

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	tieredcache "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/tiered-cache"
	servestale "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/serve-stale"
)

const (
	Edges          = 20
	ViewerRequests = 10 // requests per edge per second for the new episode's manifest
	EdgeTTL        = 30 * time.Second
	ShieldTTL      = 60 * time.Second
	OriginCapacity = 5  // fetches per second the origin can serve
	CrashFor       = 60 // seconds the origin stays down after being overloaded
	OutageFrom     = 240
	OutageTo       = 330 // an unrelated origin outage (a bad deploy) in [OutageFrom, OutageTo)
	Duration       = 420
	episode        = "s2e1/manifest"
)

var (
	errOverloaded = errors.New("origin: overloaded, crashing")
	errDown       = errors.New("origin: 503 service unavailable")
)

// Report is what a run measured.
type Report struct {
	Name string
	// OriginCalls[s] is how many fetches reached the origin in second s.
	OriginCalls [Duration]int
	// ViewerErrors[s] is how many viewer requests failed in second s.
	ViewerErrors [Duration]int
	// OriginCrashes counts how many times the origin was overloaded and fell over.
	OriginCrashes int
	// StaleServed counts answers the shield served from stale content.
	StaleServed uint64
}

// TotalViewerErrors sums ViewerErrors.
func (r Report) TotalViewerErrors() int {
	n := 0
	for _, e := range r.ViewerErrors {
		n += e
	}
	return n
}

// PeakOriginCalls returns the busiest origin second.
func (r Report) PeakOriginCalls() int {
	p := 0
	for _, c := range r.OriginCalls {
		p = max(p, c)
	}
	return p
}

// Print writes a timeline and a summary.
func (r Report) Print(w io.Writer) {
	total := Edges * ViewerRequests
	fmt.Fprintf(w, "%s\n", r.Name)
	fmt.Fprintf(w, "%d edges × %d requests/s for %s; origin capacity %d fetches/s; origin outage t=%d-%ds\n\n",
		Edges, ViewerRequests, episode, OriginCapacity, OutageFrom, OutageTo)
	fmt.Fprintln(w, "  window      viewer errors          origin calls")
	for start := 0; start < Duration; start += 30 {
		errs, calls := 0, 0
		for s := start; s < start+30; s++ {
			errs += r.ViewerErrors[s]
			calls += r.OriginCalls[s]
		}
		pct := 100 * float64(errs) / float64(total*30)
		note := ""
		if start < OutageTo && start+30 > OutageFrom {
			note = "  (origin outage)"
		}
		fmt.Fprintf(w, "  t=%3d-%3ds  %5.1f%% %-12s  %4d%s\n", start, start+29, pct,
			strings.Repeat("█", int(pct/10)), calls, note)
	}
	fmt.Fprintf(w, "\n  failed viewer requests: %d of %d\n", r.TotalViewerErrors(), total*Duration)
	fmt.Fprintf(w, "  busiest origin second: %d fetches (capacity %d); origin crashes: %d\n",
		r.PeakOriginCalls(), OriginCapacity, r.OriginCrashes)
	if r.StaleServed > 0 {
		fmt.Fprintf(w, "  shield refreshes answered from stale content during the outage: %d\n", r.StaleServed)
	}
}

type sim struct {
	mu  sync.Mutex
	sec int
	r   *Report
	// downUntil is when the origin recovers after being overloaded.
	downUntil int
}

func (s *sim) clock() time.Time { s.mu.Lock(); defer s.mu.Unlock(); return time.Unix(int64(s.sec), 0) }

// origin is the origin's fetch handler, with capacity and crash behaviour.
func (s *sim) origin(_ context.Context, key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.r.OriginCalls[s.sec]++
	switch {
	case s.sec < s.downUntil, s.sec >= OutageFrom && s.sec < OutageTo:
		return "", errDown
	case s.r.OriginCalls[s.sec] > OriginCapacity:
		s.downUntil = s.sec + CrashFor
		s.r.OriginCrashes++
		return "", errOverloaded
	}
	return "manifest for " + key, nil
}

func (s *sim) run(edges []*tieredcache.Tier[string]) {
	ctx := context.Background()
	for sec := range Duration {
		s.mu.Lock()
		s.sec = sec
		s.mu.Unlock()
		for _, e := range edges {
			// A second's viewers at one edge arrive together and share the
			// edge's in-flight fetch, so one failure fails all of them
			// rather than each retrying the origin.
			if _, err := e.Fetch(ctx, episode); err != nil {
				s.r.ViewerErrors[sec] += ViewerRequests
				continue
			}
			for range ViewerRequests - 1 {
				if _, err := e.Fetch(ctx, episode); err != nil {
					s.r.ViewerErrors[sec]++
				}
			}
		}
	}
}

func newEdges(parent tieredcache.Fetcher[string], clock func() time.Time) []*tieredcache.Tier[string] {
	edges := make([]*tieredcache.Tier[string], Edges)
	for i := range edges {
		e, err := tieredcache.NewTier(fmt.Sprintf("edge-%d", i), EdgeTTL, parent, tieredcache.WithClock(clock))
		if err != nil {
			panic(err)
		}
		edges[i] = e
	}
	return edges
}

// Broken: edges fetch straight from the origin, nothing is pre-warmed, and
// nothing is served once it expires.
func Broken() Report {
	r := Report{Name: "BROKEN: edges fetch straight from the origin, no pre-warming, no stale serving"}
	s := &sim{r: &r}
	s.run(newEdges(s.origin, s.clock))
	return r
}

// Fixed: the episode is pushed into the shield and every edge before
// release; edges miss into an origin shield; and the shield's origin access
// goes through a serve-stale cache with a 1h grace period and a 10s retry
// interval, so an origin outage is answered from stale content.
func Fixed() Report {
	r := Report{Name: "FIXED: pre-warmed + origin shield + serve-stale at the shield"}
	s := &sim{r: &r}
	stale, err := servestale.New(s.origin, ShieldTTL, time.Hour,
		servestale.WithClock(s.clock), servestale.WithRetryInterval(10*time.Second))
	if err != nil {
		panic(err)
	}
	viaStale := func(ctx context.Context, key string) (string, error) {
		res, err := stale.Get(ctx, key)
		return res.Value, err
	}
	shield, err := tieredcache.NewTier("shield", ShieldTTL, viaStale, tieredcache.WithClock(s.clock))
	if err != nil {
		panic(err)
	}
	edges := newEdges(shield.Fetch, s.clock)

	// Before release: fetch once through the serve-stale layer (so it holds a
	// last-good copy) and push to the shield and every edge. This happens
	// off-peak, so it is not counted against launch-day origin load.
	ctx := context.Background()
	warm := func(ctx context.Context, key string) (string, error) {
		v, err := viaStale(ctx, key)
		s.mu.Lock()
		s.r.OriginCalls[0] = 0
		s.mu.Unlock()
		return v, err
	}
	if err := tieredcache.Prewarm(ctx, warm, []string{episode}, append([]*tieredcache.Tier[string]{shield}, edges...)...); err != nil {
		panic(err)
	}

	s.run(edges)
	r.StaleServed = stale.Stats().Stale
	return r
}
