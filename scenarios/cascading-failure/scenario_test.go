package cascadingfailure

import (
	"reflect"
	"sync"
	"testing"
)

// Each run simulates five minutes of traffic, so the tests share one of each.
var (
	broken = sync.OnceValue(Broken)
	fixed  = sync.OnceValue(Fixed)
)

// The invariant: in every phase at least 99.9% of product pages render
// (degraded pages count as available), and the core worker pool never runs
// out of workers.
func TestBrokenViolatesInvariant(t *testing.T) {
	r := broken()
	if !r.PoolExhausted() {
		t.Error("broken run should exhaust the shared worker pool")
	}
	for i := 1; i <= 2; i++ {
		if a := r.Phases[i].Availability(); a >= AvailabilityTarget {
			t.Errorf("%s: broken availability %.2f%%, want below %.1f%%", Phases[i].Name, a, AvailabilityTarget)
		}
	}
	// One bad instance out of four is enough to fail most pages, not just a quarter.
	if f := pct(r.Phases[1].Failed, r.Phases[1].Pages); f < 25 {
		t.Errorf("phase 1: broken failed %.2f%% of pages, want more than the slow instance's 25%% share", f)
	}
	// Gateway retries amplify the load on the product service.
	if p := r.Phases[2]; p.Attempts < 2*p.Pages {
		t.Errorf("phase 2: %d attempts for %d pages, want retry amplification", p.Attempts, p.Pages)
	}
	if r.Wasted == 0 {
		t.Error("broken run should finish work the gateway had already given up on")
	}
	// Before anything goes wrong the broken design is fine; it is not simply
	// under-provisioned. (Pages are counted by arrival, so a few that arrive
	// just before t=30s and meet the slow instance fail.)
	if a := r.Phases[0].Availability(); a < AvailabilityTarget {
		t.Errorf("baseline availability %.2f%%: broken should be healthy before the incident", a)
	}
}

func TestFixedHoldsInvariant(t *testing.T) {
	r := fixed()
	if r.PoolExhausted() {
		t.Error("fixed run exhausted the worker pool")
	}
	for i, p := range r.Phases {
		if a := p.Availability(); a < AvailabilityTarget {
			t.Errorf("%s: availability %.2f%%, want >= %.1f%%", Phases[i].Name, a, AvailabilityTarget)
		}
		if p.P99 >= GatewayTimeout {
			t.Errorf("%s: p99 %s reaches the gateway timeout", Phases[i].Name, p.P99)
		}
		if p.Attempts != p.Pages {
			t.Errorf("%s: %d attempts for %d pages, gateway should never need to retry", Phases[i].Name, p.Attempts, p.Pages)
		}
	}
	if r.Ejections == 0 {
		t.Errorf("outlier detection should have ejected %s", SlowInstance)
	}
	if r.BreakerOpens == 0 {
		t.Error("breaker should have opened when all of recommendations was slow")
	}
}

// One slow instance is handled by ejecting it, not by degrading every page:
// phase 1 costs barely more degraded pages than the healthy baseline.
func TestFixedPhase1EjectsInsteadOfDegrading(t *testing.T) {
	r := fixed()
	base, p1 := pct(r.Phases[0].Degraded, r.Phases[0].Pages), pct(r.Phases[1].Degraded, r.Phases[1].Pages)
	if p1 > base+0.5 {
		t.Errorf("phase 1 degraded %.2f%% vs baseline %.2f%%: the slow instance should be ejected", p1, base)
	}
}

// The cost of the fix: while all of recommendations is slow, shoppers get
// "popular items" instead of personalised recommendations.
func TestFixedCostIsDegradedPagesInPhase2(t *testing.T) {
	p := fixed().Phases[2]
	if p.Failed != 0 {
		t.Errorf("phase 2: %d failed pages, want 0", p.Failed)
	}
	if d := pct(p.Degraded, p.Pages); d < 99 {
		t.Errorf("phase 2: only %.2f%% degraded; every page should have fallen back", d)
	}
}

// Fresh runs, side by side, must match the shared ones exactly.
func TestDeterministicAndIndependent(t *testing.T) {
	var b, f Report
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); b = Broken() }()
	go func() { defer wg.Done(); f = Fixed() }()
	wg.Wait()
	if !reflect.DeepEqual(b, broken()) || !reflect.DeepEqual(f, fixed()) {
		t.Fatal("runs differ; the simulation must be deterministic and share no state")
	}
	// Identical traffic: both designs see the same pages in every phase.
	for i := range Phases {
		if b.Phases[i].Pages != f.Phases[i].Pages {
			t.Errorf("%s: broken got %d pages, fixed %d", Phases[i].Name, b.Phases[i].Pages, f.Phases[i].Pages)
		}
	}
}
