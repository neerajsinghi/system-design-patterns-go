package servicediscovery

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// threeInstances registers a, b, c with a long lease.
func threeInstances(t *testing.T, clk *fakeClock) *Registry {
	t.Helper()
	r := NewRegistry(clk.Now)
	for _, id := range []string{"a", "b", "c"} {
		if err := r.Register(Instance{Service: "api", ID: id, Addr: "10.0.0." + id}, time.Hour); err != nil {
			t.Fatal(err)
		}
	}
	return r
}

// countingSource wraps a Lookuper and counts queries.
type countingSource struct {
	Lookuper
	mu sync.Mutex
	n  int
}

func (c *countingSource) Lookup(s string) []Instance {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
	return c.Lookuper.Lookup(s)
}

func TestCachingResolverValidation(t *testing.T) {
	reg := NewRegistry(nil)
	if _, err := NewCachingResolver(nil, time.Second); err == nil {
		t.Error("nil source accepted")
	}
	if _, err := NewCachingResolver(reg, -time.Second); err == nil {
		t.Error("negative ttl accepted")
	}
	if _, err := NewCachingResolver(reg, time.Second, WithFailureEviction(0)); err == nil {
		t.Error("zero quarantine accepted")
	}
	r, _ := NewCachingResolver(reg, time.Second)
	if _, err := r.Pick("missing"); !errors.Is(err, ErrNoInstances) {
		t.Errorf("err = %v, want ErrNoInstances", err)
	}
}

func TestCachingResolverHonoursTTL(t *testing.T) {
	clk := newClock()
	src := &countingSource{Lookuper: threeInstances(t, clk)}
	r, _ := NewCachingResolver(src, 5*time.Second, WithResolverClock(clk.Now))

	steps := []struct {
		advance     time.Duration
		wantLookups int
	}{
		{0, 1},               // first resolve queries the source
		{4 * time.Second, 1}, // within TTL: cached
		{time.Second, 2},     // exactly at TTL: expired, re-query
		{4999 * time.Millisecond, 2},
		{time.Millisecond, 3},
	}
	for i, s := range steps {
		clk.Advance(s.advance)
		if _, err := r.Resolve("api"); err != nil {
			t.Fatal(err)
		}
		if src.n != s.wantLookups {
			t.Errorf("step %d: lookups = %d, want %d", i, src.n, s.wantLookups)
		}
	}
	if st := r.Stats(); st.Lookups != 3 || st.CacheHits != 2 {
		t.Errorf("stats = %+v", st)
	}

	zero, _ := NewCachingResolver(src, 0, WithResolverClock(clk.Now))
	before := src.n
	for range 3 {
		_, _ = zero.Pick("api")
	}
	if src.n-before != 3 {
		t.Errorf("ttl 0 should query every time, got %d lookups for 3 picks", src.n-before)
	}
}

// staleErrors removes instance c from the registry at removeAt and sends one
// request per 100ms for total. It counts picks that land on c after its
// removal (each one is a failed call).
func staleErrors(t *testing.T, ttl time.Duration, opts ...CachingOption) (errs int, lastErr time.Duration) {
	t.Helper()
	clk := newClock()
	reg := threeInstances(t, clk)
	r, err := NewCachingResolver(reg, ttl, append([]CachingOption{WithResolverClock(clk.Now)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	const step, total, removeAt = 100 * time.Millisecond, 60 * time.Second, 12400 * time.Millisecond
	removed := false
	for el := time.Duration(0); el < total; el += step {
		if !removed && el >= removeAt {
			reg.Deregister("api", "c")
			removed = true
		}
		inst, err := r.Pick("api")
		if err != nil {
			t.Fatal(err)
		}
		if removed && inst.ID == "c" {
			errs++
			lastErr = el - removeAt
			r.ReportFailure("api", inst)
		}
		clk.Advance(step)
	}
	return errs, lastErr
}

func TestStaleEndpointErrorsGrowWithTTL(t *testing.T) {
	tests := []struct {
		name      string
		ttl       time.Duration
		opts      []CachingOption
		maxErrors int
		minErrors int
		maxWindow time.Duration // last error at most this long after removal
	}{
		{"no cache", 0, nil, 0, 0, 0},
		// Removal at 12.4s; caches were filled at multiples of the TTL, so
		// the stale window is the rest of the current TTL period.
		{"ttl 1s", time.Second, nil, 3, 1, time.Second},
		{"ttl 5s", 5 * time.Second, nil, 10, 8, 5 * time.Second},
		{"ttl 30s", 30 * time.Second, nil, 60, 55, 30 * time.Second},
		{"ignore ttl", 30 * time.Second, []CachingOption{WithIgnoreTTL()}, 160, 155, 48 * time.Second},
		// Negative feedback: the first failed call evicts c, whatever the TTL.
		{"ttl 30s + evict on failure", 30 * time.Second, []CachingOption{WithFailureEviction(time.Minute)}, 1, 1, 300 * time.Millisecond},
		{"ignore ttl + evict on failure", time.Hour, []CachingOption{WithIgnoreTTL(), WithFailureEviction(time.Minute)}, 1, 1, 300 * time.Millisecond},
	}
	prev := -1
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			errs, last := staleErrors(t, tc.ttl, tc.opts...)
			t.Logf("%-30s errors=%3d last error %v after removal", tc.name, errs, last)
			if errs < tc.minErrors || errs > tc.maxErrors {
				t.Errorf("errors = %d, want in [%d, %d]", errs, tc.minErrors, tc.maxErrors)
			}
			if last > tc.maxWindow {
				t.Errorf("last error %v after removal, want <= %v", last, tc.maxWindow)
			}
		})
	}
	// Monotonic in TTL for the plain cases.
	for _, tc := range tests[:5] {
		errs, _ := staleErrors(t, tc.ttl, tc.opts...)
		if errs < prev {
			t.Errorf("%s: %d errors, fewer than a shorter TTL's %d", tc.name, errs, prev)
		}
		prev = errs
	}
}

// A crashed instance is still listed by the registry until its lease runs
// out. Quarantine keeps the re-resolve from putting it straight back.
func TestQuarantineSurvivesLaggingRegistry(t *testing.T) {
	clk := newClock()
	reg := threeInstances(t, clk) // c's lease is an hour: the registry won't notice the crash
	r, _ := NewCachingResolver(reg, 30*time.Second, WithResolverClock(clk.Now), WithFailureEviction(10*time.Second))
	crashed := Instance{Service: "api", ID: "c", Addr: "10.0.0.c"}
	_, _ = r.Resolve("api")
	r.ReportFailure("api", crashed)

	for range 30 {
		inst, _ := r.Pick("api")
		if inst.ID == "c" {
			t.Fatal("quarantined endpoint picked while the registry still lists it")
		}
		clk.Advance(100 * time.Millisecond)
	}
	if st := r.Stats(); st.Evictions != 1 || st.Lookups != 2 {
		t.Errorf("stats = %+v, want 1 eviction and 2 lookups (initial + forced re-resolve)", st)
	}
	// After the quarantine, and once the cached answer expires, c comes back
	// (maybe it recovered). If it's still dead, the next failure bans it again.
	clk.Advance(30 * time.Second)
	seen := false
	for range 3 {
		if inst, _ := r.Pick("api"); inst.ID == "c" {
			seen = true
		}
	}
	if !seen {
		t.Error("endpoint never retried after quarantine expired")
	}
}

func TestQuarantinePanicModeWhenAllBanned(t *testing.T) {
	clk := newClock()
	reg := NewRegistry(clk.Now)
	_ = reg.Register(Instance{Service: "api", ID: "only", Addr: "10.0.0.1"}, time.Hour)
	r, _ := NewCachingResolver(reg, time.Minute, WithResolverClock(clk.Now), WithFailureEviction(time.Minute))
	inst, _ := r.Pick("api")
	r.ReportFailure("api", inst)
	if got, err := r.Pick("api"); err != nil || got.ID != "only" {
		t.Errorf("all endpoints quarantined: got %v, %v; want the full list as a last resort", got, err)
	}
}

func TestReportFailureIsNoOpWithoutEviction(t *testing.T) {
	clk := newClock()
	src := &countingSource{Lookuper: threeInstances(t, clk)}
	r, _ := NewCachingResolver(src, time.Minute, WithResolverClock(clk.Now))
	inst, _ := r.Pick("api")
	r.ReportFailure("api", inst)
	_, _ = r.Pick("api")
	if src.n != 1 || r.Stats().Evictions != 0 {
		t.Errorf("lookups = %d, stats = %+v; want the plain TTL cache untouched", src.n, r.Stats())
	}
}

func TestCachingResolverConcurrent(t *testing.T) {
	clk := newClock()
	reg := threeInstances(t, clk)
	r, _ := NewCachingResolver(reg, time.Second, WithResolverClock(clk.Now), WithFailureEviction(time.Second))
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 300 {
				inst, err := r.Pick("api")
				if err != nil {
					t.Error(err)
					return
				}
				if (g+i)%17 == 0 {
					r.ReportFailure("api", inst)
				}
				if i%25 == 0 {
					clk.Advance(100 * time.Millisecond)
				}
				if g == 0 && i == 150 {
					reg.Deregister("api", "b")
					_ = reg.Register(Instance{Service: "api", ID: fmt.Sprint("d", i), Addr: "10.0.0.d"}, time.Hour)
				}
			}
		}()
	}
	wg.Wait()
}
