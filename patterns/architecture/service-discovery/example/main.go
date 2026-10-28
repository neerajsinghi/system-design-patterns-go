// Command example registers three loopback instances, lets one stop
// heartbeating, and shows the client-side resolver routing around it. It then
// simulates DNS-style TTL caching and the stale-endpoint problem: how many
// calls go to a removed or crashed instance for different cache TTLs, and how
// evict-on-failure cuts that to one.
package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	sd "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/service-discovery"
)

// clock is a manual clock, so the lease timeline is deterministic.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func main() {
	clk := &clock{t: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	reg := sd.NewRegistry(clk.Now)
	const ttl = 10 * time.Second

	var servers []*httptest.Server
	for _, id := range []string{"pay-1", "pay-2", "pay-3"} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, "charged by %s", id)
		}))
		defer srv.Close()
		servers = append(servers, srv)
		if err := reg.Register(sd.Instance{Service: "payments", ID: id, Addr: srv.URL}, ttl); err != nil {
			fmt.Println("register:", err)
			return
		}
		fmt.Printf("t=0s   %s registered at %s (ttl %s)\n", id, srv.URL, ttl)
	}

	resolver := sd.NewResolver(reg, 2*time.Second, clk.Now)
	call := func(n int) {
		for i := 0; i < n; i++ {
			inst, err := resolver.Pick("payments")
			if err != nil {
				fmt.Println("       pick:", err)
				return
			}
			resp, err := http.Get(inst.Addr + "/charge")
			if err != nil {
				fmt.Println("       call failed:", err)
				resolver.Invalidate("payments")
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			fmt.Printf("       -> %s\n", body)
		}
	}

	fmt.Println("\nclient calls (round robin over the cached lookup):")
	call(3)

	fmt.Println("\nt=6s   pay-1 and pay-2 heartbeat. pay-3 hangs and stops heartbeating")
	clk.Advance(6 * time.Second)
	_ = reg.Heartbeat("payments", "pay-1")
	_ = reg.Heartbeat("payments", "pay-2")

	clk.Advance(5 * time.Second)
	fmt.Printf("t=11s  pay-3's lease has expired. live instances: %d\n", len(reg.Lookup("payments")))
	fmt.Println("       client cache is older than 2s, so the next pick refreshes it:")
	call(4)

	fmt.Println("\nt=11s  pay-1 shuts down gracefully and deregisters")
	reg.Deregister("payments", "pay-1")
	servers[0].Close()
	resolver.Invalidate("payments")
	call(2)

	clk.Advance(20 * time.Second)
	fmt.Printf("\nt=31s  nobody heartbeated. sweep removed %d lease(s)\n", reg.Sweep())
	call(1)

	staleEndpoints()
}

// simulate runs 60 simulated seconds of traffic (10 calls/s, round robin)
// against three instances, a, b and c, through a CachingResolver. At 12.4s, c
// goes away. With crash=false it is deregistered at once (a graceful
// scale-in). With crash=true it just stops heartbeating, so the registry keeps
// listing it until its 10s lease runs out. A call that lands on c fails, and
// is reported to the resolver.
func simulate(crash bool, ttl time.Duration, opts ...sd.CachingOption) (errs int, lookups uint64) {
	clk := &clock{t: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	reg := sd.NewRegistry(clk.Now)
	const lease = 10 * time.Second
	for _, id := range []string{"a", "b", "c"} {
		_ = reg.Register(sd.Instance{Service: "api", ID: id, Addr: "10.0.0." + id + ":80"}, lease)
	}
	r, err := sd.NewCachingResolver(reg, ttl, append([]sd.CachingOption{sd.WithResolverClock(clk.Now)}, opts...)...)
	if err != nil {
		panic(err)
	}
	const step, total, goneAt = 100 * time.Millisecond, 60 * time.Second, 12400 * time.Millisecond
	gone := false
	for el := time.Duration(0); el < total; el += step {
		if !gone && el >= goneAt {
			gone = true
			if !crash {
				reg.Deregister("api", "c")
			}
		}
		if el%(3*time.Second) == 0 { // live instances heartbeat every 3s
			for _, id := range []string{"a", "b", "c"} {
				if id == "c" && gone {
					continue
				}
				_ = reg.Heartbeat("api", id)
			}
		}
		inst, err := r.Pick("api")
		if err == nil && gone && inst.ID == "c" {
			errs++
			r.ReportFailure("api", inst) // a no-op unless eviction is on
		}
		clk.Advance(step)
	}
	return errs, r.Stats().Lookups
}

func staleEndpoints() {
	fmt.Println("\n--- DNS-style TTL caching and the stale-endpoint problem ---")
	fmt.Println("600 calls over 60s (10/s, round robin over a, b, c). Instance c goes away at t=12.4s.")
	fmt.Println("scale-in: c deregisters at once. crash: c stops heartbeating, and its 10s lease lapses at t=22s.")
	fmt.Printf("\n%-34s %16s %13s %10s\n", "client resolver", "errors scale-in", "errors crash", "lookups")
	rows := []struct {
		name string
		ttl  time.Duration
		opts []sd.CachingOption
	}{
		{"no cache (lookup every call)", 0, nil},
		{"TTL 1s", time.Second, nil},
		{"TTL 5s", 5 * time.Second, nil},
		{"TTL 30s", 30 * time.Second, nil},
		{"ignores TTL (old JVM default)", 30 * time.Second, []sd.CachingOption{sd.WithIgnoreTTL()}},
		{"TTL 30s + evict on failure", 30 * time.Second, []sd.CachingOption{sd.WithFailureEviction(30 * time.Second)}},
		{"TTL 5s + evict on failure", 5 * time.Second, []sd.CachingOption{sd.WithFailureEviction(30 * time.Second)}},
	}
	for _, row := range rows {
		scaleIn, lookups := simulate(false, row.ttl, row.opts...)
		crash, _ := simulate(true, row.ttl, row.opts...)
		fmt.Printf("%-34s %16d %13d %10d\n", row.name, scaleIn, crash, lookups)
	}
	fmt.Println("\nWithout feedback, errors grow with the TTL: the client keeps using its cached answer until it")
	fmt.Println("expires, and if it ignores TTL it never recovers (every 3rd call fails for the remaining 47.6s).")
	fmt.Println("In a crash, even \"no cache\" fails for about 10s, because the registry itself only learns when")
	fmt.Println("the lease lapses: a short client TTL can't beat the health signal behind it. The lags add up:")
	fmt.Println("with TTL 5s, the cache refreshed at t=20s still got c (lease valid to 22s) and kept it until 25s.")
	fmt.Println("Evict-on-failure needs one failed call. It drops c from the cache, re-resolves, and quarantines")
	fmt.Println("c's address, so a registry that still lists it can't hand it straight back.")
}
