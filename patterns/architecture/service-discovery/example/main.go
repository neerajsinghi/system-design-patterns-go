// Command example registers three loopback instances, lets one stop
// heartbeating, and shows the client-side resolver routing around it.
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
}
