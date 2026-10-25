// Command example migrates a legacy monolith to a new service one endpoint
// at a time, using a strangler-fig router in front of both.
package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"

	sf "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/strangler-fig"
)

func backend(name string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "%s handled %s %s", name, r.Method, r.URL.Path)
	}))
}

func proxyTo(srv *httptest.Server) http.Handler {
	u, _ := url.Parse(srv.URL)
	return httputil.NewSingleHostReverseProxy(u)
}

func main() {
	legacy := backend("monolith")
	defer legacy.Close()
	modern := backend("orders-svc")
	defer modern.Close()

	router := sf.NewRouter(proxyTo(legacy), proxyTo(modern), nil)
	front := httptest.NewServer(router)
	defer front.Close()

	get := func(path, user string) string {
		req, _ := http.NewRequest(http.MethodGet, front.URL+path, nil)
		req.Header.Set("X-User-ID", user)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return "error: " + err.Error()
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return string(b)
	}
	share := func(path string) int {
		modern := 0
		for i := 0; i < 200; i++ {
			req, _ := http.NewRequest(http.MethodGet, front.URL+path, nil)
			req.Header.Set("X-User-ID", fmt.Sprintf("user-%d", i))
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				continue
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.Header.Get(sf.HeaderServedBy) == "modern" {
				modern++
			}
		}
		return modern
	}

	fmt.Println("Phase 0: everything still goes to the monolith")
	fmt.Println("  ", get("/orders/1", "alice"))
	fmt.Println("  ", get("/billing/1", "alice"))

	_ = router.SetRule(sf.Rule{Prefix: "/orders", Percent: 10})
	fmt.Printf("\nPhase 1: canary /orders at 10%% -> %d of 200 users on orders-svc\n", share("/orders/1"))

	_ = router.SetRule(sf.Rule{Prefix: "/orders", Percent: 50})
	fmt.Printf("Phase 2: /orders at 50%%       -> %d of 200 users on orders-svc\n", share("/orders/1"))

	_ = router.SetRule(sf.Rule{Prefix: "/orders", Percent: 0})
	fmt.Printf("Phase 3: error spike. Roll back to 0%% -> %d of 200 users on orders-svc\n", share("/orders/1"))

	_ = router.SetRule(sf.Rule{Prefix: "/orders", Percent: 100})
	_ = router.SetRule(sf.Rule{Prefix: "/orders/export", Percent: 0})
	fmt.Println("\nPhase 4: fix shipped. /orders fully migrated, but /orders/export isn't ported yet")
	fmt.Println("  ", get("/orders/1", "alice"))
	fmt.Println("  ", get("/orders/export", "alice"))
	fmt.Println("  ", get("/billing/1", "alice"))

	l, m := router.Counts()
	fmt.Printf("\nTotals: monolith served %d, orders-svc served %d. Next up: /billing.\n", l, m)
}
