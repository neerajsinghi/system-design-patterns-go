// Command example puts a sidecar proxy in front of an unmodified, flaky
// application. The sidecar adds request IDs, retries and metrics without the
// app changing a line.
package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/sidecar"
)

func main() {
	// The "legacy" app: no retries, no metrics, no tracing. Every third
	// request fails with 503.
	var n atomic.Int32
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1)%3 == 0 {
			fmt.Printf("    app: %s %s -> 503 (id %s)\n", r.Method, r.URL.Path, r.Header.Get("X-Request-ID"))
			http.Error(w, "busy", http.StatusServiceUnavailable)
			return
		}
		fmt.Printf("    app: %s %s -> 200 (id %s)\n", r.Method, r.URL.Path, r.Header.Get("X-Request-ID"))
		fmt.Fprintln(w, "hello from app")
	}))
	defer app.Close()

	target, _ := url.Parse(app.URL)
	sc := sidecar.New(target, sidecar.Config{
		MaxRetries: 2,
		Backoff:    5 * time.Millisecond,
		OnRetry: func(r *http.Request, attempt int) {
			fmt.Printf("    sidecar: retry #%d for %s %s\n", attempt, r.Method, r.URL.Path)
		},
	})
	proxy := httptest.NewServer(sc) // callers talk only to the sidecar
	defer proxy.Close()

	send := func(method, path string) {
		req, _ := http.NewRequest(method, proxy.URL+path, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Println("error:", err)
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		fmt.Printf("client: %s %s => %d\n\n", method, path, resp.StatusCode)
	}

	send(http.MethodGet, "/a")
	send(http.MethodGet, "/b")
	send(http.MethodGet, "/c") // the app's 3rd call fails, so the sidecar retries
	send(http.MethodGet, "/d")
	send(http.MethodPost, "/e") // the app's 6th call fails. POST is not idempotent, so no retry

	resp, err := http.Get(proxy.URL + "/sidecar/metrics")
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Printf("sidecar metrics (served by the sidecar, not the app):\n  %s", body)
}
