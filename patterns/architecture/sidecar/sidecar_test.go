package sidecar

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// flakyApp fails the first `failures` requests with 503, then succeeds.
// It knows nothing about the sidecar.
func flakyApp(t *testing.T, failures int32) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= failures {
			http.Error(w, "warming up", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "ok id="+r.Header.Get("X-Request-ID"))
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func noSleep(ctx context.Context, _ time.Duration) error { return ctx.Err() }

func newSidecar(t *testing.T, target string, retries int) *Sidecar {
	t.Helper()
	u, err := url.Parse(target)
	if err != nil {
		t.Fatal(err)
	}
	return New(u, Config{MaxRetries: retries, Sleep: noSleep})
}

func do(s *Sidecar, method, path string, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

func TestRetries(t *testing.T) {
	tests := []struct {
		name        string
		failures    int32
		retries     int
		method      string
		wantStatus  int
		wantCalls   int32
		wantRetries int
	}{
		{"GET recovers within budget", 2, 3, http.MethodGet, 200, 3, 2},
		{"GET exhausts budget", 5, 2, http.MethodGet, 503, 3, 2},
		{"POST is never retried", 1, 3, http.MethodPost, 503, 1, 0},
		{"no retries configured", 1, 0, http.MethodGet, 503, 1, 0},
		{"healthy app, no retries needed", 0, 3, http.MethodGet, 200, 1, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			app, calls := flakyApp(t, tc.failures)
			s := newSidecar(t, app.URL, tc.retries)
			rec := do(s, tc.method, "/work", nil)
			if rec.Code != tc.wantStatus {
				t.Errorf("status %d, want %d", rec.Code, tc.wantStatus)
			}
			if calls.Load() != tc.wantCalls {
				t.Errorf("app saw %d calls, want %d", calls.Load(), tc.wantCalls)
			}
			m := s.Snapshot()
			if m.Retries != tc.wantRetries || m.Requests != 1 || m.ByStatus[tc.wantStatus] != 1 {
				t.Errorf("metrics = %+v", m)
			}
		})
	}
}

func TestUpstreamDown(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	addr := dead.URL
	dead.Close()
	s := newSidecar(t, addr, 2)
	if rec := do(s, http.MethodGet, "/", nil); rec.Code != http.StatusBadGateway {
		t.Errorf("status %d", rec.Code)
	}
	if m := s.Snapshot(); m.Retries != 2 || m.ByStatus[502] != 1 {
		t.Errorf("metrics = %+v", m)
	}
}

func TestRequestIDAndMetricsEndpoint(t *testing.T) {
	app, calls := flakyApp(t, 0)
	s := newSidecar(t, app.URL, 0)

	rec := do(s, http.MethodGet, "/", map[string]string{"X-Request-ID": "given-1"})
	if body := rec.Body.String(); body != "ok id=given-1" {
		t.Errorf("existing request ID not preserved: %q", body)
	}
	rec = do(s, http.MethodGet, "/", nil)
	if body := rec.Body.String(); !strings.HasPrefix(body, "ok id=") || len(body) != len("ok id=")+16 {
		t.Errorf("request ID not injected: %q", body)
	}

	rec = do(s, http.MethodGet, "/sidecar/metrics", nil)
	var m Metrics
	if err := json.NewDecoder(rec.Body).Decode(&m); err != nil {
		t.Fatal(err)
	}
	if m.Requests != 2 || calls.Load() != 2 {
		t.Errorf("metrics endpoint should not be proxied or counted: %+v, app calls %d", m, calls.Load())
	}
}

func TestLatencyUsesInjectedClock(t *testing.T) {
	app, _ := flakyApp(t, 0)
	u, _ := url.Parse(app.URL)
	var mu sync.Mutex
	now := time.Unix(0, 0)
	clock := func() time.Time { // every call advances 5ms
		mu.Lock()
		defer mu.Unlock()
		now = now.Add(5 * time.Millisecond)
		return now
	}
	s := New(u, Config{Now: clock})
	for i := 0; i < 3; i++ {
		do(s, http.MethodGet, "/", nil)
	}
	if got := s.Snapshot().TotalLatency; got != 15*time.Millisecond {
		t.Errorf("total latency = %v, want 15ms", got)
	}
}

func TestBackoffHonoursCancellation(t *testing.T) {
	app, calls := flakyApp(t, 100)
	u, _ := url.Parse(app.URL)
	s := New(u, Config{MaxRetries: 50, Backoff: time.Hour})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
	start := time.Now()
	s.ServeHTTP(httptest.NewRecorder(), req)
	if time.Since(start) > time.Second || calls.Load() != 1 {
		t.Errorf("cancellation not honoured: took %v, %d calls", time.Since(start), calls.Load())
	}
}

func TestConcurrent(t *testing.T) {
	app, _ := flakyApp(t, 0)
	s := newSidecar(t, app.URL, 1)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); do(s, http.MethodGet, "/", nil) }()
	}
	wg.Wait()
	if m := s.Snapshot(); m.Requests != 20 || m.ByStatus[200] != 20 {
		t.Errorf("metrics = %+v", m)
	}
}
