// Package sidecar simulates the sidecar pattern in-process. A reverse proxy
// runs next to an unmodified application and adds cross-cutting behaviour
// that the app knows nothing about: request IDs, retries of idempotent
// requests, and metrics. In production the sidecar is a separate process or
// container on the same host (Envoy, Linkerd-proxy), and the app talks only
// to localhost.
package sidecar

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"time"
)

// Config tunes a Sidecar. Zero values pick sensible defaults.
type Config struct {
	MaxRetries  int                                        // extra attempts for idempotent requests. 0 means none
	Backoff     time.Duration                              // fixed delay between attempts
	Transport   http.RoundTripper                          // nil means http.DefaultTransport
	MetricsPath string                                     // served by the sidecar itself. "" means /sidecar/metrics
	Now         func() time.Time                           // injectable clock for latency. nil means time.Now
	Sleep       func(context.Context, time.Duration) error // injectable for tests. nil means a context-aware timer
	OnRetry     func(*http.Request, int)                   // optional hook, called before each retry
}

// Metrics is a snapshot of what the sidecar observed.
type Metrics struct {
	Requests     int           `json:"requests"`
	ByStatus     map[int]int   `json:"byStatus"`
	Retries      int           `json:"retries"`
	TotalLatency time.Duration `json:"totalLatencyNs"`
}

// Sidecar is an http.Handler that proxies to the application at target.
type Sidecar struct {
	cfg   Config
	proxy *httputil.ReverseProxy

	mu      sync.Mutex
	metrics Metrics
}

// New returns a sidecar in front of target.
func New(target *url.URL, cfg Config) *Sidecar {
	if cfg.Transport == nil {
		cfg.Transport = http.DefaultTransport
	}
	if cfg.MetricsPath == "" {
		cfg.MetricsPath = "/sidecar/metrics"
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Sleep == nil {
		cfg.Sleep = sleepCtx
	}
	s := &Sidecar{cfg: cfg, metrics: Metrics{ByStatus: map[int]int{}}}
	s.proxy = &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.Out.Host = pr.In.Host
		},
		Transport: &retryTransport{s: s},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			http.Error(w, "sidecar: upstream unavailable", http.StatusBadGateway)
		},
	}
	return s
}

// ServeHTTP serves the metrics endpoint itself and proxies everything else.
func (s *Sidecar) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == s.cfg.MetricsPath {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.Snapshot())
		return
	}
	if r.Header.Get("X-Request-ID") == "" {
		var b [8]byte
		_, _ = rand.Read(b[:])
		r.Header.Set("X-Request-ID", hex.EncodeToString(b[:]))
	}
	start := s.cfg.Now()
	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	s.proxy.ServeHTTP(rec, r)
	elapsed := s.cfg.Now().Sub(start)

	s.mu.Lock()
	s.metrics.Requests++
	s.metrics.ByStatus[rec.status]++
	s.metrics.TotalLatency += elapsed
	s.mu.Unlock()
}

// Snapshot returns a copy of the current metrics.
func (s *Sidecar) Snapshot() Metrics {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.metrics
	m.ByStatus = make(map[int]int, len(s.metrics.ByStatus))
	for k, v := range s.metrics.ByStatus {
		m.ByStatus[k] = v
	}
	return m
}

// retryTransport retries idempotent requests on transport errors and on
// 502/503/504. Non-idempotent requests (POST, PATCH) are sent exactly once,
// because a retry could apply them twice.
type retryTransport struct{ s *Sidecar }

func (t *retryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	cfg := t.s.cfg
	attempts := 1
	// A request body is a stream that the first attempt consumes, so only
	// bodiless requests are replayed.
	if idempotent(req.Method) && (req.Body == nil || req.Body == http.NoBody) {
		attempts += max(cfg.MaxRetries, 0)
	}
	for i := 0; ; i++ {
		resp, err := cfg.Transport.RoundTrip(req)
		if i == attempts-1 || !retryable(resp, err) || req.Context().Err() != nil {
			return resp, err
		}
		if resp != nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
			resp.Body.Close()
		}
		t.s.mu.Lock()
		t.s.metrics.Retries++
		t.s.mu.Unlock()
		if cfg.OnRetry != nil {
			cfg.OnRetry(req, i+1)
		}
		if err := cfg.Sleep(req.Context(), cfg.Backoff); err != nil {
			return nil, err
		}
	}
}

func idempotent(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodPut, http.MethodDelete:
		return true
	}
	return false
}

func retryable(resp *http.Response, err error) bool {
	if err != nil {
		return true
	}
	switch resp.StatusCode {
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// Unwrap lets http.ResponseController reach the underlying writer
// (for example to flush).
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }
