// Package apigateway implements a small API gateway: a single entry point that
// routes requests to backend services by path prefix, authenticates callers,
// stamps every request with a correlation ID, and can aggregate several
// backend calls into one response.
package apigateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// Header names the gateway reads or writes.
const (
	HeaderRequestID = "X-Request-ID"
	HeaderSubject   = "X-Authenticated-Subject"
)

// Route forwards every request whose path starts with Prefix to Target.
// The prefix is stripped, so with Prefix "/users" a request for
// "/users/42" reaches the backend as Target.Path + "/42".
type Route struct {
	Prefix string
	Target *url.URL
	// Public routes skip authentication.
	Public bool
}

// Aggregate answers GET Path by calling every URL in Parts concurrently and
// returning one JSON object keyed by part name. If any part fails, the whole
// response is a 502 that names the failing parts.
type Aggregate struct {
	Path    string
	Parts   map[string]string // name -> absolute backend URL
	Public  bool
	Timeout time.Duration // per aggregate; 0 means 2s
}

// Config configures a Gateway.
type Config struct {
	Routes     []Route
	Aggregates []Aggregate
	// Authenticate returns the caller's identity or an error. When nil,
	// every route is treated as public.
	Authenticate func(r *http.Request) (subject string, err error)
	// Transport is used for proxying and aggregation; nil means
	// http.DefaultTransport.
	Transport http.RoundTripper
}

// Gateway is an http.Handler. Create it with New.
type Gateway struct {
	routes     []compiledRoute
	aggregates map[string]Aggregate
	auth       func(*http.Request) (string, error)
	client     *http.Client
}

type compiledRoute struct {
	Route
	proxy *httputil.ReverseProxy
}

// New validates cfg and builds a Gateway.
func New(cfg Config) (*Gateway, error) {
	transport := cfg.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	g := &Gateway{
		aggregates: make(map[string]Aggregate),
		auth:       cfg.Authenticate,
		client:     &http.Client{Transport: transport},
	}
	for _, rt := range cfg.Routes {
		prefix := strings.TrimSuffix(rt.Prefix, "/")
		if !strings.HasPrefix(rt.Prefix, "/") || rt.Target == nil {
			return nil, fmt.Errorf("apigateway: route %q needs a /prefix and a target", rt.Prefix)
		}
		rt.Prefix = prefix
		g.routes = append(g.routes, compiledRoute{Route: rt, proxy: g.newProxy(rt, transport)})
	}
	// Longest prefix wins, so "/users/admin" beats "/users".
	sort.Slice(g.routes, func(i, j int) bool { return len(g.routes[i].Prefix) > len(g.routes[j].Prefix) })
	for _, a := range cfg.Aggregates {
		if a.Path == "" || len(a.Parts) == 0 {
			return nil, errors.New("apigateway: aggregate needs a path and at least one part")
		}
		if a.Timeout <= 0 {
			a.Timeout = 2 * time.Second
		}
		g.aggregates[a.Path] = a
	}
	return g, nil
}

func (g *Gateway) newProxy(rt Route, transport http.RoundTripper) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Transport: transport,
		Rewrite: func(pr *httputil.ProxyRequest) {
			rest := strings.TrimPrefix(pr.In.URL.Path, rt.Prefix)
			pr.Out.URL.Path, pr.Out.URL.RawPath = rest, ""
			pr.SetURL(rt.Target)
			pr.SetXForwarded()
			// Identity headers are set only by the gateway, never trusted from clients.
			pr.Out.Header.Del(HeaderSubject)
			if sub, ok := pr.In.Context().Value(subjectKey{}).(string); ok {
				pr.Out.Header.Set(HeaderSubject, sub)
			}
			pr.Out.Header.Set(HeaderRequestID, pr.In.Header.Get(HeaderRequestID))
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": "upstream unavailable"})
		},
	}
}

type subjectKey struct{}

// ServeHTTP applies request IDs, authentication and routing.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id := r.Header.Get(HeaderRequestID)
	if !validRequestID(id) {
		id = newRequestID()
	}
	r.Header.Set(HeaderRequestID, id)
	w.Header().Set(HeaderRequestID, id)

	if agg, ok := g.aggregates[r.URL.Path]; ok {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		if r, ok = g.authorize(w, r, agg.Public); ok {
			g.serveAggregate(w, r, agg)
		}
		return
	}
	for _, rt := range g.routes {
		if r.URL.Path == rt.Prefix || strings.HasPrefix(r.URL.Path, rt.Prefix+"/") {
			if r, ok := g.authorize(w, r, rt.Public); ok {
				rt.proxy.ServeHTTP(w, r)
			}
			return
		}
	}
	writeJSON(w, http.StatusNotFound, map[string]string{"error": "no route"})
}

// authorize returns the request (carrying the subject in its context) and
// whether processing should continue. It writes the 401 itself.
func (g *Gateway) authorize(w http.ResponseWriter, r *http.Request, public bool) (*http.Request, bool) {
	if public || g.auth == nil {
		return r, true
	}
	sub, err := g.auth(r)
	if err != nil || sub == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return r, false
	}
	return r.WithContext(context.WithValue(r.Context(), subjectKey{}, sub)), true
}

func (g *Gateway) serveAggregate(w http.ResponseWriter, r *http.Request, agg Aggregate) {
	ctx, cancel := context.WithTimeout(r.Context(), agg.Timeout)
	defer cancel()

	var (
		mu     sync.Mutex
		wg     sync.WaitGroup
		parts  = make(map[string]json.RawMessage, len(agg.Parts))
		failed = make(map[string]string)
	)
	for name, target := range agg.Parts {
		wg.Add(1)
		go func(name, target string) {
			defer wg.Done()
			body, err := g.fetch(ctx, r, target)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failed[name] = err.Error()
				return
			}
			parts[name] = body
		}(name, target)
	}
	wg.Wait()
	if len(failed) > 0 {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": "aggregate failed", "parts": failed})
		return
	}
	writeJSON(w, http.StatusOK, parts)
}

func (g *Gateway) fetch(ctx context.Context, in *http.Request, target string) (json.RawMessage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set(HeaderRequestID, in.Header.Get(HeaderRequestID))
	if sub, ok := in.Context().Value(subjectKey{}).(string); ok {
		req.Header.Set(HeaderSubject, sub)
	}
	resp, err := g.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	if !json.Valid(body) {
		return nil, errors.New("invalid JSON from backend")
	}
	return body, nil
}

// validRequestID accepts short IDs made of [A-Za-z0-9_-] so a client cannot
// inject arbitrary bytes into logs and downstream headers.
func validRequestID(id string) bool {
	if id == "" || len(id) > 64 {
		return false
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

func newRequestID() string {
	var b [12]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never fails on supported platforms
	return hex.EncodeToString(b[:])
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
