// Command example runs an API gateway in front of two loopback backends and
// shows routing, authentication, request IDs and aggregation, then auth
// offloading: verifying a signed token once at the edge instead of having
// every backend call an auth service.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/api-gateway"
)

func main() {
	users := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("  [users]  %s %s subject=%s id=%s\n", r.Method, r.URL.Path,
			r.Header.Get(apigateway.HeaderSubject), r.Header.Get(apigateway.HeaderRequestID))
		_ = json.NewEncoder(w).Encode(map[string]string{"name": "Alice"})
	}))
	defer users.Close()
	orders := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("  [orders] %s %s subject=%s id=%s\n", r.Method, r.URL.Path,
			r.Header.Get(apigateway.HeaderSubject), r.Header.Get(apigateway.HeaderRequestID))
		_ = json.NewEncoder(w).Encode([]string{"order-1", "order-2"})
	}))
	defer orders.Close()

	usersURL, _ := url.Parse(users.URL)
	ordersURL, _ := url.Parse(orders.URL)
	gw, err := apigateway.New(apigateway.Config{
		Authenticate: func(r *http.Request) (string, error) {
			if r.Header.Get("Authorization") == "Bearer demo-token" {
				return "alice", nil
			}
			return "", errors.New("invalid token")
		},
		Routes: []apigateway.Route{
			{Prefix: "/api/users", Target: usersURL},
			{Prefix: "/api/orders", Target: ordersURL},
		},
		Aggregates: []apigateway.Aggregate{{
			Path:  "/api/home",
			Parts: map[string]string{"user": users.URL + "/me", "orders": orders.URL + "/recent"},
		}},
	})
	if err != nil {
		log.Fatal(err)
	}
	front := httptest.NewServer(gw)
	defer front.Close()

	call := func(label, path, token, reqID string) {
		fmt.Printf("\n%s: GET %s\n", label, path)
		req, _ := http.NewRequest(http.MethodGet, front.URL+path, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		if reqID != "" {
			req.Header.Set(apigateway.HeaderRequestID, reqID)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Println("  error:", err)
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("  -> %d %s (request id %s)\n", resp.StatusCode, strings.TrimSpace(string(body)),
			resp.Header.Get(apigateway.HeaderRequestID))
	}

	call("1. routed by prefix, client-supplied request ID kept", "/api/users/42", "demo-token", "trace-abc")
	call("2. no token", "/api/orders/7", "", "")
	call("3. aggregation: one client call, two backend calls in parallel", "/api/home", "demo-token", "trace-home")
	call("4. unknown route", "/api/payments", "demo-token", "")

	authOffloading()
}

// authOffloading compares two designs for N authenticated requests:
// (a) the gateway passes the token through and every backend validates it by
// calling an auth service; (b) the gateway verifies the HS256 token once and
// backends trust the X-User-ID / X-Scopes headers it sets.
func authOffloading() {
	const (
		n           = 200
		authCallRTT = 2 * time.Millisecond // assumed cost of one auth-service round trip
	)
	secret := []byte("demo-signing-secret-0123456789")
	start := time.Unix(1_800_000_000, 0) // fixed clock keeps the output deterministic
	now := func() time.Time { return start }
	token, _ := apigateway.SignHS256(secret, apigateway.Claims{
		Subject: "alice", Scopes: []string{"orders:read"}, ExpiresAt: start.Add(15 * time.Minute),
	})

	var authCalls atomic.Int64
	authService := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authCalls.Add(1)
		if r.Header.Get("Authorization") != "Bearer "+token {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = io.WriteString(w, "alice")
	}))
	defer authService.Close()

	// Each backend is either "re-validating" (calls the auth service) or
	// "trusting" (reads the headers the gateway set).
	backend := func(revalidate bool, seen *atomic.Value) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if revalidate {
				req, _ := http.NewRequest(http.MethodGet, authService.URL, nil)
				req.Header.Set("Authorization", r.Header.Get("Authorization"))
				resp, err := http.DefaultClient.Do(req)
				if err != nil || resp.StatusCode != http.StatusOK {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				resp.Body.Close()
			}
			seen.Store(fmt.Sprintf("X-User-ID=%q X-Scopes=%q Authorization present=%v",
				r.Header.Get(apigateway.HeaderUserID), r.Header.Get(apigateway.HeaderScopes),
				r.Header.Get("Authorization") != ""))
			_, _ = io.WriteString(w, "ok")
		}))
	}

	run := func(gw http.Handler, headers map[string]string, tok string) (ok, rejected int) {
		front := httptest.NewServer(gw)
		defer front.Close()
		paths := []string{"/api/users/1", "/api/orders/1"}
		for i := range n {
			req, _ := http.NewRequest(http.MethodGet, front.URL+paths[i%2], nil)
			req.Header.Set("Authorization", "Bearer "+tok)
			for k, v := range headers {
				req.Header.Set(k, v)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				rejected++
				continue
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				ok++
			} else {
				rejected++
			}
		}
		return ok, rejected
	}

	// (a) pass-through gateway, re-validating backends.
	var seenA atomic.Value
	ua, oa := backend(true, &seenA), backend(true, &seenA)
	defer ua.Close()
	defer oa.Close()
	gwA, _ := apigateway.New(apigateway.Config{Routes: []apigateway.Route{
		{Prefix: "/api/users", Target: mustParse(ua.URL), Public: true},
		{Prefix: "/api/orders", Target: mustParse(oa.URL), Public: true},
	}})
	okA, _ := run(gwA, nil, token)
	callsA := authCalls.Load()

	// (b) verify once at the edge, trusting backends.
	authCalls.Store(0)
	var seenB atomic.Value
	ub, ob := backend(false, &seenB), backend(false, &seenB)
	defer ub.Close()
	defer ob.Close()
	verifier, _ := apigateway.NewTokenVerifier(secret, apigateway.WithVerifierClock(now),
		apigateway.WithTokenCache(1024, 5*time.Minute))
	gwB, _ := apigateway.New(apigateway.Config{Verifier: verifier, Routes: []apigateway.Route{
		{Prefix: "/api/users", Target: mustParse(ub.URL)},
		{Prefix: "/api/orders", Target: mustParse(ob.URL)},
	}})
	okB, _ := run(gwB, map[string]string{apigateway.HeaderUserID: "admin"}, token) // every request also tries to spoof X-User-ID
	callsB := authCalls.Load()
	stats := verifier.Stats()

	fmt.Printf("\n5. auth offloading: %d authenticated requests split over two backends\n", n)
	fmt.Printf("  (a) every backend re-validates:  ok=%d  auth-service calls=%d  added latency at an assumed %v per call: %v total, %v per request\n",
		okA, callsA, authCallRTT, time.Duration(callsA)*authCallRTT, authCallRTT)
	fmt.Printf("  (b) verify once at the gateway:  ok=%d  auth-service calls=%d  HMAC verifications=%d  cache hits=%d\n",
		okB, callsB, stats.Verified, stats.CacheHits)
	fmt.Printf("      a backend in (b) saw: %s\n", seenB.Load())
	fmt.Println("      (the client sent X-User-ID: admin on every request; the gateway replaced it with the verified subject)")

	// Rejections happen at the edge; backends never see these requests.
	expired, _ := apigateway.SignHS256(secret, apigateway.Claims{Subject: "alice", ExpiresAt: start.Add(-time.Minute)})
	forged, _ := apigateway.SignHS256([]byte("not-the-real-secret-at-all"), apigateway.Claims{Subject: "admin", ExpiresAt: start.Add(time.Hour)})
	parts := strings.Split(token, ".")
	algNone := "eyJhbGciOiJub25lIn0." + parts[1] + "." // header {"alg":"none"}, empty signature
	front := httptest.NewServer(gwB)
	defer front.Close()
	fmt.Println("\n6. bad tokens are rejected at the edge with 401")
	for _, c := range []struct{ label, tok string }{{"expired", expired}, {"wrong key", forged}, {"alg none", algNone}, {"missing", ""}} {
		req, _ := http.NewRequest(http.MethodGet, front.URL+"/api/orders/1", nil)
		if c.tok != "" {
			req.Header.Set("Authorization", "Bearer "+c.tok)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Println("  error:", err)
			continue
		}
		resp.Body.Close()
		fmt.Printf("  %-9s -> %d %s\n", c.label, resp.StatusCode, resp.Header.Get("WWW-Authenticate"))
	}
	fmt.Printf("  verifier rejected=%d, auth-service calls still %d\n", verifier.Stats().Rejected, authCalls.Load())
}

func mustParse(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		log.Fatal(err)
	}
	return u
}
