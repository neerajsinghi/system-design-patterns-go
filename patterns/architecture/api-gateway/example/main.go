// Command example runs an API gateway in front of two loopback backends and
// shows routing, authentication, request IDs and aggregation.
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
}
