package stranglerfig

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func named(name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, name) })
}

func serve(rt *Router, method, path, user string) string {
	req := httptest.NewRequest(method, path, nil)
	req.Header.Set("X-User-ID", user)
	rec := httptest.NewRecorder()
	rt.ServeHTTP(rec, req)
	if rec.Header().Get(HeaderServedBy) != rec.Body.String() {
		return "header mismatch"
	}
	return rec.Body.String()
}

func TestRouting(t *testing.T) {
	rt := NewRouter(named("legacy"), named("modern"), nil)
	for _, r := range []Rule{
		{Prefix: "/orders", Percent: 100},
		{Prefix: "/orders/export", Percent: 0}, // not migrated yet
		{Method: "POST", Prefix: "/users", Percent: 100},
	} {
		if err := rt.SetRule(r); err != nil {
			t.Fatal(err)
		}
	}
	tests := []struct {
		method, path, want string
	}{
		{"GET", "/orders", "modern"},
		{"GET", "/orders/42", "modern"},
		{"GET", "/orders/export", "legacy"},
		{"GET", "/orders/export/csv", "legacy"},
		{"GET", "/ordersummary", "legacy"}, // prefix boundary
		{"POST", "/users", "modern"},
		{"GET", "/users", "legacy"}, // method-specific rule
		{"GET", "/", "legacy"},
	}
	for _, tc := range tests {
		if got := serve(rt, tc.method, tc.path, "u1"); got != tc.want {
			t.Errorf("%s %s = %s, want %s", tc.method, tc.path, got, tc.want)
		}
	}
	legacy, modern := rt.Counts()
	if legacy != 5 || modern != 3 {
		t.Errorf("counts = %d/%d", legacy, modern)
	}
}

func TestPercentRolloutIsStickyAndMonotonic(t *testing.T) {
	rt := NewRouter(named("legacy"), named("modern"), nil)
	const users = 2000
	onModern := map[string]bool{}
	for _, pct := range []int{0, 5, 25, 50, 100} {
		if err := rt.SetRule(Rule{Prefix: "/search", Percent: pct}); err != nil {
			t.Fatal(err)
		}
		modern := 0
		for i := 0; i < users; i++ {
			u := fmt.Sprintf("user-%d", i)
			got := serve(rt, "GET", "/search", u)
			if got != serve(rt, "GET", "/search", u) {
				t.Fatalf("user %s flipped between requests", u)
			}
			if onModern[u] && got != "modern" {
				t.Fatalf("raising to %d%% moved %s back to legacy", pct, u)
			}
			if got == "modern" {
				onModern[u] = true
				modern++
			}
		}
		share := float64(modern) / users * 100
		if share < float64(pct)-4 || share > float64(pct)+4 {
			t.Errorf("at %d%% rollout, %.1f%% went modern", pct, share)
		}
	}
	// Instant rollback.
	_ = rt.SetRule(Rule{Prefix: "/search", Percent: 0})
	if serve(rt, "GET", "/search", "user-1") != "legacy" {
		t.Error("rollback did not take effect")
	}
}

func TestBadRules(t *testing.T) {
	rt := NewRouter(named("l"), named("m"), nil)
	for _, r := range []Rule{{Prefix: "orders", Percent: 10}, {Prefix: "/x", Percent: -1}, {Prefix: "/x", Percent: 101}} {
		if err := rt.SetRule(r); !errors.Is(err, ErrBadRule) {
			t.Errorf("%+v: err = %v", r, err)
		}
	}
}

func TestConcurrentRuleChanges(t *testing.T) {
	rt := NewRouter(named("legacy"), named("modern"), nil)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for p := 0; p <= 100; p++ {
			_ = rt.SetRule(Rule{Prefix: "/a", Percent: p})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			serve(rt, http.MethodGet, "/a/b", fmt.Sprint(i))
		}
	}()
	wg.Wait()
	if l, m := rt.Counts(); l+m != 500 {
		t.Errorf("served %d, want 500", l+m)
	}
}
