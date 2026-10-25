// Package stranglerfig implements a strangler-fig router. It sits in front
// of a legacy system and moves traffic, one endpoint at a time, to a new
// implementation. Each migrated route has a rollout percentage, so a route
// can go 0% -> 5% -> 50% -> 100% and roll back instantly. Bucketing is
// sticky per user, so a given user sees one consistent backend.
package stranglerfig

import (
	"errors"
	"hash/fnv"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Header added to every response so you can see who served it.
const HeaderServedBy = "X-Served-By"

// Rule moves requests matching Method (empty = any) and path Prefix to the
// new system for Percent% of users.
type Rule struct {
	Method  string
	Prefix  string
	Percent int // 0..100
}

// Router is an http.Handler that routes between legacy and modern. It is
// safe to change rules while it serves traffic.
type Router struct {
	legacy, modern http.Handler
	key            func(*http.Request) string

	mu    sync.RWMutex
	rules []Rule // sorted longest prefix first

	legacyCount, modernCount atomic.Int64
}

// NewRouter routes everything to legacy until rules are added. key picks
// the stickiness key. nil uses the X-User-ID header, falling back to the
// client address.
func NewRouter(legacy, modern http.Handler, key func(*http.Request) string) *Router {
	if key == nil {
		key = func(r *http.Request) string {
			if id := r.Header.Get("X-User-ID"); id != "" {
				return id
			}
			return r.RemoteAddr
		}
	}
	return &Router{legacy: legacy, modern: modern, key: key}
}

// ErrBadRule reports an invalid rule.
var ErrBadRule = errors.New("stranglerfig: prefix must start with / and percent must be 0..100")

// SetRule adds a rule or updates the one with the same method and prefix.
// Set Percent to 0 to roll back without losing the rule.
func (rt *Router) SetRule(r Rule) error {
	if !strings.HasPrefix(r.Prefix, "/") || r.Percent < 0 || r.Percent > 100 {
		return ErrBadRule
	}
	r.Method = strings.ToUpper(r.Method)
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for i := range rt.rules {
		if rt.rules[i].Method == r.Method && rt.rules[i].Prefix == r.Prefix {
			rt.rules[i].Percent = r.Percent
			return nil
		}
	}
	rt.rules = append(rt.rules, r)
	sort.SliceStable(rt.rules, func(i, j int) bool {
		if len(rt.rules[i].Prefix) != len(rt.rules[j].Prefix) {
			return len(rt.rules[i].Prefix) > len(rt.rules[j].Prefix)
		}
		return rt.rules[i].Method > rt.rules[j].Method // method-specific beats "" (any)
	})
	return nil
}

// Decide reports whether r goes to the modern system. The most specific
// matching rule wins, so a 0% rule on /orders/export can exclude one
// sub-path while /orders is at 100%.
func (rt *Router) Decide(r *http.Request) bool {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	for _, rule := range rt.rules {
		if rule.Method != "" && rule.Method != r.Method {
			continue
		}
		if !matchPrefix(r.URL.Path, rule.Prefix) {
			continue
		}
		return bucket(rule.Prefix, rt.key(r)) < rule.Percent
	}
	return false
}

func matchPrefix(path, prefix string) bool {
	p := strings.TrimSuffix(prefix, "/")
	return path == p || strings.HasPrefix(path, p+"/") || p == ""
}

// bucket maps (route, key) to 0..99. Including the route spreads the
// "first 5%" of users differently per route, so the same unlucky users are
// not always the guinea pigs. Raising the percent only adds users, and
// never moves an existing modern user back to legacy.
func bucket(route, key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(route))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % 100)
}

// ServeHTTP routes the request and tags the response with HeaderServedBy.
func (rt *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if rt.Decide(r) {
		rt.modernCount.Add(1)
		w.Header().Set(HeaderServedBy, "modern")
		rt.modern.ServeHTTP(w, r)
		return
	}
	rt.legacyCount.Add(1)
	w.Header().Set(HeaderServedBy, "legacy")
	rt.legacy.ServeHTTP(w, r)
}

// Counts returns how many requests each side has served.
func (rt *Router) Counts() (legacy, modern int64) {
	return rt.legacyCount.Load(), rt.modernCount.Load()
}
