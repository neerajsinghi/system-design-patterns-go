// Package backendforfrontend shows the Backend-for-Frontend pattern: one thin
// API layer per client type (web, mobile, ...) that calls the same shared
// domain services but shapes, trims and degrades responses for that client.
package backendforfrontend

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Profile is the user record owned by the shared profile service.
type Profile struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Email     string `json:"email"`
	Bio       string `json:"bio"`
	AvatarURL string `json:"avatarUrl"`
}

// Item is one line of an order.
type Item struct {
	SKU      string `json:"sku"`
	Name     string `json:"name"`
	Quantity int    `json:"quantity"`
	Cents    int64  `json:"cents"`
}

// Order is owned by the shared order service.
type Order struct {
	ID        string
	Status    string
	CreatedAt time.Time
	Items     []Item
}

// TotalCents sums the order's items.
func (o Order) TotalCents() int64 {
	var total int64
	for _, it := range o.Items {
		total += it.Cents * int64(it.Quantity)
	}
	return total
}

// ProfileService and OrderService are the shared downstream services. In
// production they would be RPC clients; both BFFs depend on the same ones.
type ProfileService interface {
	GetProfile(ctx context.Context, userID string) (Profile, error)
}

// OrderService lists a user's orders.
type OrderService interface {
	ListOrders(ctx context.Context, userID string) ([]Order, error)
}

// HeaderUserID carries the caller identity, which is normally set by the
// gateway after authentication.
const HeaderUserID = "X-User-ID"

// fetchBoth calls both services concurrently. The caller decides what to do
// with each error, which is where the BFFs differ.
func fetchBoth(ctx context.Context, ps ProfileService, ords OrderService, userID string) (Profile, error, []Order, error) {
	var (
		wg               sync.WaitGroup
		profile          Profile
		orders           []Order
		profErr, ordsErr error
	)
	wg.Add(2)
	go func() { defer wg.Done(); profile, profErr = ps.GetProfile(ctx, userID) }()
	go func() { defer wg.Done(); orders, ordsErr = ords.ListOrders(ctx, userID) }()
	wg.Wait()
	// Newest first, regardless of what the service returned. Copy before
	// sorting so we never reorder a slice the service still owns.
	orders = append([]Order(nil), orders...)
	sort.Slice(orders, func(i, j int) bool { return orders[i].CreatedAt.After(orders[j].CreatedAt) })
	return profile, profErr, orders, ordsErr
}

// WebResponse is the rich desktop view: full profile and full order history
// with line items.
type WebResponse struct {
	Profile Profile    `json:"profile"`
	Orders  []WebOrder `json:"orders"`
}

// WebOrder includes everything the order-history table renders.
type WebOrder struct {
	ID         string    `json:"id"`
	Status     string    `json:"status"`
	CreatedAt  time.Time `json:"createdAt"`
	TotalCents int64     `json:"totalCents"`
	Items      []Item    `json:"items"`
}

// WebBFF serves the browser app. It needs complete data, so any downstream
// failure is a 502.
type WebBFF struct {
	Profiles ProfileService
	Orders   OrderService
	Timeout  time.Duration // 0 means 2s
}

func (b *WebBFF) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get(HeaderUserID)
	if userID == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing user"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeoutOr(b.Timeout))
	defer cancel()
	profile, pErr, orders, oErr := fetchBoth(ctx, b.Profiles, b.Orders, userID)
	if pErr != nil || oErr != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "downstream unavailable"})
		return
	}
	resp := WebResponse{Profile: profile, Orders: make([]WebOrder, 0, len(orders))}
	for _, o := range orders {
		resp.Orders = append(resp.Orders, WebOrder{ID: o.ID, Status: o.Status, CreatedAt: o.CreatedAt, TotalCents: o.TotalCents(), Items: o.Items})
	}
	writeJSON(w, http.StatusOK, resp)
}

// MobileResponse is deliberately small: one screen, a slow radio, and a
// limited battery budget.
type MobileResponse struct {
	Name              string        `json:"name"`
	AvatarURL         string        `json:"avatarUrl"`
	RecentOrders      []MobileOrder `json:"recentOrders"`
	OrdersUnavailable bool          `json:"ordersUnavailable,omitempty"`
}

// MobileOrder is a summary card with no line items.
type MobileOrder struct {
	ID         string `json:"id"`
	Status     string `json:"status"`
	TotalCents int64  `json:"totalCents"`
	ItemCount  int    `json:"itemCount"`
}

// MobileBFF serves the phone app. It returns at most RecentLimit order
// summaries and still renders the header if the order service is down.
type MobileBFF struct {
	Profiles    ProfileService
	Orders      OrderService
	RecentLimit int           // 0 means 3
	Timeout     time.Duration // 0 means 2s
}

func (b *MobileBFF) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get(HeaderUserID)
	if userID == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing user"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeoutOr(b.Timeout))
	defer cancel()
	profile, pErr, orders, oErr := fetchBoth(ctx, b.Profiles, b.Orders, userID)
	if pErr != nil {
		// Without the profile there is nothing useful to render.
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "downstream unavailable"})
		return
	}
	limit := b.RecentLimit
	if limit <= 0 {
		limit = 3
	}
	resp := MobileResponse{Name: profile.Name, AvatarURL: profile.AvatarURL, RecentOrders: []MobileOrder{}}
	if oErr != nil {
		resp.OrdersUnavailable = true
	} else {
		for _, o := range orders[:min(limit, len(orders))] {
			count := 0
			for _, it := range o.Items {
				count += it.Quantity
			}
			resp.RecentOrders = append(resp.RecentOrders, MobileOrder{ID: o.ID, Status: o.Status, TotalCents: o.TotalCents(), ItemCount: count})
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func timeoutOr(d time.Duration) time.Duration {
	if d <= 0 {
		return 2 * time.Second
	}
	return d
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
