package backendforfrontend

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type fakeProfiles struct{ err error }

func (f fakeProfiles) GetProfile(_ context.Context, id string) (Profile, error) {
	if f.err != nil {
		return Profile{}, f.err
	}
	return Profile{ID: id, Name: "Ada", Email: "ada@example.com", Bio: "long bio", AvatarURL: "https://img/ada.png"}, nil
}

type fakeOrders struct {
	orders []Order
	err    error
}

func (f fakeOrders) ListOrders(context.Context, string) ([]Order, error) { return f.orders, f.err }

var base = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func sampleOrders() []Order {
	var out []Order
	for i := 0; i < 5; i++ { // oldest first, to prove the BFF sorts
		out = append(out, Order{
			ID: string(rune('a' + i)), Status: "shipped", CreatedAt: base.Add(time.Duration(i) * time.Hour),
			Items: []Item{{SKU: "s", Name: "thing", Quantity: 2, Cents: 150}},
		})
	}
	return out
}

func serve(t *testing.T, h http.Handler, user string, out any) int {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/home", nil)
	if user != "" {
		req.Header.Set(HeaderUserID, user)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if out != nil && rec.Code == http.StatusOK {
		if err := json.NewDecoder(rec.Body).Decode(out); err != nil {
			t.Fatal(err)
		}
	}
	return rec.Code
}

func TestWebBFF(t *testing.T) {
	down := errors.New("down")
	tests := []struct {
		name       string
		user       string
		profiles   fakeProfiles
		orders     fakeOrders
		wantStatus int
	}{
		{"full data", "u1", fakeProfiles{}, fakeOrders{orders: sampleOrders()}, 200},
		{"orders down is fatal for web", "u1", fakeProfiles{}, fakeOrders{err: down}, 502},
		{"profile down", "u1", fakeProfiles{err: down}, fakeOrders{}, 502},
		{"no user", "", fakeProfiles{}, fakeOrders{}, 401},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got WebResponse
			code := serve(t, &WebBFF{Profiles: tc.profiles, Orders: tc.orders}, tc.user, &got)
			if code != tc.wantStatus {
				t.Fatalf("status %d, want %d", code, tc.wantStatus)
			}
			if code != 200 {
				return
			}
			if got.Profile.Email == "" || got.Profile.Bio == "" {
				t.Error("web should get the full profile")
			}
			if len(got.Orders) != 5 || got.Orders[0].ID != "e" || len(got.Orders[0].Items) != 1 || got.Orders[0].TotalCents != 300 {
				t.Errorf("unexpected orders: %+v", got.Orders)
			}
		})
	}
}

func TestMobileBFF(t *testing.T) {
	down := errors.New("down")
	tests := []struct {
		name            string
		profiles        fakeProfiles
		orders          fakeOrders
		limit           int
		wantStatus      int
		wantIDs         []string
		wantUnavailable bool
	}{
		{"default limit 3, newest first", fakeProfiles{}, fakeOrders{orders: sampleOrders()}, 0, 200, []string{"e", "d", "c"}, false},
		{"custom limit", fakeProfiles{}, fakeOrders{orders: sampleOrders()}, 1, 200, []string{"e"}, false},
		{"fewer orders than limit", fakeProfiles{}, fakeOrders{orders: sampleOrders()[:2]}, 0, 200, []string{"b", "a"}, false},
		{"no orders", fakeProfiles{}, fakeOrders{}, 0, 200, []string{}, false},
		{"orders down degrades gracefully", fakeProfiles{}, fakeOrders{err: down}, 0, 200, []string{}, true},
		{"profile down", fakeProfiles{err: down}, fakeOrders{}, 0, 502, nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got MobileResponse
			code := serve(t, &MobileBFF{Profiles: tc.profiles, Orders: tc.orders, RecentLimit: tc.limit}, "u1", &got)
			if code != tc.wantStatus {
				t.Fatalf("status %d, want %d", code, tc.wantStatus)
			}
			if code != 200 {
				return
			}
			if got.Name != "Ada" || got.OrdersUnavailable != tc.wantUnavailable {
				t.Errorf("got %+v", got)
			}
			if len(got.RecentOrders) != len(tc.wantIDs) {
				t.Fatalf("got %d orders, want %d", len(got.RecentOrders), len(tc.wantIDs))
			}
			for i, id := range tc.wantIDs {
				if got.RecentOrders[i].ID != id || got.RecentOrders[i].ItemCount != 2 {
					t.Errorf("order %d = %+v, want id %s with 2 items", i, got.RecentOrders[i], id)
				}
			}
		})
	}
}

func TestBFFDoesNotMutateServiceData(t *testing.T) {
	orders := sampleOrders()
	serve(t, &MobileBFF{Profiles: fakeProfiles{}, Orders: fakeOrders{orders: orders}}, "u1", nil)
	if orders[0].ID != "a" {
		t.Error("BFF reordered the service's slice")
	}
}
