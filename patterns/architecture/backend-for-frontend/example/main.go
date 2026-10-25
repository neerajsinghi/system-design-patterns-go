// Command example serves a web BFF and a mobile BFF over the same shared
// services and compares what each client receives.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	bff "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/backend-for-frontend"
)

type profiles struct{}

func (profiles) GetProfile(_ context.Context, id string) (bff.Profile, error) {
	return bff.Profile{ID: id, Name: "Ada Lovelace", Email: "ada@example.com",
		Bio: "Wrote the first published algorithm.", AvatarURL: "https://cdn.example/ada.png"}, nil
}

type orders struct{ down atomic.Bool }

func (o *orders) ListOrders(context.Context, string) ([]bff.Order, error) {
	if o.down.Load() {
		return nil, errors.New("order service unavailable")
	}
	t0 := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)
	var out []bff.Order
	for i := 1; i <= 5; i++ {
		out = append(out, bff.Order{
			ID: fmt.Sprintf("ord-%d", i), Status: "delivered", CreatedAt: t0.AddDate(0, 0, i),
			Items: []bff.Item{{SKU: "book", Name: "Notes on the Engine", Quantity: i, Cents: 1999}},
		})
	}
	return out, nil
}

func main() {
	ords := &orders{}
	mux := http.NewServeMux()
	mux.Handle("/web/home", &bff.WebBFF{Profiles: profiles{}, Orders: ords})
	mux.Handle("/mobile/home", &bff.MobileBFF{Profiles: profiles{}, Orders: ords, RecentLimit: 2})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	get := func(path string) {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+path, nil)
		req.Header.Set(bff.HeaderUserID, "user-1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Println("error:", err)
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		shown := string(body)
		if len(shown) > 240 {
			shown = shown[:240] + "..."
		}
		fmt.Printf("GET %-13s -> %d, %4d bytes\n  %s\n", path, resp.StatusCode, len(body), shown)
	}

	fmt.Println("== Both clients call the same profile and order services ==")
	get("/web/home")
	get("/mobile/home")

	fmt.Println("\n== The order service goes down: each BFF applies its own client's policy ==")
	ords.down.Store(true)
	get("/web/home")
	get("/mobile/home")
}
