// Command example serves a versioned document store over loopback HTTP and
// shows the two jobs of conditional requests: (1) a client polling an
// unchanged resource 100 times, with and without ETag revalidation, counting
// the bytes that actually cross the wire; and (2) two editors changing the
// same document, first without If-Match (one edit is silently lost), then
// with If-Match (412, re-read, re-apply, both edits kept).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	conditionalrequests "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/conditional-requests"
)

const polls = 100

// meter counts bytes read from and written to the network by one client.
type meter struct{ down, up atomic.Int64 }

type countingConn struct {
	net.Conn
	m *meter
}

func (c countingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.m.down.Add(int64(n))
	return n, err
}

func (c countingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.m.up.Add(int64(n))
	return n, err
}

func meteredHTTPClient(m *meter) *http.Client {
	d := &net.Dialer{}
	return &http.Client{Transport: &http.Transport{
		DisableCompression: true,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := d.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			return countingConn{c, m}, nil
		},
	}}
}

type product struct {
	ID    int     `json:"id"`
	Name  string  `json:"name"`
	Price float64 `json:"price"`
	Stock int     `json:"stock"`
}

type doc struct {
	Title string   `json:"title"`
	Tags  []string `json:"tags"`
}

func edit(f func(*doc)) func([]byte) ([]byte, error) {
	return func(b []byte) ([]byte, error) {
		var d doc
		if err := json.Unmarshal(b, &d); err != nil {
			return nil, err
		}
		f(&d)
		return json.Marshal(d)
	}
}

func main() {
	clock := time.Date(2026, 10, 28, 9, 0, 0, 0, time.UTC)
	store := conditionalrequests.NewStore(conditionalrequests.WithClock(func() time.Time { return clock }))
	ctx := context.Background()

	// ---- Part 1: polling ----
	catalogue := make([]product, 60)
	for i := range catalogue {
		catalogue[i] = product{ID: i + 1, Name: fmt.Sprintf("Widget model %03d", i+1), Price: 9.99 + float64(i), Stock: 100 - i}
	}
	body, _ := json.Marshal(catalogue)
	res := store.Put("/catalogue", body, "application/json")

	h, err := conditionalrequests.NewHandler(store)
	if err != nil {
		log.Fatal(err)
	}
	srv := httptest.NewServer(h)
	defer srv.Close()

	fmt.Printf("1) A dashboard polls %s (%d-byte JSON, ETag %s) %d times; it never changes.\n\n",
		"/catalogue", len(body), res.ETag, polls)

	type run struct {
		label string
		st    conditionalrequests.ClientStats
		m     *meter
	}
	var runs []run
	for _, revalidate := range []bool{false, true} {
		m := &meter{}
		c := conditionalrequests.NewClient(meteredHTTPClient(m), conditionalrequests.WithRevalidation(revalidate))
		for range polls {
			if _, err := c.Get(ctx, srv.URL+"/catalogue"); err != nil {
				log.Fatal(err)
			}
		}
		label := "plain GET            "
		if revalidate {
			label = "If-None-Match + cache"
		}
		runs = append(runs, run{label, c.Stats(), m})
	}
	fmt.Println("   client                  200s  304s  body bytes  bytes down  bytes up")
	for _, r := range runs {
		fmt.Printf("   %s  %4d  %4d  %10d  %10d  %8d\n", r.label, r.st.Full, r.st.NotModified,
			r.st.BodyBytes, r.m.down.Load(), r.m.up.Load())
	}
	plain, reval := runs[0].m.down.Load(), runs[1].m.down.Load()
	fmt.Printf("\n   Revalidation downloaded %.1f%% of the bytes (%.1fx less). Each 304 is headers only,\n",
		100*float64(reval)/float64(plain), float64(plain)/float64(reval))
	fmt.Printf("   but it is still a round trip: %d requests either way. Uploads grow slightly because\n", polls)
	fmt.Println("   every revalidation sends the If-None-Match header.")

	// One change is picked up on the next poll.
	c := conditionalrequests.NewClient(srv.Client())
	_, _ = c.Get(ctx, srv.URL+"/catalogue")
	catalogue[0].Stock = 0
	body, _ = json.Marshal(catalogue)
	clock = clock.Add(time.Minute)
	res = store.Put("/catalogue", body, "application/json")
	f, _ := c.Get(ctx, srv.URL+"/catalogue")
	fmt.Printf("\n   After a stock change (new ETag %s) the next poll gets 200, revalidated=%v.\n", res.ETag, f.Revalidated)

	// ---- Part 2: two editors ----
	fmt.Println("\n2) Alice and Bob both open the same document, then save.")
	start := []byte(`{"title":"Q3 plan (draft)","tags":[]}`)
	aliceEdit := edit(func(d *doc) { d.Title = "Q3 plan" })
	bobEdit := edit(func(d *doc) { d.Tags = append(d.Tags, "finance") })

	// 2a: no If-Match.
	store.Put("/doc", start, "application/json")
	alice, bob := conditionalrequests.NewClient(srv.Client()), conditionalrequests.NewClient(srv.Client())
	url := srv.URL + "/doc"
	fa, _ := alice.Get(ctx, url)
	fb, _ := bob.Get(ctx, url)
	na, _ := aliceEdit(fa.Body)
	nb, _ := bobEdit(fb.Body)
	_, errA := alice.Put(ctx, url, na, "")
	_, errB := bob.Put(ctx, url, nb, "")
	final, _ := store.Get("/doc")
	fmt.Println("\n   a) Without If-Match:")
	fmt.Printf("      Alice PUT -> %v, Bob PUT -> %v\n", okOr(errA), okOr(errB))
	fmt.Printf("      final: %s\n", final.Body)
	fmt.Println("      Both saves \"succeeded\", but Alice's title change is gone: Bob saved the old title.")

	// 2b: If-Match required by the server.
	strict, _ := conditionalrequests.NewHandler(store, conditionalrequests.WithRequireIfMatch(true))
	srv2 := httptest.NewServer(strict)
	defer srv2.Close()
	store.Put("/doc", start, "application/json")
	url = srv2.URL + "/doc"
	alice, bob = conditionalrequests.NewClient(srv2.Client()), conditionalrequests.NewClient(srv2.Client())

	fmt.Println("\n   b) With If-Match (server built WithRequireIfMatch):")
	_, err = bob.Put(ctx, url, nb, "")
	fmt.Printf("      Bob PUT without If-Match -> %v\n", status(err))

	attempts := 0
	var conflictETag string
	r, err := bob.Update(ctx, url, 5, func(cur []byte) ([]byte, error) {
		attempts++
		if attempts == 1 {
			// Alice saves while Bob is still editing his copy.
			ar, err := alice.Update(ctx, url, 5, aliceEdit)
			if err != nil {
				return nil, err
			}
			conflictETag = ar.ETag
			fmt.Printf("      Bob reads; Alice saves first (If-Match ok, attempts=%d, new ETag %s)\n", ar.Attempts, ar.ETag)
		}
		return bobEdit(cur)
	})
	if err != nil {
		log.Fatal(err)
	}
	final, _ = store.Get("/doc")
	fmt.Printf("      Bob's first PUT -> 412 Precondition Failed (current ETag is %s)\n", conflictETag)
	fmt.Printf("      Bob re-reads and re-applies his change: saved after %d attempts, ETag %s\n", r.Attempts, r.ETag)
	fmt.Printf("      final: %s\n", final.Body)
	fmt.Printf("      Bob's client saw %d conflict(s); both edits survived.\n", bob.Stats().Conflicts)

	// Create-only write.
	_, err = bob.Put(ctx, srv2.URL+"/doc", nb, "*")
	fmt.Printf("\n   If-Match: * on an existing doc -> %v; ", okOr(err))
	req, _ := http.NewRequest(http.MethodPut, srv2.URL+"/doc", nil)
	req.Header.Set("If-None-Match", "*")
	resp, err := srv2.Client().Do(req)
	if err != nil {
		log.Fatal(err)
	}
	resp.Body.Close()
	fmt.Printf("If-None-Match: * (create-only) -> %s\n", resp.Status)
}

func okOr(err error) string {
	if err == nil {
		return "ok"
	}
	return status(err)
}

func status(err error) string {
	var se *conditionalrequests.StatusError
	if errors.As(err, &se) {
		return fmt.Sprintf("%d %s", se.Code, http.StatusText(se.Code))
	}
	return err.Error()
}
