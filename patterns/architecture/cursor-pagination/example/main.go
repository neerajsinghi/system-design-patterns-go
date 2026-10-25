// Command example pages through a feed while new items arrive, first with
// offsets (duplicates appear) and then with signed cursors (no
// duplicates). It also shows a tampered cursor being rejected.
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	cp "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/cursor-pagination"
)

var t0 = time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)

func seeded() *cp.Store {
	s := &cp.Store{}
	for i := 1; i <= 9; i++ {
		s.Insert(cp.Item{ID: int64(i), CreatedAt: t0.Add(time.Duration(i) * time.Minute), Title: fmt.Sprintf("post-%d", i)})
	}
	return s
}

func titles(items []cp.Item) string {
	var out []string
	for _, it := range items {
		out = append(out, it.Title)
	}
	return "[" + strings.Join(out, " ") + "]"
}

func main() {
	fmt.Println("== Offset pagination (?page=N) while 2 posts arrive after page 1 ==")
	s := seeded()
	fmt.Println("page 1:", titles(s.Offset(0, 3)))
	s.Insert(cp.Item{ID: 10, CreatedAt: t0.Add(time.Hour), Title: "post-10"})
	s.Insert(cp.Item{ID: 11, CreatedAt: t0.Add(2 * time.Hour), Title: "post-11"})
	fmt.Println("page 2:", titles(s.Offset(3, 3)), "<- post-8 and post-7 shown twice")

	fmt.Println("\n== Cursor pagination over HTTP, same disruption ==")
	s = seeded()
	codec, err := cp.NewCodec([]byte("demo-secret-key-at-least-32-bytes!"), 15*time.Minute, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	srv := httptest.NewServer(cp.Handler(s, codec))
	defer srv.Close()

	get := func(cursor string) (int, cp.Page) {
		resp, err := http.Get(srv.URL + "/feed?limit=3&cursor=" + cursor)
		if err != nil {
			fmt.Println("error:", err)
			return 0, cp.Page{}
		}
		defer resp.Body.Close()
		var p cp.Page
		if resp.StatusCode == http.StatusOK {
			_ = json.NewDecoder(resp.Body).Decode(&p)
		}
		return resp.StatusCode, p
	}

	cursor := ""
	for page := 1; ; page++ {
		_, p := get(cursor)
		fmt.Printf("page %d: %s\n", page, titles(p.Items))
		if page == 1 {
			fmt.Printf("        next cursor (opaque, signed): %.40s...\n", p.NextCursor)
			s.Insert(cp.Item{ID: 10, CreatedAt: t0.Add(time.Hour), Title: "post-10"})
			s.Insert(cp.Item{ID: 11, CreatedAt: t0.Add(2 * time.Hour), Title: "post-11"})
			fmt.Println("        (2 new posts arrive)")
		}
		if p.NextCursor == "" {
			break
		}
		cursor = p.NextCursor
	}
	fmt.Println("No duplicates and nothing skipped. New posts show up on the next refresh from the top.")

	fmt.Println("\n== Tampering ==")
	_, first := get("")
	tampered := "A" + first.NextCursor[1:]
	code, _ := get(tampered)
	fmt.Printf("edited first byte of a cursor -> HTTP %d\n", code)
}
