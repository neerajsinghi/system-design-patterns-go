package cursorpagination

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	key = []byte("0123456789abcdef0123456789abcdef")
	t0  = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
)

// seed inserts n items. Items 1-4 share one timestamp to exercise the ID
// tie-breaker.
func seed(n int) *Store {
	s := &Store{}
	for i := 1; i <= n; i++ {
		ts := t0.Add(time.Duration(i) * time.Minute)
		if i <= 4 {
			ts = t0
		}
		s.Insert(Item{ID: int64(i), CreatedAt: ts, Title: fmt.Sprint("item ", i)})
	}
	return s
}

func ids(items []Item) []int64 {
	out := make([]int64, len(items))
	for i, it := range items {
		out[i] = it.ID
	}
	return out
}

func walk(t *testing.T, s *Store, c *Codec, limit int, between func(page int)) []int64 {
	t.Helper()
	var all []int64
	token := ""
	for page := 0; page < 100; page++ {
		p, err := Paginate(s, c, token, "/items", limit)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, ids(p.Items)...)
		if p.NextCursor == "" {
			return all
		}
		token = p.NextCursor
		if between != nil {
			between(page)
		}
	}
	t.Fatal("pagination did not terminate")
	return nil
}

func TestOrderingWithTies(t *testing.T) {
	got := fmt.Sprint(ids(seed(6).Offset(0, 10)))
	if got != "[6 5 4 3 2 1]" {
		t.Errorf("order = %s", got)
	}
}

func TestCursorWalksEverythingOnce(t *testing.T) {
	c, _ := NewCodec(key, 0, nil)
	for _, limit := range []int{1, 2, 3, 5, 10, 100} {
		got := fmt.Sprint(walk(t, seed(10), c, limit, nil))
		if got != "[10 9 8 7 6 5 4 3 2 1]" {
			t.Errorf("limit %d: %s", limit, got)
		}
	}
}

// TestOffsetVsCursorUnderConcurrentWrites is the core lesson. New items
// arrive at the head between page requests. Offset pagination repeats
// items, and a delete makes it skip one. Cursor pagination does neither.
func TestOffsetVsCursorUnderConcurrentWrites(t *testing.T) {
	// Offset: page 1 = [10 9 8], then 2 new items arrive, then page 2 at offset 3.
	s := seed(10)
	page1 := ids(s.Offset(0, 3))
	s.Insert(Item{ID: 11, CreatedAt: t0.Add(time.Hour)})
	s.Insert(Item{ID: 12, CreatedAt: t0.Add(2 * time.Hour)})
	page2 := ids(s.Offset(3, 3))
	if fmt.Sprint(page1, page2) != "[10 9 8] [9 8 7]" {
		t.Errorf("expected offset to repeat 9 and 8, got %v %v", page1, page2)
	}

	// Offset with a delete: item 10 (already seen) is removed, so 7 is skipped.
	s = seed(10)
	page1 = ids(s.Offset(0, 3))
	s.Delete(10)
	page2 = ids(s.Offset(3, 3))
	if fmt.Sprint(page1, page2) != "[10 9 8] [6 5 4]" {
		t.Errorf("expected offset to skip 7, got %v %v", page1, page2)
	}

	// Cursor: same disruptions, including deleting the cursor's own item.
	c, _ := NewCodec(key, 0, nil)
	s = seed(10)
	got := walk(t, s, c, 3, func(page int) {
		if page == 0 {
			s.Insert(Item{ID: 11, CreatedAt: t0.Add(time.Hour)})
			s.Delete(8) // last item of page 1, the one the cursor points at
		}
	})
	if fmt.Sprint(got) != "[10 9 8 7 6 5 4 3 2 1]" {
		t.Errorf("cursor walk = %v", got)
	}
}

func TestCursorTamperResistance(t *testing.T) {
	now := t0
	c, _ := NewCodec(key, time.Minute, func() time.Time { return now })
	other, _ := NewCodec([]byte(strings.Repeat("k", 32)), 0, nil)
	good := c.Encode(Position{CreatedAt: 5, ID: 5}, "/items")

	// Forge a payload with a different position but keep the old signature.
	body, _ := json.Marshal(payload{Position: Position{CreatedAt: 1, ID: 1}, Scope: "/items"})
	_, sig, _ := strings.Cut(good, ".")
	forged := base64.RawURLEncoding.EncodeToString(body) + "." + sig

	tests := []struct {
		name   string
		token  string
		scope  string
		at     time.Time
		wantOK bool
	}{
		{"valid", good, "/items", t0, true},
		{"forged payload", forged, "/items", t0, false},
		{"signed by another key", other.Encode(Position{ID: 5}, "/items"), "/items", t0, false},
		{"wrong scope", good, "/admin/items", t0, false},
		{"expired", good, "/items", t0.Add(2 * time.Minute), false},
		{"garbage", "not-a-cursor", "/items", t0, false},
		{"bad base64", "!!!.???", "/items", t0, false},
		{"empty parts", ".", "/items", t0, false},
		{"huge", strings.Repeat("a", 1000) + ".b", "/items", t0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			now = tc.at
			p, err := c.Decode(tc.token, tc.scope)
			if tc.wantOK && (err != nil || p.ID != 5) {
				t.Errorf("got %v %v", p, err)
			}
			if !tc.wantOK && !errors.Is(err, ErrInvalidCursor) {
				t.Errorf("err = %v, want ErrInvalidCursor", err)
			}
		})
	}
}

func TestNewCodecRejectsShortKey(t *testing.T) {
	if _, err := NewCodec([]byte("short"), 0, nil); err == nil {
		t.Error("expected error")
	}
}

func TestHandler(t *testing.T) {
	c, _ := NewCodec(key, 0, nil)
	srv := httptest.NewServer(Handler(seed(5), c))
	defer srv.Close()

	get := func(q string) (int, Page) {
		resp, err := http.Get(srv.URL + "/items" + q)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var p Page
		if resp.StatusCode == 200 {
			_ = json.NewDecoder(resp.Body).Decode(&p)
		}
		return resp.StatusCode, p
	}
	tests := []struct {
		q         string
		wantCode  int
		wantItems int
	}{
		{"", 200, 5},
		{"?limit=2", 200, 2},
		{"?limit=0", 200, 1},    // clamped up
		{"?limit=1000", 200, 5}, // clamped down
		{"?limit=abc", 400, 0},
		{"?cursor=bogus", 400, 0},
	}
	for _, tc := range tests {
		code, p := get(tc.q)
		if code != tc.wantCode || len(p.Items) != tc.wantItems {
			t.Errorf("%s: %d with %d items, want %d with %d", tc.q, code, len(p.Items), tc.wantCode, tc.wantItems)
		}
	}
	_, p := get("?limit=3")
	if _, p2 := get("?limit=3&cursor=" + p.NextCursor); len(p2.Items) != 2 || p2.NextCursor != "" {
		t.Errorf("second page = %+v", p2)
	}
}

func TestConcurrentInsertAndPaginate(t *testing.T) {
	s := seed(50)
	c, _ := NewCodec(key, 0, nil)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			s.Insert(Item{ID: int64(1000 + i), CreatedAt: t0.Add(time.Duration(1000+i) * time.Hour)})
		}
	}()
	got := walk(t, s, c, 7, nil)
	wg.Wait()
	seen := map[int64]bool{}
	for _, id := range got {
		if seen[id] {
			t.Fatalf("duplicate %d", id)
		}
		seen[id] = true
	}
	for i := int64(1); i <= 50; i++ {
		if !seen[i] {
			t.Errorf("missed original item %d", i)
		}
	}
}
