// Package cursorpagination contrasts offset pagination with keyset (cursor)
// pagination. Cursors are opaque and HMAC-signed, so clients can't forge or
// edit them. They are bound to the query they were issued for and can
// expire.
//
// The ordering is newest first: (CreatedAt DESC, ID DESC). The ID
// tie-breaker makes the order total, which keyset pagination needs. Without
// it, items with equal timestamps could be skipped or repeated.
package cursorpagination

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Item is a row in the list.
type Item struct {
	ID        int64     `json:"id"`
	CreatedAt time.Time `json:"createdAt"`
	Title     string    `json:"title"`
}

// Position is the sort key of the last item on a page.
type Position struct {
	CreatedAt int64 `json:"t"` // UnixNano
	ID        int64 `json:"id"`
}

func posOf(it Item) Position { return Position{it.CreatedAt.UnixNano(), it.ID} }

// after reports whether it sorts strictly after p in newest-first order.
func after(it Item, p Position) bool {
	t := it.CreatedAt.UnixNano()
	return t < p.CreatedAt || (t == p.CreatedAt && it.ID < p.ID)
}

// Store keeps items sorted newest first. It is safe for concurrent use.
type Store struct {
	mu    sync.RWMutex
	items []Item
}

// Insert adds an item in sorted position.
func (s *Store) Insert(it Item) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := sort.Search(len(s.items), func(i int) bool { return after(s.items[i], posOf(it)) })
	s.items = append(s.items, Item{})
	copy(s.items[i+1:], s.items[i:])
	s.items[i] = it
}

// Delete removes the item with id, if present.
func (s *Store) Delete(id int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, it := range s.items {
		if it.ID == id {
			s.items = append(s.items[:i], s.items[i+1:]...)
			return
		}
	}
}

// Offset is classic LIMIT/OFFSET. If rows are inserted or deleted ahead of
// the offset between page requests, items shift and are repeated or
// skipped. A database also has to scan and discard `offset` rows.
func (s *Store) Offset(offset, limit int) []Item {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if offset < 0 || limit <= 0 || offset >= len(s.items) {
		return nil
	}
	end := min(offset+limit, len(s.items))
	return append([]Item(nil), s.items[offset:end]...)
}

// After is keyset pagination ("WHERE (created_at, id) < (?, ?) ORDER BY ...
// LIMIT ?"). It returns up to limit items strictly after p (nil p means
// from the start) and whether more remain. Inserts and deletes elsewhere in
// the list don't shift it.
func (s *Store) After(p *Position, limit int) (items []Item, more bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	start := 0
	if p != nil {
		start = sort.Search(len(s.items), func(i int) bool { return after(s.items[i], *p) })
	}
	end := min(start+limit, len(s.items))
	if limit <= 0 || start >= end {
		return nil, false
	}
	return append([]Item(nil), s.items[start:end]...), end < len(s.items)
}

// ErrInvalidCursor covers malformed, tampered, expired, and wrong-scope
// cursors. Clients should not be told which one it was.
var ErrInvalidCursor = errors.New("cursorpagination: invalid cursor")

// Codec signs and verifies cursors with HMAC-SHA256.
type Codec struct {
	key []byte
	ttl time.Duration
	now func() time.Time
}

// NewCodec requires a key of at least 32 bytes. ttl <= 0 means cursors
// never expire. A nil now uses time.Now.
func NewCodec(key []byte, ttl time.Duration, now func() time.Time) (*Codec, error) {
	if len(key) < 32 {
		return nil, errors.New("cursorpagination: key must be at least 32 bytes")
	}
	if now == nil {
		now = time.Now
	}
	return &Codec{key: append([]byte(nil), key...), ttl: ttl, now: now}, nil
}

type payload struct {
	Position
	Scope string `json:"s"` // the query this cursor belongs to, e.g. a filter
	Exp   int64  `json:"e,omitempty"`
}

// Encode returns "base64url(json).base64url(hmac)".
func (c *Codec) Encode(p Position, scope string) string {
	pl := payload{Position: p, Scope: scope}
	if c.ttl > 0 {
		pl.Exp = c.now().Add(c.ttl).Unix()
	}
	body, _ := json.Marshal(pl) // cannot fail for this struct
	b := base64.RawURLEncoding.EncodeToString(body)
	return b + "." + base64.RawURLEncoding.EncodeToString(c.sign(b))
}

// Decode verifies the signature first (constant time), then expiry, then
// scope.
func (c *Codec) Decode(token, scope string) (Position, error) {
	b, sig, ok := strings.Cut(token, ".")
	if !ok || len(token) > 512 {
		return Position{}, ErrInvalidCursor
	}
	got, err := base64.RawURLEncoding.DecodeString(sig)
	if err != nil || !hmac.Equal(got, c.sign(b)) {
		return Position{}, ErrInvalidCursor
	}
	body, err := base64.RawURLEncoding.DecodeString(b)
	if err != nil {
		return Position{}, ErrInvalidCursor
	}
	var pl payload
	if err := json.Unmarshal(body, &pl); err != nil {
		return Position{}, ErrInvalidCursor
	}
	if pl.Exp != 0 && c.now().Unix() > pl.Exp {
		return Position{}, ErrInvalidCursor
	}
	if pl.Scope != scope {
		return Position{}, ErrInvalidCursor
	}
	return pl.Position, nil
}

func (c *Codec) sign(s string) []byte {
	m := hmac.New(sha256.New, c.key)
	m.Write([]byte(s))
	return m.Sum(nil)
}

// Page is one API response.
type Page struct {
	Items      []Item `json:"items"`
	NextCursor string `json:"nextCursor,omitempty"`
}

// Paginate returns the page after token ("" = first page).
func Paginate(s *Store, c *Codec, token, scope string, limit int) (Page, error) {
	var p *Position
	if token != "" {
		pos, err := c.Decode(token, scope)
		if err != nil {
			return Page{}, err
		}
		p = &pos
	}
	items, more := s.After(p, limit)
	page := Page{Items: items}
	if page.Items == nil {
		page.Items = []Item{}
	}
	if more {
		page.NextCursor = c.Encode(posOf(items[len(items)-1]), scope)
	}
	return page, nil
}

// Handler serves GET ?cursor=...&limit=... with limit clamped to 1..100
// (default 20).
func Handler(s *Store, c *Codec) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		limit := 20
		if v := q.Get("limit"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				http.Error(w, `{"error":"bad limit"}`, http.StatusBadRequest)
				return
			}
			limit = max(1, min(n, 100))
		}
		page, err := Paginate(s, c, q.Get("cursor"), r.URL.Path, limit)
		if err != nil {
			http.Error(w, `{"error":"invalid cursor"}`, http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(page)
	})
}
