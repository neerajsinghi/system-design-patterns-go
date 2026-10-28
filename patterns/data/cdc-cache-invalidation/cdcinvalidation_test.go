package cdcinvalidation

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (f *fakeClock) Now() time.Time          { f.mu.Lock(); defer f.mu.Unlock(); return f.t }
func (f *fakeClock) Advance(d time.Duration) { f.mu.Lock(); f.t = f.t.Add(d); f.mu.Unlock() }

func newClock() *fakeClock { return &fakeClock{t: time.Unix(1000, 0)} }

func mustCache(t *testing.T, ttl time.Duration, opts ...Option) *Cache {
	t.Helper()
	c, err := NewCache(ttl, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func mustWrite(t *testing.T, db *DB, k, v string) Change {
	t.Helper()
	c, err := db.Write(k, v)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestValidation(t *testing.T) {
	db := NewDB()
	c := mustCache(t, time.Minute)
	if _, err := NewCache(0); err == nil {
		t.Error("zero ttl accepted")
	}
	if _, err := NewCache(time.Second, WithTombstoneTTL(-1)); err == nil {
		t.Error("negative tombstone ttl accepted")
	}
	inv := []struct {
		name  string
		db    *DB
		cache *Cache
		opts  []Option
	}{
		{"nil db", nil, c, nil},
		{"nil cache", db, nil, nil},
		{"zero batch", db, c, []Option{WithBatchSize(0)}},
		{"bad mode", db, c, []Option{WithMode(Mode(7))}},
	}
	for _, tt := range inv {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewInvalidator(tt.db, tt.cache, tt.opts...); err == nil {
				t.Fatal("expected error")
			}
		})
	}
	if _, err := db.Write("", "x"); !errors.Is(err, ErrEmptyKey) {
		t.Errorf("Write empty key: %v", err)
	}
	if _, err := db.Delete("missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Delete missing: %v", err)
	}
	i, _ := NewInvalidator(db, c)
	if err := i.Rewind(5); !errors.Is(err, ErrBadRewind) {
		t.Errorf("Rewind forward: %v", err)
	}
	var s Scheduler
	if err := s.Add("x", "nothing", nil).Run(nil); err == nil {
		t.Error("nil step accepted")
	}
}

func TestDBLogMatchesCommits(t *testing.T) {
	db := NewDB()
	mustWrite(t, db, "a", "1")
	mustWrite(t, db, "b", "1")
	mustWrite(t, db, "a", "2")
	if _, err := db.Delete("b"); err != nil {
		t.Fatal(err)
	}
	all := db.ChangesSince(0, 0)
	if len(all) != 4 || db.LastVersion() != 4 {
		t.Fatalf("log has %d changes, last %d", len(all), db.LastVersion())
	}
	for i, c := range all {
		if c.Version != uint64(i+1) {
			t.Fatalf("change %d has version %d", i, c.Version)
		}
	}
	if !all[3].Deleted || all[3].Key != "b" {
		t.Fatalf("last change = %+v", all[3])
	}
	if r, _ := db.Read("a"); r.Version != 3 || r.Value != "2" {
		t.Fatalf("row a = %+v", r)
	}
	if got := db.ChangesSince(1, 2); len(got) != 2 || got[0].Version != 2 {
		t.Fatalf("ChangesSince(1,2) = %+v", got)
	}
	if got := db.ChangesSince(9, 0); got != nil {
		t.Fatalf("ChangesSince past end = %+v", got)
	}
}

// Every committed change, from any writer, must leave the cache unable to
// serve the pre-change value once the invalidator has caught up.
func TestEveryCommittedChangeInvalidates(t *testing.T) {
	for _, mode := range []Mode{ModeDelete, ModeUpdate} {
		t.Run(fmt.Sprint("mode", mode), func(t *testing.T) {
			db := NewDB()
			cache := mustCache(t, time.Hour)
			inv, _ := NewInvalidator(db, cache, WithMode(mode), WithBatchSize(7))
			r := rand.New(rand.NewPCG(1, 2))
			for i := range 2000 {
				k := fmt.Sprintf("k%d", r.IntN(50))
				if _, ok := db.Read(k); ok && r.IntN(10) == 0 {
					if _, err := db.Delete(k); err != nil {
						t.Fatal(err)
					}
				} else {
					mustWrite(t, db, k, fmt.Sprint(i))
				}
				cache.ReadThrough(db, fmt.Sprintf("k%d", r.IntN(50))) // warm random keys
				if r.IntN(5) == 0 {
					inv.Poll()
				}
			}
			inv.CatchUp()
			for k := range 50 {
				key := fmt.Sprintf("k%d", k)
				want, inDB := db.Read(key)
				got, inCache := cache.Get(key)
				if inCache && (!inDB || got != want) {
					t.Fatalf("%s: cache %+v, db %+v (exists %v)", key, got, want, inDB)
				}
			}
			if lag := inv.Lag(); lag.Events != 0 || lag.Time != 0 {
				t.Fatalf("lag after catch-up = %+v", lag)
			}
		})
	}
}

func TestOlderVersionSetRejected(t *testing.T) {
	c := mustCache(t, time.Hour)
	if !c.Set("k", "v5", 5) {
		t.Fatal("first set rejected")
	}
	for _, v := range []uint64{4, 5, 0} {
		if c.Set("k", "old", v) {
			t.Fatalf("set at version %d accepted over version 5", v)
		}
	}
	if !c.Set("k", "v6", 6) {
		t.Fatal("newer set rejected")
	}
	if r, _ := c.Get("k"); r.Value != "v6" || r.Version != 6 {
		t.Fatalf("got %+v", r)
	}
	if s := c.Stats(); s.Sets != 2 || s.RejectedSets != 3 {
		t.Fatalf("stats %+v", s)
	}
}

// The delete-then-stale-set race, scripted step by step.
func raceScript(c *Cache, db *DB, invalidate func(Change)) {
	var read Row
	var s Scheduler
	s.Add("reader", "cache miss", func() string { c.Get("price"); return "" }).
		Add("reader", "read DB", func() string { read, _ = db.Read("price"); return "" }).
		Add("writer", "update DB, invalidate", func() string { ch, _ := db.Write("price", "120"); invalidate(ch); return "" }).
		Add("reader", "set cache with what it read", func() string { c.Set("price", read.Value, read.Version); return "" })
	_ = s.Run(nil)
}

func TestTombstoneBlocksLateStaleSet(t *testing.T) {
	tests := []struct {
		name      string
		opts      []Option
		wantStale bool
	}{
		{"unversioned cache serves stale", []Option{WithoutVersionCheck()}, true},
		{"versioned cache rejects late set", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clk := newClock()
			db := NewDB(WithClock(clk.Now))
			mustWrite(t, db, "price", "100")
			c := mustCache(t, time.Minute, append(tt.opts, WithClock(clk.Now))...)
			raceScript(c, db, func(ch Change) { c.Invalidate(ch.Key, ch.Version) })
			r, ok := c.Get("price")
			stale := ok && r.Value == "100"
			if stale != tt.wantStale {
				t.Fatalf("after race got %+v (hit %v), wantStale %v", r, ok, tt.wantStale)
			}
			if tt.wantStale {
				clk.Advance(59 * time.Second)
				if r, _ := c.Get("price"); r.Value != "100" {
					t.Fatal("stale value should last until TTL")
				}
				clk.Advance(time.Second)
				if _, ok := c.Get("price"); ok {
					t.Fatal("stale value should expire at TTL")
				}
				return
			}
			if r, _ := c.ReadThrough(db, "price"); r.Value != "120" {
				t.Fatalf("refill got %+v", r)
			}
		})
	}
}

func TestTombstoneAllowsSameVersionFillAndExpires(t *testing.T) {
	clk := newClock()
	c := mustCache(t, time.Minute, WithClock(clk.Now), WithTombstoneTTL(5*time.Second))
	c.Invalidate("k", 7)
	if c.Set("k", "v6", 6) {
		t.Fatal("older set accepted over tombstone")
	}
	if !c.Set("k", "v7", 7) {
		t.Fatal("fill at the tombstone's own version rejected")
	}
	c.Invalidate("k", 9)
	clk.Advance(5 * time.Second)
	// Tombstone expired: an old set is accepted again. That is the documented
	// limit, and why the tombstone TTL must exceed the slowest fill.
	if !c.Set("k", "v8", 8) {
		t.Fatal("set after tombstone expiry rejected")
	}
}

func TestDeletedRowTombstone(t *testing.T) {
	db := NewDB()
	c := mustCache(t, time.Hour)
	inv, _ := NewInvalidator(db, c, WithMode(ModeUpdate))
	mustWrite(t, db, "u", "alice")
	stale, _ := db.Read("u")
	if _, err := db.Delete("u"); err != nil {
		t.Fatal(err)
	}
	inv.CatchUp()
	if c.Set("u", stale.Value, stale.Version) {
		t.Fatal("late set of a deleted row accepted")
	}
	if _, ok := c.ReadThrough(db, "u"); ok {
		t.Fatal("deleted row served")
	}
}

func TestDuplicateAndReplayedEventsIdempotent(t *testing.T) {
	for _, mode := range []Mode{ModeDelete, ModeUpdate} {
		t.Run(fmt.Sprint("mode", mode), func(t *testing.T) {
			db := NewDB()
			c := mustCache(t, time.Hour)
			inv, _ := NewInvalidator(db, c, WithMode(mode))
			first := mustWrite(t, db, "k", "v1")
			mustWrite(t, db, "k", "v2")
			inv.CatchUp()
			r, _ := c.ReadThrough(db, "k")
			if r.Value != "v2" {
				t.Fatalf("fill got %+v", r)
			}
			// Duplicate of an old event, and a full replay from the start.
			inv.Apply(first)
			if err := inv.Rewind(0); err != nil {
				t.Fatal(err)
			}
			if n := inv.CatchUp(); n != 2 {
				t.Fatalf("replayed %d changes, want 2", n)
			}
			got, ok := c.Get("k")
			if !ok || got.Value != "v2" {
				t.Fatalf("after duplicates cache = %+v (hit %v), want v2 still cached", got, ok)
			}
			if s := inv.Stats(); s.Redelivered != 3 || s.Applied != 5 {
				t.Fatalf("stats %+v", s)
			}
		})
	}
}

func TestLagReported(t *testing.T) {
	clk := newClock()
	db := NewDB(WithClock(clk.Now))
	c := mustCache(t, time.Hour, WithClock(clk.Now))
	inv, _ := NewInvalidator(db, c, WithClock(clk.Now), WithBatchSize(2))
	if l := inv.Lag(); l != (Lag{}) {
		t.Fatalf("empty lag = %+v", l)
	}
	for i := range 5 {
		mustWrite(t, db, fmt.Sprint(i), "x")
		clk.Advance(100 * time.Millisecond)
	}
	// Commits at t=0,100..400ms; now is 500ms.
	if l := inv.Lag(); l.Events != 5 || l.Time != 500*time.Millisecond {
		t.Fatalf("lag = %+v", l)
	}
	inv.Poll()
	if l := inv.Lag(); l.Events != 3 || l.Time != 300*time.Millisecond || inv.Position() != 2 {
		t.Fatalf("lag after one poll = %+v, position %d", l, inv.Position())
	}
	inv.CatchUp()
	if l := inv.Lag(); l != (Lag{}) {
		t.Fatalf("lag after catch-up = %+v", l)
	}
}

func TestSchedulerTrace(t *testing.T) {
	var b strings.Builder
	var order []int
	var s Scheduler
	s.Add("a", "first", func() string { order = append(order, 1); return "ok" }).
		Add("b", "second", func() string { order = append(order, 2); return "" })
	if err := s.Run(&b); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(order) != "[1 2]" || !strings.Contains(b.String(), "-> ok") || !strings.Contains(b.String(), "2. b") {
		t.Fatalf("order %v trace:\n%s", order, b.String())
	}
}

// Real goroutines: writers, cache-aside readers and the invalidator all run
// at once. Whatever the interleaving, once writes stop and the invalidator
// catches up, no cached value may differ from the database.
func TestConcurrentConvergence(t *testing.T) {
	for _, mode := range []Mode{ModeDelete, ModeUpdate} {
		t.Run(fmt.Sprint("mode", mode), func(t *testing.T) {
			db := NewDB()
			c := mustCache(t, time.Hour)
			inv, _ := NewInvalidator(db, c, WithMode(mode), WithBatchSize(3))
			const keys = 8
			var wg sync.WaitGroup
			done := make(chan struct{})
			for w := range 4 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := range 500 {
						_, _ = db.Write(fmt.Sprintf("k%d", (w+i)%keys), fmt.Sprintf("w%d-%d", w, i))
					}
				}()
			}
			var readers sync.WaitGroup
			for r := range 4 {
				readers.Add(1)
				go func() {
					defer readers.Done()
					for i := 0; ; i++ {
						select {
						case <-done:
							return
						default:
						}
						c.ReadThrough(db, fmt.Sprintf("k%d", (r+i)%keys))
					}
				}()
			}
			readers.Add(1)
			go func() {
				defer readers.Done()
				for {
					select {
					case <-done:
						return
					default:
						inv.Poll()
					}
				}
			}()
			wg.Wait()
			close(done)
			readers.Wait()
			inv.CatchUp()
			for k := range keys {
				key := fmt.Sprintf("k%d", k)
				want, _ := db.Read(key)
				if got, ok := c.Get(key); ok && got != want {
					t.Fatalf("%s: cache %+v, db %+v", key, got, want)
				}
			}
		})
	}
}
