package writethroughcache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestNewRejectsNilStore(t *testing.T) {
	if _, err := New[int](nil); err == nil {
		t.Fatal("expected error")
	}
}

func TestWriteThroughSemantics(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		failPuts  int
		wantErr   bool
		wantCache int // value expected from Get afterwards
		wantStore int
	}{
		{"successful write reaches store and cache", 0, false, 2, 2},
		{"failed store write leaves cache untouched", 1, true, 1, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMemStore[int]()
			c, _ := New[int](store)
			if err := c.Set(ctx, "k", 1); err != nil {
				t.Fatal(err)
			}
			store.FailNextPuts(tt.failPuts)
			err := c.Set(ctx, "k", 2)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tt.wantErr)
			}
			if tt.wantErr && !errors.Is(err, ErrInjected) {
				t.Fatalf("want ErrInjected, got %v", err)
			}
			if v, _ := c.Get(ctx, "k"); v != tt.wantCache {
				t.Fatalf("cache=%d want %d", v, tt.wantCache)
			}
			if v, _, _ := store.Get(ctx, "k"); v != tt.wantStore {
				t.Fatalf("store=%d want %d", v, tt.wantStore)
			}
		})
	}
}

func TestColdReadPopulatesCache(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore[string]()
	_ = store.Put(ctx, "a", "from-db")
	c, _ := New[string](store)

	if _, err := c.Get(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	for i := range 3 {
		v, err := c.Get(ctx, "a")
		if err != nil || v != "from-db" {
			t.Fatalf("read %d: %q %v", i, v, err)
		}
	}
	s := c.Stats()
	if s.StoreReads != 2 || s.Hits != 2 {
		t.Fatalf("stats=%+v, want 2 store reads (missing + first a) and 2 hits", s)
	}
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore[int]()
	c, _ := New[int](store)
	_ = c.Set(ctx, "k", 1)
	if err := c.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(ctx, "k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

// Concurrent writers to the same keys must leave the cache and store agreeing.
func TestConcurrentWritesStayConsistent(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore[int]()
	c, _ := New[int](store)
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 200 {
				key := fmt.Sprintf("k%d", i%5)
				_ = c.Set(ctx, key, w*1000+i)
				_, _ = c.Get(ctx, key)
			}
		}()
	}
	wg.Wait()
	for i := range 5 {
		key := fmt.Sprintf("k%d", i)
		cv, _ := c.Get(ctx, key)
		sv, _, _ := store.Get(ctx, key)
		if cv != sv {
			t.Fatalf("%s: cache=%d store=%d", key, cv, sv)
		}
	}
}
