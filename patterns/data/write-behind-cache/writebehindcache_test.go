package writebehindcache

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"
)

func TestNewValidation(t *testing.T) {
	store := NewMemStore[int]()
	tests := []struct {
		name  string
		store BatchStore[int]
		cfg   Config
	}{
		{"nil store", nil, Config{MaxBatch: 1, Interval: time.Second}},
		{"zero batch", store, Config{MaxBatch: 0, Interval: time.Second}},
		{"no interval or tick", store, Config{MaxBatch: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.store, tt.cfg); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

// newManual returns a cache whose timer only fires when the test says so.
func newManual(t *testing.T, store *MemStore[int], maxBatch int) (*Cache[int], chan time.Time) {
	t.Helper()
	tick := make(chan time.Time)
	c, err := New[int](store, Config{MaxBatch: maxBatch, Tick: tick})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c, tick
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestWritesAreAcknowledgedBeforePersisting(t *testing.T) {
	store := NewMemStore[int]()
	c, tick := newManual(t, store, 100)
	_ = c.Set("a", 1)
	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Fatal("write not visible in cache")
	}
	if _, ok := store.Get("a"); ok {
		t.Fatal("store written synchronously; expected deferred write")
	}
	tick <- time.Time{}
	waitFor(t, func() bool { _, ok := store.Get("a"); return ok })
}

func TestCoalescing(t *testing.T) {
	store := NewMemStore[int]()
	c, _ := newManual(t, store, 100)
	for i := range 5 {
		_ = c.Set("k", i)
	}
	if err := c.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if v, _ := store.Get("k"); v != 4 {
		t.Fatalf("store has %d, want last write 4", v)
	}
	if b := store.Batches(); !slices.Equal(b, []int{1}) {
		t.Fatalf("batches=%v, want one batch of one key", b)
	}
	if s := c.Stats(); s.Coalesced != 4 || s.Writes != 5 {
		t.Fatalf("stats=%+v", s)
	}
}

func TestBatchSizeTriggersEarlyFlushAndChunks(t *testing.T) {
	store := NewMemStore[int]()
	c, _ := newManual(t, store, 4)
	for i := range 10 {
		_ = c.Set(fmt.Sprintf("k%d", i), i)
	}
	// No tick sent: the batch-size kick alone must persist data.
	waitFor(t, func() bool { return store.Len() >= 4 })
	_ = c.Flush(context.Background())
	if store.Len() != 10 {
		t.Fatalf("persisted %d keys, want 10", store.Len())
	}
	for _, n := range store.Batches() {
		if n > 4 {
			t.Fatalf("batch of %d exceeds MaxBatch 4", n)
		}
	}
}

func TestFailedFlushRequeuesWithoutClobberingNewerWrites(t *testing.T) {
	store := NewMemStore[int]()
	c, _ := newManual(t, store, 100)
	_ = c.Set("a", 1)
	_ = c.Set("b", 1)
	store.FailNext(1)
	if err := c.Flush(context.Background()); !errors.Is(err, ErrInjected) {
		t.Fatalf("want ErrInjected, got %v", err)
	}
	if c.Dirty() != 2 {
		t.Fatalf("dirty=%d, want 2 re-queued", c.Dirty())
	}
	_ = c.Set("a", 2) // newer write while the old one is re-queued
	if err := c.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if v, _ := store.Get("a"); v != 2 {
		t.Fatalf("a=%d, want 2", v)
	}
	if c.Stats().FlushErrors != 1 {
		t.Fatal("flush error not counted")
	}
}

func TestCloseFlushesAndRejectsWrites(t *testing.T) {
	store := NewMemStore[int]()
	c, err := New[int](store, Config{MaxBatch: 100, Interval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Set("a", 1)
	if err := c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.Get("a"); !ok {
		t.Fatal("Close did not flush")
	}
	if err := c.Set("b", 2); !errors.Is(err, ErrClosed) {
		t.Fatalf("want ErrClosed, got %v", err)
	}
	if err := c.Close(context.Background()); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestCrashLosesUnflushedWrites(t *testing.T) {
	store := NewMemStore[int]()
	c, err := New[int](store, Config{MaxBatch: 100, Interval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Set("a", 1)
	_ = c.Flush(context.Background())
	_ = c.Set("b", 2)
	_ = c.Set("c", 3)
	if lost := c.Crash(); lost != 2 {
		t.Fatalf("lost=%d, want 2", lost)
	}
	if _, ok := store.Get("b"); ok {
		t.Fatal("b should have been lost")
	}
	if _, ok := store.Get("a"); !ok {
		t.Fatal("flushed write a should survive")
	}
}
