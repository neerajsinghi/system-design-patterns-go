package hotkeysalting

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newClock() *fakeClock { return &fakeClock{now: time.Unix(1_000, 0)} }

func (c *fakeClock) Now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fakeClock) add(d time.Duration) { c.mu.Lock(); c.now = c.now.Add(d); c.mu.Unlock() }

func counterTable(t *testing.T, parts, capacity int, clk *fakeClock) *Table[int64] {
	t.Helper()
	tb, err := NewTable[int64](parts, capacity, clk.Now)
	if err != nil {
		t.Fatal(err)
	}
	return tb
}

func TestTableThrottlesPerPartitionPerSecond(t *testing.T) {
	clk := newClock()
	tb := counterTable(t, 4, 10, clk)
	inc := func(old int64, _ bool) int64 { return old + 1 }
	var throttled int
	for range 15 {
		if err := tb.Write("k", inc); errors.Is(err, ErrThrottled) {
			throttled++
		}
	}
	if throttled != 5 {
		t.Fatalf("throttled %d of 15 writes in one second, want 5", throttled)
	}
	clk.add(time.Second)
	if err := tb.Write("k", inc); err != nil {
		t.Fatalf("write in the next second: %v", err)
	}
	if v, _ := tb.Read("k"); v != 11 {
		t.Fatalf("value = %d, want 11 (throttled writes must not apply)", v)
	}
	st := tb.Stats()[tb.PartitionFor("k")]
	if st.Writes != 11 || st.Throttled != 5 || st.Reads != 1 {
		t.Fatalf("partition stats = %+v", st)
	}
}

func TestEvenSpreadAcrossSuffixes(t *testing.T) {
	tests := []struct {
		name   string
		pick   func(int) int
		lo, hi int64
	}{
		{"round robin", RoundRobin(), 1000, 1000},
		{"seeded random", rand.New(rand.NewPCG(1, 2)).IntN, 880, 1120},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tb := counterTable(t, 16, 1_000_000, newClock())
			s, err := New(tb, Sum, WithMode(SaltAll), WithShards(8), WithPicker(tc.pick))
			if err != nil {
				t.Fatal(err)
			}
			for range 8000 {
				if err := s.Write("likes", 1); err != nil {
					t.Fatal(err)
				}
			}
			for _, k := range s.PhysicalKeys("likes") {
				if n, _ := tb.Read(k); n < tc.lo || n > tc.hi {
					t.Errorf("%s got %d of 8000 writes, want %d..%d", k, n, tc.lo, tc.hi)
				}
			}
		})
	}
}

func TestReadMergesAllSuffixes(t *testing.T) {
	t.Run("counter sum", func(t *testing.T) {
		tb := counterTable(t, 8, 1_000_000, newClock())
		s, _ := New(tb, Sum, WithMode(SaltAll), WithShards(4), WithPicker(RoundRobin()))
		for i := range 10 {
			s.Write("c", int64(i))
		}
		v, found, err := s.Read("c")
		if err != nil || !found || v != 45 {
			t.Fatalf("Read = %d, %v, %v; want 45", v, found, err)
		}
		if got := s.Stats().Lookups; got != 4 {
			t.Fatalf("lookups = %d, want 4", got)
		}
	})
	t.Run("item list concat", func(t *testing.T) {
		tb, _ := NewTable[[]string](8, 1_000_000, newClock().Now)
		s, _ := New(tb, Concat[string], WithMode(SaltAll), WithShards(3), WithPicker(RoundRobin()))
		want := []string{"a", "b", "c", "d", "e", "f", "g"}
		for _, item := range want {
			s.Write("cart", []string{item})
		}
		for i, k := range s.PhysicalKeys("cart") {
			if _, ok := tb.Read(k); !ok {
				t.Fatalf("suffix %d (%s) is empty; round robin should fill all 3", i, k)
			}
		}
		got, _, _ := s.Read("cart")
		slices.Sort(got)
		if !slices.Equal(got, want) {
			t.Fatalf("merged items = %v, want %v", got, want)
		}
	})
	t.Run("missing key", func(t *testing.T) {
		tb := counterTable(t, 8, 10, newClock())
		s, _ := New(tb, Sum)
		if _, found, err := s.Read("nobody"); found || err != nil {
			t.Fatalf("found=%v err=%v for a key never written", found, err)
		}
	})
}

func TestAdaptiveSaltsOnlyHotKeys(t *testing.T) {
	clk := newClock()
	tb := counterTable(t, 8, 1_000_000, clk)
	s, err := New(tb, Sum, WithMode(Adaptive), WithShards(8), WithDetector(100, time.Second),
		WithClock(clk.Now), WithPicker(RoundRobin()))
	if err != nil {
		t.Fatal(err)
	}
	for i := range 2000 {
		s.Write("hot", 1)
		s.Write(fmt.Sprintf("cold%d", i%50), 1) // 40 writes per cold key
		clk.add(time.Millisecond)               // whole run: 2 seconds
	}
	if got := s.HotKeys(); !slices.Equal(got, []string{"hot"}) {
		t.Fatalf("hot keys = %v, want [hot]", got)
	}
	if s.Fanout("hot") != 8 || s.Fanout("cold0") != 1 {
		t.Fatalf("fanout hot=%d cold=%d, want 8 and 1", s.Fanout("hot"), s.Fanout("cold0"))
	}
	before := s.Stats().Lookups
	v, _, _ := s.Read("cold7")
	if v != 40 || s.Stats().Lookups-before != 1 {
		t.Fatalf("cold read = %d with %d lookups, want 40 with 1", v, s.Stats().Lookups-before)
	}
	if s.Stats().Promotions != 1 {
		t.Fatalf("promotions = %d, want 1", s.Stats().Promotions)
	}
}

func TestDetectorWindowResets(t *testing.T) {
	clk := newClock()
	tb := counterTable(t, 8, 1_000_000, clk)
	s, _ := New(tb, Sum, WithDetector(10, time.Second), WithClock(clk.Now))
	// 9 writes per second, forever below the 10-per-window threshold.
	for range 5 {
		for range 9 {
			s.Write("steady", 1)
		}
		clk.add(time.Second)
	}
	if s.Fanout("steady") != 1 {
		t.Fatal("a key under the threshold in every window was salted")
	}
}

func TestNoLostWritesAcrossHotTransition(t *testing.T) {
	clk := newClock()
	tb := counterTable(t, 8, 1_000_000, clk)
	s, _ := New(tb, Sum, WithDetector(50, time.Second), WithClock(clk.Now), WithPicker(RoundRobin()))
	for range 49 {
		s.Write("k", 1)
	}
	if s.Fanout("k") != 1 {
		t.Fatal("promoted too early")
	}
	if v, _ := tb.Read("k"); v != 49 {
		t.Fatalf("unsalted key holds %d, want 49", v)
	}
	for range 951 {
		s.Write("k", 1)
	}
	if s.Fanout("k") != 8 {
		t.Fatal("not promoted after crossing the threshold")
	}
	if v, _, _ := s.Read("k"); v != 1000 {
		t.Fatalf("after promotion Read = %d, want 1000: pre-promotion writes were lost", v)
	}
	if keys := s.PhysicalKeys("k"); keys[0] != "k" {
		t.Fatalf("suffix 0 = %q, want the original key", keys[0])
	}
}

func TestSaltingRemovesThrottling(t *testing.T) {
	run := func(mode Mode) Stats {
		clk := newClock()
		tb := counterTable(t, 16, 100, clk)
		s, _ := New(tb, Sum, WithMode(mode), WithShards(8), WithPicker(RoundRobin()), WithClock(clk.Now))
		for range 3 {
			for range 400 { // 400 writes/s against 100 writes/s per partition
				s.Write("hot", 1)
			}
			clk.add(time.Second)
		}
		return s.Stats()
	}
	off, all := run(Unsalted), run(SaltAll)
	if off.Throttled != 900 {
		t.Errorf("unsalted throttled %d of 1200, want 900", off.Throttled)
	}
	if all.Throttled >= off.Throttled/3 {
		t.Errorf("salted throttled %d, want far fewer than %d", all.Throttled, off.Throttled)
	}
}

func TestStaticHotKeysInUnsaltedMode(t *testing.T) {
	tb := counterTable(t, 8, 1000, newClock())
	s, err := New(tb, Sum, WithMode(Unsalted), WithShards(4), WithHotKeys("sale:noon"))
	if err != nil {
		t.Fatal(err)
	}
	if s.Fanout("sale:noon") != 4 || s.Fanout("other") != 1 {
		t.Fatalf("fanout = %d/%d, want 4/1", s.Fanout("sale:noon"), s.Fanout("other"))
	}
	for range 2000 {
		s.Write("other", 1)
	}
	if s.Fanout("other") != 1 {
		t.Fatal("unsalted mode must not detect hot keys")
	}
}

func TestValidation(t *testing.T) {
	clk := newClock()
	tb := counterTable(t, 4, 10, clk)
	if _, err := NewTable[int64](0, 10, nil); err == nil {
		t.Error("NewTable accepted 0 partitions")
	}
	if _, err := NewTable[int64](4, 0, nil); err == nil {
		t.Error("NewTable accepted 0 capacity")
	}
	tests := []struct {
		name  string
		table *Table[int64]
		merge func(a, b int64) int64
		opts  []Option
	}{
		{"nil table", nil, Sum, nil},
		{"nil merge", tb, nil, nil},
		{"one shard", tb, Sum, []Option{WithShards(1)}},
		{"zero threshold", tb, Sum, []Option{WithDetector(0, time.Second)}},
		{"zero window", tb, Sum, []Option{WithDetector(5, 0)}},
		{"unknown mode", tb, Sum, []Option{WithMode(Mode(9))}},
		{"nil picker", tb, Sum, []Option{WithPicker(nil)}},
		{"nil clock", tb, Sum, []Option{WithClock(nil)}},
		{"bad hot key", tb, Sum, []Option{WithHotKeys("a#1")}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.table, tc.merge, tc.opts...); err == nil {
				t.Error("New accepted invalid input")
			}
		})
	}

	s, _ := New(tb, Sum)
	for _, key := range []string{"", "post#2"} {
		if err := s.Write(key, 1); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("Write(%q) err = %v", key, err)
		}
		if _, _, err := s.Read(key); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("Read(%q) err = %v", key, err)
		}
	}
	bad, _ := New(tb, Sum, WithMode(SaltAll), WithPicker(func(n int) int { return n }))
	if err := bad.Write("k", 1); err == nil {
		t.Error("out-of-range picker result was accepted")
	}
}

func TestConcurrentWritesAndReads(t *testing.T) {
	clk := newClock()
	tb := counterTable(t, 8, 1_000_000_000, clk)
	s, _ := New(tb, Sum, WithShards(8), WithDetector(200, time.Second), WithClock(clk.Now))
	const workers, perG = 8, 1000
	var wg sync.WaitGroup
	for g := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perG {
				if err := s.Write("hot", 1); err != nil {
					t.Error(err)
				}
				if err := s.Write(fmt.Sprintf("cold%d-%d", g, i%100), 1); err != nil {
					t.Error(err)
				}
				if i%50 == 0 {
					s.Read("hot")
					clk.add(time.Millisecond)
				}
			}
		}()
	}
	wg.Wait()
	if v, _, _ := s.Read("hot"); v != workers*perG {
		t.Fatalf("hot = %d, want %d: writes lost during concurrent promotion", v, workers*perG)
	}
	if got := s.HotKeys(); !slices.Equal(got, []string{"hot"}) {
		t.Fatalf("hot keys = %v", got)
	}
	var total int64
	for g := range workers {
		for i := range 100 {
			v, _, _ := s.Read(fmt.Sprintf("cold%d-%d", g, i))
			total += v
		}
	}
	if total != workers*perG {
		t.Fatalf("cold total = %d, want %d", total, workers*perG)
	}
}
