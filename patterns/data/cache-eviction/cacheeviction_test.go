package cacheeviction

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
)

func TestConstructorValidation(t *testing.T) {
	tests := []struct {
		name string
		new  func() error
	}{
		{"LRU zero", func() error { _, err := NewLRU[int, int](0); return err }},
		{"FIFO negative", func() error { _, err := NewFIFO[int, int](-1); return err }},
		{"LFU zero", func() error { _, err := NewLFU[int, int](0); return err }},
		{"TinyLFU one", func() error { _, err := NewTinyLFU[int, int](1); return err }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.new() == nil {
				t.Fatal("expected error")
			}
		})
	}
}

// present reports which of keys are cached, using a Get that would perturb
// LRU/LFU state, so call it only at the end of a test.
func present(p Policy[string, int], keys ...string) string {
	out := ""
	for _, k := range keys {
		if _, ok := p.Get(k); ok {
			out += k
		}
	}
	return out
}

func TestEvictionOrder(t *testing.T) {
	// Capacity 3. Insert a, b, c, read a twice and b once, then insert d.
	lru, _ := NewLRU[string, int](3)
	fifo, _ := NewFIFO[string, int](3)
	lfu, _ := NewLFU[string, int](3)

	tests := []struct {
		p    Policy[string, int]
		want string // survivors among a, b, c, d
	}{
		{lru, "abd"},  // c is least recently used
		{fifo, "bcd"}, // a was inserted first; reads don't save it
		{lfu, "abd"},  // c has the lowest count
	}
	for _, tt := range tests {
		t.Run(tt.p.Name(), func(t *testing.T) {
			for i, k := range []string{"a", "b", "c"} {
				tt.p.Put(k, i)
			}
			tt.p.Get("a")
			tt.p.Get("a")
			tt.p.Get("b")
			tt.p.Put("d", 3)
			if tt.p.Len() != 3 {
				t.Fatalf("Len = %d, want 3", tt.p.Len())
			}
			if got := present(tt.p, "a", "b", "c", "d"); got != tt.want {
				t.Fatalf("survivors = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestLFUTieBreaksByRecency(t *testing.T) {
	c, _ := NewLFU[string, int](2)
	c.Put("old", 1)
	c.Put("new", 2) // both have count 1; "old" is less recent
	c.Put("x", 3)
	if got := present(c, "old", "new", "x"); got != "newx" {
		t.Fatalf("survivors = %q, want %q", got, "newx")
	}
}

func TestUpdateKeepsSingleEntry(t *testing.T) {
	mk := []func() Policy[string, int]{
		func() Policy[string, int] { c, _ := NewLRU[string, int](4); return c },
		func() Policy[string, int] { c, _ := NewFIFO[string, int](4); return c },
		func() Policy[string, int] { c, _ := NewLFU[string, int](4); return c },
		func() Policy[string, int] { c, _ := NewTinyLFU[string, int](4); return c },
	}
	for _, f := range mk {
		p := f()
		t.Run(p.Name(), func(t *testing.T) {
			p.Put("k", 1)
			p.Put("k", 2)
			if v, ok := p.Get("k"); !ok || v != 2 || p.Len() != 1 {
				t.Fatalf("got (%d, %v) len %d, want (2, true) len 1", v, ok, p.Len())
			}
		})
	}
}

func TestTinyLFURejectsScan(t *testing.T) {
	c, _ := NewTinyLFU[string, int](100)
	hot := make([]string, 99) // fills the main region exactly
	for i := range hot {
		hot[i] = fmt.Sprintf("hot%d", i)
	}
	// Read the hot set repeatedly so the sketch knows it. The sketch ages
	// (halves) once during this test, so the hot keys need a clear margin.
	for range 10 {
		Replay[string, int](c, hot, func(string) int { return 1 })
	}
	// A one-pass scan of 500 keys (5x capacity) nobody will read again.
	scan := make([]string, 500)
	for i := range scan {
		scan[i] = fmt.Sprintf("scan%d", i)
	}
	Replay[string, int](c, scan, func(string) int { return 0 })

	kept := 0
	for _, k := range hot {
		if _, ok := c.Get(k); ok {
			kept++
		}
	}
	// The sketch can over-estimate a scan key that collides with hot keys in
	// every row, so allow a small admission error rather than demand perfection.
	if kept < len(hot)-3 {
		t.Fatalf("TinyLFU kept %d/%d hot keys after a scan, want nearly all", kept, len(hot))
	}

	// Plain LRU loses the entire hot set to the same scan.
	l, _ := NewLRU[string, int](100)
	Replay[string, int](l, hot, func(string) int { return 1 })
	Replay[string, int](l, scan, func(string) int { return 0 })
	if h, _ := Replay[string, int](l, hot, func(string) int { return 1 }); h != 0 {
		t.Fatalf("LRU kept %d hot keys after a scan, want 0", h)
	}
}

func TestTinyLFUAdmitsNewlyHotKey(t *testing.T) {
	c, _ := NewTinyLFU[string, int](100)
	for i := range 99 {
		k := fmt.Sprintf("k%d", i)
		c.Get(k)
		c.Put(k, i)
	}
	// "rising" is read many times; the window cycles it out to compete for main.
	for range 10 {
		c.Get("rising")
	}
	c.Put("rising", 1)
	c.Put("pusher", 2) // window holds one entry, so this evicts "rising" into the admission check
	if _, ok := c.Get("rising"); !ok {
		t.Fatal("frequently read key was not admitted to the main region")
	}
}

func TestSketch(t *testing.T) {
	s := NewSketch(64)
	for range 7 {
		s.Increment("a")
	}
	if got := s.Estimate("a"); got < 7 {
		t.Fatalf("Estimate(a) = %d, want >= 7 (count-min never under-counts)", got)
	}
	for range 100 {
		s.Increment("b")
	}
	if got := s.Estimate("b"); got != sketchMaxHits {
		t.Fatalf("Estimate(b) = %d, want saturation at %d", got, sketchMaxHits)
	}
	before := s.Estimate("b")
	s.reset()
	if got := s.Estimate("b"); got != before/2 {
		t.Fatalf("after reset Estimate(b) = %d, want %d", got, before/2)
	}
}

func TestSketchAgesAutomatically(t *testing.T) {
	s := NewSketch(8) // sampleSize = 80
	for range 15 {
		s.Increment("old")
	}
	for i := range 80 {
		s.Increment(i)
	}
	if got := s.Estimate("old"); got > 8 {
		t.Fatalf("Estimate(old) = %d after a reset, want it roughly halved", got)
	}
}

// zipfTrace draws n keys from a skewed popularity distribution and splices
// in one-off scan keys, the workload that separates the policies.
func zipfTrace(seed uint64, n, keys, scanEvery, scanLen int) []string {
	r := rand.New(rand.NewPCG(seed, seed))
	z := rand.NewZipf(r, 1.1, 1, uint64(keys-1))
	var trace []string
	scanID := 0
	for i := range n {
		trace = append(trace, fmt.Sprintf("k%d", z.Uint64()))
		if scanEvery > 0 && i%scanEvery == 0 {
			for range scanLen {
				trace = append(trace, fmt.Sprintf("scan%d", scanID))
				scanID++
			}
		}
	}
	return trace
}

func TestTinyLFUBeatsLRUOnScanPollutedZipf(t *testing.T) {
	trace := zipfTrace(42, 50_000, 5_000, 500, 300)
	ratio := func(p Policy[string, int]) float64 {
		h, m := Replay(p, trace, func(string) int { return 0 })
		return float64(h) / float64(h+m)
	}
	l, _ := NewLRU[string, int](250)
	tl, _ := NewTinyLFU[string, int](250)
	lr, tr := ratio(l), ratio(tl)
	if tr < lr+0.05 {
		t.Fatalf("W-TinyLFU hit ratio %.3f not clearly above LRU %.3f", tr, lr)
	}
}

func TestConcurrentUse(t *testing.T) {
	mk := []func() Policy[int, int]{
		func() Policy[int, int] { c, _ := NewLRU[int, int](50); return c },
		func() Policy[int, int] { c, _ := NewFIFO[int, int](50); return c },
		func() Policy[int, int] { c, _ := NewLFU[int, int](50); return c },
		func() Policy[int, int] { c, _ := NewTinyLFU[int, int](50); return c },
	}
	for _, f := range mk {
		p := f()
		t.Run(p.Name(), func(t *testing.T) {
			var wg sync.WaitGroup
			for g := range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := range 2_000 {
						k := (i * (g + 1)) % 200
						if _, ok := p.Get(k); !ok {
							p.Put(k, i)
						}
					}
				}()
			}
			wg.Wait()
			if p.Len() > 50 {
				t.Fatalf("Len = %d exceeds capacity 50", p.Len())
			}
		})
	}
}
