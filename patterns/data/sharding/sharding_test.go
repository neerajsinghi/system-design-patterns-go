package sharding

import (
	"fmt"
	"slices"
	"sync"
	"testing"
)

func TestConstructorsValidate(t *testing.T) {
	if _, err := NewHashRouter(0); err == nil {
		t.Error("NewHashRouter(0): expected error")
	}
	for _, splits := range [][]string{{""}, {"b", "a"}, {"a", "a"}} {
		if _, err := NewRangeRouter(splits...); err == nil {
			t.Errorf("NewRangeRouter(%q): expected error", splits)
		}
	}
	if _, err := NewCluster(nil); err == nil {
		t.Error("NewCluster(nil): expected error")
	}
	if _, err := NewCluster(HashRouter{}); err == nil {
		t.Error("NewCluster(zero HashRouter): expected error")
	}
	if c, err := NewCluster(RangeRouter{}); err != nil || c.router.NumShards() != 1 {
		t.Error("zero RangeRouter should act as one shard")
	}
}

func TestRangeRouting(t *testing.T) {
	r, err := NewRangeRouter("g", "p")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		key  string
		want int
	}{
		{"", 0}, {"apple", 0}, {"fzzz", 0}, {"g", 1}, {"go", 1}, {"p", 2}, {"zebra", 2},
	}
	for _, tt := range tests {
		if got := r.ShardFor(tt.key); got != tt.want {
			t.Errorf("ShardFor(%q)=%d want %d", tt.key, got, tt.want)
		}
	}
	rangeTests := []struct {
		lo, hi string
		want   []int
	}{
		{"a", "c", []int{0}},
		{"h", "o", []int{1}},
		{"f", "q", []int{0, 1, 2}},
		{"q", "", []int{2}},
		{"g", "p", []int{1}},
		{"z", "a", nil},
	}
	for _, tt := range rangeTests {
		if got := r.ShardsForRange(tt.lo, tt.hi); !slices.Equal(got, tt.want) {
			t.Errorf("ShardsForRange(%q,%q)=%v want %v", tt.lo, tt.hi, got, tt.want)
		}
	}
}

func TestSplit(t *testing.T) {
	r, _ := NewRangeRouter("m")
	if _, err := r.Split("m"); err == nil {
		t.Fatal("splitting at an existing boundary should fail")
	}
	r2, err := r.Split("t") // [m, ∞) -> [m, t) on 1, [t, ∞) on new shard 2
	if err != nil {
		t.Fatal(err)
	}
	if r2.NumShards() != 3 || r2.ShardFor("n") != 1 || r2.ShardFor("u") != 2 || r2.ShardFor("a") != 0 {
		t.Fatalf("unexpected routing after split: %+v", r2)
	}
	if r.ShardFor("u") != 1 {
		t.Fatal("Split must not mutate the original router")
	}
}

func fill(c *Cluster, n int) {
	for i := range n {
		c.Put(fmt.Sprintf("user:%05d", i), fmt.Sprint(i))
	}
}

func TestReshardMovement(t *testing.T) {
	const n = 10_000
	h4, _ := NewHashRouter(4)
	h5, _ := NewHashRouter(5)
	rr, _ := NewRangeRouter("user:02500", "user:05000", "user:07500")
	rsplit, _ := rr.Split("user:08750")

	tests := []struct {
		name       string
		from, to   Router
		minF, maxF float64
	}{
		{"hash 4->5 moves most keys", h4, h5, 0.75, 0.85},
		{"range split moves only half of one shard", rr, rsplit, 0.124, 0.126},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewCluster(tt.from)
			if err != nil {
				t.Fatal(err)
			}
			fill(c, n)
			moved, err := c.Reshard(tt.to)
			if err != nil {
				t.Fatal(err)
			}
			if f := float64(moved) / n; f < tt.minF || f > tt.maxF {
				t.Fatalf("moved %.3f, want [%.3f, %.3f]", f, tt.minF, tt.maxF)
			}
			// Every key is still readable and on the right shard.
			for i := range n {
				k := fmt.Sprintf("user:%05d", i)
				if v, ok := c.Get(k); !ok || v != fmt.Sprint(i) {
					t.Fatalf("lost %s after reshard", k)
				}
			}
			total := 0
			for _, x := range c.Counts() {
				total += x
			}
			if total != n {
				t.Fatalf("total keys %d, want %d", total, n)
			}
		})
	}
}

func TestReshardShrink(t *testing.T) {
	h4, _ := NewHashRouter(4)
	h2, _ := NewHashRouter(2)
	c, _ := NewCluster(h4)
	fill(c, 1000)
	if _, err := c.Reshard(h2); err != nil {
		t.Fatal(err)
	}
	counts := c.Counts()
	if len(counts) != 2 || counts[0]+counts[1] != 1000 {
		t.Fatalf("counts after shrink: %v", counts)
	}
}

func TestScanTouchesFewerShardsWithRanges(t *testing.T) {
	hash, _ := NewHashRouter(4)
	rng, _ := NewRangeRouter("user:02500", "user:05000", "user:07500")
	for _, tt := range []struct {
		name   string
		r      Router
		shards int
	}{{"hash", hash, 4}, {"range", rng, 1}} {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := NewCluster(tt.r)
			fill(c, 10_000)
			keys, queried := c.Scan("user:01000", "user:01010")
			if len(keys) != 10 || keys[0] != "user:01000" || queried != tt.shards {
				t.Fatalf("got %d keys from %d shards, want 10 from %d", len(keys), queried, tt.shards)
			}
		})
	}
}

func TestConcurrentPutGet(t *testing.T) {
	h, _ := NewHashRouter(8)
	c, _ := NewCluster(h)
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				k := fmt.Sprintf("w%d-%d", w, i)
				c.Put(k, k)
				if v, ok := c.Get(k); !ok || v != k {
					t.Errorf("read back %q", v)
					return
				}
			}
		}()
	}
	h16, _ := NewHashRouter(16)
	_, _ = c.Reshard(h16) // concurrent with the writers
	wg.Wait()
}
