package consistenthashing

import (
	"fmt"
	"sync"
	"testing"
)

func keys(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("key-%d", i)
	}
	return out
}

func ring(t *testing.T, vnodes int, nodes ...string) *Ring {
	t.Helper()
	r, err := New(vnodes)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range nodes {
		r.Add(n)
	}
	return r
}

func TestNewRejectsBadVnodes(t *testing.T) {
	for _, v := range []int{0, -1} {
		if _, err := New(v); err == nil {
			t.Fatalf("New(%d): expected error", v)
		}
	}
}

func TestEmptyRing(t *testing.T) {
	r := ring(t, 10)
	if _, ok := r.Get("k"); ok {
		t.Fatal("empty ring returned a node")
	}
	if got := r.GetN("k", 3); got != nil {
		t.Fatalf("GetN on empty ring = %v", got)
	}
}

func TestDeterministicAndIdempotent(t *testing.T) {
	a := ring(t, 50, "a", "b", "c")
	b := ring(t, 50, "c", "b", "a", "a") // different order, duplicate add
	for _, k := range keys(1000) {
		x, _ := a.Get(k)
		y, _ := b.Get(k)
		if x != y {
			t.Fatalf("%s: %s vs %s", k, x, y)
		}
	}
	b.Remove("zzz") // unknown node: no-op
	if len(b.Nodes()) != 3 {
		t.Fatal("unexpected node count")
	}
}

func TestDistributionImprovesWithVnodes(t *testing.T) {
	ks := keys(50_000)
	tests := []struct {
		vnodes  int
		maxSkew float64 // max |share - 1/N| / (1/N)
	}{
		{vnodes: 200, maxSkew: 0.25},
		{vnodes: 1000, maxSkew: 0.12},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("vnodes=%d", tt.vnodes), func(t *testing.T) {
			r := ring(t, tt.vnodes, "n1", "n2", "n3", "n4", "n5")
			counts := map[string]int{}
			for _, owner := range r.Assign(ks) {
				counts[owner]++
			}
			ideal := float64(len(ks)) / 5
			for n, c := range counts {
				if skew := abs(float64(c)-ideal) / ideal; skew > tt.maxSkew {
					t.Errorf("%s has %d keys (skew %.2f > %.2f)", n, c, skew, tt.maxSkew)
				}
			}
		})
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func TestMinimalMovement(t *testing.T) {
	ks := keys(20_000)
	tests := []struct {
		name   string
		change func(*Ring)
		// keys must only move to/from this node
		node       string
		joining    bool
		minF, maxF float64
	}{
		{"add 5th node", func(r *Ring) { r.Add("n5") }, "n5", true, 0.12, 0.28},
		{"remove a node", func(r *Ring) { r.Remove("n2") }, "n2", false, 0.15, 0.35},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := ring(t, 200, "n1", "n2", "n3", "n4")
			before := r.Assign(ks)
			tt.change(r)
			after := r.Assign(ks)
			for k := range before {
				if before[k] == after[k] {
					continue
				}
				if tt.joining && after[k] != tt.node {
					t.Fatalf("%s moved %s->%s, expected only moves to %s", k, before[k], after[k], tt.node)
				}
				if !tt.joining && before[k] != tt.node {
					t.Fatalf("%s moved %s->%s, expected only moves off %s", k, before[k], after[k], tt.node)
				}
			}
			f := float64(Moved(before, after)) / float64(len(ks))
			if f < tt.minF || f > tt.maxF {
				t.Fatalf("moved fraction %.3f outside [%.2f, %.2f]", f, tt.minF, tt.maxF)
			}
		})
	}
}

func TestGetNReturnsDistinctNodes(t *testing.T) {
	r := ring(t, 20, "a", "b", "c")
	tests := []struct{ n, want int }{{0, 0}, {1, 1}, {2, 2}, {3, 3}, {10, 3}}
	for _, tt := range tests {
		got := r.GetN("some-key", tt.n)
		if len(got) != tt.want {
			t.Fatalf("GetN(%d) = %v", tt.n, got)
		}
		seen := map[string]bool{}
		for _, n := range got {
			if seen[n] {
				t.Fatalf("duplicate node in %v", got)
			}
			seen[n] = true
		}
		if owner, _ := r.Get("some-key"); tt.n > 0 && got[0] != owner {
			t.Fatalf("first replica %s != owner %s", got[0], owner)
		}
	}
}

func TestConcurrentAccess(t *testing.T) {
	r := ring(t, 50, "a", "b")
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := range 50 {
				n := fmt.Sprintf("x%d-%d", i, j)
				r.Add(n)
				r.Remove(n)
			}
		}()
		go func() {
			defer wg.Done()
			for _, k := range keys(500) {
				if _, ok := r.Get(k); !ok {
					t.Error("lost all nodes")
					return
				}
			}
		}()
	}
	wg.Wait()
}
