package leaderboard

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"testing"
)

func sortedCounts(counts map[string]int64) []ItemCount {
	out := make([]ItemCount, 0, len(counts))
	for k, v := range counts {
		out = append(out, ItemCount{k, v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].before(out[j]) })
	return out
}

func TestTopKMatchesSort(t *testing.T) {
	r := rand.New(rand.NewPCG(4, 4))
	for _, size := range []int{0, 1, 5, 100, 2000} {
		counts := map[string]int64{}
		for i := range size {
			counts[fmt.Sprintf("i%d", i)] = r.Int64N(30) // many ties
		}
		all := sortedCounts(counts)
		for _, k := range []int{1, 3, 10, size, size + 5} {
			if k <= 0 {
				continue
			}
			got, err := TopK(counts, k)
			if err != nil {
				t.Fatal(err)
			}
			want := all[:min(k, len(all))]
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("size %d k %d: got %v, want %v", size, k, got, want)
			}
		}
	}
}

func zipfStream(seed uint64, n int, distinct uint64) []string {
	r := rand.New(rand.NewPCG(seed, seed+1))
	z := rand.NewZipf(r, 1.1, 1, distinct-1)
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("#tag%d", z.Uint64())
	}
	return out
}

func checkGuarantees(t *testing.T, ss *SpaceSaving, exact map[string]int64) {
	t.Helper()
	n, k, minC := ss.N(), int64(ss.K()), ss.MinCount()
	if minC > n/k {
		t.Fatalf("MinCount %d > N/k = %d", minC, n/k)
	}
	for item, c := range exact {
		est, ok := ss.Estimate(item)
		if c > minC && !ok {
			t.Fatalf("%s has true count %d > min %d but is not monitored", item, c, minC)
		}
		if c*k > n && !ok {
			t.Fatalf("%s has true count %d > N/k but is not monitored", item, c)
		}
		if !ok {
			continue
		}
		if est.Count < c || est.Count-est.Error > c {
			t.Fatalf("%s: true %d outside [%d, %d]", item, c, est.Count-est.Error, est.Count)
		}
		if est.Error > minC {
			t.Fatalf("%s: error %d exceeds min count %d", item, est.Error, minC)
		}
	}
	var sum int64
	for _, e := range ss.Top(-1) {
		sum += e.Count
	}
	if sum != n { // each unit of weight lands in exactly one counter
		t.Fatalf("sum of counters %d != N %d", sum, n)
	}
}

func TestSpaceSavingGuarantees(t *testing.T) {
	streams := map[string][]string{
		"zipf": zipfStream(1, 100_000, 5000),
	}
	// Adversarial-ish: a few heavy items interleaved with a long tail of
	// distinct items, which forces constant evictions.
	var adv []string
	for i := range 30_000 {
		adv = append(adv, fmt.Sprintf("heavy%d", i%5))
		adv = append(adv, fmt.Sprintf("tail%d", i))
	}
	streams["heavy+tail"] = adv
	for name, stream := range streams {
		for _, k := range []int{10, 50, 200} {
			t.Run(fmt.Sprintf("%s/k=%d", name, k), func(t *testing.T) {
				ss, _ := NewSpaceSaving(k)
				exact := map[string]int64{}
				for _, it := range stream {
					ss.Add(it, 1)
					exact[it]++
				}
				checkGuarantees(t, ss, exact)
			})
		}
	}
}

func TestSpaceSavingWeighted(t *testing.T) {
	r := rand.New(rand.NewPCG(9, 9))
	ss, _ := NewSpaceSaving(20)
	exact := map[string]int64{}
	for range 20_000 {
		it := fmt.Sprintf("x%d", int(r.ExpFloat64()*30))
		d := 1 + r.Int64N(5)
		ss.Add(it, d)
		exact[it] += d
	}
	checkGuarantees(t, ss, exact)
}

func TestSpaceSavingExactWhenFewItems(t *testing.T) {
	ss, _ := NewSpaceSaving(10)
	for i := range 5 {
		ss.Add(fmt.Sprint(i), int64(i+1))
	}
	if ss.MinCount() != 0 {
		t.Fatalf("MinCount with free counters = %d, want 0", ss.MinCount())
	}
	for i := range 5 {
		e, ok := ss.Estimate(fmt.Sprint(i))
		if !ok || e.Count != int64(i+1) || e.Error != 0 {
			t.Fatalf("estimate %d = %+v", i, e)
		}
	}
	top := ss.Top(2)
	if len(top) != 2 || top[0].Item != "4" || top[1].Item != "3" {
		t.Fatalf("Top(2) = %+v", top)
	}
	if _, ok := ss.Estimate("missing"); ok {
		t.Fatal("unexpected estimate")
	}
}

func TestSpaceSavingConcurrent(t *testing.T) {
	ss, _ := NewSpaceSaving(16)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 1000 {
				ss.Add(fmt.Sprintf("t%d", (g+i)%40), 1)
				if i%100 == 0 {
					ss.Top(5)
					ss.MinCount()
					ss.Estimate("t1")
				}
			}
		}()
	}
	wg.Wait()
	if ss.N() != 8000 {
		t.Fatalf("N = %d, want 8000", ss.N())
	}
}

func TestTopKValidation(t *testing.T) {
	if _, err := NewSpaceSaving(0); err == nil {
		t.Error("expected error for k=0")
	}
	ss, _ := NewSpaceSaving(1)
	if err := ss.Add("a", 0); err == nil {
		t.Error("expected error for zero delta")
	}
	if err := ss.Add("a", -3); err == nil {
		t.Error("expected error for negative delta")
	}
	if ss.N() != 0 {
		t.Error("rejected Add changed N")
	}
	if _, err := TopK(map[string]int64{"a": 1}, 0); err == nil {
		t.Error("expected error for TopK k=0")
	}
}
