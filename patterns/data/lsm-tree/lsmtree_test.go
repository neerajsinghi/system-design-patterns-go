package lsmtree

import (
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"testing"
)

func newT(t *testing.T, opts Options) *Tree {
	t.Helper()
	tr, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func TestNewValidation(t *testing.T) {
	for _, o := range []Options{{MemtableLimit: 0}, {MemtableLimit: 1, MaxSegments: -1}} {
		if _, err := New(o); err == nil {
			t.Errorf("New(%+v): expected error", o)
		}
	}
}

func TestReadPathPrecedence(t *testing.T) {
	tests := []struct {
		name   string
		ops    func(*Tree)
		key    string
		want   string
		wantOK bool
	}{
		{"memtable hit", func(tr *Tree) { tr.Put("a", "1") }, "a", "1", true},
		{"segment hit", func(tr *Tree) { tr.Put("a", "1"); tr.Flush() }, "a", "1", true},
		{"newer segment shadows older", func(tr *Tree) {
			tr.Put("a", "old")
			tr.Flush()
			tr.Put("a", "new")
			tr.Flush()
		}, "a", "new", true},
		{"memtable shadows segment", func(tr *Tree) { tr.Put("a", "old"); tr.Flush(); tr.Put("a", "new") }, "a", "new", true},
		{"tombstone in memtable hides segment value", func(tr *Tree) { tr.Put("a", "1"); tr.Flush(); tr.Delete("a") }, "a", "", false},
		{"tombstone in newer segment hides older", func(tr *Tree) {
			tr.Put("a", "1")
			tr.Flush()
			tr.Delete("a")
			tr.Flush()
		}, "a", "", false},
		{"re-put after delete", func(tr *Tree) {
			tr.Put("a", "1")
			tr.Flush()
			tr.Delete("a")
			tr.Flush()
			tr.Put("a", "2")
		}, "a", "2", true},
		{"missing key", func(tr *Tree) { tr.Put("b", "1"); tr.Flush() }, "a", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := newT(t, Options{MemtableLimit: 100})
			tt.ops(tr)
			for _, compacted := range []bool{false, true} {
				got, ok := tr.Get(tt.key)
				if got != tt.want || ok != tt.wantOK {
					t.Fatalf("compacted=%v: got (%q,%v) want (%q,%v)", compacted, got, ok, tt.want, tt.wantOK)
				}
				tr.Compact() // compaction must not change any answer
			}
		})
	}
}

func TestAutoFlushAndCompaction(t *testing.T) {
	tr := newT(t, Options{MemtableLimit: 10, MaxSegments: 4})
	for i := range 45 {
		tr.Put(fmt.Sprintf("k%03d", i), "v")
	}
	s := tr.Stats()
	if s.Flushes != 4 || s.MemtableKeys != 5 || s.Segments != 4 || s.Compactions != 0 {
		t.Fatalf("before 5th flush: %+v", s)
	}
	for i := 45; i < 50; i++ {
		tr.Put(fmt.Sprintf("k%03d", i), "v")
	}
	s = tr.Stats()
	if s.Flushes != 5 || s.Segments != 1 || s.Compactions != 1 || s.SegmentEntries != 50 {
		t.Fatalf("after 5th flush: %+v", s)
	}
}

func TestCompactionReclaimsSpaceAndCutsProbes(t *testing.T) {
	tr := newT(t, Options{MemtableLimit: 1000})
	for round := range 5 { // rewrite the same 100 keys five times
		for i := range 100 {
			tr.Put(fmt.Sprintf("k%03d", i), fmt.Sprint(round))
		}
		tr.Flush()
	}
	for i := range 50 {
		tr.Delete(fmt.Sprintf("k%03d", i))
	}
	tr.Flush()

	before := tr.Stats()
	if before.Segments != 6 || before.SegmentEntries != 550 {
		t.Fatalf("before: %+v", before)
	}
	_, _ = tr.Get("k099") // only in older segments: probes several
	probesBefore := tr.Stats().SegmentProbes - before.SegmentProbes

	tr.Compact()
	after := tr.Stats()
	if after.Segments != 1 || after.SegmentEntries != 50 {
		t.Fatalf("after: %+v (want 1 segment, 50 live entries, tombstones dropped)", after)
	}
	v, ok := tr.Get("k099")
	if !ok || v != "4" {
		t.Fatalf("k099=%q %v", v, ok)
	}
	if probesAfter := tr.Stats().SegmentProbes - after.SegmentProbes; probesAfter != 1 || probesBefore <= 1 {
		t.Fatalf("probes before=%d after=%d", probesBefore, probesAfter)
	}
}

func TestScanMergesAllLevels(t *testing.T) {
	tr := newT(t, Options{MemtableLimit: 100})
	tr.Put("a", "1")
	tr.Put("c", "1")
	tr.Flush()
	tr.Put("b", "2")
	tr.Put("c", "2")
	tr.Delete("a")
	tr.Flush()
	tr.Put("d", "3")
	got := tr.Scan("", "")
	want := []KV{{"b", "2"}, {"c", "2"}, {"d", "3"}}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	if got := tr.Scan("b", "d"); !slices.Equal(got, want[:2]) {
		t.Fatalf("bounded scan got %v", got)
	}
	if got := tr.Scan("z", "a"); len(got) != 0 {
		t.Fatalf("inverted range got %v", got)
	}
}

// TestMatchesMapModel runs random operations against both the tree and a
// plain map and checks they always agree.
func TestMatchesMapModel(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	tr := newT(t, Options{MemtableLimit: 16, MaxSegments: 5})
	model := map[string]string{}
	for i := range 5000 {
		k := fmt.Sprintf("k%02d", rng.Intn(60))
		switch r := rng.Intn(10); {
		case r < 6:
			v := fmt.Sprint(i)
			tr.Put(k, v)
			model[k] = v
		case r < 9:
			tr.Delete(k)
			delete(model, k)
		default:
			tr.Compact()
		}
		got, ok := tr.Get(k)
		want, wok := model[k]
		if got != want || ok != wok {
			t.Fatalf("op %d key %s: tree (%q,%v) model (%q,%v)", i, k, got, ok, want, wok)
		}
	}
	scan := tr.Scan("", "")
	if len(scan) != len(model) {
		t.Fatalf("scan has %d keys, model %d", len(scan), len(model))
	}
	for _, kv := range scan {
		if model[kv.Key] != kv.Value {
			t.Fatalf("scan %s=%s, model %s", kv.Key, kv.Value, model[kv.Key])
		}
	}
}

func TestConcurrentReadersAndWriters(t *testing.T) {
	tr := newT(t, Options{MemtableLimit: 8, MaxSegments: 3})
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := range 300 {
				tr.Put(fmt.Sprintf("w%d-%d", w, i), "v")
			}
		}()
		go func() {
			defer wg.Done()
			for i := range 300 {
				_, _ = tr.Get(fmt.Sprintf("w%d-%d", w, i))
				_ = tr.Scan("w", "x")
			}
		}()
	}
	wg.Wait()
	if n := len(tr.Scan("", "")); n != 1200 {
		t.Fatalf("got %d keys, want 1200", n)
	}
}
