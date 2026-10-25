package bloomfilter

import (
	"fmt"
	"math"
	"sync"
	"testing"
)

func TestNewValidation(t *testing.T) {
	tests := []struct {
		name string
		n    uint64
		p    float64
	}{
		{"zero n", 0, 0.01},
		{"p zero", 100, 0},
		{"p one", 100, 1},
		{"p negative", 100, -0.1},
		{"p NaN", 100, math.NaN()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.n, tt.p); err == nil {
				t.Fatal("expected error")
			}
		})
	}
	if _, err := NewWithParams(0, 3); err == nil {
		t.Fatal("m=0: expected error")
	}
	if _, err := NewWithParams(1<<41, 3); err == nil {
		t.Fatal("huge m: expected error")
	}
}

func TestOptimalParams(t *testing.T) {
	// Well-known reference values: 1% at n=1M needs ~9.59 bits/item and 7 hashes.
	m, k := OptimalParams(1_000_000, 0.01)
	if bpi := float64(m) / 1e6; bpi < 9.5 || bpi > 9.7 || k != 7 {
		t.Fatalf("m=%d (%.2f bits/item) k=%d", m, bpi, k)
	}
	if _, k := OptimalParams(1000, 0.001); k != 10 {
		t.Fatalf("p=0.1%%: k=%d, want 10", k)
	}
}

func TestNoFalseNegatives(t *testing.T) {
	f, _ := New(5000, 0.01)
	for i := range 5000 {
		f.AddString(fmt.Sprintf("member-%d", i))
	}
	for i := range 5000 {
		if !f.TestString(fmt.Sprintf("member-%d", i)) {
			t.Fatalf("false negative for member-%d", i)
		}
	}
	if f.Count() != 5000 {
		t.Fatalf("count=%d", f.Count())
	}
}

// TestMeasuredFPR inserts n items and probes 10n items that were never added.
// The measured false-positive rate must be close to the target.
func TestMeasuredFPR(t *testing.T) {
	const n = 10_000
	for _, p := range []float64{0.1, 0.01, 0.001} {
		t.Run(fmt.Sprintf("p=%g", p), func(t *testing.T) {
			f, err := New(n, p)
			if err != nil {
				t.Fatal(err)
			}
			for i := range n {
				f.AddString(fmt.Sprintf("in-%d", i))
			}
			const probes = 10 * n
			fp := 0
			for i := range probes {
				if f.TestString(fmt.Sprintf("out-%d", i)) {
					fp++
				}
			}
			measured := float64(fp) / probes
			t.Logf("target %.4f  measured %.4f  estimated %.4f  (m=%d k=%d)", p, measured, f.EstimatedFPR(), f.M(), f.K())
			if measured > p*1.5 || measured < p*0.5 {
				t.Fatalf("measured FPR %.5f not within [0.5p, 1.5p] of %.5f", measured, p)
			}
			if est := f.EstimatedFPR(); est > p*1.2 {
				t.Fatalf("estimated FPR %.5f exceeds target %.5f at design load", est, p)
			}
		})
	}
}

func TestOverfillingRaisesFPR(t *testing.T) {
	f, _ := New(1000, 0.01)
	for i := range 1000 {
		f.AddString(fmt.Sprint(i))
	}
	atDesign := f.EstimatedFPR()
	for i := 1000; i < 4000; i++ {
		f.AddString(fmt.Sprint(i))
	}
	if over := f.EstimatedFPR(); over < 10*atDesign {
		t.Fatalf("expected FPR to blow up when 4x over capacity: %.4f -> %.4f", atDesign, over)
	}
}

func TestConcurrentAddTest(t *testing.T) {
	f, _ := New(10_000, 0.01)
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 1000 {
				s := fmt.Sprintf("%d-%d", w, i)
				f.AddString(s)
				if !f.TestString(s) {
					t.Errorf("false negative for %s", s)
					return
				}
			}
		}()
	}
	wg.Wait()
}
