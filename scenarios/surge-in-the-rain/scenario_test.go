package surgerain

import (
	"sync"
	"testing"
)

// The runs are deterministic, so compute each once for all tests.
var (
	once          sync.Once
	broken, fixed Report
)

func runs() (Report, Report) {
	once.Do(func() { broken, fixed = Broken(), Fixed() })
	return broken, fixed
}

// violations lists how a run breaks the invariant: every rain cell reaches
// SurgeTarget within SurgeWithin minutes of 18:00 and is back to 1.0x within
// RecoverWithin minutes of 18:30, and no dry cell is priced more than
// OverTolerance above its own true multiplier.
func violations(r *Report) []string {
	var out []string
	for c := range Cells {
		if !IsRainCell(c) {
			continue
		}
		if t := r.TimeToSurge(c); t < 0 || t > SurgeWithin {
			out = append(out, r.CellHash[c]+": surge too late or never")
		}
		if t := r.Recovery(c); t < 0 || t > RecoverWithin {
			out = append(out, r.CellHash[c]+": surge outlived the rain")
		}
	}
	if _, _, over, _ := r.DryOverpricing(); over > 0 {
		out = append(out, "dry cells over-priced")
	}
	return out
}

func TestBrokenViolatesInvariant(t *testing.T) {
	b, _ := runs()
	v := violations(&b)
	if len(v) == 0 {
		t.Fatal("broken run should violate the invariant")
	}
	for c := range Cells {
		if IsRainCell(c) && b.TimeToSurge(c) >= 0 {
			t.Errorf("broken: rain cell %s reached %.1fx; the city-wide average should never get there", b.CellHash[c], SurgeTarget)
		}
	}
	if _, _, over, _ := b.DryOverpricing(); over == 0 {
		t.Error("broken: city-wide averaging should over-price the dry suburbs")
	}
	if b.CountedWrongMinute == 0 {
		t.Error("broken: delayed requests should be counted in their arrival minute")
	}
}

func TestFixedHoldsInvariant(t *testing.T) {
	_, f := runs()
	if v := violations(&f); len(v) > 0 {
		t.Fatalf("fixed run violates the invariant: %v", v)
	}
}

// Late requests are counted in the windows of the minute they happened in:
// every emitted window's final count equals the number of requests with an
// event time inside it that arrived before the window's allowed lateness ran
// out. Only a small share are too late for every window.
func TestFixedAttributesLateEventsByEventTime(t *testing.T) {
	_, f := runs()
	if f.WindowsChecked == 0 || f.WindowsWrong != 0 {
		t.Fatalf("%d of %d windows have a count that differs from event time", f.WindowsWrong, f.WindowsChecked)
	}
	if f.LateFolded == 0 {
		t.Fatal("expected late requests folded into already-emitted windows")
	}
	if f.Dropped == 0 || float64(f.Dropped) > 0.01*float64(f.Requests) {
		t.Fatalf("dropped %d of %d requests, want some (the 9-15 min delays) but under 1%%", f.Dropped, f.Requests)
	}
}

func TestFixedMatchesRidersFaster(t *testing.T) {
	b, f := runs()
	bw, _ := b.WaitingRiderMinutes()
	fw, _ := f.WaitingRiderMinutes()
	if fw*4 > bw {
		t.Fatalf("waiting rider-minutes: fixed %d, broken %d; want at least 4x fewer", fw, bw)
	}
	if f.DriverMoves == 0 || b.DriverMoves != 0 {
		t.Fatalf("driver moves: fixed %d, broken %d; only per-cell surge should pull drivers", f.DriverMoves, b.DriverMoves)
	}
}

// The cost of the fix: a surge value per cell per minute plus revisions for
// late data instead of one number every 15 minutes, per-cell window state,
// and a lag behind the true multiplier from the watermark delay and the
// one-minute slide.
func TestFixedCost(t *testing.T) {
	b, f := runs()
	if f.Emissions < 100*b.Emissions || f.Updates == 0 {
		t.Fatalf("emissions: fixed %d (%d revisions), broken %d", f.Emissions, f.Updates, b.Emissions)
	}
	if want := Cells * int(Window/Slide); f.PeakOpen < want {
		t.Fatalf("peak open windows %d, want at least %d (cells x windows per event)", f.PeakOpen, want)
	}
	for c := range Cells {
		if !IsRainCell(c) {
			continue
		}
		if got, ideal := f.TimeToSurge(c), f.TrueTimeToSurge(c); got <= ideal {
			t.Errorf("cell %s: fixed surged after %d min, true after %d; expected the watermark to cost at least a minute",
				f.CellHash[c], got, ideal)
		}
	}
}

// Runs share no mutable state, so two at once (under -race) give the same
// numbers as the cached run.
func TestDeterministic(t *testing.T) {
	_, f := runs()
	var wg sync.WaitGroup
	again := make([]Report, 2)
	for i := range again {
		wg.Add(1)
		go func() { defer wg.Done(); again[i] = Fixed() }()
	}
	wg.Wait()
	for _, r := range again {
		if r.Applied != f.Applied || r.Waiting != f.Waiting {
			t.Fatal("two fixed runs differ")
		}
	}
}
