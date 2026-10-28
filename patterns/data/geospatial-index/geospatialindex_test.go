package geospatialindex

import (
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"testing"
)

func TestEncodeKnownValues(t *testing.T) {
	tests := []struct {
		lat, lon float64
		prec     int
		want     string
	}{
		{57.64911, 10.40744, 11, "u4pruydqqvj"}, // the classic Wikipedia example
		{57.64911, 10.40744, 5, "u4pru"},
		{0, 0, 1, "s"},
		{-90, -180, 4, "0000"},
		{90, 180, 4, "zzzz"},
		{42.6, -5.6, 5, "ezs42"},
	}
	for _, tt := range tests {
		got, err := Encode(tt.lat, tt.lon, tt.prec)
		if err != nil || got != tt.want {
			t.Errorf("Encode(%v, %v, %d) = %q, %v; want %q", tt.lat, tt.lon, tt.prec, got, err, tt.want)
		}
	}
}

func TestDecodeRoundTrip(t *testing.T) {
	b, err := Decode("u4pruydqqvj")
	if err != nil {
		t.Fatal(err)
	}
	if !(b.MinLat <= 57.64911 && 57.64911 <= b.MaxLat && b.MinLon <= 10.40744 && 10.40744 <= b.MaxLon) {
		t.Fatalf("box %+v does not contain the encoded point", b)
	}
	if h := b.MaxLat - b.MinLat; h > 2e-6 {
		t.Fatalf("11-char cell height = %v degrees, want ~1.3e-6", h)
	}

	rng := rand.New(rand.NewPCG(1, 2))
	for range 2000 {
		lat, lon := rng.Float64()*180-90, rng.Float64()*360-180
		for p := 1; p <= MaxPrecision; p++ {
			h, _ := Encode(lat, lon, p)
			b, err := Decode(h)
			if err != nil {
				t.Fatal(err)
			}
			if lat < b.MinLat || lat > b.MaxLat || lon < b.MinLon || lon > b.MaxLon {
				t.Fatalf("Decode(Encode(%v,%v,%d)=%q) = %+v, point outside", lat, lon, p, h, b)
			}
			clat, clon := b.Center()
			if back, _ := Encode(clat, clon, p); back != h {
				t.Fatalf("centre of %q re-encodes to %q", h, back)
			}
		}
	}
}

func TestPrefixIsContainment(t *testing.T) {
	long, _ := Encode(12.9716, 77.5946, 9)
	for p := 1; p < 9; p++ {
		short, _ := Encode(12.9716, 77.5946, p)
		if long[:p] != short {
			t.Fatalf("precision %d: %q is not a prefix of %q", p, short, long)
		}
	}
}

func TestNeighboursKnownHash(t *testing.T) {
	got, err := Neighbours("dqcjq")
	if err != nil {
		t.Fatal(err)
	}
	want := [8]string{
		North: "dqcjw", NorthEast: "dqcjx", East: "dqcjr", SouthEast: "dqcjp",
		South: "dqcjn", SouthWest: "dqcjj", West: "dqcjm", NorthWest: "dqcjt",
	}
	if got != want {
		t.Fatalf("Neighbours(dqcjq) = %v, want %v", got, want)
	}
}

func TestNeighboursAreAdjacent(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	for range 500 {
		lat, lon := rng.Float64()*170-85, rng.Float64()*360-180
		h, _ := Encode(lat, lon, 1+rng.IntN(8))
		b, _ := Decode(h)
		nb, err := Neighbours(h)
		if err != nil {
			t.Fatal(err)
		}
		n, _ := Decode(nb[North])
		e, _ := Decode(nb[East])
		if b.MaxLat != 90 && (n.MinLat != b.MaxLat || n.MinLon != b.MinLon) {
			t.Fatalf("north of %q (%+v) is %q (%+v)", h, b, nb[North], n)
		}
		if b.MaxLon != 180 && (e.MinLon != b.MaxLon || e.MinLat != b.MinLat) {
			t.Fatalf("east of %q (%+v) is %q (%+v)", h, b, nb[East], e)
		}
	}
}

func TestNeighboursAntimeridian(t *testing.T) {
	east, _ := Encode(10, 179.99, 4) // touches +180
	nb, err := Neighbours(east)
	if err != nil {
		t.Fatal(err)
	}
	for _, dir := range []int{NorthEast, East, SouthEast} {
		b, _ := Decode(nb[dir])
		if b.MinLon != -180 {
			t.Errorf("neighbour %d of %q = %q starts at lon %v, want -180", dir, east, nb[dir], b.MinLon)
		}
	}
	west, _ := Encode(10, -179.99, 4)
	if nb[East] != west {
		t.Errorf("east of %q = %q, want %q", east, nb[East], west)
	}
	nbw, _ := Neighbours(west)
	if nbw[West] != east {
		t.Errorf("west of %q = %q, want %q", west, nbw[West], east)
	}
}

func TestNeighboursAtPole(t *testing.T) {
	top, _ := Encode(89.99, 0, 3)
	nb, err := Neighbours(top)
	if err != nil {
		t.Fatal(err)
	}
	for _, dir := range []int{NorthWest, North, NorthEast} {
		if nb[dir] != "" {
			t.Errorf("neighbour %d of polar cell %q = %q, want none", dir, top, nb[dir])
		}
	}
	if nb[South] == "" || nb[East] == "" || nb[West] == "" {
		t.Errorf("polar cell %q should still have E/W/S neighbours: %v", top, nb)
	}
}

func TestValidation(t *testing.T) {
	nan := math.NaN()
	for _, c := range []struct{ lat, lon float64 }{{91, 0}, {-91, 0}, {0, 181}, {0, -181}, {nan, 0}, {0, nan}} {
		if _, err := Encode(c.lat, c.lon, 5); err == nil {
			t.Errorf("Encode(%v, %v) accepted", c.lat, c.lon)
		}
	}
	for _, p := range []int{0, -1, 13} {
		if _, err := Encode(0, 0, p); err == nil {
			t.Errorf("Encode precision %d accepted", p)
		}
	}
	for _, h := range []string{"", "a", "u4pi", "ilo", "0123456789bcd"} {
		if _, err := Decode(h); err == nil {
			t.Errorf("Decode(%q) accepted", h)
		}
		if _, err := Neighbours(h); err == nil {
			t.Errorf("Neighbours(%q) accepted", h)
		}
	}
	for _, opt := range []Option{WithPrecision(0), WithPrecision(13), nil} {
		if _, err := New(opt); err == nil {
			t.Error("New accepted a bad option")
		}
	}
	ix, _ := New()
	if err := ix.Update("", 0, 0); err == nil {
		t.Error("empty id accepted")
	}
	if err := ix.Update("a", 100, 0); err == nil {
		t.Error("bad latitude accepted")
	}
	for _, q := range []struct {
		r     float64
		limit int
	}{{0, 0}, {-1, 0}, {nan, 0}, {math.Inf(1), 0}, {100, -1}} {
		if _, err := ix.Nearby(0, 0, q.r, q.limit); err == nil {
			t.Errorf("Nearby radius %v limit %d accepted", q.r, q.limit)
		}
		if _, _, err := ix.BruteForce(0, 0, q.r, q.limit); err == nil {
			t.Errorf("BruteForce radius %v limit %d accepted", q.r, q.limit)
		}
	}
	if _, err := ix.IDsInCell("tdr1y1234"); err == nil {
		t.Error("IDsInCell accepted a hash longer than the storage precision")
	}
}

func TestDistance(t *testing.T) {
	// One degree of latitude is ~111.2 km; 180 degrees along the equator is
	// half the circumference.
	if d := Distance(0, 0, 1, 0); math.Abs(d-111195) > 5 {
		t.Errorf("1 degree lat = %v m", d)
	}
	if d := Distance(0, 179.999, 0, -179.999); math.Abs(d-222.4) > 1 {
		t.Errorf("across antimeridian = %v m, want ~222", d)
	}
	if d := Distance(0, 0, 0, 180); math.Abs(d-math.Pi*EarthRadiusMeters) > 1 {
		t.Errorf("half circumference = %v", d)
	}
}

func equal(a, b []Result) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestNearbyMatchesBruteForce(t *testing.T) {
	type area struct {
		name                           string
		minLat, maxLat, minLon, maxLon float64
		radii                          []float64
	}
	areas := []area{
		{"city", 12.8, 13.2, 77.4, 77.8, []float64{50, 500, 2000, 8000}},
		{"high latitude", 69.5, 70.5, 18, 20, []float64{300, 3000}},
		{"antimeridian", -17, -16, 179.5, 180.5, []float64{1000, 20000}}, // lon wrapped below
		{"near pole", 89.8, 90, -180, 180, []float64{1000, 30000}},
		{"world", -90, 90, -180, 180, []float64{500e3, 3000e3}},
	}
	for i, a := range areas {
		t.Run(a.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(uint64(i), 99))
			ix, _ := New()
			pt := func() (float64, float64) {
				lat := a.minLat + rng.Float64()*(a.maxLat-a.minLat)
				lon := wrapLon(a.minLon + rng.Float64()*(a.maxLon-a.minLon))
				return lat, lon
			}
			for n := range 2000 {
				lat, lon := pt()
				if err := ix.Update(fmt.Sprint("d", n), lat, lon); err != nil {
					t.Fatal(err)
				}
			}
			total := 0
			for range 50 {
				lat, lon := pt()
				for _, r := range a.radii {
					got, st, err := ix.NearbyStats(lat, lon, r, 0)
					if err != nil {
						t.Fatal(err)
					}
					want, _, _ := ix.BruteForce(lat, lon, r, 0)
					if !equal(got, want) {
						t.Fatalf("(%v,%v) r=%v: index %d results, brute force %d (stats %+v)", lat, lon, r, len(got), len(want), st)
					}
					total += len(got)
					lim, _ := ix.Nearby(lat, lon, r, 5)
					if !equal(lim, want[:min(5, len(want))]) {
						t.Fatalf("limit 5 = %v, want %v", lim, want[:min(5, len(want))])
					}
				}
			}
			if total == 0 {
				t.Fatal("test data produced no matches at all")
			}
		})
	}
}

func TestNearbyExaminesFewerCandidates(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	ix, _ := New()
	for n := range 20000 {
		_ = ix.Update(fmt.Sprint(n), 12.8+rng.Float64()*0.4, 77.4+rng.Float64()*0.4)
	}
	_, st, _ := ix.NearbyStats(13.0, 77.6, 2000, 0)
	if st.FullScan || st.Precision != 5 || st.Candidates >= ix.Len()/2 {
		t.Fatalf("stats %+v: expected a precision-5 cell search well under N=%d", st, ix.Len())
	}
	_, st, _ = ix.NearbyStats(89.99, 0, 2000, 0)
	if !st.FullScan {
		t.Fatalf("a circle crossing the pole should fall back to a scan, got %+v", st)
	}
	if s := ix.Stats(); s.Queries != 2 || s.FullScans != 1 {
		t.Fatalf("stats %+v", s)
	}
}

func TestOwnCellOnlyMissesEdgeResults(t *testing.T) {
	ix, _ := New()
	// Two points 200 m apart on either side of a precision-5 cell boundary.
	cell, _ := Encode(13.0, 77.6, 5)
	b, _ := Decode(cell)
	edge := b.MaxLon
	_ = ix.Update("inside", 13.0, edge-0.0009)
	_ = ix.Update("across", 13.0, edge+0.0009)
	res, st, _ := ix.NearbyStats(13.0, edge-0.0009, 2000, 0)
	if len(res) != 2 || st.Precision != 5 {
		t.Fatalf("neighbour search found %v (stats %+v), want both", res, st)
	}
	own, _ := ix.IDsInCell(cell)
	if len(own) != 1 || own[0] != "inside" {
		t.Fatalf("own cell holds %v, want only [inside]: searching it alone misses a driver 200 m away", own)
	}
}

func TestUpdateMovesBetweenCells(t *testing.T) {
	ix, _ := New(WithPrecision(6))
	if err := ix.Update("car", 13.0, 77.6); err != nil {
		t.Fatal(err)
	}
	c1, _ := ix.Cell("car")
	_ = ix.Update("car", 13.0, 77.6000001) // tiny move, same cell
	if s := ix.Stats(); s.Updates != 2 || s.CellChanges != 0 {
		t.Fatalf("stats after in-cell move %+v", s)
	}
	_ = ix.Update("car", 13.05, 77.65)
	c2, _ := ix.Cell("car")
	if c1 == c2 || ix.Stats().CellChanges != 1 {
		t.Fatalf("expected a cell change, %q -> %q", c1, c2)
	}
	for p := 1; p <= 6; p++ {
		old, _ := ix.IDsInCell(c1[:p])
		cur, _ := ix.IDsInCell(c2[:p])
		if len(cur) != 1 {
			t.Fatalf("precision %d: new cell %q holds %v", p, c2[:p], cur)
		}
		if c1[:p] != c2[:p] && len(old) != 0 {
			t.Fatalf("precision %d: old cell %q still holds %v", p, c1[:p], old)
		}
	}
	if r, _ := ix.Nearby(13.0, 77.6, 500, 0); len(r) != 0 {
		t.Fatalf("found car at its old position: %v", r)
	}
	if r, _ := ix.Nearby(13.05, 77.65, 500, 0); len(r) != 1 {
		t.Fatalf("car not found at its new position: %v", r)
	}
	if lat, lon, ok := ix.Position("car"); !ok || lat != 13.05 || lon != 77.65 {
		t.Fatalf("Position = %v %v %v", lat, lon, ok)
	}

	if !ix.Remove("car") || ix.Remove("car") || ix.Len() != 0 {
		t.Fatal("Remove should succeed once")
	}
	for p := 1; p <= 6; p++ {
		if ids, _ := ix.IDsInCell(c2[:p]); len(ids) != 0 {
			t.Fatalf("removed id still in %q", c2[:p])
		}
	}
}

func TestConcurrentUpdatesAndQueries(t *testing.T) {
	ix, _ := New()
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w), 7))
			for i := range 2000 {
				id := fmt.Sprint(w, "-", i%100)
				lat, lon := 12.9+rng.Float64()*0.2, 77.5+rng.Float64()*0.2
				switch i % 10 {
				case 0:
					ix.Remove(id)
				case 1, 2:
					if _, err := ix.Nearby(lat, lon, 1500, 10); err != nil {
						t.Error(err)
						return
					}
				default:
					if err := ix.Update(id, lat, lon); err != nil {
						t.Error(err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	_ = ix.Stats()

	// After the dust settles, index and brute force must agree.
	got, _ := ix.Nearby(13.0, 77.6, 3000, 0)
	want, _, _ := ix.BruteForce(13.0, 77.6, 3000, 0)
	if !equal(got, want) {
		t.Fatalf("after concurrent churn: index %d, brute force %d", len(got), len(want))
	}
}
