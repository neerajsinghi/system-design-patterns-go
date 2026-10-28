// Command example indexes 100,000 simulated drivers spread over a city-sized
// box and answers "drivers within 2 km of me" two ways: a brute-force scan
// and the geohash index. It compares the candidates each one examines,
// checks the answers are identical, shows how many drivers you'd miss by
// searching only the query's own cell, moves drivers across cells with live
// updates, and finds a driver on the other side of the antimeridian.
package main

import (
	"fmt"
	"log"
	"math/rand/v2"

	geospatialindex "github.com/neerajsinghi/system-design-patterns-go/patterns/data/geospatial-index"
)

const (
	drivers = 100_000
	queries = 500
	checked = 10     // queries also answered by brute force (it is the slow part)
	radius  = 2000.0 // metres

	// A box roughly the size of Bengaluru: 0.5 x 0.5 degrees, ~55 x 54 km.
	minLat, maxLat = 12.75, 13.25
	minLon, maxLon = 77.35, 77.85
)

func equal(a, b []geospatialindex.Result) bool {
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

func main() {
	rng := rand.New(rand.NewPCG(42, 2026))
	point := func() (float64, float64) {
		return minLat + rng.Float64()*(maxLat-minLat), minLon + rng.Float64()*(maxLon-minLon)
	}

	ix, err := geospatialindex.New(geospatialindex.WithPrecision(7))
	if err != nil {
		log.Fatal(err)
	}
	ids := make([]string, drivers)
	for i := range drivers {
		ids[i] = fmt.Sprintf("driver-%06d", i)
		lat, lon := point()
		if err := ix.Update(ids[i], lat, lon); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Printf("1. Indexed %d drivers in a %.1f x %.1f degree box (storage precision %d).\n\n",
		ix.Len(), maxLat-minLat, maxLon-minLon, ix.Precision())

	type q struct{ lat, lon float64 }
	qs := make([]q, queries)
	for i := range qs {
		qs[i].lat, qs[i].lon = point()
	}

	fmt.Printf("2. %d queries: drivers within %.0f m, index vs brute force.\n", queries, radius)
	var ixCand, cells, results, prec int
	answers := make([][]geospatialindex.Result, queries)
	precisions := make([]int, queries)
	for i, p := range qs {
		got, st, _ := ix.NearbyStats(p.lat, p.lon, radius, 0)
		answers[i], precisions[i] = got, st.Precision
		ixCand += st.Candidates
		cells += st.CellsSearched
		results += len(got)
		prec = st.Precision
	}
	bruteCand, mismatches := 0, 0
	for i, p := range qs[:checked] {
		want, bst, _ := ix.BruteForce(p.lat, p.lon, radius, 0)
		bruteCand += bst.Candidates
		if !equal(answers[i], want) {
			mismatches++
		}
	}
	perBrute := float64(bruteCand) / checked
	perIx := float64(ixCand) / queries
	fmt.Printf("   brute force: %8.0f candidates examined per query\n", perBrute)
	fmt.Printf("   index:       %8.0f candidates examined per query (precision-%d cells, %.1f cells searched on average)\n",
		perIx, prec, float64(cells)/queries)
	fmt.Printf("   results per query: %.0f on average\n", float64(results)/queries)
	fmt.Printf("   first %d queries re-run by brute force: %d answers differ\n", checked, mismatches)
	fmt.Printf("   the index computes %.0fx fewer distances.\n\n", perBrute/perIx)

	fmt.Println("3. The edge-of-cell bug: search only the query's own cell, skip the neighbours.")
	missed, total, affected := 0, 0, 0
	for i, p := range qs {
		want := answers[i]
		own, _ := geospatialindex.Encode(p.lat, p.lon, precisions[i])
		inOwn := map[string]bool{}
		ids, _ := ix.IDsInCell(own)
		for _, id := range ids {
			inOwn[id] = true
		}
		m := 0
		for _, r := range want {
			if !inOwn[r.ID] {
				m++
			}
		}
		missed += m
		total += len(want)
		if m > 0 {
			affected++
		}
	}
	fmt.Printf("   own cell only would miss %d of %d results (%.1f%%), in %d of %d queries:\n",
		missed, total, 100*float64(missed)/float64(total), affected, queries)
	fmt.Println("   any query closer than 2 km to a cell edge has matches in the cell next door.")
	fmt.Println()

	fmt.Println("4. Live updates: 2 ticks, every driver moves up to ~30 m north/south and east/west")
	fmt.Println("   (about 3 s of driving at 36 km/h).")
	for tick := 1; tick <= 2; tick++ {
		before := ix.Stats()
		for _, id := range ids {
			lat, lon, _ := ix.Position(id)
			lat += (rng.Float64()*2 - 1) * 0.00027 // ~30 m
			lon += (rng.Float64()*2 - 1) * 0.00027
			lat = min(max(lat, minLat), maxLat)
			lon = min(max(lon, minLon), maxLon)
			if err := ix.Update(id, lat, lon); err != nil {
				log.Fatal(err)
			}
		}
		after := ix.Stats()
		fmt.Printf("   tick %d: %d updates, %d moved to a different precision-7 cell (%.1f%%)\n",
			tick, after.Updates-before.Updates, after.CellChanges-before.CellChanges,
			100*float64(after.CellChanges-before.CellChanges)/float64(after.Updates-before.Updates))
	}
	mismatches = 0
	for _, p := range qs[checked : 2*checked] {
		want, _, _ := ix.BruteForce(p.lat, p.lon, radius, 0)
		got, _ := ix.Nearby(p.lat, p.lon, radius, 0)
		if !equal(got, want) {
			mismatches++
		}
	}
	fmt.Printf("   after the moves, %d queries checked against brute force: %d answers differ.\n\n", checked, mismatches)

	fmt.Println("5. Antimeridian: a rider at lon 179.999 and a driver at lon -179.999 (Fiji).")
	fiji, _ := geospatialindex.New()
	_ = fiji.Update("driver-east-of-dateline", -17.0, -179.999)
	res, st, _ := fiji.NearbyStats(-17.0, 179.999, 1000, 0)
	own, _ := geospatialindex.Encode(-17.0, 179.999, st.Precision)
	other, _ := fiji.Cell("driver-east-of-dateline")
	fmt.Printf("   rider cell %q, driver cell %q: different cells on opposite edges of the map.\n", own, other[:st.Precision])
	for _, r := range res {
		fmt.Printf("   found %s at %.0f m via the wrapped east neighbour.\n", r.ID, r.Meters)
	}
}
