// Package surgerain simulates the "surge pricing doesn't react to the rain"
// incident at a ride-hailing company. At 18:00 it starts raining downtown and
// ride requests there triple within three minutes. The pricing service
// computes one demand/supply ratio for the whole city from a 15-minute batch
// grouped by the time requests reached the server. Surge therefore lags the
// rain by a quarter of an hour, is averaged across the city (downtown is
// under-priced, the dry suburbs are over-priced), does nothing to pull
// drivers toward the rain, and is still applied after the rain has stopped.
// Requests from phones on a bad network in the rain arrive late and are
// counted in the minute they arrived, not the minute they happened.
//
// Broken is that design. Fixed keys demand by geohash cell (precision 5,
// a few kilometres across) and aggregates it in event time with 5-minute
// windows sliding every minute, a 30-second watermark delay and 2 minutes of
// allowed lateness (patterns/distributed/windowed-stream-aggregation).
// Supply is the number of drivers in each cell, read from a geohash index
// that every driver's position is written to every minute
// (patterns/data/geospatial-index); the same index answers "nearest idle
// driver" for matching. Each cell's surge is clamp(demand / supply).
//
// Both designs get the same seeded riders (positions, request times, network
// delays, drop-off points, price tolerance) and the same drivers and driver
// behaviour: only the pricing pipeline differs.
package surgerain

import (
	"cmp"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	geo "github.com/neerajsinghi/system-design-patterns-go/patterns/data/geospatial-index"
	windowedstream "github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/windowed-stream-aggregation"
)

const (
	// City layout: a Rows x Cols block of geohash cells.
	Precision = 5 // about 4.9 km x 4.9 km at the equator, 4.9 km x 3.7 km here
	Rows      = 4
	Cols      = 4
	Cells     = Rows * Cols

	// Simulated time: Minutes minutes starting at 17:30. The rain falls over
	// the rain cells in [RainFrom, RainTo).
	Minutes  = 90
	RainFrom = 30 // 18:00
	RainTo   = 60 // 18:30

	BaseRate    = 6.0 // ride requests per cell per minute when dry
	RainFactor  = 3   // demand multiplier in a rain cell while it rains
	RampMinutes = 3   // demand reaches RainFactor x within this many minutes

	DriversPerCell    = 48
	TripMinutes       = 6    // every trip takes this long, then the driver is free again in their cell
	RepositionMinutes = 3    // driving to a neighbouring cell
	MatchRadius       = 2500 // metres: furthest idle driver a rider is matched with
	MaxWait           = 10   // minutes a rider waits unmatched before giving up

	// Pricing.
	MaxSurge       = 3.0
	Window         = 5 * time.Minute
	Slide          = time.Minute
	WatermarkDelay = 30 * time.Second
	Lateness       = 2 * time.Minute
	BatchMinutes   = 15 // broken: one city-wide batch every 15 minutes

	// Driver behaviour (the same in both designs): an idle driver in an
	// unsurged cell drives to the neighbouring cell with the highest surge if
	// that is at least MoveGap higher, with probability MoveProb per minute.
	MoveGap  = 0.5
	MoveProb = 0.15

	// The invariant.
	SurgeTarget   = 1.5 // rain cells reach at least this ...
	SurgeWithin   = 7   // ... within this many minutes of 18:00,
	RecoverWithin = 8   // are back to 1.0x within this many minutes of 18:30,
	OverTolerance = 0.1 // and no dry cell is priced more than this above its true multiplier.
	DryCap        = 1.1 // reported only: dry cell-minutes above this

	seed1, seed2 = 2026, 1028
)

var (
	t0 = time.Date(2026, 10, 28, 17, 30, 0, 0, time.UTC)
	// baseLat/baseLon pick the south-west cell of the grid.
	baseLat, baseLon = 40.70, -74.02
)

// IsRainCell reports whether cell i (row-major) is under the shower: rows 1-2,
// columns 1-3, six of the sixteen cells.
func IsRainCell(i int) bool {
	r, c := i/Cols, i%Cols
	return r >= 1 && r <= 2 && c >= 1
}

// Clock formats a simulated minute as a wall-clock time.
func Clock(m int) string { return t0.Add(time.Duration(m) * time.Minute).Format("15:04") }

// Surge turns a demand count for one 5-minute window and a driver count into
// a multiplier: demand / capacity, where capacity is how many trips the
// drivers can start in the window, clamped to [1, MaxSurge].
func Surge(requests, drivers int) float64 {
	capacity := float64(drivers) * float64(Window/time.Minute) / TripMinutes
	if capacity <= 0 {
		return MaxSurge
	}
	return min(MaxSurge, max(1, float64(requests)/capacity))
}

// Report is what a run measured.
type Report struct {
	Name     string
	CellHash [Cells]string

	// Applied[c][m] is the surge in effect in cell c at the start of minute m.
	Applied [Cells][Minutes]float64
	// True[c][m] is the multiplier a perfect, instant pricing service would
	// show at the start of minute m: every request that happened in
	// [m-5, m), wherever and whenever it arrived, against the drivers in the
	// cell at that moment.
	True [Cells][Minutes]float64
	// Drivers[c][m] is the number of drivers in cell c at the end of minute
	// m, read from the geohash index.
	Drivers [Cells][Minutes]int

	Requests    int
	PricedOut   [2]int // requests declined because of the surge price: [dry, rain]
	Matched     int
	Abandoned   int          // riders who gave up after MaxWait minutes unmatched
	Waiting     [Minutes]int // riders still unmatched at the end of each minute
	DriverMoves int          // drivers who repositioned toward a higher surge

	// Event-time attribution.
	CountedWrongMinute int // broken: requests counted in their arrival minute, not their event minute
	LateFolded         uint64
	Dropped            uint64
	WindowsChecked     int // fixed: emitted windows whose final count was checked
	WindowsWrong       int // fixed: windows whose count differs from the event-time truth for the events that arrived in time

	// Cost.
	Emissions    uint64 // surge values computed
	Updates      uint64 // re-emissions caused by late events
	PeakOpen     int    // peak (cell, window) states held
	EventsStored int    // broken: requests buffered until the batch runs (peak)
}

// TimeToSurge returns, for a rain cell, how many minutes after 18:00 the
// applied surge first reached SurgeTarget, or -1 if it never did.
func (r *Report) TimeToSurge(c int) int { return firstAtLeast(r.Applied[c][:], SurgeTarget) }

// TrueTimeToSurge is TimeToSurge for the true multiplier.
func (r *Report) TrueTimeToSurge(c int) int { return firstAtLeast(r.True[c][:], SurgeTarget) }

func firstAtLeast(s []float64, x float64) int {
	for m := RainFrom; m < Minutes; m++ {
		if s[m] >= x {
			return m - RainFrom
		}
	}
	return -1
}

// Recovery returns how many minutes after 18:30 cell c's applied surge
// settled at 1.0x for the rest of the run, or -1 if it never did.
func (r *Report) Recovery(c int) int {
	last := RainTo - 1
	for m := RainTo; m < Minutes; m++ {
		if r.Applied[c][m] > 1 {
			last = m
		}
	}
	if last == Minutes-1 {
		return -1
	}
	return last + 1 - RainTo
}

// MeanError returns the mean |applied - true| over the cell-minutes of rain
// cells during the rain (plus the RecoverWithin minutes after it), and over
// every dry cell-minute.
func (r *Report) MeanError() (rain, dry float64) {
	var sr, sd float64
	var nr, nd int
	for c := range Cells {
		for m := range Minutes {
			e := math.Abs(r.Applied[c][m] - r.True[c][m])
			if IsRainCell(c) {
				if m >= RainFrom && m < RainTo+RecoverWithin {
					sr += e
					nr++
				}
			} else {
				sd += e
				nd++
			}
		}
	}
	return sr / float64(nr), sd / float64(nd)
}

// DryOverpricing describes dry cells: the highest surge applied, the number
// of cell-minutes above DryCap, the number of cell-minutes priced more than
// OverTolerance above the cell's own true multiplier, and the largest such
// excess.
func (r *Report) DryOverpricing() (peak float64, aboveCap, over int, maxExcess float64) {
	for c := range Cells {
		if IsRainCell(c) {
			continue
		}
		for m := range Minutes {
			a := r.Applied[c][m]
			peak = max(peak, a)
			maxExcess = max(maxExcess, a-r.True[c][m])
			if a > DryCap {
				aboveCap++
			}
			if a > r.True[c][m]+OverTolerance {
				over++
			}
		}
	}
	return peak, aboveCap, over, maxExcess
}

// WaitingRiderMinutes is the rider-wait proxy: the sum over minutes of
// riders still unmatched at the end of the minute.
func (r *Report) WaitingRiderMinutes() (total, peak int) {
	for _, w := range r.Waiting {
		total += w
		peak = max(peak, w)
	}
	return total, peak
}

func (r *Report) avg(m int, rain bool, s *[Cells][Minutes]float64) float64 {
	var sum float64
	n := 0
	for c := range Cells {
		if IsRainCell(c) == rain {
			sum += s[c][m]
			n++
		}
	}
	return sum / float64(n)
}

// Print writes a timeline and a summary.
func (r *Report) Print(w io.Writer) {
	fmt.Fprintf(w, "%s\n", r.Name)
	fmt.Fprintf(w, "%d geohash-%d cells, %d drivers each; rain over 6 cells %s-%s, demand there x%d\n\n",
		Cells, Precision, DriversPerCell, Clock(RainFrom), Clock(RainTo), RainFactor)
	fmt.Fprintln(w, "  time   rain cells: applied / true   dry cells: applied / true   riders waiting")
	for m := 20; m < Minutes; m += 5 {
		note := ""
		if m >= RainFrom && m < RainTo {
			note = "  raining"
		}
		ra, rt := r.avg(m, true, &r.Applied), r.avg(m, true, &r.True)
		fmt.Fprintf(w, "  %s   %4.2fx / %4.2fx %-10s   %4.2fx / %4.2fx           %4d%s\n", Clock(m),
			ra, rt, strings.Repeat("█", int((ra-1)*5+0.5)), r.avg(m, false, &r.Applied), r.avg(m, false, &r.True), r.Waiting[m], note)
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "  rain cell   reached 1.5x after (true)   back to 1.0x after rain")
	for c := range Cells {
		if !IsRainCell(c) {
			continue
		}
		fmt.Fprintf(w, "  %s       %-8s (%s)              %s\n", r.CellHash[c],
			minutesOrNever(r.TimeToSurge(c)), minutesOrNever(r.TrueTimeToSurge(c)), minutesOrNever(r.Recovery(c)))
	}
	er, ed := r.MeanError()
	peak, aboveCap, over, excess := r.DryOverpricing()
	total, peakWait := r.WaitingRiderMinutes()
	fmt.Fprintf(w, "\n  mean |applied - true| per cell-minute: rain cells %.2f, dry cells %.2f\n", er, ed)
	fmt.Fprintf(w, "  dry cells: %d cell-minutes over-priced (> true + %.1f; worst +%.2f); peak surge %.2fx, %d cell-minutes above %.1fx; %d riders priced out\n",
		over, OverTolerance, excess, peak, aboveCap, DryCap, r.PricedOut[0])
	fmt.Fprintf(w, "  riders: %d requests, %d matched, %d gave up after %d min, %d rain-cell riders priced out\n",
		r.Requests, r.Matched, r.Abandoned, MaxWait, r.PricedOut[1])
	fmt.Fprintf(w, "  rider wait proxy: %d rider-minutes unmatched, peak %d waiting; %d drivers repositioned\n",
		total, peakWait, r.DriverMoves)
	if r.WindowsChecked > 0 {
		fmt.Fprintf(w, "  late requests folded into their own windows: %d; dropped as too late: %d; windows with a wrong count: %d of %d\n",
			r.LateFolded, r.Dropped, r.WindowsWrong, r.WindowsChecked)
		fmt.Fprintf(w, "  cost: %d surge values computed (%d revisions from late data), peak %d window states held\n",
			r.Emissions, r.Updates, r.PeakOpen)
	} else {
		fmt.Fprintf(w, "  requests counted in the wrong minute (arrival, not event time): %d\n", r.CountedWrongMinute)
		fmt.Fprintf(w, "  cost: %d surge values computed, up to %d requests buffered per batch\n", r.Emissions, r.EventsStored)
	}
}

func minutesOrNever(m int) string {
	if m < 0 {
		return "never"
	}
	return fmt.Sprintf("%d min", m)
}

// ---------------------------------------------------------------- traffic

type request struct {
	id         int
	cell       int
	lat, lon   float64
	at, arrive time.Time
	// drop is where the driver ends up after the trip, as fractions of the
	// driver's own cell (see match).
	drop      [2]float64
	tolerance float64 // the rider accepts a surge s if tolerance < 1/s
}

type city struct {
	hash  [Cells]string
	box   [Cells]geo.Box
	index map[string]int
}

func newCity() *city {
	b, err := geo.Encode(baseLat, baseLon, Precision)
	if err != nil {
		panic(err)
	}
	box, _ := geo.Decode(b)
	h, w := box.MaxLat-box.MinLat, box.MaxLon-box.MinLon
	ct := &city{index: make(map[string]int)}
	for i := range Cells {
		lat := box.MinLat + (float64(i/Cols)+0.5)*h
		lon := box.MinLon + (float64(i%Cols)+0.5)*w
		hash, err := geo.Encode(lat, lon, Precision)
		if err != nil {
			panic(err)
		}
		ct.hash[i] = hash
		ct.box[i], _ = geo.Decode(hash)
		ct.index[hash] = i
	}
	return ct
}

// point returns a uniformly random point inside cell c, away from its edges
// so it encodes back to c.
func (ct *city) point(rng *rand.Rand, c int) (lat, lon float64) {
	b := ct.box[c]
	return b.MinLat + (0.02+0.96*rng.Float64())*(b.MaxLat-b.MinLat),
		b.MinLon + (0.02+0.96*rng.Float64())*(b.MaxLon-b.MinLon)
}

func rate(c, m int) float64 {
	if !IsRainCell(c) {
		return BaseRate
	}
	f := 1.0
	switch {
	case m >= RainFrom && m < RainTo:
		f = min(RainFactor, 1+(RainFactor-1)*float64(m-RainFrom+1)/RampMinutes)
	case m >= RainTo && m < RainTo+RampMinutes:
		f = RainFactor - (RainFactor-1)*float64(m-RainTo+1)/RampMinutes
	}
	return BaseRate * f
}

// traffic generates the riders, sorted by arrival time. Each minute a cell
// gets its rate's worth of requests, give or take one, at random seconds.
// Phones in a rain cell while it rains are slow: 30% of their requests
// arrive 20-150 s late and 2% 9-15 minutes late.
func traffic(ct *city) []request {
	rng := rand.New(rand.NewPCG(seed1, seed2))
	var out []request
	carry := [Cells]float64{}
	for m := range Minutes {
		for c := range Cells {
			carry[c] += rate(c, m)
			n := int(carry[c]) + rng.IntN(3) - 1
			n = max(0, n)
			carry[c] -= float64(int(carry[c]))
			for range n {
				at := t0.Add(time.Duration(m)*time.Minute + time.Duration(rng.Float64()*float64(time.Minute)))
				delay := time.Duration((1 + 4*rng.Float64()) * float64(time.Second))
				if IsRainCell(c) && m >= RainFrom && m < RainTo {
					switch p := rng.Float64(); {
					case p < 0.02:
						delay = time.Duration((9 + 6*rng.Float64()) * float64(time.Minute))
					case p < 0.32:
						delay = time.Duration((20 + 130*rng.Float64()) * float64(time.Second))
					}
				}
				lat, lon := ct.point(rng, c)
				drop := [2]float64{0.02 + 0.96*rng.Float64(), 0.02 + 0.96*rng.Float64()}
				out = append(out, request{
					id: len(out), cell: c, lat: lat, lon: lon, at: at, arrive: at.Add(delay),
					drop: drop, tolerance: rng.Float64(),
				})
			}
		}
	}
	slices.SortStableFunc(out, func(a, b request) int { return a.arrive.Compare(b.arrive) })
	return out
}

func minuteOf(t time.Time) int { return int(t.Sub(t0) / time.Minute) }

// ---------------------------------------------------------------- world

type driver struct {
	id        string
	cell      int
	lat, lon  float64
	busyUntil int // minute the driver is free again
}

type waiting struct {
	req   request
	since int
}

// pricer is the part that differs between the designs.
type pricer interface {
	// observe sees a request as it reaches the server.
	observe(req request)
	// endMinute runs after minute m's matching and driver snapshot.
	endMinute(m int)
}

type world struct {
	ct      *city
	r       *Report
	ix      *geo.Index // every driver: supply per cell
	idle    *geo.Index // drivers free right now: matching
	drivers []*driver
	byID    map[string]*driver
	surge   [Cells]float64 // published surge per cell
	queue   []waiting
	rng     *rand.Rand // driver movement
}

func newWorld(name string) *world {
	ct := newCity()
	ix, err := geo.New(geo.WithPrecision(6))
	if err != nil {
		panic(err)
	}
	idle, err := geo.New(geo.WithPrecision(6))
	if err != nil {
		panic(err)
	}
	w := &world{ct: ct, r: &Report{Name: name, CellHash: ct.hash}, ix: ix, idle: idle,
		byID: make(map[string]*driver), rng: rand.New(rand.NewPCG(seed2, seed1))}
	for c := range Cells {
		w.surge[c] = 1
		for k := range DriversPerCell {
			d := &driver{id: fmt.Sprintf("d%02d-%02d", c, k), cell: c}
			d.lat, d.lon = ct.point(w.rng, c)
			w.drivers = append(w.drivers, d)
			w.byID[d.id] = d
		}
	}
	return w
}

// place records a driver's position in minute m, in the all-drivers index
// and, if the driver is free, in the idle index.
func (w *world) place(d *driver, c int, lat, lon float64, m int) {
	d.cell, d.lat, d.lon = c, lat, lon
	if err := w.ix.Update(d.id, lat, lon); err != nil {
		panic(err)
	}
	if d.busyUntil > m {
		w.idle.Remove(d.id)
	} else if err := w.idle.Update(d.id, lat, lon); err != nil {
		panic(err)
	}
}

func (w *world) run(p pricer, reqs []request) {
	next := 0
	for m := range Minutes {
		for c := range Cells {
			w.r.Applied[c][m] = w.surge[c]
		}
		// Every driver reports a position every tick: idle drivers drift
		// within their cell.
		for _, d := range w.drivers {
			lat, lon := d.lat, d.lon
			if d.busyUntil <= m {
				b := w.ct.box[d.cell]
				lat = min(b.MaxLat-1e-6, max(b.MinLat+1e-6, lat+(w.rng.Float64()-0.5)*0.002))
				lon = min(b.MaxLon-1e-6, max(b.MinLon+1e-6, lon+(w.rng.Float64()-0.5)*0.002))
			}
			w.place(d, d.cell, lat, lon, m)
		}
		// Requests that reach the server this minute.
		end := t0.Add(time.Duration(m+1) * time.Minute)
		for ; next < len(reqs) && reqs[next].arrive.Before(end); next++ {
			req := reqs[next]
			w.r.Requests++
			p.observe(req)
			if req.tolerance >= 1/w.surge[req.cell] {
				w.r.PricedOut[b2i(IsRainCell(req.cell))]++
				continue
			}
			w.queue = append(w.queue, waiting{req: req, since: m})
		}
		w.match(m)
		w.r.Waiting[m] = len(w.queue)
		for c := range Cells {
			ids, err := w.ix.IDsInCell(w.ct.hash[c])
			if err != nil {
				panic(err)
			}
			w.r.Drivers[c][m] = len(ids)
		}
		p.endMinute(m)
		w.reposition(m)
	}
	w.r.True = truth(reqs, &w.r.Drivers)
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

// match gives each waiting rider, oldest first, the nearest idle driver
// within MatchRadius, found with the geohash index.
func (w *world) match(m int) {
	kept := w.queue[:0]
	for _, q := range w.queue {
		if m-q.since >= MaxWait {
			w.r.Abandoned++
			continue
		}
		near, err := w.idle.Nearby(q.req.lat, q.req.lon, MatchRadius, 1)
		if err != nil {
			panic(err)
		}
		if len(near) == 0 {
			kept = append(kept, q)
			continue
		}
		found := w.byID[near[0].ID]
		w.r.Matched++
		found.busyUntil = m + TripMinutes
		// Simplification: each driver works one cell. Trips end back in the
		// driver's cell (one who crossed the border for a pickup drives
		// back), so only repositioning moves supply between cells. The
		// driver is placed at the end point straight away and is busy until
		// the trip ends.
		b := w.ct.box[found.cell]
		w.place(found, found.cell, b.MinLat+q.req.drop[0]*(b.MaxLat-b.MinLat), b.MinLon+q.req.drop[1]*(b.MaxLon-b.MinLon), m)
	}
	w.queue = kept
}

// reposition moves idle drivers in unsurged cells toward a clearly higher
// surge next door. The rule is the same for both designs.
func (w *world) reposition(m int) {
	for _, d := range w.drivers {
		if d.busyUntil > m+1 || w.surge[d.cell] > 1 {
			continue
		}
		nbs, err := geo.Neighbours(w.ct.hash[d.cell])
		if err != nil {
			panic(err)
		}
		best, bestSurge := -1, w.surge[d.cell]+MoveGap
		for _, h := range nbs {
			if c, ok := w.ct.index[h]; ok && w.surge[c] >= bestSurge {
				best, bestSurge = c, w.surge[c]
			}
		}
		if best < 0 || w.rng.Float64() >= MoveProb {
			continue
		}
		lat, lon := w.ct.point(w.rng, best)
		d.busyUntil = m + 1 + RepositionMinutes
		w.place(d, best, lat, lon, m)
		w.r.DriverMoves++
	}
}

// truth computes the multiplier an instant, perfectly informed pricer would
// show at the start of each minute.
func truth(reqs []request, drivers *[Cells][Minutes]int) [Cells][Minutes]float64 {
	var perMinute [Cells][Minutes]int
	for _, q := range reqs {
		perMinute[q.cell][minuteOf(q.at)]++
	}
	var out [Cells][Minutes]float64
	for c := range Cells {
		for m := range Minutes {
			if m == 0 {
				out[c][m] = 1
				continue
			}
			n := 0
			for k := max(0, m-5); k < m; k++ {
				n += perMinute[c][k]
			}
			out[c][m] = Surge(n, drivers[c][m-1])
		}
	}
	return out
}

// ---------------------------------------------------------------- broken

type batchPricer struct {
	w        *world
	buffered []request
}

func (b *batchPricer) observe(req request) {
	b.buffered = append(b.buffered, req)
	if minuteOf(req.arrive) != minuteOf(req.at) {
		b.w.r.CountedWrongMinute++
	}
}

// endMinute: every BatchMinutes, divide the city's requests (grouped by
// arrival) by the city's drivers and apply the result everywhere for the
// next BatchMinutes.
func (b *batchPricer) endMinute(m int) {
	if (m+1)%BatchMinutes != 0 {
		return
	}
	b.w.r.EventsStored = max(b.w.r.EventsStored, len(b.buffered))
	drivers := 0
	for c := range Cells {
		drivers += b.w.r.Drivers[c][m]
	}
	perWindow := len(b.buffered) * int(Window/time.Minute) / BatchMinutes
	s := Surge(perWindow, drivers)
	for c := range Cells {
		b.w.surge[c] = s
	}
	b.w.r.Emissions++
	b.buffered = b.buffered[:0]
}

// Broken prices the whole city from one 15-minute batch of requests grouped
// by arrival time.
func Broken() Report {
	w := newWorld("BROKEN: one city-wide surge from a 15-minute processing-time batch")
	w.run(&batchPricer{w: w}, traffic(w.ct))
	return *w.r
}

// ---------------------------------------------------------------- fixed

type streamPricer struct {
	w       *world
	agg     *windowedstream.Aggregator[int, struct{}, int]
	lastEnd [Cells]int // newest window end (as a minute) published per cell
	// final[c][e] is the latest emitted count of the window ending at
	// minute e; inTime[c][e] counts the events that reached it in time.
	final, inTime map[[2]int]int
}

func countFold(n int, _ struct{}) int { return n + 1 }

func (s *streamPricer) endOf(win windowedstream.Window) int { return minuteOf(win.End) }

func (s *streamPricer) publish(results []windowedstream.Result[int, int]) {
	for _, res := range results {
		e := s.endOf(res.Window)
		s.final[[2]int{res.Key, e}] = res.Events
		if e < s.lastEnd[res.Key] {
			continue // a late revision of a window a newer one has replaced
		}
		s.lastEnd[res.Key] = e
		// The window covers [e-5, e); supply is the snapshot taken at e.
		s.w.surge[res.Key] = Surge(res.Value, s.w.r.Drivers[res.Key][e-1])
	}
}

func (s *streamPricer) observe(req request) {
	wm, ok := s.agg.Watermark()
	for _, win := range windowedstream.Sliding(Window, Slide).Assign(req.at) {
		if !ok || win.End.Add(Lateness).After(wm) {
			s.inTime[[2]int{req.cell, s.endOf(win)}]++
		}
	}
	s.publish(s.agg.Add(windowedstream.Event[int, struct{}]{Key: req.cell, Time: req.at}))
	s.w.r.PeakOpen = max(s.w.r.PeakOpen, s.agg.Stats().OpenWindows)
}

// endMinute advances the watermark as a heartbeat (so a quiet cell still
// closes its windows) and resets cells that had no demand in the newest
// closed window.
func (s *streamPricer) endMinute(m int) {
	now := t0.Add(time.Duration(m+1) * time.Minute)
	s.publish(s.agg.AdvanceWatermark(now.Add(-WatermarkDelay)))
	wm, _ := s.agg.Watermark()
	closed := minuteOf(wm)
	for c := range Cells {
		if s.lastEnd[c] < closed {
			s.lastEnd[c] = closed
			s.w.surge[c] = 1
		}
	}
}

// Fixed prices each geohash cell from event-time sliding windows, with
// supply read from the driver index.
func Fixed() Report {
	w := newWorld("FIXED: per-cell event-time sliding windows (5 min, 1 min slide) + driver geo-index")
	agg, err := windowedstream.New[int, struct{}, int](windowedstream.Sliding(Window, Slide), countFold,
		windowedstream.WithWatermarkDelay(WatermarkDelay), windowedstream.WithAllowedLateness(Lateness))
	if err != nil {
		panic(err)
	}
	s := &streamPricer{w: w, agg: agg, final: map[[2]int]int{}, inTime: map[[2]int]int{}}
	w.run(s, traffic(w.ct))

	st := agg.Stats()
	w.r.LateFolded, w.r.Dropped = st.Late, st.Dropped
	w.r.Emissions, w.r.Updates = st.Emitted+st.Updates, st.Updates
	keys := make([][2]int, 0, len(s.final))
	for k := range s.final {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b [2]int) int { return cmp.Or(cmp.Compare(a[0], b[0]), cmp.Compare(a[1], b[1])) })
	for _, k := range keys {
		w.r.WindowsChecked++
		if s.final[k] != s.inTime[k] {
			w.r.WindowsWrong++
		}
	}
	return *w.r
}
