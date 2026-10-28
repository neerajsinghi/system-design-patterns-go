// Command example simulates a surge-pricing pipeline during a rain shower.
// Ride requests and driver-availability pings from three geocells arrive late
// and out of order (phones on bad networks). It compares processing-time
// windows with event-time windows plus a watermark and allowed lateness, then
// compares how quickly a 5-minute tumbling and a 5-minute sliding window
// react to the shower.
package main

import (
	"cmp"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"slices"
	"time"

	windowedstream "github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/windowed-stream-aggregation"
)

const (
	minutes         = 20
	rainFrom        = 8 // rain falls over the wet cells in minutes [8, 13)
	rainTo          = 13
	watermarkDelay  = 10 * time.Second
	allowedLateness = 2 * time.Minute
	minSurge        = 1.0
	maxSurge        = 3.0
)

var start = time.Date(2026, 10, 28, 18, 0, 0, 0, time.UTC)

type cell struct {
	name              string
	requests, drivers int // per minute in dry weather
	wet               bool
}

// The keys stand in for geocell ids (an H3 or geohash string in production).
var cells = []cell{
	{"downtown", 9, 14, true},
	{"midtown", 6, 10, true},
	{"suburbs", 4, 8, false},
}

// Demand multiplier on requests for each rain minute, 8..12.
var rainBoost = []float64{2.5, 4, 4.5, 3.5, 2}

// Ping is one observation: a ride request or an "I'm available" driver ping.
type Ping struct{ Request bool }

// Demand is the per-window aggregate.
type Demand struct{ Requests, Drivers int }

func fold(d Demand, p Ping) Demand {
	if p.Request {
		d.Requests++
	} else {
		d.Drivers++
	}
	return d
}

func surge(d Demand) float64 {
	if d.Requests == 0 {
		return minSurge
	}
	if d.Drivers == 0 {
		return maxSurge
	}
	x := math.Max(minSurge, math.Min(maxSurge, float64(d.Requests)/float64(d.Drivers)))
	return math.Round(x*10) / 10
}

type arrival struct {
	ev     windowedstream.Event[string, Ping]
	at     time.Time // when the pipeline receives it
	minute int       // true event-time minute
}

// generate builds the stream with explicit event times and a seeded delay per
// event, and returns it sorted by arrival time, plus the true per-minute
// demand.
func generate(rng *rand.Rand) ([]arrival, map[string][]Demand) {
	truth := map[string][]Demand{}
	var out []arrival
	for _, c := range cells {
		truth[c.name] = make([]Demand, minutes)
		for m := range minutes {
			raining := c.wet && m >= rainFrom && m < rainTo
			req, drv := c.requests, c.drivers
			if raining {
				req = int(math.Round(float64(req) * rainBoost[m-rainFrom]))
				drv = drv * 85 / 100 // some drivers head home
			}
			req += rng.IntN(3) - 1
			drv += rng.IntN(3) - 1
			for i := range req + drv {
				p := Ping{Request: i < req}
				t := start.Add(time.Duration(m)*time.Minute + time.Duration(rng.IntN(60000))*time.Millisecond)
				out = append(out, arrival{
					ev:     windowedstream.Event[string, Ping]{Key: c.name, Time: t, Value: p},
					at:     t.Add(delay(rng, raining)),
					minute: m,
				})
				truth[c.name][m] = fold(truth[c.name][m], p)
			}
		}
	}
	slices.SortStableFunc(out, func(a, b arrival) int { return a.at.Compare(b.at) })
	return out, truth
}

// delay models the network: normally a few seconds, but rain-soaked phones
// often buffer events for a minute or more, and a few for several minutes.
func delay(rng *rand.Rand, raining bool) time.Duration {
	ms := func(lo, hi int) time.Duration { return time.Duration(lo+rng.IntN(hi-lo)) * time.Millisecond }
	r := rng.Float64()
	switch {
	case raining && r < 0.05:
		return ms(150_000, 420_000)
	case raining && r < 0.40:
		return ms(15_000, 100_000)
	case !raining && r < 0.01:
		return ms(15_000, 60_000)
	default:
		return ms(200, 4_000)
	}
}

type perMinute struct {
	first, final Demand
	firstAt      time.Time
	revisions    int
}

func minuteOf(t time.Time) int { return int(t.Sub(start) / time.Minute) }

func clock(t time.Time) string { return t.Format("15:04:05") }

func main() {
	rng := rand.New(rand.NewPCG(7, 2026))
	stream, truth := generate(rng)

	late10, late60, late130 := 0, 0, 0
	for _, a := range stream {
		d := a.at.Sub(a.ev.Time)
		if d > watermarkDelay {
			late10++
		}
		if d > time.Minute {
			late60++
		}
		if d > watermarkDelay+allowedLateness {
			late130++
		}
	}
	fmt.Printf("1) The stream: %d events (requests + driver pings) from %d cells over %d minutes, starting %s.\n",
		len(stream), len(cells), minutes, start.Format("15:04"))
	fmt.Printf("   Rain over downtown and midtown from %s to %s. Arrival order is shuffled by seeded network delay:\n",
		start.Add(rainFrom*time.Minute).Format("15:04"), start.Add(rainTo*time.Minute).Format("15:04"))
	fmt.Printf("   %d events arrive more than %v after they happened, %d more than 1m, %d more than %v.\n\n",
		late10, watermarkDelay, late60, late130, watermarkDelay+allowedLateness)

	procRes := runProcessingTime(stream)
	evtRes, st := runEventTime(stream, watermarkDelay)
	slowRes, _ := runEventTime(stream, 6*watermarkDelay)

	fmt.Println("2) Downtown, per minute: requests/drivers -> surge")
	fmt.Println("   minute | truth          | processing time | event time, first emit @ wm  | event time, final")
	for m := 5; m < 16; m++ {
		tr, pr, e := truth["downtown"][m], procRes["downtown"][m], evtRes["downtown"][m]
		mark := ""
		if m >= rainFrom && m < rainTo {
			mark = "  rain"
		}
		fmt.Printf("   %s  | %3d/%-3d %.1fx   | %3d/%-3d %.1fx     | %3d/%-3d %.1fx at %s  | %3d/%-3d %.1fx (%d upd)%s\n",
			start.Add(time.Duration(m)*time.Minute).Format("15:04"),
			tr.Requests, tr.Drivers, surge(tr),
			pr.Requests, pr.Drivers, surge(pr),
			e.first.Requests, e.first.Drivers, surge(e.first), clock(e.firstAt),
			e.final.Requests, e.final.Drivers, surge(e.final), e.revisions, mark)
	}

	fmt.Println("   Processing time files delayed rain requests and driver pings under the minute they arrived in, so")
	fmt.Println("   rain minutes and the minutes after them get the wrong counts, and nothing can correct them later.")
	fmt.Println("   Event time's first emission (when the watermark passes the minute) is an early answer from what has")
	fmt.Println("   arrived so far; each late event re-emits the window, converging on the truth minus dropped events.")

	type score struct {
		wrong, total    int
		absErr, rainErr float64
		rainTotal       int
	}
	scoreOf := func(get func(c string, m int) Demand) score {
		var s score
		for _, c := range cells {
			for m := range minutes {
				got, want := surge(get(c.name, m)), surge(truth[c.name][m])
				e := math.Abs(got - want)
				s.total++
				s.absErr += e
				if e >= 0.2-1e-9 {
					s.wrong++
				}
				if c.wet && m >= rainFrom && m < rainTo {
					s.rainTotal++
					s.rainErr += e
				}
			}
		}
		return s
	}
	fmt.Println()
	fmt.Println("3) All cells, all minutes: how often is the surge for a minute off by 0.2x or more?")
	fmt.Println("   approach                                  | wrong cell-minutes | mean |error| | in rain minutes")
	for _, row := range []struct {
		name string
		s    score
	}{
		{"processing-time windows", scoreOf(func(c string, m int) Demand { return procRes[c][m] })},
		{"event time, first emission (10s delay)", scoreOf(func(c string, m int) Demand { return evtRes[c][m].first })},
		{"event time, first emission (60s delay)", scoreOf(func(c string, m int) Demand { return slowRes[c][m].first })},
		{"event time, after late updates", scoreOf(func(c string, m int) Demand { return evtRes[c][m].final })},
	} {
		fmt.Printf("   %-41s | %6d of %-3d      | %.2fx        | %.2fx\n", row.name, row.s.wrong, row.s.total,
			row.s.absErr/float64(row.s.total), row.s.rainErr/float64(row.s.rainTotal))
	}
	fmt.Printf("   event time (10s delay): %d late events folded in as updates (%d re-emissions), %d dropped beyond %v lateness, %d windows open after the flush\n",
		st.Late, st.Updates, st.Dropped, allowedLateness, st.OpenWindows)

	fmt.Println()
	fmt.Println("   A longer watermark delay makes first emissions more complete, but every window closes 50s later.")
	fmt.Println("   Allowed lateness repairs answers without delaying them, at the cost of re-emissions and state.")
	fmt.Println()

	responsiveness(stream)
}

// runProcessingTime windows each event by its arrival time. Arrival
// timestamps only move forward, so it never sees "late" data: a delayed event
// silently lands in whatever minute it arrived in.
func runProcessingTime(stream []arrival) map[string][]Demand {
	proc, err := windowedstream.New[string](windowedstream.Tumbling(time.Minute), fold)
	if err != nil {
		log.Fatal(err)
	}
	res := map[string][]Demand{}
	for _, c := range cells {
		res[c.name] = make([]Demand, minutes+10) // late arrivals spill past minute 20
	}
	record := func(rs []windowedstream.Result[string, Demand]) {
		for _, r := range rs {
			res[r.Key][minuteOf(r.Window.Start)] = r.Value
		}
	}
	for _, a := range stream {
		record(proc.Add(windowedstream.Event[string, Ping]{Key: a.ev.Key, Time: a.at, Value: a.ev.Value}))
	}
	record(proc.AdvanceWatermark(start.Add(24 * time.Hour)))
	return res
}

// runEventTime windows each event by when it happened, with a watermark that
// trails the newest event time by delay and the allowed lateness.
func runEventTime(stream []arrival, delay time.Duration) (map[string][]perMinute, windowedstream.Stats) {
	evt, err := windowedstream.New[string](windowedstream.Tumbling(time.Minute), fold,
		windowedstream.WithWatermarkDelay(delay), windowedstream.WithAllowedLateness(allowedLateness))
	if err != nil {
		log.Fatal(err)
	}
	res := map[string][]perMinute{}
	for _, c := range cells {
		res[c.name] = make([]perMinute, minutes)
	}
	record := func(rs []windowedstream.Result[string, Demand]) {
		for _, r := range rs {
			pm := &res[r.Key][minuteOf(r.Window.Start)]
			if !r.IsUpdate() {
				pm.first, pm.firstAt = r.Value, r.Watermark
			}
			pm.final, pm.revisions = r.Value, r.Revision
		}
	}
	for _, a := range stream {
		record(evt.Add(a.ev))
	}
	record(evt.AdvanceWatermark(start.Add(24 * time.Hour))) // end of the bounded demo stream
	return res, evt.Stats()
}

// responsiveness feeds the same downtown events into a 5-minute tumbling and
// a 5-minute sliding (1-minute slide) event-time aggregator and prints what
// each emits, and when.
func responsiveness(stream []arrival) {
	opts := []windowedstream.Option{
		windowedstream.WithWatermarkDelay(watermarkDelay), windowedstream.WithAllowedLateness(allowedLateness),
	}
	tumble, err := windowedstream.New[string](windowedstream.Tumbling(5*time.Minute), fold, opts...)
	if err != nil {
		log.Fatal(err)
	}
	slide, err := windowedstream.New[string](windowedstream.Sliding(5*time.Minute, time.Minute), fold, opts...)
	if err != nil {
		log.Fatal(err)
	}
	type emission struct {
		window windowedstream.Window
		at     time.Time
		d      Demand
	}
	var tum, sli []emission
	keep := func(dst *[]emission, rs []windowedstream.Result[string, Demand]) {
		for _, r := range rs {
			if !r.IsUpdate() {
				*dst = append(*dst, emission{r.Window, r.Watermark, r.Value})
			}
		}
	}
	maxTum, maxSli := 0, 0
	for _, a := range stream {
		if a.ev.Key != "downtown" {
			continue
		}
		keep(&tum, tumble.Add(a.ev))
		keep(&sli, slide.Add(a.ev))
		maxTum = max(maxTum, tumble.Stats().OpenWindows)
		maxSli = max(maxSli, slide.Stats().OpenWindows)
	}
	slices.SortFunc(sli, func(a, b emission) int { return cmp.Compare(a.window.End.Unix(), b.window.End.Unix()) })

	const threshold = 1.5
	fmt.Printf("4) Downtown, 5-minute windows (first emissions). Rain starts at %s.\n", start.Add(rainFrom*time.Minute).Format("15:04"))
	fmt.Println("   watermark  | sliding 5m/1m window -> surge | tumbling 5m window -> surge")
	firstS, firstT := "", ""
	peakS, peakT := 0.0, 0.0
	for _, s := range sli {
		if s.window.End.Before(start.Add(6*time.Minute)) || s.window.End.After(start.Add(minutes*time.Minute)) {
			continue
		}
		tcol := ""
		for _, t := range tum {
			if t.window.End.Equal(s.window.End) {
				tcol = fmt.Sprintf("%s-%s %3d/%-3d %.1fx", t.window.Start.Format("15:04"), t.window.End.Format("15:04"), t.d.Requests, t.d.Drivers, surge(t.d))
				peakT = max(peakT, surge(t.d))
				if firstT == "" && surge(t.d) >= threshold {
					firstT = clock(t.at)
				}
			}
		}
		peakS = max(peakS, surge(s.d))
		if firstS == "" && surge(s.d) >= threshold {
			firstS = clock(s.at)
		}
		fmt.Printf("   %s   | %s-%s %3d/%-3d %.1fx       | %s\n", clock(s.at),
			s.window.Start.Format("15:04"), s.window.End.Format("15:04"), s.d.Requests, s.d.Drivers, surge(s.d), tcol)
	}
	fmt.Printf("   first emission (by watermark) showing surge >= %.1fx: sliding at %s, tumbling at %s\n", threshold, orNever(firstS), orNever(firstT))
	fmt.Printf("   peak surge: sliding %.1fx, tumbling %.1fx\n", peakS, peakT)
	fmt.Printf("   cost: sliding emitted %d results and held up to %d windows for this cell; tumbling emitted %d and held up to %d\n",
		len(sli), maxSli, len(tum), maxTum)
}

func orNever(s string) string {
	if s == "" {
		return "never"
	}
	return s
}
