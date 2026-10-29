// Command example simulates a client spreading traffic over five backends
// while one of them turns slow-but-alive (its health check still passes), then
// recovers, and then the whole pool stalls on a shared dependency. It runs the
// same traffic three times: plain round robin, outlier detection with a 40%
// max-ejection cap, and outlier detection with no effective cap.
package main

import (
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	outlierdetection "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/outlier-detection"
)

const (
	interval   = 10 * time.Millisecond // one request every 10ms (100 rps)
	timeout    = time.Second           // client timeout: slower counts as an error
	slowHost   = "api-3"
	slowFrom   = 0 * time.Second
	slowUntil  = 25 * time.Second
	stallFrom  = 60 * time.Second // shared dependency stalls: every host is slow
	stallUntil = 65 * time.Second
	end        = 80 * time.Second
)

var hosts = []string{"api-1", "api-2", "api-3", "api-4", "api-5"}

type phase struct {
	name       string
	from, till time.Duration
}

var phases = []phase{
	{"1. detection window    0-1s", 0, time.Second},
	{"2. api-3 slow          1-25s", time.Second, slowUntil},
	{"3. api-3 recovered    25-60s", slowUntil, stallFrom},
	{"4. pool-wide stall    60-65s", stallFrom, stallUntil},
	{"5. after the stall    65-80s", stallUntil, end},
}

type result struct {
	lat    [][]time.Duration // successful latencies per phase
	slow   []int             // successful but slower than 200ms
	errs   []int             // failed requests per phase
	total  []int
	events []string
}

// latency is what a backend would take for a request at elapsed time t.
func latency(rng *rand.Rand, name string, t time.Duration) time.Duration {
	jitter := func(base, spread time.Duration) time.Duration {
		return base - spread + time.Duration(rng.Int64N(int64(2*spread)+1))
	}
	switch {
	case t >= stallFrom && t < stallUntil:
		return jitter(1500*time.Millisecond, 100*time.Millisecond)
	case name == slowHost && t >= slowFrom && t < slowUntil:
		return jitter(800*time.Millisecond, 50*time.Millisecond)
	default:
		return jitter(20*time.Millisecond, 5*time.Millisecond)
	}
}

func run(opts ...outlierdetection.Option) result {
	start := time.Unix(0, 0)
	now := start
	res := result{lat: make([][]time.Duration, len(phases)), slow: make([]int, len(phases)), errs: make([]int, len(phases)), total: make([]int, len(phases))}
	onEvent := func(e outlierdetection.Event) {
		at := e.At.Sub(start).Seconds()
		var s string
		switch e.Kind {
		case outlierdetection.Ejected:
			s = fmt.Sprintf("t=%6.2fs  %s ejected for %v (%s", at, e.Host, e.Duration, e.Reason)
			if e.Reason == outlierdetection.SlowLatency {
				s += fmt.Sprintf(": EWMA %v vs median %v", e.EWMA.Round(time.Millisecond), e.Median.Round(time.Millisecond))
			}
			s += ")"
		case outlierdetection.Readmitted:
			s = fmt.Sprintf("t=%6.2fs  %s readmitted, must prove itself again", at, e.Host)
		case outlierdetection.EjectionCapped:
			s = fmt.Sprintf("t=%6.2fs  %s qualifies (%s) but max-ejection cap reached: stays in", at, e.Host, e.Reason)
		}
		res.events = append(res.events, s)
	}
	opts = append([]outlierdetection.Option{
		outlierdetection.WithClock(func() time.Time { return now }),
		outlierdetection.WithOnEvent(onEvent),
	}, opts...)
	pool, err := outlierdetection.New(hosts, opts...)
	if err != nil {
		log.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(42, 7)) // same traffic for every run
	for t := time.Duration(0); t < end; t += interval {
		now = start.Add(t)
		p := 0
		for t >= phases[p].till {
			p++
		}
		res.total[p]++
		name, err := pool.Pick()
		if errors.Is(err, outlierdetection.ErrAllEjected) {
			res.errs[p]++
			continue
		}
		d := latency(rng, name, t)
		var reqErr error
		if d >= timeout {
			d, reqErr = timeout, errors.New("timeout")
		}
		// Simplification: the response is reported at send time, so the
		// detector sees it immediately rather than d later.
		pool.Report(name, d, reqErr)
		if reqErr != nil {
			res.errs[p]++
		} else {
			res.lat[p] = append(res.lat[p], d)
			if d > 200*time.Millisecond {
				res.slow[p]++
			}
		}
	}
	return res
}

func pct(v []time.Duration, q float64) string {
	if len(v) == 0 {
		return "-"
	}
	s := slices.Clone(v)
	slices.Sort(s)
	return s[int(q*float64(len(s)-1))].Round(time.Millisecond).String()
}

func main() {
	fmt.Println("5 backends, 100 rps round robin, client timeout 1s.")
	fmt.Printf("%s is slow (~800ms) but alive (health checks pass) from %v to %v; every host stalls (~1.5s) from %v to %v.\n",
		slowHost, slowFrom, slowUntil, stallFrom, stallUntil)
	fmt.Println()

	disabled := run(outlierdetection.WithConsecutiveErrors(0), outlierdetection.WithLatencyFactor(0))
	detection := []outlierdetection.Option{
		outlierdetection.WithConsecutiveErrors(5),
		outlierdetection.WithLatencyFactor(3),
		outlierdetection.WithMinRequests(5),
		outlierdetection.WithEjectionTime(10*time.Second, 60*time.Second),
	}
	capped := run(append(detection, outlierdetection.WithMaxEjectionPercent(40))...)
	uncapped := run(append(detection, outlierdetection.WithMaxEjectionPercent(100))...)

	table := func(title string, r result) {
		fmt.Println(title)
		fmt.Printf("   %-28s %8s %8s %12s %12s\n", "phase", "p50", "p99", ">200ms", "failed")
		for i, ph := range phases {
			fmt.Printf("   %-28s %8s %8s %12s %12s\n", ph.name, pct(r.lat[i], 0.5), pct(r.lat[i], 0.99),
				fmt.Sprintf("%d/%d", r.slow[i], r.total[i]), fmt.Sprintf("%d/%d", r.errs[i], r.total[i]))
		}
		fmt.Println()
	}
	fmt.Println("p50/p99 are over successful requests; failed = timeouts plus requests with no host to go to.")
	fmt.Println()
	table("A) Plain round robin, no outlier detection", disabled)
	table("B) Outlier detection, max ejection 40% (2 of 5 hosts)", capped)
	table("C) Outlier detection, max ejection 100% (no effective cap)", uncapped)
	fmt.Println("Ejection events, B) detection with a 40% cap (at most 2 of 5 hosts out):")
	for _, e := range capped.events {
		fmt.Println("   " + e)
	}
	fmt.Println()
	fmt.Println("C) with a 100% cap, during the stall:")
	for _, e := range uncapped.events {
		if strings.Contains(e, "t= 6") || strings.Contains(e, "t= 7") {
			fmt.Println("   " + e)
		}
	}
}
