// Package cachestampede simulates the "Redis cache causes a production
// outage" incident: a deploy warms 1,000 product pages into the cache at the
// same instant, and exactly one TTL later they all expire together. Every
// read of every expired key goes to the database at once, the database is
// pushed past its capacity, and reads start failing.
//
// Broken runs a naive cache (fixed TTL, no request coalescing). Fixed runs
// the refresh-ahead pattern (TTL jitter, probabilistic early refresh, and
// coalesced misses). Both see identical traffic and an identical database.
package cachestampede

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	refreshahead "github.com/neerajsinghi/system-design-patterns-go/patterns/data/refresh-ahead"
)

const (
	Products     = 1_000
	ReadsPerKey  = 5 // reads of every product per second
	TTL          = 60 * time.Second
	DBCapacity   = 300 // queries per second the database can serve
	Duration     = 180 // simulated seconds
	loadCostHint = 2 * time.Second
)

var errOverloaded = errors.New("db: too many connections")

// Report is what a run measured.
type Report struct {
	Name string
	// QueriesPerSecond[s] is how many queries reached the DB in second s.
	QueriesPerSecond [Duration]int
	// FailedReads are reads that returned an error to the user.
	FailedReads int
	// WaitedReads are reads that had to wait on the DB after warm-up.
	WaitedReads int
}

// Peak returns the busiest second after the warm-up second.
func (r Report) Peak() int {
	peak := 0
	for _, q := range r.QueriesPerSecond[1:] {
		peak = max(peak, q)
	}
	return peak
}

// SecondsOverCapacity counts seconds in which the DB was asked for more than it can serve.
func (r Report) SecondsOverCapacity() int {
	n := 0
	for _, q := range r.QueriesPerSecond[1:] {
		if q > DBCapacity {
			n++
		}
	}
	return n
}

// Print writes a timeline and a summary.
func (r Report) Print(w io.Writer) {
	fmt.Fprintf(w, "%s\n", r.Name)
	fmt.Fprintf(w, "%d products × %d reads/s, TTL %v, database capacity %d queries/s\n\n", Products, ReadsPerKey, TTL, DBCapacity)
	for start := 0; start < Duration; start += 10 {
		peak := 0
		for _, q := range r.QueriesPerSecond[start : start+10] {
			peak = max(peak, q)
		}
		bar := strings.Repeat("█", min(peak, 6_000)/100)
		over := ""
		if peak > DBCapacity && start > 0 {
			over = "  OVER CAPACITY"
		}
		if start == 0 {
			over = "  (warm-up, before traffic)"
		}
		fmt.Fprintf(w, "  t=%3d-%3ds  peak %5d q/s  %s%s\n", start, start+9, peak, bar, over)
	}
	fmt.Fprintf(w, "\n  busiest second: %d queries (capacity %d)\n", r.Peak(), DBCapacity)
	fmt.Fprintf(w, "  seconds over capacity: %d\n", r.SecondsOverCapacity())
	fmt.Fprintf(w, "  reads that waited on the DB: %d\n", r.WaitedReads)
	fmt.Fprintf(w, "  reads that FAILED: %d\n", r.FailedReads)
}

// db counts queries per simulated second and rejects anything past capacity.
// Second 0 is the warm-up job, which runs before traffic is switched over.
type db struct {
	mu  sync.Mutex
	now *int
	r   *Report
}

func (d *db) query(int) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.r.QueriesPerSecond[*d.now]++
	if *d.now > 0 && d.r.QueriesPerSecond[*d.now] > DBCapacity {
		return "", errOverloaded
	}
	return "product page", nil
}

// Broken runs a naive cache: every key gets the same fixed TTL, and a miss
// queries the DB directly with no coalescing. Under load a query takes about
// a second, so results land at the end of the tick, and every read of an
// expired key during that second misses and queries too.
func Broken() Report {
	r := Report{Name: "BROKEN: naive cache, fixed TTL, no request coalescing"}
	sec := 0
	d := &db{now: &sec, r: &r}
	expires := make(map[int]int) // key -> second it expires
	for sec = range Duration {
		landed := map[int]bool{}
		for k := range Products {
			for range ReadsPerKey {
				if exp, ok := expires[k]; ok && sec < exp {
					continue // hit
				}
				if sec > 0 {
					r.WaitedReads++
				}
				if _, err := d.query(k); err != nil {
					r.FailedReads++
					continue
				}
				landed[k] = true
			}
		}
		for k := range landed {
			expires[k] = sec + 1 + int(TTL/time.Second)
		}
	}
	return r
}

// Fixed runs the refresh-ahead cache: TTLs are jittered by up to 10%, hot
// keys are refreshed early by one reader (XFetch, beta 1), and concurrent
// misses share a single query. Reads here run one after another, so a load
// completes before the next read; with real concurrency the coalescing gives
// the same one-query-per-key outcome (see the refresh-ahead tests).
func Fixed() Report {
	r := Report{Name: "FIXED: refresh-ahead (TTL jitter + early refresh + coalescing)"}
	sec := 0
	d := &db{now: &sec, r: &r}
	var mu sync.Mutex
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return time.Unix(int64(sec), 0) }
	cache, err := refreshahead.New(
		func(_ context.Context, k int) (string, error) { return d.query(k) },
		TTL,
		refreshahead.WithClock(clock),
		refreshahead.WithRand(rand.New(rand.NewPCG(1, 2)).Float64), // reproducible output
		refreshahead.WithJitter(0.1),
		refreshahead.WithEarlyRefresh(1),
		refreshahead.WithLoadCost(func(int) time.Duration { return loadCostHint }),
	)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	for s := range Duration {
		mu.Lock()
		sec = s
		mu.Unlock()
		before := cache.Stats().Misses
		for k := range Products {
			for range ReadsPerKey {
				if _, err := cache.Get(ctx, k); err != nil {
					r.FailedReads++
				}
			}
		}
		if s > 0 {
			r.WaitedReads += int(cache.Stats().Misses - before)
		}
	}
	return r
}
