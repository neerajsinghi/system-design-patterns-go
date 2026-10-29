// Package hotpartition simulates the "one poll throttles the votes table"
// incident. A live TV final opens audience voting in the app, and the votes
// for that one poll land on a DynamoDB-style table keyed by poll ID that also
// holds hundreds of ordinary community polls. The table's total write
// capacity is well above the traffic, but the finale's poll ID hashes to one
// partition, which is asked for four times what it can absorb. Votes for the
// finale are throttled, and so are votes for every unrelated poll that
// happens to share that partition. Doubling the partition count changes
// nothing, because one key still maps to one partition.
//
// Broken writes each vote to the plain poll key. Fixed writes through the
// hot-key salting pattern in adaptive mode: a per-key write-rate detector
// notices the finale poll within its first second and splits it across K
// physical keys, while cold polls stay as single keys. Both runs get the same
// seeded traffic and the same table (partitions and per-partition capacity).
package hotpartition

import (
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	hks "github.com/neerajsinghi/system-design-patterns-go/patterns/data/hot-key-salting"
)

const (
	Partitions   = 10
	Capacity     = 1_000 // writes per second one partition accepts
	ColdPolls    = 400   // ordinary community polls sharing the table
	ColdPerSec   = 2_000 // votes per second spread over the cold polls
	FinalePerSec = 4_000 // votes per second for the finale poll once voting opens
	VotingOpens  = 2     // the finale poll starts receiving votes at this second
	Duration     = 12    // simulated seconds
	Shards       = 20    // K: physical keys a salted poll is split into
	Threshold    = Capacity / 2
	finale       = "poll:finale"
)

// Report is what a run measured.
type Report struct {
	Name       string
	Partitions int
	// Throttled[s] is how many votes the table rejected in second s.
	Throttled [Duration]uint64
	// BusiestLoad[s] is the most votes any one partition was asked to take in
	// second s (accepted plus throttled).
	BusiestLoad [Duration]uint64
	// FinaleThrottled and ColdThrottled split the rejected votes by poll:
	// ColdThrottled are collateral damage, votes for polls that were never hot.
	FinaleThrottled, ColdThrottled uint64
	// Accepted is how many votes the table stored.
	Accepted uint64
	// CountMismatches is how many polls read back a total different from the
	// number of votes the table accepted for them.
	CountMismatches int
	// FinaleReadLookups and ColdReadLookups are the physical lookups one read
	// of the finale poll, and of a cold poll, cost at the end of the run.
	FinaleReadLookups, ColdReadLookups uint64
	// DetectedAt is the second the finale poll was salted (-1 if never).
	DetectedAt int
	// ScaledOut, if set, is the same broken design re-run with twice the
	// partitions.
	ScaledOut *Report
}

// TotalThrottled sums Throttled.
func (r Report) TotalThrottled() uint64 {
	var n uint64
	for _, t := range r.Throttled {
		n += t
	}
	return n
}

// ThrottledFrom sums the throttled votes from second s onward.
func (r Report) ThrottledFrom(s int) uint64 {
	var n uint64
	for _, t := range r.Throttled[s:] {
		n += t
	}
	return n
}

// PeakLoad returns the highest per-partition, per-second load.
func (r Report) PeakLoad() uint64 {
	var peak uint64
	for _, l := range r.BusiestLoad {
		peak = max(peak, l)
	}
	return peak
}

// Print writes a per-second timeline and a summary.
func (r Report) Print(w io.Writer) {
	fmt.Fprintf(w, "%s\n", r.Name)
	fmt.Fprintf(w, "%d partitions x %d writes/s = %d writes/s of table capacity\n", r.Partitions, Capacity, r.Partitions*Capacity)
	fmt.Fprintf(w, "%d votes/s over %d cold polls; the finale poll adds %d votes/s from t=%ds\n\n",
		ColdPerSec, ColdPolls, FinalePerSec, VotingOpens)
	fmt.Fprintln(w, "    t   busiest partition (votes asked)   throttled")
	for s := range Duration {
		l := r.BusiestLoad[s]
		over := ""
		if l > Capacity {
			over = fmt.Sprintf(" %.1fx", float64(l)/Capacity)
		}
		mark := ""
		if s == r.DetectedAt {
			mark = "  <- finale poll salted"
		}
		fmt.Fprintf(w, "  %3ds  %5d %-20s%-5s %6d%s\n", s, l, strings.Repeat("█", int(min(l, 5_000)/250)), over, r.Throttled[s], mark)
	}
	total := uint64(Duration-VotingOpens)*FinalePerSec + Duration*ColdPerSec
	fmt.Fprintf(w, "\n  votes throttled: %d of %d (%.1f%%)\n", r.TotalThrottled(), total, 100*float64(r.TotalThrottled())/float64(total))
	fmt.Fprintf(w, "    finale poll: %d   unrelated cold polls: %d\n", r.FinaleThrottled, r.ColdThrottled)
	fmt.Fprintf(w, "  throttled after the first second of voting: %d\n", r.ThrottledFrom(VotingOpens+1))
	fmt.Fprintf(w, "  polls whose read-back total != accepted votes: %d\n", r.CountMismatches)
	fmt.Fprintf(w, "  read cost: finale poll %d lookup(s), cold poll %d lookup(s)\n", r.FinaleReadLookups, r.ColdReadLookups)
	if r.ScaledOut != nil {
		so := r.ScaledOut
		fmt.Fprintf(w, "  scaled out to %d partitions: busiest partition still %d votes/s, %d votes throttled\n",
			so.Partitions, so.PeakLoad(), so.TotalThrottled())
	}
}

// traffic returns each second's votes in arrival order. It uses its own
// seeded generator, so every run sees exactly the same votes.
func traffic() [Duration][]string {
	rng := rand.New(rand.NewPCG(41, 42))
	var out [Duration][]string
	for s := range Duration {
		ops := make([]string, 0, ColdPerSec+FinalePerSec)
		for range ColdPerSec {
			ops = append(ops, fmt.Sprintf("poll:%d", rng.IntN(ColdPolls)))
		}
		if s >= VotingOpens {
			for range FinalePerSec {
				ops = append(ops, finale)
			}
		}
		rng.Shuffle(len(ops), func(i, j int) { ops[i], ops[j] = ops[j], ops[i] })
		out[s] = ops
	}
	return out
}

func run(name string, partitions int, opts ...hks.Option) Report {
	r := Report{Name: name, Partitions: partitions, DetectedAt: -1}
	var mu sync.Mutex
	now := time.Unix(1_700_000_000, 0)
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }

	table, err := hks.NewTable[int64](partitions, Capacity, clock)
	if err != nil {
		panic(err)
	}
	picker := rand.New(rand.NewPCG(7, 8))
	opts = append([]hks.Option{hks.WithClock(clock), hks.WithShards(Shards), hks.WithPicker(picker.IntN)}, opts...)
	s, err := hks.New(table, hks.Sum, opts...)
	if err != nil {
		panic(err)
	}

	accepted := make(map[string]int64)
	for sec, ops := range traffic() {
		before := table.Stats()
		step := time.Second / time.Duration(len(ops))
		for i, key := range ops {
			mu.Lock()
			now = time.Unix(1_700_000_000+int64(sec), 0).Add(time.Duration(i) * step)
			mu.Unlock()
			switch err := s.Write(key, 1); {
			case err == nil:
				accepted[key]++
				r.Accepted++
			case errors.Is(err, hks.ErrThrottled):
				if key == finale {
					r.FinaleThrottled++
				} else {
					r.ColdThrottled++
				}
			default:
				panic(err)
			}
		}
		after := table.Stats()
		for p := range after {
			t := after[p].Throttled - before[p].Throttled
			r.Throttled[sec] += t
			r.BusiestLoad[sec] = max(r.BusiestLoad[sec], after[p].Writes-before[p].Writes+t)
		}
		if r.DetectedAt < 0 && s.Fanout(finale) > 1 {
			r.DetectedAt = sec
		}
	}

	// Read every poll back and compare with what the table accepted.
	keys := []string{finale}
	for i := range ColdPolls {
		keys = append(keys, fmt.Sprintf("poll:%d", i))
	}
	for _, k := range keys {
		v, _, err := s.Read(k)
		if err != nil {
			panic(err)
		}
		if v != accepted[k] {
			r.CountMismatches++
		}
	}
	lookups := func(key string) uint64 {
		before := s.Stats().Lookups
		if _, _, err := s.Read(key); err != nil {
			panic(err)
		}
		return s.Stats().Lookups - before
	}
	r.FinaleReadLookups = lookups(finale)
	r.ColdReadLookups = lookups("poll:0")
	return r
}

// Broken writes every vote to its plain poll key, so the finale poll lives on
// one partition. It also re-runs the same design with twice the partitions.
func Broken() Report {
	r := run("BROKEN: plain poll keys; the finale poll lives on one partition", Partitions, hks.WithMode(hks.Unsalted))
	so := run("BROKEN, scaled out", 2*Partitions, hks.WithMode(hks.Unsalted))
	r.ScaledOut = &so
	return r
}

// Fixed writes through an adaptive Salter: any poll that takes Threshold votes
// within one second is split across Shards physical keys. Nothing names the
// finale poll in advance; the detector finds it.
func Fixed() Report {
	return run(fmt.Sprintf("FIXED: adaptive hot-key salting (K=%d, promote at %d writes/s)", Shards, Threshold),
		Partitions, hks.WithMode(hks.Adaptive), hks.WithDetector(Threshold, time.Second))
}
