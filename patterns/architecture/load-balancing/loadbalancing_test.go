package loadbalancing

import (
	"errors"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
)

func backends(names ...string) []*Backend {
	out := make([]*Backend, len(names))
	for i, n := range names {
		out[i] = &Backend{Name: n}
	}
	return out
}

func pickNames(t *testing.T, b Balancer, n int) string {
	t.Helper()
	var sb strings.Builder
	for i := 0; i < n; i++ {
		be, done, err := b.Pick()
		if err != nil {
			t.Fatal(err)
		}
		sb.WriteString(be.Name)
		done()
	}
	return sb.String()
}

func TestSequences(t *testing.T) {
	weighted := backends("a", "b", "c")
	weighted[0].Weight, weighted[1].Weight, weighted[2].Weight = 5, 1, 1
	zeroWeights := backends("x", "y") // weights < 1 count as 1
	tests := []struct {
		name string
		bal  Balancer
		n    int
		want string
	}{
		{"round robin", NewRoundRobin(backends("a", "b", "c")...), 7, "abcabca"},
		{"smooth weighted 5:1:1", NewWeightedRoundRobin(weighted...), 14, "aabacaaaabacaa"},
		{"weighted with zero weights", NewWeightedRoundRobin(zeroWeights...), 4, "xyxy"},
		{"least conns rotates ties when idle", NewLeastConnections(backends("a", "b", "c")...), 4, "abca"},
		{"p2c single backend", NewPowerOfTwo(nil, backends("solo")...), 2, "solosolo"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := pickNames(t, tc.bal, tc.n); got != tc.want {
				t.Errorf("got %s, want %s", got, tc.want)
			}
		})
	}
}

func TestEmpty(t *testing.T) {
	for _, b := range []Balancer{NewRoundRobin(), NewWeightedRoundRobin(), NewLeastConnections(), NewPowerOfTwo(nil)} {
		if _, _, err := b.Pick(); !errors.Is(err, ErrNoBackends) {
			t.Errorf("%T: err = %v", b, err)
		}
	}
}

func TestDoneIsIdempotent(t *testing.T) {
	b := backends("a")
	_, done, _ := NewRoundRobin(b...).Pick()
	done()
	done()
	if b[0].Active() != 0 {
		t.Errorf("active = %d", b[0].Active())
	}
}

func TestWeightedDistribution(t *testing.T) {
	bs := backends("a", "b", "c")
	bs[0].Weight, bs[1].Weight, bs[2].Weight = 3, 2, 1
	w := NewWeightedRoundRobin(bs...)
	counts := map[string]int{}
	for i := 0; i < 600; i++ {
		b, done, _ := w.Pick()
		counts[b.Name]++
		done()
	}
	if counts["a"] != 300 || counts["b"] != 200 || counts["c"] != 100 {
		t.Errorf("counts = %v, want exactly 300/200/100", counts)
	}
}

// simulate sends one request per tick. A request to backend i takes cost[i]
// ticks. It returns how many requests each backend received and the highest
// in-flight count seen on the slow backend (index 0).
func simulate(t *testing.T, bal Balancer, bs []*Backend, cost []int, ticks int) (counts []int, peakSlow int64) {
	t.Helper()
	counts = make([]int, len(bs))
	pending := map[int][]func(){} // tick -> done callbacks
	for tick := 0; tick < ticks; tick++ {
		for _, done := range pending[tick] {
			done()
		}
		delete(pending, tick)
		b, done, err := bal.Pick()
		if err != nil {
			t.Fatal(err)
		}
		for i := range bs {
			if bs[i] == b {
				counts[i]++
				pending[tick+cost[i]] = append(pending[tick+cost[i]], done)
			}
		}
		peakSlow = max(peakSlow, bs[0].Active())
	}
	return counts, peakSlow
}

// TestSlowBackend compares strategies when one backend is 20x slower. Load
// blind round robin keeps sending it a full share, so requests pile up there.
// Load-aware strategies route around it.
func TestSlowBackend(t *testing.T) {
	cost := []int{20, 1, 1, 1}
	type result struct {
		share float64
		peak  int64
	}
	run := func(mk func([]*Backend) Balancer) result {
		bs := backends("slow", "f1", "f2", "f3")
		counts, peak := simulate(t, mk(bs), bs, cost, 4000)
		return result{float64(counts[0]) / 4000, peak}
	}
	rr := run(func(bs []*Backend) Balancer { return NewRoundRobin(bs...) })
	lc := run(func(bs []*Backend) Balancer { return NewLeastConnections(bs...) })
	p2c := run(func(bs []*Backend) Balancer { return NewPowerOfTwo(rand.New(rand.NewPCG(1, 2)), bs...) })
	t.Logf("slow backend share/peak in-flight: rr=%.2f/%d lc=%.2f/%d p2c=%.2f/%d",
		rr.share, rr.peak, lc.share, lc.peak, p2c.share, p2c.peak)

	if rr.share < 0.24 || rr.peak < 5 {
		t.Errorf("round robin should give the slow node its full 25%% and pile up: %+v", rr)
	}
	for name, r := range map[string]result{"least-connections": lc, "p2c": p2c} {
		if r.share > rr.share/2 || r.peak > 2 {
			t.Errorf("%s should route around the slow node: %+v", name, r)
		}
	}
}

// randomBalancer is the baseline P2C improves on: one random choice.
type randomBalancer struct {
	bs  []*Backend
	rng *rand.Rand
}

func (r *randomBalancer) Pick() (*Backend, func(), error) {
	return acquire(r.bs[r.rng.IntN(len(r.bs))])
}

// TestPowerOfTwoVsRandom places long-lived connections that never finish.
// With one random choice the max-min gap grows like sqrt(n). With two
// choices it stays tiny, which is the "power of two choices" result.
func TestPowerOfTwoVsRandom(t *testing.T) {
	spread := func(bs []*Backend) int64 {
		lo, hi := bs[0].Active(), bs[0].Active()
		for _, b := range bs {
			lo, hi = min(lo, b.Active()), max(hi, b.Active())
		}
		return hi - lo
	}
	const n, conns = 20, 20000
	randBs, p2cBs := backends(make([]string, n)...), backends(make([]string, n)...)
	rnd := &randomBalancer{bs: randBs, rng: rand.New(rand.NewPCG(7, 7))}
	p2c := NewPowerOfTwo(rand.New(rand.NewPCG(7, 7)), p2cBs...)
	for i := 0; i < conns; i++ {
		_, _, _ = rnd.Pick()
		_, _, _ = p2c.Pick()
	}
	rs, ps := spread(randBs), spread(p2cBs)
	t.Logf("max-min connections: random=%d p2c=%d", rs, ps)
	if ps > 6 || rs < 5*ps {
		t.Errorf("p2c spread %d should be tiny and far below random's %d", ps, rs)
	}
}

func TestConcurrentPicks(t *testing.T) {
	bs := backends("a", "b", "c", "d")
	for _, bal := range []Balancer{NewRoundRobin(bs...), NewWeightedRoundRobin(bs...), NewLeastConnections(bs...), NewPowerOfTwo(nil, bs...)} {
		var wg sync.WaitGroup
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 500; i++ {
					_, done, err := bal.Pick()
					if err != nil {
						t.Error(err)
						return
					}
					done()
				}
			}()
		}
		wg.Wait()
		for _, b := range bs {
			if b.Active() != 0 {
				t.Errorf("%T: %s active = %d after all done", bal, b.Name, b.Active())
			}
		}
	}
}
