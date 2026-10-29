package loadbalancing

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// tickClock is a fake clock advanced by hand, safe for concurrent reads.
type tickClock struct{ ns atomic.Int64 }

func (c *tickClock) Now() time.Time          { return time.Unix(0, c.ns.Load()) }
func (c *tickClock) Advance(d time.Duration) { c.ns.Add(int64(d)) }

func TestLeastResponseTimeValidation(t *testing.T) {
	for _, a := range []float64{-0.1, 1.5} {
		if _, err := NewLeastResponseTime(LRTConfig{Alpha: a}, backends("a")...); err == nil {
			t.Errorf("alpha %v accepted", a)
		}
	}
	if _, err := NewLeastResponseTime(LRTConfig{}, nil); err == nil {
		t.Error("nil backend accepted")
	}
	l, err := NewLeastResponseTime(LRTConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := l.Pick(); !errors.Is(err, ErrNoBackends) {
		t.Errorf("empty pick err = %v", err)
	}
}

func TestLeastResponseTimeEWMA(t *testing.T) {
	clk := &tickClock{}
	bs := backends("a")
	l, _ := NewLeastResponseTime(LRTConfig{Now: clk.Now, Alpha: 0.5}, bs...)
	if _, ok := l.Latency(bs[0]); ok {
		t.Fatal("latency known before any sample")
	}
	for _, d := range []time.Duration{100 * time.Millisecond, 20 * time.Millisecond} {
		_, done, _ := l.Pick()
		clk.Advance(d)
		done()
		done() // idempotent: must not record a second sample
	}
	// first sample sets 100ms, then 0.5*20 + 0.5*100 = 60ms
	if got, _ := l.Latency(bs[0]); got != 60*time.Millisecond {
		t.Errorf("ewma = %v, want 60ms", got)
	}
	if bs[0].Active() != 0 {
		t.Errorf("active = %d", bs[0].Active())
	}
}

// Least connections ties a 1ms and a 50ms backend whenever both have the
// same in-flight count. LRT tells them apart.
func TestLeastResponseTimePrefersFastOverEquallyBusySlow(t *testing.T) {
	clk := &tickClock{}
	bs := backends("slow", "fast")
	l, _ := NewLeastResponseTime(LRTConfig{Now: clk.Now}, bs...)
	lat := map[string]time.Duration{"slow": 50 * time.Millisecond, "fast": time.Millisecond}
	// Warm up: one sample each (rotation tries both when unmeasured).
	for range 2 {
		b, done, _ := l.Pick()
		clk.Advance(lat[b.Name])
		done()
	}
	// Sequential traffic: in-flight is always 0, so LC would alternate.
	counts := map[string]int{}
	for range 100 {
		b, done, _ := l.Pick()
		counts[b.Name]++
		clk.Advance(lat[b.Name])
		done()
	}
	if counts["fast"] != 100 {
		t.Errorf("counts = %v, want all 100 on fast", counts)
	}
}

// When the fast backend gets busy enough, the slow one is worth using: the
// in-flight factor makes LRT spill over instead of piling everything on one node.
func TestLeastResponseTimeSpillsOverUnderLoad(t *testing.T) {
	clk := &tickClock{}
	bs := backends("slow", "fast")
	l, _ := NewLeastResponseTime(LRTConfig{Now: clk.Now}, bs...)
	// Seed EWMAs: the first pick goes to slow (tie, rotation starts at 0),
	// the second to fast (slow's 10ms is now the prior for unmeasured fast).
	for _, want := range []struct {
		name string
		d    time.Duration
	}{{"slow", 10 * time.Millisecond}, {"fast", 2 * time.Millisecond}} {
		b, done, _ := l.Pick()
		if b.Name != want.name {
			t.Fatalf("seed pick = %s, want %s", b.Name, want.name)
		}
		clk.Advance(want.d)
		done()
	}
	slow, _ := l.Latency(bs[0])
	fast, _ := l.Latency(bs[1])
	if slow != 10*time.Millisecond || fast != 2*time.Millisecond {
		t.Fatalf("seeded ewmas slow=%v fast=%v", slow, fast)
	}
	// Hold 24 requests open at once. fast's score 2*(k+1) exceeds slow's
	// 10*(j+1) once fast holds ~5x as many.
	var dones []func()
	counts := map[string]int{}
	for range 24 {
		b, done, _ := l.Pick()
		counts[b.Name]++
		dones = append(dones, done)
	}
	for _, d := range dones {
		d()
	}
	if counts["slow"] < 3 || counts["slow"] > 5 {
		t.Errorf("counts = %v, want about 1 slow per 5 fast (5:1 latency ratio)", counts)
	}
}

func TestLeastResponseTimeNewBackendNotFlooded(t *testing.T) {
	clk := &tickClock{}
	bs := backends("a", "b", "new")
	l, _ := NewLeastResponseTime(LRTConfig{Now: clk.Now}, bs...)
	// Measure a and b only; "new" has no samples and scores at their mean.
	for _, i := range []int{0, 1} {
		l.mu.Lock()
		l.ewma[i], l.seen[i] = 0.010, true
		l.mu.Unlock()
	}
	var dones []func()
	counts := map[string]int{}
	for range 30 {
		b, done, _ := l.Pick()
		counts[b.Name]++
		dones = append(dones, done)
	}
	for _, d := range dones {
		d()
	}
	if counts["new"] != 10 {
		t.Errorf("counts = %v, want the unmeasured backend to get its fair 10, not all 30", counts)
	}
}

func clientKeys(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("10.%d.%d.%d", i>>16&255, i>>8&255, i&255)
	}
	return out
}

func assign(t *testing.T, h *IPHash, keys []string) map[string]string {
	t.Helper()
	m := make(map[string]string, len(keys))
	for _, k := range keys {
		b, done, err := h.PickFor(k)
		if err != nil {
			t.Fatal(err)
		}
		done()
		m[k] = b.Name
	}
	return m
}

func TestIPHashAffinityAndMovement(t *testing.T) {
	keys := clientKeys(10000)
	tests := []struct {
		mode               HashMode
		minMoved, maxMoved float64 // fraction of all clients that move when 1 of 5 backends goes
	}{
		{HashModulo, 0.70, 0.90},     // expect about 4/5
		{HashRendezvous, 0.17, 0.23}, // expect about 1/5: only the removed backend's clients
	}
	for _, tc := range tests {
		t.Run(map[HashMode]string{HashModulo: "modulo", HashRendezvous: "rendezvous"}[tc.mode], func(t *testing.T) {
			h, err := NewIPHash(tc.mode, backends("b0", "b1", "b2", "b3", "b4")...)
			if err != nil {
				t.Fatal(err)
			}
			before := assign(t, h, keys)
			if again := assign(t, h, keys); fmt.Sprint(again) != fmt.Sprint(before) {
				t.Fatal("same client mapped to different backends with an unchanged pool")
			}
			perBackend := map[string]int{}
			for _, b := range before {
				perBackend[b]++
			}
			for name, c := range perBackend {
				if math.Abs(float64(c)-2000) > 200 {
					t.Errorf("%s got %d clients, want about 2000", name, c)
				}
			}
			if !h.Remove("b2") || h.Remove("b2") {
				t.Fatal("Remove should succeed once")
			}
			after := assign(t, h, keys)
			moved, movedOffSurvivor := 0, 0
			for k, b := range before {
				if after[k] != b {
					moved++
					if b != "b2" {
						movedOffSurvivor++
					}
				}
				if after[k] == "b2" {
					t.Fatalf("client %s still on removed backend", k)
				}
			}
			frac := float64(moved) / float64(len(keys))
			t.Logf("mode %d: %.1f%% of clients moved (%d were on surviving backends)", tc.mode, 100*frac, movedOffSurvivor)
			if frac < tc.minMoved || frac > tc.maxMoved {
				t.Errorf("moved fraction %.3f outside [%.2f, %.2f]", frac, tc.minMoved, tc.maxMoved)
			}
			if tc.mode == HashRendezvous && movedOffSurvivor != 0 {
				t.Errorf("rendezvous moved %d clients whose backend survived", movedOffSurvivor)
			}
			// Adding it back restores the original mapping in both modes.
			if !h.Add(&Backend{Name: "b2"}) || h.Add(&Backend{Name: "b2"}) {
				t.Fatal("Add should succeed once")
			}
			restored := assign(t, h, keys)
			same := 0
			for k := range before {
				if restored[k] == before[k] {
					same++
				}
			}
			if tc.mode == HashRendezvous && same != len(keys) {
				t.Errorf("rendezvous: only %d/%d clients back on their original backend", same, len(keys))
			}
		})
	}
}

func TestIPHashValidationAndFallback(t *testing.T) {
	if _, err := NewIPHash(HashMode(9), backends("a")...); err == nil {
		t.Error("bad mode accepted")
	}
	if _, err := NewIPHash(HashModulo, backends("a", "a")...); err == nil {
		t.Error("duplicate names accepted")
	}
	if _, err := NewIPHash(HashModulo, &Backend{}); err == nil {
		t.Error("unnamed backend accepted")
	}
	h, _ := NewIPHash(HashRendezvous)
	if _, _, err := h.PickFor("1.2.3.4"); !errors.Is(err, ErrNoBackends) {
		t.Errorf("empty PickFor err = %v", err)
	}
	if _, _, err := h.Pick(); !errors.Is(err, ErrNoBackends) {
		t.Errorf("empty Pick err = %v", err)
	}
	h.Add(&Backend{Name: "a"})
	h.Add(&Backend{Name: "b"})
	var bal Balancer = h // keyless requests round robin
	if got := pickNames(t, bal, 4); got != "abab" {
		t.Errorf("keyless picks = %s, want abab", got)
	}
}

func TestClientKey(t *testing.T) {
	for in, want := range map[string]string{
		"10.0.0.1:52344":    "10.0.0.1",
		"[2001:db8::1]:443": "2001:db8::1",
		"10.0.0.1":          "10.0.0.1",
	} {
		if got := ClientKey(in); got != want {
			t.Errorf("ClientKey(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNewStrategiesConcurrent(t *testing.T) {
	bs := backends("a", "b", "c", "d")
	clk := &tickClock{}
	lrt, _ := NewLeastResponseTime(LRTConfig{Now: clk.Now}, bs...)
	ih, _ := NewIPHash(HashRendezvous, bs...)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				_, done, err := lrt.Pick()
				if err != nil {
					t.Error(err)
					return
				}
				clk.Advance(time.Microsecond)
				done()
				_, done, _ = ih.PickFor(fmt.Sprintf("10.0.%d.%d", g, i%256))
				done()
				if i == 250 && g == 0 {
					ih.Remove("d")
					ih.Add(&Backend{Name: "e"})
				}
			}
		}()
	}
	wg.Wait()
	for _, b := range bs {
		if b.Active() != 0 {
			t.Errorf("%s active = %d after all done", b.Name, b.Active())
		}
	}
}
