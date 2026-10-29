package outlierdetection

import (
	"errors"
	"math/rand/v2"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (f *fakeClock) Now() time.Time          { f.mu.Lock(); defer f.mu.Unlock(); return f.t }
func (f *fakeClock) Advance(d time.Duration) { f.mu.Lock(); f.t = f.t.Add(d); f.mu.Unlock() }

var errServer = errors.New("503")

var five = []string{"a", "b", "c", "d", "e"}

// recorder collects events.
type recorder struct {
	mu     sync.Mutex
	events []Event
}

func (r *recorder) on(e Event) { r.mu.Lock(); r.events = append(r.events, e); r.mu.Unlock() }
func (r *recorder) count(k EventKind) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, e := range r.events {
		if e.Kind == k {
			n++
		}
	}
	return n
}

func newPool(t *testing.T, clk *fakeClock, opts ...Option) *Pool {
	t.Helper()
	p, err := New(five, append([]Option{WithClock(clk.Now)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// drive sends n round-robin requests; latency(host) decides each outcome.
func drive(p *Pool, n int, latency func(host string) (time.Duration, error)) map[string]int {
	got := map[string]int{}
	for range n {
		h, err := p.Pick()
		if err != nil {
			continue
		}
		got[h]++
		d, rerr := latency(h)
		p.Report(h, d, rerr)
	}
	return got
}

func status(p *Pool, name string) HostStatus {
	for _, h := range p.Hosts() {
		if h.Name == name {
			return h
		}
	}
	return HostStatus{}
}

func TestNewValidation(t *testing.T) {
	tests := []struct {
		name  string
		hosts []string
		opts  []Option
	}{
		{"no hosts", nil, nil},
		{"empty host name", []string{"a", ""}, nil},
		{"duplicate host", []string{"a", "a"}, nil},
		{"negative consecutive errors", five, []Option{WithConsecutiveErrors(-1)}},
		{"latency factor <= 1", five, []Option{WithLatencyFactor(1)}},
		{"negative latency factor", five, []Option{WithLatencyFactor(-2)}},
		{"negative min latency", five, []Option{WithMinLatency(-time.Millisecond)}},
		{"alpha zero", five, []Option{WithEWMAAlpha(0)}},
		{"alpha above one", five, []Option{WithEWMAAlpha(1.5)}},
		{"min requests zero", five, []Option{WithMinRequests(0)}},
		{"min hosts zero", five, []Option{WithMinHosts(0)}},
		{"zero base ejection", five, []Option{WithEjectionTime(0, time.Second)}},
		{"max below base", five, []Option{WithEjectionTime(time.Minute, time.Second)}},
		{"percent zero", five, []Option{WithMaxEjectionPercent(0)}},
		{"percent above 100", five, []Option{WithMaxEjectionPercent(101)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.hosts, tt.opts...); err == nil {
				t.Fatal("expected error")
			}
		})
	}
	if _, err := New(five, WithConsecutiveErrors(0), WithLatencyFactor(0)); err != nil {
		t.Fatalf("both detectors disabled should be valid: %v", err)
	}
}

func TestRoundRobinWithoutDetection(t *testing.T) {
	clk := &fakeClock{}
	p := newPool(t, clk, WithConsecutiveErrors(0), WithLatencyFactor(0))
	got := drive(p, 500, func(h string) (time.Duration, error) {
		if h == "c" {
			return time.Second, errServer
		}
		return time.Millisecond, nil
	})
	for _, h := range five {
		if got[h] != 100 {
			t.Fatalf("host %s got %d requests, want 100 (detection disabled): %v", h, got[h], got)
		}
	}
}

func TestLatencyEjectionThreshold(t *testing.T) {
	tests := []struct {
		name      string
		slow      time.Duration
		minLat    time.Duration
		wantEject bool
	}{
		{"well above 3x median", 800 * time.Millisecond, 0, true},
		{"just above 3x median", 61 * time.Millisecond, 0, true},
		{"just below 3x median", 59 * time.Millisecond, 0, false},
		{"above factor but under min latency", 61 * time.Millisecond, 100 * time.Millisecond, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clk := &fakeClock{}
			rec := &recorder{}
			p := newPool(t, clk, WithLatencyFactor(3), WithEWMAAlpha(1), WithMinRequests(3),
				WithMinLatency(tt.minLat), WithMaxEjectionPercent(20), WithOnEvent(rec.on))
			drive(p, 50, func(h string) (time.Duration, error) {
				if h == "c" {
					return tt.slow, nil
				}
				return 20 * time.Millisecond, nil
			})
			if got := status(p, "c").Ejected; got != tt.wantEject {
				t.Fatalf("ejected = %v, want %v", got, tt.wantEject)
			}
			for _, h := range []string{"a", "b", "d", "e"} {
				if status(p, h).Ejected {
					t.Fatalf("healthy host %s was ejected", h)
				}
			}
			if tt.wantEject {
				e := rec.events[0]
				if e.Kind != Ejected || e.Reason != SlowLatency || e.Host != "c" || e.Median != 20*time.Millisecond {
					t.Fatalf("unexpected event %+v", e)
				}
			}
		})
	}
}

func TestLatencyNeedsEnoughSamplesAndHosts(t *testing.T) {
	clk := &fakeClock{}
	p := newPool(t, clk, WithMinRequests(10), WithEWMAAlpha(1))
	// 9 rounds: each host has 9 samples, one short of MinRequests.
	drive(p, 45, func(h string) (time.Duration, error) {
		if h == "c" {
			return time.Second, nil
		}
		return 10 * time.Millisecond, nil
	})
	if status(p, "c").Ejected {
		t.Fatal("ejected before MinRequests samples")
	}

	// Only two hosts: the median cannot be trusted with MinHosts 3.
	p2, err := New([]string{"x", "y"}, WithClock(clk.Now), WithMinRequests(1), WithMaxEjectionPercent(100))
	if err != nil {
		t.Fatal(err)
	}
	for range 20 {
		p2.Report("x", 10*time.Millisecond, nil)
		p2.Report("y", time.Second, nil)
	}
	if p2.Ejected() != 0 {
		t.Fatal("ejected with fewer than MinHosts hosts reporting")
	}
}

func TestConsecutiveErrorEjection(t *testing.T) {
	clk := &fakeClock{}
	rec := &recorder{}
	p := newPool(t, clk, WithConsecutiveErrors(3), WithLatencyFactor(0), WithOnEvent(rec.on))

	// Errors interrupted by a success do not eject.
	for _, err := range []error{errServer, errServer, nil, errServer, errServer} {
		p.Report("b", time.Millisecond, err)
	}
	if status(p, "b").Ejected {
		t.Fatal("a success should reset the consecutive-error count")
	}
	p.Report("b", time.Millisecond, errServer) // third in a row
	s := status(p, "b")
	if !s.Ejected || s.EjectedUntil != clk.Now().Add(30*time.Second) {
		t.Fatalf("want ejected for 30s, got %+v", s)
	}
	if e := rec.events[0]; e.Reason != ConsecutiveErrors || e.Duration != 30*time.Second {
		t.Fatalf("unexpected event %+v", e)
	}
	got := drive(p, 40, func(string) (time.Duration, error) { return time.Millisecond, nil })
	if got["b"] != 0 || got["a"] != 10 {
		t.Fatalf("ejected host still picked or others unbalanced: %v", got)
	}
	// A late response from an ejected host is ignored.
	p.Report("b", time.Millisecond, errServer)
	if status(p, "b").ConsecutiveErrors != 3 {
		t.Fatal("report for ejected host should be ignored")
	}
}

func TestMaxEjectionPercentCap(t *testing.T) {
	tests := []struct {
		percent, want int
	}{
		{10, 1}, // 10% of 5 rounds to 0, but one host may always be ejected
		{40, 2},
		{60, 3},
		{100, 5},
	}
	for _, tt := range tests {
		clk := &fakeClock{}
		rec := &recorder{}
		p := newPool(t, clk, WithConsecutiveErrors(2), WithMaxEjectionPercent(tt.percent), WithOnEvent(rec.on))
		drive(p, 100, func(string) (time.Duration, error) { return time.Millisecond, errServer })
		if got := p.Ejected(); got != tt.want {
			t.Fatalf("percent %d: %d ejected, want %d", tt.percent, got, tt.want)
		}
		if capped := rec.count(EjectionCapped); capped != 5-tt.want {
			t.Fatalf("percent %d: %d capped events, want one per blocked host (%d)", tt.percent, capped, 5-tt.want)
		}
		_, err := p.Pick()
		if tt.want == 5 {
			if !errors.Is(err, ErrAllEjected) {
				t.Fatalf("want ErrAllEjected, got %v", err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
	}
}

func TestReadmissionAndGrowingEjection(t *testing.T) {
	clk := &fakeClock{}
	rec := &recorder{}
	p := newPool(t, clk, WithConsecutiveErrors(1), WithLatencyFactor(0),
		WithEjectionTime(10*time.Second, 25*time.Second), WithOnEvent(rec.on))

	wantDurations := []time.Duration{10 * time.Second, 20 * time.Second, 25 * time.Second, 25 * time.Second}
	for i, want := range wantDurations {
		p.Report("a", time.Millisecond, errServer)
		s := status(p, "a")
		if !s.Ejected || s.EjectedUntil.Sub(clk.Now()) != want || s.TotalEjections != i+1 {
			t.Fatalf("ejection %d: want %v, got %+v", i+1, want, s)
		}
		clk.Advance(want - time.Nanosecond)
		if !status(p, "a").Ejected {
			t.Fatalf("ejection %d: readmitted early", i+1)
		}
		clk.Advance(time.Nanosecond)
		s = status(p, "a")
		if s.Ejected || s.Samples != 0 || s.ConsecutiveErrors != 0 {
			t.Fatalf("ejection %d: want readmitted with a clean slate, got %+v", i+1, s)
		}
	}
	if got := rec.count(Readmitted); got != 4 {
		t.Fatalf("%d readmitted events, want 4", got)
	}

	// Healthy time decays the multiplier: one step per base ejection time.
	// Multiplier is 4; after 25s healthy it drops by 2, then +1 = 3 → 25s cap.
	// After a further long healthy stretch it drops to 0, so the next is 10s.
	clk.Advance(25 * time.Second)
	p.Report("a", time.Millisecond, errServer)
	if s := status(p, "a"); s.Ejections != 3 {
		t.Fatalf("want multiplier 3 after decay, got %+v", s)
	}
	clk.Advance(25 * time.Second) // readmitted
	status(p, "a")
	clk.Advance(5 * time.Minute)
	p.Report("a", time.Millisecond, errServer)
	if s := status(p, "a"); s.Ejections != 1 || s.EjectedUntil.Sub(clk.Now()) != 10*time.Second {
		t.Fatalf("want multiplier reset to 1 (10s) after long healthy period, got %+v", s)
	}
}

func TestReadmittedHostMustProveItselfAgain(t *testing.T) {
	clk := &fakeClock{}
	p := newPool(t, clk, WithMinRequests(3), WithEWMAAlpha(1), WithEjectionTime(time.Second, time.Minute))
	slow := func(h string) (time.Duration, error) {
		if h == "c" {
			return 500 * time.Millisecond, nil
		}
		return 10 * time.Millisecond, nil
	}
	drive(p, 50, slow)
	if !status(p, "c").Ejected {
		t.Fatal("slow host not ejected")
	}
	clk.Advance(time.Second)
	// Re-admitted with no samples: it gets traffic again and, still slow,
	// is ejected after MinRequests samples, now for 2s.
	got := drive(p, 50, slow)
	if got["c"] != 3 {
		t.Fatalf("readmitted host got %d requests before re-ejection, want 3", got["c"])
	}
	s := status(p, "c")
	if !s.Ejected || s.EjectedUntil.Sub(clk.Now()) != 2*time.Second {
		t.Fatalf("want re-ejected for 2s, got %+v", s)
	}
	clk.Advance(2 * time.Second)
	// Now recovered: it stays in rotation.
	got = drive(p, 100, func(string) (time.Duration, error) { return 10 * time.Millisecond, nil })
	if got["c"] != 20 || status(p, "c").Ejected {
		t.Fatalf("recovered host should stay in rotation: %v", got)
	}
}

func TestConcurrentUse(t *testing.T) {
	clk := &fakeClock{}
	p := newPool(t, clk, WithMaxEjectionPercent(40), WithEjectionTime(time.Millisecond, 10*time.Millisecond))
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(g), 1))
			for range 2000 {
				h, err := p.Pick()
				if err != nil {
					t.Error(err)
					return
				}
				var rerr error
				d := time.Duration(10+rng.IntN(10)) * time.Millisecond
				if h == "c" {
					d = 300 * time.Millisecond
				}
				if rng.IntN(20) == 0 {
					rerr = errServer
				}
				p.Report(h, d, rerr)
				if rng.IntN(50) == 0 {
					clk.Advance(time.Millisecond)
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 200 {
			if n := p.Ejected(); n > 2 {
				t.Errorf("%d hosts ejected, cap is 2", n)
			}
			_ = p.Hosts()
		}
	}()
	wg.Wait()
	if n := p.Ejected(); n > 2 {
		t.Fatalf("%d hosts ejected, cap is 2", n)
	}
}
