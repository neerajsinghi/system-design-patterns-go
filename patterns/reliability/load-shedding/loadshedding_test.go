package loadshedding

import (
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Add(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func TestPriorityShedding(t *testing.T) {
	tests := []struct {
		name     string
		inFlight int // occupied by Critical before the probe
		probe    Priority
		wantShed bool
	}{
		{"sheddable under half", 4, Sheddable, false},
		{"sheddable at half", 5, Sheddable, true},
		{"normal under 80%", 7, Normal, false},
		{"normal at 80%", 8, Normal, true},
		{"critical under limit", 9, Critical, false},
		{"critical at limit", 10, Critical, true},
		{"unknown priority treated as sheddable", 5, Priority(42), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New(Config{InitialLimit: 10})
			for i := 0; i < tt.inFlight; i++ {
				if _, err := s.Acquire(Critical); err != nil {
					t.Fatalf("setup acquire %d: %v", i, err)
				}
			}
			_, err := s.Acquire(tt.probe)
			if got := errors.Is(err, ErrShed); got != tt.wantShed {
				t.Fatalf("shed = %v, want %v (err %v)", got, tt.wantShed, err)
			}
		})
	}
}

func TestDoneReleasesOnce(t *testing.T) {
	s := New(Config{InitialLimit: 1})
	done, err := s.Acquire(Critical)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Acquire(Critical); !errors.Is(err, ErrShed) {
		t.Fatal("limit 1 should shed the second request")
	}
	done()
	done() // must not drive inFlight negative
	if st := s.Stats(); st.InFlight != 0 || st.Shed[Critical] != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestAdaptiveLimit(t *testing.T) {
	tests := []struct {
		name      string
		latency   time.Duration
		rounds    int
		wantBelow int // limit must end strictly below this (0 = unchecked)
		wantAbove int // limit must end strictly above this (0 = unchecked)
	}{
		{"slow responses shrink the limit", 200 * time.Millisecond, 10, 20, 0},
		{"fast responses at full load grow it", 10 * time.Millisecond, 10, 0, 20},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clk := &fakeClock{t: time.Unix(0, 0)}
			s := New(Config{InitialLimit: 20, MaxLimit: 100, TargetLatency: 50 * time.Millisecond, Now: clk.Now})
			for r := 0; r < tt.rounds; r++ {
				var dones []func()
				for {
					d, err := s.Acquire(Critical)
					if err != nil {
						break
					}
					dones = append(dones, d)
				}
				clk.Add(tt.latency)
				for _, d := range dones {
					d()
				}
			}
			l := s.Stats().Limit
			if tt.wantBelow > 0 && l >= tt.wantBelow {
				t.Fatalf("limit = %d, want < %d", l, tt.wantBelow)
			}
			if tt.wantAbove > 0 && l <= tt.wantAbove {
				t.Fatalf("limit = %d, want > %d", l, tt.wantAbove)
			}
		})
	}
}

func TestLimitBounds(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	s := New(Config{InitialLimit: 5, MinLimit: 3, MaxLimit: 6, TargetLatency: time.Millisecond, Now: clk.Now})
	for i := 0; i < 100; i++ {
		d, err := s.Acquire(Critical)
		if err != nil {
			t.Fatal(err)
		}
		clk.Add(time.Second)
		d()
	}
	if l := s.Stats().Limit; l != 3 {
		t.Fatalf("limit = %d, want MinLimit 3", l)
	}
}

func TestIdleDoesNotInflate(t *testing.T) {
	s := New(Config{InitialLimit: 50, TargetLatency: time.Second})
	for i := 0; i < 1000; i++ {
		d, _ := s.Acquire(Critical)
		d()
	}
	if l := s.Stats().Limit; l != 50 {
		t.Fatalf("limit = %d, want 50 (one-at-a-time traffic is no evidence of capacity)", l)
	}
}

func TestConfigSanitised(t *testing.T) {
	s := New(Config{InitialLimit: -1, MinLimit: 10, MaxLimit: 2, Backoff: 7,
		Share: map[Priority]float64{Normal: 3, Sheddable: -1}})
	if l := s.Stats().Limit; l != 10 {
		t.Fatalf("limit = %d, want 10", l)
	}
	if _, err := s.Acquire(Sheddable); !errors.Is(err, ErrShed) {
		t.Fatal("share 0 should shed all sheddable traffic")
	}
}

func TestConcurrent(t *testing.T) {
	s := New(Config{InitialLimit: 8, TargetLatency: time.Millisecond})
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if d, err := s.Acquire(Priority(g % 3)); err == nil {
					d()
				}
			}
		}(g)
	}
	wg.Wait()
	if st := s.Stats(); st.InFlight != 0 {
		t.Fatalf("in-flight = %d after all done", st.InFlight)
	}
}
