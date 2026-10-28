package healthcheck

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// newFakeClock reuses fakeClock from healthcheck_test.go.
func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func TestHeartbeatMonitorConfig(t *testing.T) {
	cases := []struct {
		interval time.Duration
		misses   int
	}{{0, 3}, {-time.Second, 3}, {time.Second, 0}, {time.Second, -1}}
	for _, c := range cases {
		if _, err := NewHeartbeatMonitor(c.interval, c.misses); !errors.Is(err, ErrHeartbeatConfig) {
			t.Errorf("NewHeartbeatMonitor(%v, %d) err=%v", c.interval, c.misses, err)
		}
	}
}

func TestHeartbeatDeadAfterKMissedIntervals(t *testing.T) {
	tests := []struct {
		name     string
		silence  time.Duration
		wantDead bool
		wantMiss int
	}{
		{"just beat", 0, false, 0},
		{"almost one interval", 999 * time.Millisecond, false, 0},
		{"one missed", time.Second, false, 1},
		{"two missed", 2500 * time.Millisecond, false, 2},
		{"three missed: dead", 3 * time.Second, true, 3},
		{"long gone", time.Minute, true, 60},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clk := newFakeClock()
			m, err := NewHeartbeatMonitor(time.Second, 3, WithHeartbeatClock(clk.Now))
			if err != nil {
				t.Fatal(err)
			}
			m.Beat("c1")
			clk.Add(tt.silence)
			if n, ok := m.Missed("c1"); !ok || n != tt.wantMiss {
				t.Fatalf("Missed = %d, %v; want %d", n, ok, tt.wantMiss)
			}
			dead := m.Sweep()
			if got := len(dead) == 1; got != tt.wantDead {
				t.Fatalf("dead = %v, want dead=%v", dead, tt.wantDead)
			}
			if tt.wantDead {
				if dead[0].ID != "c1" || dead[0].Missed != tt.wantMiss {
					t.Fatalf("dead[0] = %+v", dead[0])
				}
				if _, ok := m.Missed("c1"); ok || m.Len() != 0 {
					t.Fatal("a dead connection must be removed")
				}
			}
		})
	}
}

func TestHeartbeatBeatResetsCount(t *testing.T) {
	clk := newFakeClock()
	m, _ := NewHeartbeatMonitor(time.Second, 3, WithHeartbeatClock(clk.Now))
	m.Beat("flaky")
	// Miss two intervals, beat, miss two more: never three in a row, so never dead.
	for range 5 {
		clk.Add(2 * time.Second)
		if dead := m.Sweep(); len(dead) != 0 {
			t.Fatalf("flaky declared dead: %v", dead)
		}
		m.Beat("flaky")
	}
}

func TestHeartbeatCallbacks(t *testing.T) {
	clk := newFakeClock()
	var deadIDs []string
	late := map[string]int{}
	m, _ := NewHeartbeatMonitor(time.Second, 3,
		WithHeartbeatClock(clk.Now),
		WithOnDead(func(d DeadConn) { deadIDs = append(deadIDs, d.ID) }),
		WithOnLate(func(id string, missed int) { late[id] = missed }),
	)
	for _, id := range []string{"a", "b", "c"} {
		m.Beat(id)
	}
	clk.Add(1500 * time.Millisecond)
	m.Beat("a")
	clk.Add(1500 * time.Millisecond) // a: 1.5s, b and c: 3s
	m.Remove("c")                    // clean close: never reported dead
	dead := m.Sweep()
	if len(dead) != 1 || dead[0].ID != "b" || fmt.Sprint(deadIDs) != "[b]" {
		t.Fatalf("dead=%v callback=%v", dead, deadIDs)
	}
	if len(late) != 1 || late["a"] != 1 {
		t.Fatalf("late=%v", late)
	}
	// A second sweep must not report b again.
	if dead := m.Sweep(); len(dead) != 0 {
		t.Fatalf("second sweep: %v", dead)
	}
}

func TestHeartbeatRejoinAfterDeath(t *testing.T) {
	clk := newFakeClock()
	m, _ := NewHeartbeatMonitor(time.Second, 2, WithHeartbeatClock(clk.Now))
	m.Beat("x")
	clk.Add(2 * time.Second)
	if len(m.Sweep()) != 1 {
		t.Fatal("x should be dead")
	}
	m.Beat("x") // the client reconnected
	if n, ok := m.Missed("x"); !ok || n != 0 {
		t.Fatalf("Missed = %d, %v", n, ok)
	}
}

func TestHeartbeatMonitorConcurrent(t *testing.T) {
	clk := newFakeClock()
	m, _ := NewHeartbeatMonitor(time.Second, 3, WithHeartbeatClock(clk.Now))
	const conns = 50
	var wg sync.WaitGroup
	for i := range conns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := fmt.Sprintf("c%d", i)
			for range 100 {
				m.Beat(id)
				m.Missed(id)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 100 {
			clk.Add(time.Millisecond)
			m.Sweep()
		}
	}()
	wg.Wait()
	// The clock moved 100ms in total, far below 3 intervals: nobody can be dead.
	if m.Len() != conns {
		t.Fatalf("Len = %d, want %d", m.Len(), conns)
	}
	clk.Add(3 * time.Second)
	if dead := m.Sweep(); len(dead) != conns {
		t.Fatalf("after silence, dead = %d, want %d", len(dead), conns)
	}
}
