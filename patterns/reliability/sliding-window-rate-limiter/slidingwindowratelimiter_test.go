package slidingwindowratelimiter

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Add(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

type limiter interface{ Allow() bool }

type call struct {
	advance time.Duration
	want    bool
}

func run(t *testing.T, l limiter, clk *fakeClock, calls []call) {
	t.Helper()
	for i, c := range calls {
		clk.Add(c.advance)
		if got := l.Allow(); got != c.want {
			t.Fatalf("call %d (t=%v): Allow = %v, want %v", i, clk.Now().Sub(time.Unix(0, 0)), got, c.want)
		}
	}
}

func TestLog(t *testing.T) {
	tests := []struct {
		name  string
		limit int
		calls []call
	}{
		{"limit then deny", 2, []call{{0, true}, {0, true}, {0, false}}},
		{"entry expires exactly at window", 1, []call{{0, true}, {999 * time.Millisecond, false}, {time.Millisecond, true}}},
		{"no boundary double-spend", 3, []call{
			{900 * time.Millisecond, true}, {0, true}, {0, true}, // end of one second
			{200 * time.Millisecond, false}, // t=1.1s: the three at 0.9s still count
			{800 * time.Millisecond, true},  // t=1.9s: those expired
		}},
		{"zero limit denies all", 0, []call{{0, false}, {time.Hour, false}}},
		{"negative limit denies all", -3, []call{{0, false}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clk := &fakeClock{t: time.Unix(0, 0)}
			run(t, NewLog(tt.limit, time.Second, clk.Now), clk, tt.calls)
		})
	}
}

func TestLogRetryAfter(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	l := NewLog(2, time.Second, clk.Now)
	l.Allow()
	clk.Add(300 * time.Millisecond)
	l.Allow()
	if d := l.RetryAfter(); d != 700*time.Millisecond {
		t.Fatalf("RetryAfter = %v, want 700ms", d)
	}
	clk.Add(700 * time.Millisecond)
	if d := l.RetryAfter(); d != 0 {
		t.Fatalf("RetryAfter = %v, want 0", d)
	}
	if d := NewLog(0, time.Second, clk.Now).RetryAfter(); d != time.Second {
		t.Fatalf("zero-limit RetryAfter = %v, want window", d)
	}
}

func TestCounter(t *testing.T) {
	tests := []struct {
		name  string
		limit int
		calls []call
	}{
		{"limit then deny", 2, []call{{0, true}, {0, true}, {0, false}}},
		{"previous window weighs in", 4, []call{
			{0, true}, {0, true}, {0, true}, {0, true}, // 4 in window [0,1s)
			{1250 * time.Millisecond, true}, // est = 4*0.75 + 0 = 3 -> allow (now 4)
			{0, false},                      // est = 3 + 1 = 4 -> deny
			{500 * time.Millisecond, true},  // t=1.75: est = 4*0.25 + 1 = 2 -> allow
		}},
		{"long silence resets both windows", 1, []call{{0, true}, {5 * time.Second, true}, {0, false}}},
		{"zero limit denies all", 0, []call{{0, false}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clk := &fakeClock{t: time.Unix(0, 0)}
			run(t, NewCounter(tt.limit, time.Second, clk.Now), clk, tt.calls)
		})
	}
}

func TestCounterEstimate(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	c := NewCounter(100, time.Second, clk.Now)
	for i := 0; i < 10; i++ {
		c.Allow()
	}
	clk.Add(1100 * time.Millisecond)
	if e := c.Estimate(); math.Abs(e-9) > 1e-9 {
		t.Fatalf("estimate = %v, want 9 (10 * 0.9)", e)
	}
}

func TestConcurrentExactLimit(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	for _, l := range []limiter{NewLog(50, time.Second, clk.Now), NewCounter(50, time.Second, clk.Now)} {
		var ok atomic.Int64
		var wg sync.WaitGroup
		for g := 0; g < 10; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 20; i++ {
					if l.Allow() {
						ok.Add(1)
					}
				}
			}()
		}
		wg.Wait()
		if ok.Load() != 50 {
			t.Fatalf("%T allowed %d, want 50", l, ok.Load())
		}
	}
}
