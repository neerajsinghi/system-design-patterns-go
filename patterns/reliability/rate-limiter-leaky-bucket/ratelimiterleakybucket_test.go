package ratelimiterleakybucket

import (
	"context"
	"errors"
	"math"
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

func TestNewValidation(t *testing.T) {
	tests := []struct {
		rate    float64
		wantErr bool
	}{{10, false}, {0, true}, {-1, true}, {math.NaN(), true}, {1e12, false}}
	for _, tt := range tests {
		_, err := New(tt.rate, 1, nil)
		if (err != nil) != tt.wantErr {
			t.Fatalf("New(%v) err = %v, wantErr %v", tt.rate, err, tt.wantErr)
		}
	}
}

func TestReserveSchedule(t *testing.T) {
	type call struct {
		advance   time.Duration
		wantDelay time.Duration
		wantErr   error
	}
	tests := []struct {
		name     string
		rate     float64
		capacity int
		calls    []call
	}{
		{
			name: "burst is spaced out then overflows", rate: 10, capacity: 2,
			calls: []call{
				{0, 0, nil},
				{0, 100 * time.Millisecond, nil},
				{0, 200 * time.Millisecond, nil},
				{0, 0, ErrOverflow},
			},
		},
		{
			name: "queue drains over time", rate: 10, capacity: 1,
			calls: []call{
				{0, 0, nil},
				{0, 100 * time.Millisecond, nil},
				{0, 0, ErrOverflow},
				{100 * time.Millisecond, 100 * time.Millisecond, nil},
			},
		},
		{
			name: "idle bucket does not bank credit", rate: 10, capacity: 1,
			calls: []call{
				{0, 0, nil},
				{time.Hour, 0, nil},
				{0, 100 * time.Millisecond, nil},
			},
		},
		{
			name: "capacity zero is strict pacing", rate: 10, capacity: 0,
			calls: []call{
				{0, 0, nil},
				{0, 0, ErrOverflow},
				{50 * time.Millisecond, 0, ErrOverflow},
				{50 * time.Millisecond, 0, nil},
			},
		},
		{
			name: "negative capacity treated as zero", rate: 10, capacity: -4,
			calls: []call{{0, 0, nil}, {0, 0, ErrOverflow}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clk := &fakeClock{t: time.Unix(0, 0)}
			l, err := New(tt.rate, tt.capacity, clk.Now)
			if err != nil {
				t.Fatal(err)
			}
			for i, c := range tt.calls {
				clk.Add(c.advance)
				d, err := l.Reserve()
				if !errors.Is(err, c.wantErr) || (c.wantErr == nil && err != nil) {
					t.Fatalf("call %d err = %v, want %v", i, err, c.wantErr)
				}
				if d != c.wantDelay {
					t.Fatalf("call %d delay = %v, want %v", i, d, c.wantDelay)
				}
			}
		})
	}
}

func TestAllowAndQueued(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	l, _ := New(4, 3, clk.Now) // every 250ms
	if !l.Allow() || l.Allow() {
		t.Fatal("Allow should pass once then pace")
	}
	for i := 0; i < 3; i++ {
		if _, err := l.Reserve(); err != nil {
			t.Fatal(err)
		}
	}
	if q := l.Queued(); q != 3 {
		t.Fatalf("queued = %d, want 3", q)
	}
	clk.Add(500 * time.Millisecond)
	if q := l.Queued(); q != 1 {
		t.Fatalf("queued after 500ms = %d, want 1", q)
	}
	if l.Allow() {
		t.Fatal("Allow must not jump the queue")
	}
}

func TestOutputRateUnderConcurrency(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	l, _ := New(100, 1000, clk.Now) // 10ms apart
	var mu sync.Mutex
	seen := map[time.Duration]bool{}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, err := l.Reserve()
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			seen[d] = true
			mu.Unlock()
		}()
	}
	wg.Wait()
	for i := 0; i < 50; i++ {
		if !seen[time.Duration(i)*10*time.Millisecond] {
			t.Fatalf("missing release slot %d: slots must be unique and evenly spaced", i)
		}
	}
}

func TestWait(t *testing.T) {
	l, _ := New(1000, 5, nil) // 1ms apart, real clock
	for i := 0; i < 3; i++ {
		if err := l.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	slow, _ := New(0.5, 5, nil) // 2s apart
	_ = slow.Allow()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := slow.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
}
