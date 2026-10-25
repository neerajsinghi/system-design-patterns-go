package ratelimitertokenbucket

import (
	"context"
	"errors"
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

func TestAllowSequence(t *testing.T) {
	type call struct {
		advance time.Duration
		n       int
		want    bool
	}
	tests := []struct {
		name  string
		rate  float64
		burst int
		calls []call
	}{
		{"burst then deny", 1, 3, []call{{0, 1, true}, {0, 1, true}, {0, 1, true}, {0, 1, false}}},
		{"refills over time", 10, 1, []call{{0, 1, true}, {0, 1, false}, {50 * time.Millisecond, 1, false}, {50 * time.Millisecond, 1, true}}},
		{"refill capped at burst", 100, 2, []call{{time.Hour, 2, true}, {0, 1, false}}},
		{"AllowN all or nothing", 1, 5, []call{{0, 3, true}, {0, 3, false}, {0, 2, true}}},
		{"n larger than burst", 100, 2, []call{{0, 3, false}, {0, 2, true}}},
		{"zero or negative n always allowed", 1, 1, []call{{0, 0, true}, {0, -1, true}, {0, 1, true}}},
		{"zero rate never refills", 0, 1, []call{{0, 1, true}, {time.Hour, 1, false}}},
		{"negative rate treated as zero", -5, 1, []call{{0, 1, true}, {time.Hour, 1, false}}},
		{"burst below one treated as one", 1, 0, []call{{0, 1, true}, {0, 1, false}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clk := &fakeClock{t: time.Unix(0, 0)}
			l := New(tt.rate, tt.burst, clk.Now)
			for i, c := range tt.calls {
				clk.Add(c.advance)
				if got := l.AllowN(c.n); got != c.want {
					t.Fatalf("call %d AllowN(%d) = %v, want %v (tokens %.2f)", i, c.n, got, c.want, l.Tokens())
				}
			}
		})
	}
}

func TestReserve(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	l := New(10, 2, clk.Now) // 1 token per 100ms
	if d, _, ok := l.Reserve(2); !ok || d != 0 {
		t.Fatalf("first reserve = %v, %v", d, ok)
	}
	d, cancel, ok := l.Reserve(1)
	if !ok || d != 100*time.Millisecond {
		t.Fatalf("second reserve delay = %v, want 100ms", d)
	}
	cancel()
	cancel() // idempotent
	if tk := l.Tokens(); tk != 0 {
		t.Fatalf("tokens after cancel = %v, want 0", tk)
	}
	if _, _, ok := l.Reserve(3); ok {
		t.Fatal("reserve beyond burst must fail")
	}
}

func TestWait(t *testing.T) {
	l := New(1000, 1, nil) // 1 token per ms, real clock
	if err := l.Wait(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := l.Wait(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 50*time.Millisecond {
		t.Fatalf("waited too long: %v", time.Since(start))
	}
	if err := l.Wait(context.Background(), 2); !errors.Is(err, ErrExceedsBurst) {
		t.Fatalf("err = %v, want ErrExceedsBurst", err)
	}

	slow := New(0.001, 1, nil)
	_ = slow.Allow()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := slow.Wait(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}

	cctx, ccancel := context.WithCancel(context.Background())
	l2 := New(1, 1, nil)
	_ = l2.Allow()
	go func() { time.Sleep(2 * time.Millisecond); ccancel() }()
	if err := l2.Wait(cctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want Canceled", err)
	}
}

func TestConcurrentNeverOverspends(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	l := New(1, 100, clk.Now)
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if l.Allow() {
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if a := allowed.Load(); a != 100 {
		t.Fatalf("allowed = %d, want exactly burst (100)", a)
	}
}

func TestKeyed(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	k := NewKeyed(1, 1, clk.Now)
	if !k.Allow("alice") || k.Allow("alice") {
		t.Fatal("alice should get exactly one token")
	}
	if !k.Allow("bob") {
		t.Fatal("bob has his own bucket")
	}
	clk.Add(time.Minute)
	_ = k.Allow("bob")
	if n := k.Sweep(30 * time.Second); n != 1 || k.Len() != 1 {
		t.Fatalf("sweep removed %d, len %d; want 1, 1", n, k.Len())
	}
}
