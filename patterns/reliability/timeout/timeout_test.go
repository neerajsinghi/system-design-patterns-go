package timeout

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBudget(t *testing.T) {
	now := time.Unix(1000, 0)
	tests := []struct {
		name        string
		p           HopPolicy
		remaining   time.Duration
		hasDeadline bool
		want        time.Duration
		wantErr     bool
	}{
		{"no deadline uses default", HopPolicy{Default: 300 * time.Millisecond}, 0, false, 300 * time.Millisecond, false},
		{"no deadline zero default", HopPolicy{}, 0, false, time.Second, false},
		{"no deadline capped by max", HopPolicy{Default: time.Second, Max: 200 * time.Millisecond}, 0, false, 200 * time.Millisecond, false},
		{"remaining minus reserve", HopPolicy{Reserve: 50 * time.Millisecond}, 500 * time.Millisecond, true, 450 * time.Millisecond, false},
		{"capped by max", HopPolicy{Max: 100 * time.Millisecond}, 500 * time.Millisecond, true, 100 * time.Millisecond, false},
		{"below min fails fast", HopPolicy{Reserve: 50 * time.Millisecond, Min: 20 * time.Millisecond}, 60 * time.Millisecond, true, 0, true},
		{"deadline already passed", HopPolicy{}, -time.Second, true, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.p.Budget(now.Add(tt.remaining), tt.hasDeadline, now)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && !errors.Is(err, ErrInsufficientBudget) {
				t.Fatalf("err = %v, want ErrInsufficientBudget", err)
			}
			if got != tt.want {
				t.Fatalf("budget = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWithHop(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	hctx, hcancel, err := WithHop(parent, HopPolicy{Max: time.Minute})
	defer hcancel()
	if err != nil {
		t.Fatal(err)
	}
	dl, ok := hctx.Deadline()
	if !ok || time.Until(dl) > time.Minute {
		t.Fatalf("hop deadline %v not capped at 1m", time.Until(dl))
	}

	short, cancel2 := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel2()
	_, c, err := WithHop(short, HopPolicy{Min: time.Second})
	c() // must be safe to call on error
	if !errors.Is(err, ErrInsufficientBudget) {
		t.Fatalf("err = %v, want ErrInsufficientBudget", err)
	}
}

func TestRun(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
		fn      func(ctx context.Context) (int, error)
		want    int
		wantErr error
	}{
		{"fast fn", time.Second, func(context.Context) (int, error) { return 7, nil }, 7, nil},
		{"fn error", time.Second, func(context.Context) (int, error) { return 0, errors.New("x") }, 0, nil},
		{"fn ignores ctx", 5 * time.Millisecond, func(context.Context) (int, error) {
			time.Sleep(50 * time.Millisecond)
			return 1, nil
		}, 0, context.DeadlineExceeded},
		{"fn honours ctx", 5 * time.Millisecond, func(ctx context.Context) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		}, 0, context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), tt.timeout)
			defer cancel()
			start := time.Now()
			got, err := Run(ctx, tt.fn)
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("got %d, want %d", got, tt.want)
			}
			if time.Since(start) > 40*time.Millisecond {
				t.Fatalf("Run did not return promptly: %v", time.Since(start))
			}
		})
	}
}

func TestRunAlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	_, err := Run(ctx, func(context.Context) (int, error) { called = true; return 0, nil })
	if !errors.Is(err, context.Canceled) || called {
		t.Fatalf("err = %v called = %v", err, called)
	}
}

func TestHeaderRoundTrip(t *testing.T) {
	now := time.Now()
	ctx, cancel := context.WithDeadline(context.Background(), now.Add(1500*time.Millisecond))
	defer cancel()
	v, ok := FormatRemaining(ctx, now)
	if !ok || v != "1500" {
		t.Fatalf("FormatRemaining = %q, %v", v, ok)
	}
	if _, ok := FormatRemaining(context.Background(), now); ok {
		t.Fatal("no deadline should report ok=false")
	}

	tests := []struct {
		value        string
		wantDeadline bool
	}{
		{"250", true},
		{"0", true},
		{"", false},
		{"abc", false},
		{"-5", false},
		{"99999999999999", false},
	}
	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			c, cancel := FromHeader(context.Background(), tt.value)
			defer cancel()
			if _, ok := c.Deadline(); ok != tt.wantDeadline {
				t.Fatalf("deadline set = %v, want %v", ok, tt.wantDeadline)
			}
		})
	}
}
