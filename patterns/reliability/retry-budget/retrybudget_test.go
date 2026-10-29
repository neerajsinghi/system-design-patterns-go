package retrybudget

import (
	"context"
	"errors"
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

func (f *fakeClock) Now() time.Time          { f.mu.Lock(); defer f.mu.Unlock(); return f.t }
func (f *fakeClock) Advance(d time.Duration) { f.mu.Lock(); f.t = f.t.Add(d); f.mu.Unlock() }

func newClock() *fakeClock { return &fakeClock{t: time.Unix(1_000, 0)} }

var errDown = errors.New("sms provider: 503")

func TestNewValidation(t *testing.T) {
	tests := []struct {
		name  string
		ratio float64
		opts  []Option
	}{
		{"negative ratio", -0.1, nil},
		{"ratio too large", 11, nil},
		{"NaN ratio", math.NaN(), nil},
		{"max balance below 1", 0.1, []Option{WithMaxBalance(0.5)}},
		{"infinite max balance", 0.1, []Option{WithMaxBalance(math.Inf(1))}},
		{"negative floor", 0.1, []Option{WithMinRetriesPerSecond(-1)}},
		{"NaN floor", 0.1, []Option{WithMinRetriesPerSecond(math.NaN())}},
		{"nothing could ever retry", 0, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.ratio, tt.opts...); err == nil {
				t.Fatal("expected error")
			}
		})
	}
	if _, err := New(0, WithMinRetriesPerSecond(1)); err != nil {
		t.Fatalf("floor-only budget should be valid: %v", err)
	}
}

func TestDoValidation(t *testing.T) {
	b, _ := New(0.1)
	if err := b.Do(context.Background(), 0, func(context.Context) error { return nil }); err == nil {
		t.Fatal("expected error for maxAttempts 0")
	}
	if err := b.Do(context.Background(), 3, nil); err == nil {
		t.Fatal("expected error for nil fn")
	}
}

func TestRefillsWithRequests(t *testing.T) {
	b, _ := New(0.1)
	if b.Allow() {
		t.Fatal("a new budget without a floor has no tokens")
	}
	for range 9 {
		b.OnRequest()
	}
	if b.Allow() {
		t.Fatalf("9 requests deposit 0.9 tokens, balance %.2f should not pay for a retry", b.Balance())
	}
	b.OnRequest()
	if !b.Allow() {
		t.Fatal("10 requests at ratio 0.1 should pay for one retry")
	}
	if b.Allow() {
		t.Fatal("the one retry should have spent the balance")
	}
	if got, want := b.Stats(), (Stats{Requests: 10, Retries: 1, Denied: 3}); got != want {
		t.Fatalf("stats = %+v, want %+v", got, want)
	}
}

func TestDeniedOnceSpent(t *testing.T) {
	b, _ := New(0.5, WithMaxBalance(100))
	for range 20 {
		b.OnRequest() // 10 tokens
	}
	allowed := 0
	for range 50 {
		if b.Allow() {
			allowed++
		}
	}
	if allowed != 10 {
		t.Fatalf("allowed %d retries, want 10", allowed)
	}
}

func TestMaxBalanceCaps(t *testing.T) {
	b, _ := New(0.1, WithMaxBalance(3))
	for range 1000 { // would be 100 tokens without the cap
		b.OnRequest()
	}
	if got := b.Balance(); math.Abs(got-3) > 1e-9 {
		t.Fatalf("balance = %.2f, want capped at 3", got)
	}
	allowed := 0
	for range 10 {
		if b.Allow() {
			allowed++
		}
	}
	if allowed != 3 {
		t.Fatalf("allowed %d retries after cap, want 3", allowed)
	}
}

func TestMinRetriesFloor(t *testing.T) {
	clock := newClock()
	b, _ := New(0, WithMinRetriesPerSecond(2), WithClock(clock.Now))

	// The floor starts full: one second's worth.
	if !b.Allow() || !b.Allow() || b.Allow() {
		t.Fatal("want exactly 2 retries from a full floor of 2/s")
	}
	clock.Advance(250 * time.Millisecond) // +0.5 token
	if b.Allow() {
		t.Fatal("half a token should not pay for a retry")
	}
	clock.Advance(250 * time.Millisecond) // +0.5 token = 1
	if !b.Allow() {
		t.Fatal("a full token should have accrued after 500ms at 2/s")
	}

	// A long idle period accrues only one second's worth.
	clock.Advance(time.Hour)
	allowed := 0
	for range 10 {
		if b.Allow() {
			allowed++
		}
	}
	if allowed != 2 {
		t.Fatalf("after an hour idle allowed %d, want 2 (floor capped at 1s worth)", allowed)
	}

	// Over 10 simulated seconds of constant demand, ~2/s get through.
	allowed = 0
	for range 100 {
		clock.Advance(100 * time.Millisecond)
		for b.Allow() {
			allowed++
		}
	}
	if allowed < 19 || allowed > 20 {
		t.Fatalf("allowed %d retries in 10s at 2/s, want ~20", allowed)
	}
}

func TestFloorIsBelowOneToken(t *testing.T) {
	clock := newClock()
	b, _ := New(0.1, WithMinRetriesPerSecond(0.1), WithClock(clock.Now))
	if !b.Allow() {
		t.Fatal("a sub-1/s floor still holds one token at start")
	}
	clock.Advance(5 * time.Second)
	if b.Allow() {
		t.Fatal("0.1/s for 5s is half a token")
	}
	clock.Advance(5 * time.Second)
	if !b.Allow() {
		t.Fatal("0.1/s for 10s is one token")
	}
}

func TestClockGoingBackwards(t *testing.T) {
	clock := newClock()
	b, _ := New(0, WithMinRetriesPerSecond(1), WithClock(clock.Now))
	b.Allow()
	clock.Advance(-time.Hour)
	if b.Allow() {
		t.Fatal("a clock moving backwards must not refill the floor")
	}
	clock.Advance(time.Hour) // back to where we were: still no time elapsed
	if b.Allow() {
		t.Fatal("returning to the old time must not refill the floor")
	}
}

func TestDo(t *testing.T) {
	tests := []struct {
		name      string
		failures  int // fn fails this many times, then succeeds
		attempts  int
		prefill   int // OnRequest calls before Do (each deposits 0.5)
		wantCalls int
		wantErr   error
	}{
		{"success first try", 0, 3, 0, 1, nil},
		{"retries allowed then success", 2, 3, 4, 3, nil},
		{"attempts exhausted", 5, 3, 10, 3, errDown},
		{"budget refuses", 5, 3, 0, 1, ErrBudgetExhausted},
		{"budget allows one retry only", 5, 3, 1, 2, ErrBudgetExhausted},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, _ := New(0.5, WithMaxBalance(100))
			for range tt.prefill {
				b.OnRequest()
			}
			calls := 0
			err := b.Do(context.Background(), tt.attempts, func(context.Context) error {
				calls++
				if calls <= tt.failures {
					return errDown
				}
				return nil
			})
			if calls != tt.wantCalls {
				t.Errorf("calls = %d, want %d", calls, tt.wantCalls)
			}
			if tt.wantErr == nil && err != nil {
				t.Errorf("err = %v, want nil", err)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
			if errors.Is(err, ErrBudgetExhausted) && !errors.Is(err, errDown) {
				t.Errorf("budget error should also wrap the last attempt's error: %v", err)
			}
		})
	}
}

func TestDoStopsOnContextErrors(t *testing.T) {
	b, _ := New(1, WithMaxBalance(100))
	for range 10 {
		b.OnRequest()
	}
	calls := 0
	err := b.Do(context.Background(), 5, func(context.Context) error {
		calls++
		return context.DeadlineExceeded
	})
	if calls != 1 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("calls=%d err=%v; a deadline error must not be retried", calls, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	calls = 0
	err = b.Do(ctx, 5, func(context.Context) error {
		calls++
		cancel()
		return errDown
	})
	if calls != 1 || !errors.Is(err, context.Canceled) || !errors.Is(err, errDown) {
		t.Fatalf("calls=%d err=%v; a cancelled ctx stops retries", calls, err)
	}
}

// TestRatioUnderConcurrency checks that, with a dependency that always fails,
// concurrent callers sharing a budget send no more than ratio extra calls
// (plus the cap) however the goroutines interleave.
func TestRatioUnderConcurrency(t *testing.T) {
	const (
		workers  = 16
		perWork  = 500
		ratio    = 0.1
		capacity = 5
	)
	b, _ := New(ratio, WithMaxBalance(capacity))
	var calls atomic.Int64
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perWork {
				_ = b.Do(context.Background(), 3, func(context.Context) error {
					calls.Add(1)
					return errDown
				})
			}
		}()
	}
	wg.Wait()

	requests := int64(workers * perWork)
	retries := calls.Load() - requests
	maxRetries := int64(ratio*float64(requests)) + capacity
	if retries > maxRetries {
		t.Fatalf("%d retries for %d requests, want <= %d", retries, requests, maxRetries)
	}
	if retries < int64(ratio*float64(requests))-capacity {
		t.Fatalf("only %d retries for %d requests; the budget should be spent", retries, requests)
	}
	st := b.Stats()
	if st.Requests != uint64(requests) || int64(st.Retries) != retries {
		t.Fatalf("stats %+v disagree with %d requests and %d retries", st, requests, retries)
	}
}

// TestSporadicFailuresStillRetried: in a healthy period where 2% of calls
// fail, a 10% budget never gets in the way.
func TestSporadicFailuresStillRetried(t *testing.T) {
	b, _ := New(0.1)
	n := 0
	failed := 0
	for range 5000 {
		err := b.Do(context.Background(), 3, func(context.Context) error {
			n++
			if n%50 == 0 { // every 50th call fails: 2%
				return errDown
			}
			return nil
		})
		if err != nil {
			failed++
		}
	}
	if st := b.Stats(); st.Denied != 0 || failed != 0 {
		t.Fatalf("denied %d retries and failed %d requests in a healthy period", st.Denied, failed)
	}
}
