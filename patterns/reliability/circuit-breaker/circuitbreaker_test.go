package circuitbreaker

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

var errBoom = errors.New("boom")

// step is one action in a scripted scenario.
type step struct {
	advance   time.Duration // move the clock before the call
	callErr   error         // what the protected call returns
	wantErr   error         // what Execute should return
	wantState State         // state after the call
}

func TestScenarios(t *testing.T) {
	tests := []struct {
		name  string
		s     Settings
		steps []step
	}{
		{
			name: "trips after threshold consecutive failures",
			s:    Settings{FailureThreshold: 3, OpenTimeout: time.Second},
			steps: []step{
				{callErr: errBoom, wantErr: errBoom, wantState: Closed},
				{callErr: errBoom, wantErr: errBoom, wantState: Closed},
				{callErr: errBoom, wantErr: errBoom, wantState: Open},
				{callErr: nil, wantErr: ErrOpen, wantState: Open},
			},
		},
		{
			name: "success resets consecutive failure count",
			s:    Settings{FailureThreshold: 2, OpenTimeout: time.Second},
			steps: []step{
				{callErr: errBoom, wantErr: errBoom, wantState: Closed},
				{callErr: nil, wantErr: nil, wantState: Closed},
				{callErr: errBoom, wantErr: errBoom, wantState: Closed},
				{callErr: errBoom, wantErr: errBoom, wantState: Open},
			},
		},
		{
			name: "half-open success closes",
			s:    Settings{FailureThreshold: 1, OpenTimeout: time.Second},
			steps: []step{
				{callErr: errBoom, wantErr: errBoom, wantState: Open},
				{advance: 999 * time.Millisecond, wantErr: ErrOpen, wantState: Open},
				{advance: time.Millisecond, callErr: nil, wantErr: nil, wantState: Closed},
			},
		},
		{
			name: "half-open failure reopens and restarts timer",
			s:    Settings{FailureThreshold: 1, OpenTimeout: time.Second},
			steps: []step{
				{callErr: errBoom, wantErr: errBoom, wantState: Open},
				{advance: time.Second, callErr: errBoom, wantErr: errBoom, wantState: Open},
				{advance: 500 * time.Millisecond, wantErr: ErrOpen, wantState: Open},
				{advance: 500 * time.Millisecond, callErr: nil, wantErr: nil, wantState: Closed},
			},
		},
		{
			name: "needs multiple probe successes",
			s:    Settings{FailureThreshold: 1, OpenTimeout: time.Second, SuccessThreshold: 2},
			steps: []step{
				{callErr: errBoom, wantErr: errBoom, wantState: Open},
				{advance: time.Second, callErr: nil, wantErr: nil, wantState: HalfOpen},
				{callErr: nil, wantErr: nil, wantState: Closed},
			},
		},
		{
			name: "ignored errors do not trip",
			s: Settings{FailureThreshold: 1, OpenTimeout: time.Second,
				IsFailure: func(err error) bool { return err != nil && !errors.Is(err, errBoom) }},
			steps: []step{
				{callErr: errBoom, wantErr: errBoom, wantState: Closed},
				{callErr: errBoom, wantErr: errBoom, wantState: Closed},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clk := &fakeClock{t: time.Unix(0, 0)}
			tt.s.Now = clk.Now
			b := New(tt.s)
			for i, st := range tt.steps {
				clk.Add(st.advance)
				err := b.Execute(func() error { return st.callErr })
				if !errors.Is(err, st.wantErr) || (st.wantErr == nil && err != nil) {
					t.Fatalf("step %d: err = %v, want %v", i, err, st.wantErr)
				}
				if got := b.State(); got != st.wantState {
					t.Fatalf("step %d: state = %v, want %v", i, got, st.wantState)
				}
			}
		})
	}
}

func TestHalfOpenLimitsProbes(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	b := New(Settings{FailureThreshold: 1, OpenTimeout: time.Second, Now: clk.Now})
	_ = b.Execute(func() error { return errBoom })
	clk.Add(time.Second)

	done, err := b.Allow()
	if err != nil {
		t.Fatalf("first probe: %v", err)
	}
	if _, err := b.Allow(); !errors.Is(err, ErrTooManyProbes) {
		t.Fatalf("second probe err = %v, want ErrTooManyProbes", err)
	}
	done(nil)
	done(errBoom) // duplicate report must be ignored
	if s := b.State(); s != Closed {
		t.Fatalf("state = %v, want closed", s)
	}
}

func TestStaleResultIgnored(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	b := New(Settings{FailureThreshold: 1, OpenTimeout: time.Second, Now: clk.Now})
	slow, _ := b.Allow()                           // starts while closed
	_ = b.Execute(func() error { return errBoom }) // trips to open
	clk.Add(time.Second)                           // -> half-open
	slow(nil)                                      // late success from the closed era
	if s := b.State(); s != HalfOpen {
		t.Fatalf("state = %v, want half-open (stale result must not close it)", s)
	}
}

func TestOnStateChange(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	var got []string
	b := New(Settings{FailureThreshold: 1, OpenTimeout: time.Second, Now: clk.Now,
		OnStateChange: func(from, to State) { got = append(got, from.String()+"->"+to.String()) }})
	_ = b.Execute(func() error { return errBoom })
	clk.Add(time.Second)
	_ = b.Execute(func() error { return nil })
	want := []string{"closed->open", "open->half-open", "half-open->closed"}
	if len(got) != len(want) {
		t.Fatalf("transitions = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("transitions = %v, want %v", got, want)
		}
	}
}

func TestConcurrent(t *testing.T) {
	b := New(Settings{FailureThreshold: 10, OpenTimeout: time.Millisecond})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_ = b.Execute(func() error {
					if (i+j)%3 == 0 {
						return errBoom
					}
					return nil
				})
				_ = b.State()
			}
		}(i)
	}
	wg.Wait()
}
