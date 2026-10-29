package fallback

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	errSMSDown   = errors.New("503 from provider")
	errBadNumber = errors.New("invalid phone number")
)

// script is a fake provider: it fails while down is set, takes cost of
// simulated time on the clock, and records every call.
type script struct {
	name string
	clk  *fakeClock
	cost time.Duration

	mu    sync.Mutex
	down  bool
	err   error // returned while down; defaults to errSMSDown
	calls int
}

func (s *script) setDown(down bool) { s.mu.Lock(); s.down = down; s.mu.Unlock() }

func (s *script) provider(log *[]string, logMu *sync.Mutex) Provider[string, string] {
	return Provider[string, string]{
		Name: s.name,
		Call: func(ctx context.Context, to string) (string, error) {
			if log != nil {
				logMu.Lock()
				*log = append(*log, s.name)
				logMu.Unlock()
			}
			s.mu.Lock()
			s.calls++
			down, err := s.down, s.err
			s.mu.Unlock()
			if s.clk != nil {
				s.clk.Add(s.cost)
			}
			if to == "bad" {
				return "", Permanent(errBadNumber)
			}
			if down {
				if err == nil {
					err = errSMSDown
				}
				return "", err
			}
			return s.name + "->" + to, nil
		},
	}
}

func (s *script) Calls() int { s.mu.Lock(); defer s.mu.Unlock(); return s.calls }

func newChain(t *testing.T, opt ChainOptions, ss ...*script) (*Chain[string, string], *[]string) {
	t.Helper()
	var log []string
	var mu sync.Mutex
	ps := make([]Provider[string, string], len(ss))
	for i, s := range ss {
		ps[i] = s.provider(&log, &mu)
	}
	c, err := NewChain(ps, opt)
	if err != nil {
		t.Fatal(err)
	}
	return c, &log
}

func TestNewChainValidation(t *testing.T) {
	ok := func(context.Context, string) (string, error) { return "", nil }
	tests := []struct {
		name string
		ps   []Provider[string, string]
		opt  ChainOptions
		want string
	}{
		{"no providers", nil, ChainOptions{}, "at least one"},
		{"unnamed", []Provider[string, string]{{Call: ok}}, ChainOptions{}, "no name"},
		{"duplicate", []Provider[string, string]{{Name: "a", Call: ok}, {Name: "a", Call: ok}}, ChainOptions{}, "duplicate"},
		{"nil call", []Provider[string, string]{{Name: "a"}}, ChainOptions{}, "no Call"},
		{"negative timeout", []Provider[string, string]{{Name: "a", Call: ok, Timeout: -1}}, ChainOptions{}, "negative timeout"},
		{"negative deadline", []Provider[string, string]{{Name: "a", Call: ok}}, ChainOptions{Deadline: -1}, "negative chain deadline"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewChain(tt.ps, tt.opt)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tt.want)
			}
		})
	}
}

func TestChainOrder(t *testing.T) {
	tests := []struct {
		name      string
		down      []bool // per provider: a, b, voice
		wantLog   string
		wantBy    string
		wantErrs  int // failed attempts joined into Errors
		wantError bool
	}{
		{"primary serves", []bool{false, false, false}, "a", "a", 0, false},
		{"secondary serves", []bool{true, false, false}, "a,b", "b", 1, false},
		{"voice serves", []bool{true, true, false}, "a,b,voice", "voice", 2, false},
		{"all fail", []bool{true, true, true}, "a,b,voice", "", 3, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, b, v := &script{name: "a"}, &script{name: "b"}, &script{name: "voice"}
			for i, s := range []*script{a, b, v} {
				s.setDown(tt.down[i])
			}
			c, log := newChain(t, ChainOptions{FailureThreshold: -1}, a, b, v)
			res, err := c.Do(context.Background(), "+15550100")
			if got := strings.Join(*log, ","); got != tt.wantLog {
				t.Fatalf("attempt order = %s, want %s", got, tt.wantLog)
			}
			if (err != nil) != tt.wantError {
				t.Fatalf("err = %v", err)
			}
			if res.Provider != tt.wantBy {
				t.Fatalf("served by %q, want %q", res.Provider, tt.wantBy)
			}
			if tt.wantBy != "" && res.Value != tt.wantBy+"->+15550100" {
				t.Fatalf("value = %q", res.Value)
			}
			if n := strings.Count(fmt.Sprint(res.Errors), errSMSDown.Error()); n != tt.wantErrs {
				t.Fatalf("joined %d failures, want %d: %v", n, tt.wantErrs, res.Errors)
			}
			if len(res.Attempts) != len(*log) {
				t.Fatalf("attempts = %+v", res.Attempts)
			}
		})
	}
}

func TestChainErrorsJoined(t *testing.T) {
	errA, errB := errors.New("a timed out"), errors.New("b quota exceeded")
	a := &script{name: "a", down: true, err: errA}
	b := &script{name: "b", down: true, err: errB}
	c, _ := newChain(t, ChainOptions{}, a, b)
	res, err := c.Do(context.Background(), "+1")
	if !errors.Is(err, errA) || !errors.Is(err, errB) {
		t.Fatalf("err = %v, want both provider errors", err)
	}
	if !errors.Is(res.Errors, errA) || !errors.Is(res.Errors, errB) {
		t.Fatalf("res.Errors = %v", res.Errors)
	}
	if !strings.Contains(err.Error(), "a: a timed out") || !strings.Contains(err.Error(), "b: b quota exceeded") {
		t.Fatalf("errors should be prefixed with provider names: %q", err)
	}

	// Served by the second: the first's error is still reported.
	b.setDown(false)
	res, err = c.Do(context.Background(), "+1")
	if err != nil || res.Provider != "b" || !errors.Is(res.Errors, errA) {
		t.Fatalf("res = %+v, err = %v", res, err)
	}
}

func TestChainNonRetryableStops(t *testing.T) {
	a, b, v := &script{name: "a"}, &script{name: "b"}, &script{name: "voice"}
	c, log := newChain(t, ChainOptions{FailureThreshold: 1}, a, b, v)
	res, err := c.Do(context.Background(), "bad")
	if !errors.Is(err, errBadNumber) || !IsPermanent(err) {
		t.Fatalf("err = %v, want permanent invalid number", err)
	}
	if got := strings.Join(*log, ","); got != "a" {
		t.Fatalf("attempts = %s, want only a", got)
	}
	if res.Provider != "" {
		t.Fatalf("served by %q", res.Provider)
	}
	// A permanent error is the caller's fault, not the provider's: even with
	// threshold 1 the provider stays healthy.
	if !c.Healthy("a") {
		t.Fatal("a non-retryable error must not put the provider in cooldown")
	}

	// A custom Retryable can make any error stop the chain.
	stop := errors.New("quota for this user exhausted")
	a.mu.Lock()
	a.down, a.err = true, stop
	a.mu.Unlock()
	c2, log2 := newChain(t, ChainOptions{Retryable: func(err error) bool { return !errors.Is(err, stop) }}, a, b)
	if _, err := c2.Do(context.Background(), "+1"); !errors.Is(err, stop) || len(*log2) != 1 {
		t.Fatalf("err = %v, log = %v", err, *log2)
	}
}

func TestPermanentNil(t *testing.T) {
	if Permanent(nil) != nil || IsPermanent(nil) || IsPermanent(errSMSDown) {
		t.Fatal("Permanent/IsPermanent mishandle plain errors")
	}
	if w := fmt.Errorf("wrapped: %w", Permanent(errBadNumber)); !IsPermanent(w) || !errors.Is(w, errBadNumber) {
		t.Fatal("Permanent must survive wrapping and keep the cause")
	}
}

func TestChainCooldownSkipAndRecovery(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	a := &script{name: "a", clk: clk, cost: 3 * time.Second, down: true}
	b := &script{name: "b", clk: clk, cost: 100 * time.Millisecond}
	c, _ := newChain(t, ChainOptions{FailureThreshold: 2, Cooldown: time.Minute, Now: clk.Now}, a, b)

	for i := range 2 {
		res, err := c.Do(context.Background(), "+1")
		if err != nil || res.Provider != "b" {
			t.Fatalf("request %d: %+v %v", i, res, err)
		}
	}
	if c.Healthy("a") {
		t.Fatal("a should be cooling down after 2 consecutive failures")
	}

	// While cooling down, a is skipped: no call, no 3s timeout paid.
	before := clk.Now()
	res, err := c.Do(context.Background(), "+1")
	if err != nil || res.Provider != "b" || a.Calls() != 2 {
		t.Fatalf("res = %+v, err = %v, a calls = %d", res, err, a.Calls())
	}
	if !res.Attempts[0].Skipped || res.Attempts[0].Provider != "a" || res.Errors != nil {
		t.Fatalf("attempts = %+v, errors = %v", res.Attempts, res.Errors)
	}
	if took := clk.Now().Sub(before); took != 100*time.Millisecond {
		t.Fatalf("request took %v, want only b's 100ms", took)
	}

	// After the cooldown one probe goes to a; it is still down, so it goes
	// straight back into cooldown (one failure is enough when probing).
	clk.Add(time.Minute)
	if !c.Healthy("a") {
		t.Fatal("a should be probeable after the cooldown")
	}
	if res, _ := c.Do(context.Background(), "+1"); res.Provider != "b" || a.Calls() != 3 {
		t.Fatalf("probe: %+v, a calls = %d", res, a.Calls())
	}
	if c.Healthy("a") {
		t.Fatal("a failed its probe and should be cooling down again")
	}

	// a recovers; after the next cooldown the probe succeeds and a is primary again.
	a.setDown(false)
	clk.Add(time.Minute)
	for i := range 3 {
		res, err := c.Do(context.Background(), "+1")
		if err != nil || res.Provider != "a" {
			t.Fatalf("after recovery %d: %+v %v", i, res, err)
		}
	}
	if !c.Healthy("a") || c.Healthy("nope") {
		t.Fatal("Healthy is wrong after recovery")
	}
}

func TestChainAllCoolingDown(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	a := &script{name: "a", down: true}
	b := &script{name: "b", down: true}
	c, _ := newChain(t, ChainOptions{FailureThreshold: 1, Now: clk.Now}, a, b)
	if _, err := c.Do(context.Background(), "+1"); err == nil {
		t.Fatal("want failure")
	}
	res, err := c.Do(context.Background(), "+1")
	if !errors.Is(err, ErrNoProvider) || len(res.Attempts) != 2 || !res.Attempts[1].Skipped {
		t.Fatalf("res = %+v, err = %v", res, err)
	}
	if a.Calls() != 1 || b.Calls() != 1 {
		t.Fatal("skipped providers must not be called")
	}
}

func TestChainOverallDeadlineSimulated(t *testing.T) {
	// Each provider hangs for 4s of simulated time; the chain has 10s total,
	// so the third provider is never reached.
	clk := &fakeClock{t: time.Unix(0, 0)}
	a := &script{name: "a", clk: clk, cost: 4 * time.Second, down: true}
	b := &script{name: "b", clk: clk, cost: 4 * time.Second, down: true}
	v := &script{name: "voice", clk: clk, cost: 4 * time.Second, down: true}
	c, log := newChain(t, ChainOptions{Deadline: 8 * time.Second, FailureThreshold: -1, Now: clk.Now}, a, b, v)
	_, err := c.Do(context.Background(), "+1")
	if !errors.Is(err, ErrDeadline) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want ErrDeadline", err)
	}
	if got := strings.Join(*log, ","); got != "a,b" {
		t.Fatalf("attempts = %s, want a,b", got)
	}
}

func TestChainDeadlinesReal(t *testing.T) {
	// Real-time contexts: a hanging provider is cut off by its own timeout,
	// then by the overall deadline.
	hang := func(ctx context.Context, _ string) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}
	ok := func(context.Context, string) (string, error) { return "ok", nil }

	c, err := NewChain([]Provider[string, string]{
		{Name: "slow", Call: hang, Timeout: 10 * time.Millisecond},
		{Name: "fast", Call: ok},
	}, ChainOptions{Deadline: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.Do(context.Background(), "+1")
	if err != nil || res.Provider != "fast" || !errors.Is(res.Errors, context.DeadlineExceeded) {
		t.Fatalf("per-provider timeout: %+v %v", res, err)
	}

	var reached bool
	c, _ = NewChain([]Provider[string, string]{
		{Name: "slow", Call: hang}, // no own timeout: only the overall deadline stops it
		{Name: "fast", Call: func(context.Context, string) (string, error) { reached = true; return "ok", nil }},
	}, ChainOptions{Deadline: 20 * time.Millisecond})
	if _, err := c.Do(context.Background(), "+1"); !errors.Is(err, ErrDeadline) || reached {
		t.Fatalf("overall deadline: err = %v, reached fast = %v", err, reached)
	}
	if !c.Healthy("slow") {
		t.Fatal("an attempt cut short by the overall deadline must not count against the provider")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Do(ctx, "+1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled caller: err = %v", err)
	}
}

func TestChainConcurrent(t *testing.T) {
	a := &script{name: "a"}
	b := &script{name: "b"}
	c, _ := newChain(t, ChainOptions{FailureThreshold: 5, Cooldown: time.Millisecond}, a, b)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 200 {
				if g == 0 && i%50 == 0 {
					a.setDown(i%100 == 0) // flap the primary
				}
				if _, err := c.Do(context.Background(), "+1"); err != nil {
					t.Error(err)
					return
				}
				_ = c.Healthy("a")
			}
		}(g)
	}
	wg.Wait()
}
