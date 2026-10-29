package fanoutfeed

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Unix(1_700_000_000, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func mustDispatcher(t *testing.T, opts ...DispatcherOption) *Dispatcher {
	t.Helper()
	d, err := NewDispatcher(opts...)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func enqueue(t *testing.T, d *Dispatcher, n Notification) {
	t.Helper()
	if err := d.Enqueue(n); err != nil {
		t.Fatal(err)
	}
}

func TestDispatcherValidation(t *testing.T) {
	apns := WithProvider("apns", 10, 1)
	tests := []struct {
		name string
		opts []DispatcherOption
	}{
		{"no provider", nil},
		{"nil clock", []DispatcherOption{apns, WithDispatchClock(nil)}},
		{"zero rate", []DispatcherOption{WithProvider("apns", 0, 1)}},
		{"zero burst", []DispatcherOption{WithProvider("apns", 10, 0)}},
		{"empty name", []DispatcherOption{WithProvider("", 10, 1)}},
		{"duplicate", []DispatcherOption{apns, apns}},
		{"negative starvation", []DispatcherOption{apns, WithStarvationLimit(-1)}},
		{"negative window", []DispatcherOption{apns, WithCoalesceWindow(-time.Second)}},
		{"fifo with coalescing", []DispatcherOption{apns, WithSingleFIFO(), WithCoalesceWindow(time.Second)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewDispatcher(tc.opts...); err == nil {
				t.Error("want error")
			}
		})
	}

	d := mustDispatcher(t, apns)
	for _, n := range []Notification{
		{User: "", Provider: "apns"},
		{User: "u", Provider: "gcm"},
		{User: "u", Provider: "apns", Lane: Lane(7)},
		{User: "u", Provider: "apns", Lane: Lane(-1)},
	} {
		if err := d.Enqueue(n); err == nil {
			t.Errorf("Enqueue(%+v) should fail", n)
		}
	}
}

// TestTransactionalBeforeSocial queues social pushes first, then OTPs. Every
// OTP must be sent before any social push.
func TestTransactionalBeforeSocial(t *testing.T) {
	for _, limit := range []int{0, 20} {
		t.Run(fmt.Sprint("starvationLimit=", limit), func(t *testing.T) {
			clk := newFakeClock()
			d := mustDispatcher(t, WithDispatchClock(clk.Now), WithProvider("apns", 1000, 100), WithStarvationLimit(limit))
			for i := range 10 {
				enqueue(t, d, Notification{User: fmt.Sprint("fan", i), Provider: "apns", Lane: Social})
				enqueue(t, d, Notification{User: fmt.Sprint("m", i), Provider: "apns", Lane: Marketing})
			}
			for i := range 5 {
				enqueue(t, d, Notification{User: fmt.Sprint("otp", i), Provider: "apns", Lane: Transactional})
			}
			got := d.Dispatch()
			if len(got) != 25 {
				t.Fatalf("sent %d, want 25", len(got))
			}
			for i, del := range got {
				want := Social
				switch {
				case i < 5:
					want = Transactional
				case i >= 15:
					want = Marketing
				}
				if del.Lane != want {
					t.Fatalf("send %d is %v, want %v", i, del.Lane, want)
				}
			}
		})
	}
}

// TestSingleFIFOIsArrivalOrder shows the baseline: an OTP queued behind a
// storm waits for the storm.
func TestSingleFIFOIsArrivalOrder(t *testing.T) {
	clk := newFakeClock()
	d := mustDispatcher(t, WithDispatchClock(clk.Now), WithProvider("apns", 1000, 100), WithSingleFIFO())
	for i := range 10 {
		enqueue(t, d, Notification{User: fmt.Sprint("fan", i), Provider: "apns", Lane: Social})
	}
	enqueue(t, d, Notification{User: "otp", Provider: "apns", Lane: Transactional})
	got := d.Dispatch()
	if len(got) != 11 || got[10].Lane != Transactional {
		t.Errorf("FIFO should send the OTP last, after the 10 social pushes")
	}
}

// TestAntiStarvation keeps every lane backlogged and checks that no lane
// goes more than limit+numLanes-1 sends without being served, and that with
// the rule disabled, lower lanes are fully starved.
func TestAntiStarvation(t *testing.T) {
	const limit = 4
	clk := newFakeClock()
	d := mustDispatcher(t, WithDispatchClock(clk.Now), WithProvider("apns", 1000, 1000), WithStarvationLimit(limit))
	for _, l := range []Lane{Transactional, Social, Marketing} {
		for i := range 300 {
			enqueue(t, d, Notification{User: fmt.Sprint(l, i), Provider: "apns", Lane: l})
		}
	}
	got := d.Dispatch()[:300] // every lane is still backlogged during these sends
	var gap [numLanes]int
	var sent [numLanes]int
	for i, del := range got {
		for l := range gap {
			gap[l]++
		}
		gap[del.Lane] = 0
		sent[del.Lane]++
		for l, g := range gap {
			if g > limit+int(numLanes)-1 {
				t.Fatalf("lane %v went %d sends without service at send %d", Lane(l), g, i)
			}
		}
	}
	if sent[Transactional] < 300*limit/(limit+2) {
		t.Errorf("transactional got only %d of 300 sends", sent[Transactional])
	}
	t.Logf("share of 300 sends: %v", sent)

	strict := mustDispatcher(t, WithDispatchClock(clk.Now), WithProvider("apns", 1000, 1000), WithStarvationLimit(0))
	for _, l := range []Lane{Transactional, Social} {
		for i := range 100 {
			enqueue(t, strict, Notification{User: fmt.Sprint(l, i), Provider: "apns", Lane: l})
		}
	}
	for i, del := range strict.Dispatch()[:100] {
		if del.Lane != Transactional {
			t.Fatalf("strict priority sent %v at %d", del.Lane, i)
		}
	}
}

// TestProviderRateNeverExceeded runs a storm through two providers for 10
// simulated seconds and checks every sliding one-second window.
func TestProviderRateNeverExceeded(t *testing.T) {
	clk := newFakeClock()
	rates := map[string]struct {
		rate  float64
		burst int
	}{"apns": {200, 10}, "fcm": {300, 5}}
	d := mustDispatcher(t, WithDispatchClock(clk.Now),
		WithProvider("apns", rates["apns"].rate, rates["apns"].burst),
		WithProvider("fcm", rates["fcm"].rate, rates["fcm"].burst))
	for i := range 20_000 {
		p := "apns"
		if i%2 == 1 {
			p = "fcm"
		}
		enqueue(t, d, Notification{User: fmt.Sprint("u", i), Provider: p, Lane: Social})
	}
	sends := map[string][]time.Time{}
	for range 1000 { // 10 s in 10 ms ticks
		for _, del := range d.Dispatch() {
			sends[del.Provider] = append(sends[del.Provider], del.SentAt)
		}
		clk.Advance(10 * time.Millisecond)
	}
	for name, ts := range sends {
		limit := int(rates[name].rate) + rates[name].burst
		j := 0
		for i := range ts {
			for ts[i].Sub(ts[j]) >= time.Second {
				j++
			}
			if n := i - j + 1; n > limit {
				t.Fatalf("%s sent %d in one second, limit %d", name, n, limit)
			}
		}
		if len(ts) < int(rates[name].rate)*9 {
			t.Errorf("%s sent only %d in 10 s; the limiter is too strict", name, len(ts))
		}
	}
	if d.Stats().Throttled == 0 {
		t.Error("expected throttling")
	}
}

func TestCoalescingWindow(t *testing.T) {
	clk := newFakeClock()
	d := mustDispatcher(t, WithDispatchClock(clk.Now), WithProvider("fcm", 1000, 1000), WithCoalesceWindow(5*time.Second))
	like := func(user, actor string) {
		enqueue(t, d, Notification{User: user, Provider: "fcm", Lane: Social, Actor: actor, Text: "liked your post", CollapseKey: "like:p1"})
	}
	like("bob", "alice")
	for i := range 41 {
		clk.Advance(100 * time.Millisecond)
		like("bob", fmt.Sprint("fan", i))
	}
	like("carol", "alice")                                                                      // different user: its own push
	enqueue(t, d, Notification{User: "bob", Provider: "fcm", Lane: Transactional, Text: "otp"}) // not coalesced
	got := d.Dispatch()
	if len(got) != 1 || got[0].Lane != Transactional {
		t.Fatalf("before the window only the OTP should go out, got %+v", got)
	}

	clk.Advance(1 * time.Second) // 5.1 s after the first like
	got = d.Dispatch()
	if len(got) != 1 || got[0].User != "bob" || got[0].Count != 42 {
		t.Fatalf("want one coalesced push for bob, got %+v", got)
	}
	if s := got[0].Summary(); s != "alice and 41 others liked your post" {
		t.Errorf("summary = %q", s)
	}
	if got[0].Delay() < 5*time.Second {
		t.Errorf("delay %v, want >= window", got[0].Delay())
	}

	// A like after bob's push starts a new window rather than merging into
	// the push that already went out.
	like("bob", "dave")
	clk.Advance(5 * time.Second)
	got = d.Dispatch()
	if len(got) != 2 {
		t.Fatalf("want carol's push and bob's second push, got %d", len(got))
	}
	for _, del := range got {
		if del.User == "bob" && del.Summary() != "dave liked your post" {
			t.Errorf("bob's second push = %q", del.Summary())
		}
	}
	st := d.Stats()
	if st.Enqueued != 45 || st.Coalesced != 41 || st.Sent != 4 || d.Pending() != 0 {
		t.Errorf("stats = %+v pending %d", st, d.Pending())
	}
}

func TestDispatcherConcurrent(t *testing.T) {
	clk := newFakeClock()
	d := mustDispatcher(t, WithDispatchClock(clk.Now), WithProvider("apns", 500, 50), WithProvider("fcm", 500, 50), WithCoalesceWindow(time.Second))
	var wg sync.WaitGroup
	var mu sync.Mutex
	sent := 0
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				p := []string{"apns", "fcm"}[i%2]
				_ = d.Enqueue(Notification{User: fmt.Sprint("u", i%50), Provider: p, Lane: Lane(i % 3), CollapseKey: "k"})
				if g == 0 && i%10 == 0 {
					clk.Advance(10 * time.Millisecond)
				}
				n := len(d.Dispatch())
				mu.Lock()
				sent += n
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	for d.Pending() > 0 {
		clk.Advance(100 * time.Millisecond)
		sent += len(d.Dispatch())
	}
	st := d.Stats()
	if st.Enqueued != 4000 || st.Sent != sent || st.Sent+st.Coalesced != 4000 {
		t.Errorf("stats = %+v, counted sent %d", st, sent)
	}
}
