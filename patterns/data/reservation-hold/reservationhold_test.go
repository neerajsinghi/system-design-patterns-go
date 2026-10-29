package reservationhold

import (
	"errors"
	"fmt"
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

func newInv(t *testing.T, stock map[string]int) (*Inventory, *fakeClock) {
	t.Helper()
	clk := &fakeClock{t: time.Unix(1_000, 0)}
	inv, err := New(stock, WithClock(clk.Now))
	if err != nil {
		t.Fatal(err)
	}
	return inv, clk
}

func checkTotals(t *testing.T, inv *Inventory, want Totals) {
	t.Helper()
	if got := inv.Totals(); got != want {
		t.Fatalf("totals = %+v, want %+v", got, want)
	}
}

func TestNewValidation(t *testing.T) {
	tests := []struct {
		name  string
		stock map[string]int
	}{
		{"nil stock", nil},
		{"empty stock", map[string]int{}},
		{"empty item ID", map[string]int{"": 1}},
		{"negative stock", map[string]int{"a": -1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.stock); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestHoldValidation(t *testing.T) {
	inv, _ := newInv(t, map[string]int{"a": 1})
	tests := []struct {
		name   string
		items  []string
		holder string
		ttl    time.Duration
		is     error
	}{
		{"no items", nil, "alice", time.Minute, nil},
		{"empty holder", []string{"a"}, "", time.Minute, nil},
		{"zero ttl", []string{"a"}, "alice", 0, nil},
		{"negative ttl", []string{"a"}, "alice", -time.Second, nil},
		{"unknown item", []string{"a", "zzz"}, "alice", time.Minute, ErrUnknownItem},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := inv.Hold(tt.items, tt.holder, tt.ttl)
			if err == nil {
				t.Fatal("expected error")
			}
			if tt.is != nil && !errors.Is(err, tt.is) {
				t.Fatalf("err = %v, want %v", err, tt.is)
			}
		})
	}
	// None of the failures may have held anything.
	checkTotals(t, inv, Totals{Available: 1})
	if _, err := inv.Confirm("nope"); !errors.Is(err, ErrHoldNotFound) {
		t.Fatalf("confirm unknown: %v", err)
	}
	if err := inv.Release("nope"); !errors.Is(err, ErrHoldNotFound) {
		t.Fatalf("release unknown: %v", err)
	}
}

func TestExpiryReturnsUnitsLazily(t *testing.T) {
	inv, clk := newInv(t, map[string]int{"seat": 1})
	h, err := inv.Hold([]string{"seat"}, "alice", 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !h.ExpiresAt.Equal(clk.Now().Add(10 * time.Minute)) {
		t.Fatalf("expires at %v", h.ExpiresAt)
	}
	if _, err := inv.Hold([]string{"seat"}, "bob", time.Minute); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("second hold: %v", err)
	}

	clk.Advance(10*time.Minute - time.Nanosecond)
	if got := inv.Available("seat"); got != 0 {
		t.Fatalf("just before expiry available = %d", got)
	}
	clk.Advance(time.Nanosecond) // exactly at the deadline: expired
	// No reaper has run; Bob's Hold call returns Alice's overdue hold itself.
	if _, err := inv.Hold([]string{"seat"}, "bob", time.Minute); err != nil {
		t.Fatalf("hold after expiry: %v", err)
	}
	if g, _ := inv.Get(h.ID); g.Status != Expired {
		t.Fatalf("alice's hold status = %v", g.Status)
	}
	if s := inv.Stats(); s.ExpiredLazy != 1 || s.ExpiredReaped != 0 || s.Rejected != 1 {
		t.Fatalf("stats = %+v", s)
	}
	checkTotals(t, inv, Totals{Held: 1})
}

func TestConfirmAfterExpiryFails(t *testing.T) {
	inv, clk := newInv(t, map[string]int{"seat": 1})
	h, _ := inv.Hold([]string{"seat"}, "alice", time.Minute)
	clk.Advance(2 * time.Minute)

	// Nothing has touched the hold yet: the deadline alone decides.
	if _, err := inv.Confirm(h.ID); !errors.Is(err, ErrHoldExpired) {
		t.Fatalf("late confirm: %v", err)
	}
	checkTotals(t, inv, Totals{Available: 1})

	// The seat goes to Bob; Alice's retried payment is still refused.
	b, err := inv.Hold([]string{"seat"}, "bob", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := inv.Confirm(b.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := inv.Confirm(h.ID); !errors.Is(err, ErrHoldExpired) {
		t.Fatalf("retried late confirm: %v", err)
	}
	checkTotals(t, inv, Totals{Sold: 1})
	if s := inv.Stats(); s.LateConfirms != 2 || s.Confirmed != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestConfirmIsIdempotent(t *testing.T) {
	inv, clk := newInv(t, map[string]int{"seat": 3})
	h, _ := inv.Hold([]string{"seat", "seat"}, "alice", time.Minute)
	first, err := inv.Confirm(h.ID)
	if err != nil {
		t.Fatal(err)
	}
	// A retried callback, even after the deadline, gets the same success.
	clk.Advance(time.Hour)
	for range 3 {
		again, err := inv.Confirm(h.ID)
		if err != nil {
			t.Fatalf("repeat confirm: %v", err)
		}
		if again.ID != first.ID || again.Status != Confirmed || again.Units() != 2 {
			t.Fatalf("repeat confirm returned %+v", again)
		}
	}
	checkTotals(t, inv, Totals{Available: 1, Sold: 2})
	if s := inv.Stats(); s.Confirmed != 1 {
		t.Fatalf("confirmed counted %d times", s.Confirmed)
	}
	if err := inv.Release(h.ID); !errors.Is(err, ErrHoldConfirmed) {
		t.Fatalf("release of a sale: %v", err)
	}
	if got := inv.Reap(); len(got) != 0 {
		t.Fatalf("reaper took a sale: %+v", got)
	}
}

func TestRelease(t *testing.T) {
	inv, clk := newInv(t, map[string]int{"seat": 1})
	h, _ := inv.Hold([]string{"seat"}, "alice", time.Minute)
	if err := inv.Release(h.ID); err != nil {
		t.Fatal(err)
	}
	if err := inv.Release(h.ID); err != nil {
		t.Fatalf("second release: %v", err)
	}
	checkTotals(t, inv, Totals{Available: 1})
	if _, err := inv.Confirm(h.ID); !errors.Is(err, ErrHoldReleased) {
		t.Fatalf("confirm after release: %v", err)
	}

	// Releasing an overdue hold records it as expired, not released.
	h2, _ := inv.Hold([]string{"seat"}, "bob", time.Minute)
	clk.Advance(time.Minute)
	if err := inv.Release(h2.ID); err != nil {
		t.Fatal(err)
	}
	if g, _ := inv.Get(h2.ID); g.Status != Expired {
		t.Fatalf("status = %v", g.Status)
	}
	if s := inv.Stats(); s.Released != 1 || s.ExpiredLazy != 1 {
		t.Fatalf("stats = %+v", s)
	}
	checkTotals(t, inv, Totals{Available: 1})
}

func TestMultiItemHoldIsAllOrNothing(t *testing.T) {
	inv, clk := newInv(t, map[string]int{"A1": 1, "A2": 1, "A3": 1, "ga": 2})
	if _, err := inv.Hold([]string{"A2"}, "bob", time.Minute); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name  string
		items []string
	}{
		{"one seat taken", []string{"A1", "A2", "A3"}},
		{"too many units", []string{"A1", "ga", "ga", "ga"}},
		{"duplicate numbered seat", []string{"A1", "A1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := inv.Hold(tt.items, "alice", time.Minute); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("err = %v", err)
			}
			for id, want := range map[string]int{"A1": 1, "A2": 0, "A3": 1, "ga": 2} {
				if got := inv.Available(id); got != want {
					t.Fatalf("%s available = %d, want %d (partial hold leaked)", id, got, want)
				}
			}
		})
	}

	// Once Bob's hold lapses, the whole row can be held, and confirmed together.
	clk.Advance(time.Minute)
	h, err := inv.Hold([]string{"A1", "A2", "A3", "ga"}, "alice", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if h.Units() != 4 {
		t.Fatalf("units = %d", h.Units())
	}
	if _, err := inv.Confirm(h.ID); err != nil {
		t.Fatal(err)
	}
	checkTotals(t, inv, Totals{Available: 1, Sold: 4})
}

func TestReapReturnsExactlyTheExpiredHolds(t *testing.T) {
	inv, clk := newInv(t, map[string]int{"ga": 10})
	ttls := []time.Duration{1, 5, 2, 9, 5, 3} // minutes
	ids := make([]string, len(ttls))
	for i, m := range ttls {
		h, err := inv.Hold([]string{"ga"}, fmt.Sprintf("u%d", i), m*time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = h.ID
	}
	// u2 (2m) pays, u5 (3m) cancels: neither should be reaped.
	if _, err := inv.Confirm(ids[2]); err != nil {
		t.Fatal(err)
	}
	if err := inv.Release(ids[5]); err != nil {
		t.Fatal(err)
	}

	clk.Advance(5 * time.Minute) // u0 (1m), u1 (5m) and u4 (5m) are due; u3 (9m) is not
	got := inv.Reap()
	want := []string{ids[0], ids[1], ids[4]} // by expiry, then ID
	if len(got) != len(want) {
		t.Fatalf("reaped %d holds: %+v", len(got), got)
	}
	for i, h := range got {
		if h.ID != want[i] || h.Status != Expired {
			t.Fatalf("reaped[%d] = %s (%v), want %s", i, h.ID, h.Status, want[i])
		}
	}
	if again := inv.Reap(); len(again) != 0 {
		t.Fatalf("second reap returned %+v", again)
	}
	checkTotals(t, inv, Totals{Available: 8, Held: 1, Sold: 1})
	if s := inv.Stats(); s.ExpiredReaped != 3 || s.ExpiredLazy != 0 {
		t.Fatalf("stats = %+v", s)
	}

	clk.Advance(4 * time.Minute)
	if got := inv.Reap(); len(got) != 1 || got[0].ID != ids[3] {
		t.Fatalf("final reap = %+v", got)
	}
	checkTotals(t, inv, Totals{Available: 9, Sold: 1})
}

func TestTotalsNeedReapButAvailableDoesNot(t *testing.T) {
	inv, clk := newInv(t, map[string]int{"ga": 2})
	inv.Hold([]string{"ga"}, "alice", time.Minute)
	clk.Advance(time.Minute)
	// Totals does not expire anything: the overdue hold still shows as held.
	checkTotals(t, inv, Totals{Available: 1, Held: 1})
	if got := inv.Available("ga"); got != 2 {
		t.Fatalf("available = %d", got)
	}
	checkTotals(t, inv, Totals{Available: 2})
}

// TestNoOversellUnderConcurrency races many shoppers for a few seats while
// the clock moves and a reaper runs. Every unit must end up in exactly one
// state, and sold units can never exceed stock.
func TestNoOversellUnderConcurrency(t *testing.T) {
	const seats, shoppers = 20, 400
	inv, clk := newInv(t, map[string]int{"ga": seats, "vip": 3})

	var wg sync.WaitGroup
	var confirmed, late atomic.Int64
	stop := make(chan struct{})
	wg.Add(1)
	go func() { // clock and reaper
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				clk.Advance(time.Second)
				inv.Reap()
				if tot := inv.Totals(); tot.Available+tot.Held+tot.Sold != seats+3 || tot.Available < 0 || tot.Held < 0 {
					t.Errorf("inconsistent totals mid-run: %+v", tot)
					return
				}
			}
		}
	}()

	var shop sync.WaitGroup
	for i := range shoppers {
		shop.Add(1)
		go func() {
			defer shop.Done()
			items := []string{"ga"}
			if i%3 == 0 {
				items = append(items, "ga", "vip") // a group: all or nothing
			}
			h, err := inv.Hold(items, fmt.Sprintf("s%d", i), 3*time.Second)
			if err != nil {
				return
			}
			switch i % 4 {
			case 0: // abandons
			case 1:
				inv.Release(h.ID)
			default:
				for range 2 { // payment callback delivered twice
					_, err := inv.Confirm(h.ID)
					switch {
					case err == nil:
						confirmed.Add(1)
					case errors.Is(err, ErrHoldExpired):
						late.Add(1)
					default:
						t.Errorf("confirm: %v", err)
					}
				}
			}
		}()
	}
	shop.Wait()
	close(stop)
	wg.Wait()

	clk.Advance(time.Hour)
	inv.Reap()
	tot := inv.Totals()
	if tot.Held != 0 || tot.Available+tot.Sold != seats+3 {
		t.Fatalf("units leaked: %+v", tot)
	}
	s := inv.Stats()
	if int(s.Confirmed)*2 != int(confirmed.Load()) {
		t.Fatalf("confirm not idempotent: %d sales, %d successful calls", s.Confirmed, confirmed.Load())
	}
	if tot.Sold > seats+3 {
		t.Fatalf("oversold: %d", tot.Sold)
	}
	if s.Holds == 0 {
		t.Fatalf("test did not create contention: %+v", s)
	}
	t.Logf("stats %+v, totals %+v, late confirm calls %d", s, tot, late.Load())
}
