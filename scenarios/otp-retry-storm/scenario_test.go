package otpretrystorm

import (
	"reflect"
	"sync"
	"testing"
)

var (
	broken = sync.OnceValue(Broken)
	fixed  = sync.OnceValue(Fixed)
)

// The invariant: in every phase, providers see at most LoadCap calls per
// legitimate login; at least 99% of legitimate users have a working code
// within SLO seconds; and no number is sent more than ResendLimit messages
// in any ResendWindow.
func checkInvariant(r Report) []string {
	var bad []string
	for i, p := range r.Phases {
		if m := p.Multiplier(); m > LoadCap {
			bad = append(bad, PhaseNames[i]+": provider load above the cap")
		}
	}
	if r.OnTimeRate() < 0.99 {
		bad = append(bad, "fewer than 99% of users got a working code in time")
	}
	if r.MaxPerNumber > ResendLimit {
		bad = append(bad, "a number received more than the resend limit")
	}
	return bad
}

func TestBrokenViolatesInvariant(t *testing.T) {
	r := broken()
	if bad := checkInvariant(r); len(bad) < 3 {
		t.Fatalf("broken run should violate all three parts of the invariant, got %v", bad)
	}
	if m := r.Phases[1].PrimaryMultiplier(); m < 9 {
		t.Fatalf("broken outage multiplier %.2f, want the retry layers to multiply load at least 9×", m)
	}
	if r.StaleCodes == 0 || r.GaveUp == 0 {
		t.Fatalf("broken run should deliver stale codes and lose users: stale %d, gave up %d", r.StaleCodes, r.GaveUp)
	}
}

func TestFixedHoldsInvariant(t *testing.T) {
	r := fixed()
	if bad := checkInvariant(r); len(bad) > 0 {
		t.Fatalf("fixed run violates the invariant: %v (on time %.4f, max per number %d)", bad, r.OnTimeRate(), r.MaxPerNumber)
	}
	if r.Phases[1].PrimaryMultiplier() > 0.1 || r.PrimarySkipped == 0 {
		t.Fatalf("the breaker should keep the outage off the primary: %.2f× primary, %d skipped",
			r.Phases[1].PrimaryMultiplier(), r.PrimarySkipped)
	}
	if r.Phases[2].Calls[Primary] < r.Phases[2].Calls[Secondary]*10 {
		t.Fatalf("after the outage traffic should be back on the primary: %v", r.Phases[2].Calls)
	}
	// The budget caps retries at RetryRatio of taps plus its banked burst (10).
	if float64(r.BudgetRetries) > RetryRatio*float64(r.BudgetRequests)+10 {
		t.Fatalf("budget allowed %d retries for %d taps", r.BudgetRetries, r.BudgetRequests)
	}
}

// The fix has a price: failover traffic goes to providers that cost more
// per message, and some taps are refused.
func TestFixedCost(t *testing.T) {
	b, f := broken(), fixed()
	if f.Sent[Secondary] == 0 || f.Sent[Voice] == 0 {
		t.Fatalf("fixed run should fail over to secondary and voice: %v", f.Sent)
	}
	if avg := float64(f.Cost) / float64(f.TotalSent()); avg <= PrimaryPrice {
		t.Fatalf("average price per message %.2f should exceed the primary's %d", avg, PrimaryPrice)
	}
	if f.ResendsBlocked == 0 || f.WaitExample == "" {
		t.Fatal("fixed run should refuse some resend taps with a wait time")
	}
	if f.CostPerLogin() >= b.CostPerLogin() {
		t.Fatalf("per successful login the fix should still be cheaper: fixed %.2f, broken %.2f paise",
			f.CostPerLogin(), b.CostPerLogin())
	}
}

func TestDeterministic(t *testing.T) {
	if !reflect.DeepEqual(Fixed(), fixed()) {
		t.Fatal("two Fixed runs differ")
	}
}
