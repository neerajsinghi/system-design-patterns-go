package notificationstorm

import (
	"reflect"
	"strings"
	"sync"
	"testing"
)

// The invariant: transactional pushes reach the provider with p99 ≤ 1s, no
// provider ever returns a 429, and no provider call goes to a device token
// that was already rejected as permanently invalid.
func TestBrokenViolatesInvariant(t *testing.T) {
	r := Broken()
	if r.TransactionalP99 <= TransactionalBudget {
		t.Errorf("broken transactional p99 %v, want above %v", r.TransactionalP99, TransactionalBudget)
	}
	if r.Received429 == 0 || r.PeakCallsPerSec <= ProviderLimit {
		t.Errorf("broken run should overrun the providers: %d 429s, busiest second %d calls", r.Received429, r.PeakCallsPerSec)
	}
	if r.WastedOnInvalid == 0 || r.DLQDepth != 0 {
		t.Errorf("broken run should keep retrying dead tokens: wasted %d, DLQ %d", r.WastedOnInvalid, r.DLQDepth)
	}
	if r.MaxLikePushes() != LikesPerCommenter {
		t.Errorf("broken run sent %d like pushes for one comment, want one per like (%d)", r.MaxLikePushes(), LikesPerCommenter)
	}
}

func TestFixedHoldsInvariant(t *testing.T) {
	r := Fixed()
	if r.TransactionalP99 > TransactionalBudget {
		t.Errorf("fixed transactional p99 %v, budget %v", r.TransactionalP99, TransactionalBudget)
	}
	if r.Received429 != 0 || r.PeakCallsPerSec > ProviderLimit {
		t.Errorf("fixed run: %d 429s, busiest provider-second %d calls (limit %d)", r.Received429, r.PeakCallsPerSec, ProviderLimit)
	}
	if r.WastedOnInvalid != 0 {
		t.Errorf("fixed run made %d calls to tokens already known invalid", r.WastedOnInvalid)
	}
	if r.DLQDepth == 0 {
		t.Error("fixed run should have parked the dead tokens' pushes in the DLQ")
	}
	if r.StormUndelivered != 0 {
		t.Errorf("fixed run left %d social pushes undelivered", r.StormUndelivered)
	}
	if b := Broken(); r.StormDrain >= b.StormDrain {
		t.Errorf("fixed storm drained at %v, broken at %v; staying under the limit should still drain faster", r.StormDrain, b.StormDrain)
	}
}

// The cost of coalescing: no social push goes out before the window has
// passed, and each commenter gets one push summarising every like.
func TestCoalescingCost(t *testing.T) {
	r := Fixed()
	if r.MinSocialDelay < CoalesceWindow {
		t.Errorf("a social push went out after %v, before the %v window", r.MinSocialDelay, CoalesceWindow)
	}
	if len(r.LikePushes) == 0 {
		t.Fatal("no like pushes recorded")
	}
	for u, n := range r.LikePushes {
		if n != 1 {
			t.Errorf("%s got %d like pushes, want 1", u, n)
		}
	}
	if r.Coalesced == 0 || r.BusiestSummary == "" {
		t.Fatalf("expected coalesced pushes, got %d coalesced, summary %q", r.Coalesced, r.BusiestSummary)
	}
	if want := " and 41 others liked your comment"; !strings.HasSuffix(r.BusiestSummary, want) {
		t.Errorf("busiest push reads %q, want it to end in %q", r.BusiestSummary, want)
	}
}

// Runs are deterministic and share no state, so concurrent runs agree.
func TestDeterministicUnderConcurrency(t *testing.T) {
	var wg sync.WaitGroup
	got := make([]Report, 4)
	for i := range got {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if i%2 == 0 {
				got[i] = Fixed()
			} else {
				got[i] = Broken()
			}
		}()
	}
	wg.Wait()
	if !reflect.DeepEqual(got[0], got[2]) || !reflect.DeepEqual(got[1], got[3]) {
		t.Fatal("identical runs produced different reports")
	}
}
