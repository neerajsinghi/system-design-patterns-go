package llmbill

import (
	"reflect"
	"sync"
	"testing"
)

var (
	once          sync.Once
	broken, fixed Report
)

func runs() (Report, Report) {
	once.Do(func() { broken, fixed = Broken(), Fixed() })
	return broken, fixed
}

// maxRequestCost bounds what one request can add after the budget check
// passes: the hard limit can be overshot by at most one request.
const maxRequestCost = 0.05

// The invariant: total cost at most 30% of the broken design's, answer
// quality (% of well-behaved tenants' requests answered correctly, cache
// false hits and rejections counted as wrong) no more than 2 points below
// it, the runaway tenant held to its hard budget, and no other tenant
// downgraded or rejected.
func TestBrokenViolatesInvariant(t *testing.T) {
	b, _ := runs()
	if got := b.Tenant(Runaway).Cost; got <= HardBudget+maxRequestCost {
		t.Fatalf("broken run should let %s spend past its budget: spent $%.2f", Runaway, got)
	}
	if b.CacheHits != 0 || b.Served["large"] != b.Requests {
		t.Fatalf("broken run should send everything to the large model: %v", b.Served)
	}
}

func TestFixedHoldsInvariant(t *testing.T) {
	b, f := runs()
	if f.Cost > 0.30*b.Cost {
		t.Errorf("total cost $%.2f is more than 30%% of broken's $%.2f", f.Cost, b.Cost)
	}
	// Excluding the loop, so the saving is not just the budget cap.
	if f.RegularCost > 0.30*b.RegularCost {
		t.Errorf("well-behaved tenants' cost $%.2f is more than 30%% of broken's $%.2f", f.RegularCost, b.RegularCost)
	}
	if f.QualityPct() < b.QualityPct()-2 {
		t.Errorf("quality %.1f%% is more than 2 points below broken's %.1f%%", f.QualityPct(), b.QualityPct())
	}
	if got := f.Tenant(Runaway).Cost; got > HardBudget+maxRequestCost {
		t.Errorf("%s spent $%.4f, budget $%.2f", Runaway, got, HardBudget)
	}
	for _, name := range Tenants {
		if ts := f.Tenant(name); ts.Downgraded != 0 || ts.Rejected != 0 {
			t.Errorf("%s was affected by another tenant's loop: %+v", name, ts)
		}
	}
	if f.Regular != b.Regular || f.Requests != b.Requests {
		t.Fatalf("runs saw different traffic: %d/%d vs %d/%d", f.Regular, f.Requests, b.Regular, b.Requests)
	}
}

// The fix has costs: some cache answers are for the wrong question, some
// requests pay for two model calls, and the runaway tenant's requests
// (including its real users') are downgraded and then refused.
func TestFixCosts(t *testing.T) {
	_, f := runs()
	if f.FalseHits == 0 {
		t.Error("expected some cache false hits on production traffic")
	}
	if rate := float64(f.FalseHits) / float64(f.CacheHits); rate > FalseHitTarget {
		t.Errorf("false-hit share %.2f%% exceeds the %.0f%% target the threshold was chosen for", 100*rate, 100*FalseHitTarget)
	}
	if f.Escalated == 0 || f.EscalationCost <= 0 {
		t.Errorf("expected escalations to pay for a second call: %d escalated, $%.2f", f.Escalated, f.EscalationCost)
	}
	rw := f.Tenant(Runaway)
	if rw.Downgraded == 0 || rw.Rejected == 0 {
		t.Errorf("%s should be downgraded, then rejected: %+v", Runaway, rw)
	}
}

func TestCalibrationPicksLowestSafeThreshold(t *testing.T) {
	_, f := runs()
	th, sweep := f.Threshold, f.Sweep
	for i, s := range sweep {
		if s.Threshold == th {
			if s.FalseHitRate > FalseHitTarget {
				t.Fatalf("chosen %.2f has false-hit rate %.3f", th, s.FalseHitRate)
			}
			if i > 0 && sweep[i-1].FalseHitRate <= FalseHitTarget {
				t.Fatalf("%.2f is also within target, so %.2f is not the lowest", sweep[i-1].Threshold, th)
			}
			if i > 0 && sweep[i-1].HitRate <= s.HitRate {
				t.Fatal("a lower threshold should answer more from cache")
			}
			return
		}
	}
	t.Fatalf("chosen threshold %.2f not in sweep", th)
}

// Both runs depend only on seeded traffic, so generating it twice must give
// the same requests.
func TestTrafficIsDeterministic(t *testing.T) {
	if !reflect.DeepEqual(traffic(), traffic()) {
		t.Fatal("two traffic() calls differ")
	}
}
