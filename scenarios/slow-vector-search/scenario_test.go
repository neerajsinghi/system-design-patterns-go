package slowvectorsearch

import (
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
)

var (
	once          sync.Once
	broken, fixed Report
)

func runs() (Report, Report) {
	// The two designs run concurrently (they share the lazily built query
	// sets), which -race checks.
	once.Do(func() {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); broken = Broken() }()
		go func() { defer wg.Done(); fixed = Fixed() }()
		wg.Wait()
	})
	return broken, fixed
}

// The invariant: at every growth stage, modelled p99 search latency is within
// the SLO, and recall@10 is at least RecallTarget for queries on the launch
// categories and on categories added later.
func TestBrokenViolatesInvariant(t *testing.T) {
	r, _ := runs()
	slo, recall := r.Violations()
	if slo == 0 || recall == 0 {
		t.Fatalf("broken run should break both the SLO and the recall target: %d SLO stages, %d recall stages", slo, recall)
	}
	// Flat search: latency grows in step with the catalogue and crosses the SLO.
	for i := 1; i <= BrokenIVFAfter; i++ {
		s, first := r.Stages[i], r.Stages[0]
		if s.Index != "flat" || s.P99 <= SLO {
			t.Fatalf("stage %d: %s at p99 %v, want flat over the %v SLO", i+1, s.Index, s.P99, SLO)
		}
		if got, want := float64(s.P99)/float64(first.P99), float64(s.Products)/float64(first.Products); math.Abs(got-want) > 1e-9 {
			t.Fatalf("stage %d: flat latency grew %.2fx for %.2fx the products, want linear", i+1, got, want)
		}
	}
	// The one-off IVF index: fast, but wrong for the categories it never saw.
	for i := BrokenIVFAfter + 1; i < len(Stages); i++ {
		s := r.Stages[i]
		if !s.HasNew || s.RecallNew >= RecallTarget {
			t.Fatalf("stage %d: recall on new categories %.3f, want below %.2f with stale centroids", i+1, s.RecallNew, RecallTarget)
		}
		if s.RecallLaunch < RecallTarget-0.05 {
			t.Fatalf("stage %d: recall on launch categories %.3f; the stale index should still mostly serve them", i+1, s.RecallLaunch)
		}
	}
	// Turning nprobe up instead of retraining costs the SLO.
	if r.RescueRecallNew < RecallTarget || r.RescueP99 <= SLO {
		t.Fatalf("rescue by nprobe=%d: recall new %.3f at p99 %v, want recall restored but over the SLO",
			r.RescueNProbe, r.RescueRecallNew, r.RescueP99)
	}
}

func TestFixedHoldsInvariant(t *testing.T) {
	_, r := runs()
	if len(r.Stages) != len(Stages) {
		t.Fatalf("fixed run has %d stages, want %d", len(r.Stages), len(Stages))
	}
	for i, s := range r.Stages {
		if !s.Holds() {
			t.Errorf("stage %d (%s): p99 %v, recall launch %.3f, recall new %.3f (has new %v)",
				i+1, s.Index, s.P99, s.RecallLaunch, s.RecallNew, s.HasNew)
		}
		if s.Index == "flat" || s.NProbe <= 0 || s.NProbe >= NList {
			t.Errorf("stage %d: index %q nprobe %d, want an IVF probing a minority of lists", i+1, s.Index, s.NProbe)
		}
	}
}

func TestFixedRetrainsOnDriftNotOnEveryStage(t *testing.T) {
	_, r := runs()
	for i, s := range r.Stages {
		newCats := Stages[i].WaveA > 0 && (i == 0 || Stages[i-1].WaveA == 0) ||
			Stages[i].WaveB > 0 && (i == 0 || Stages[i-1].WaveB == 0)
		if newCats && (s.Drift < DriftTrigger || !strings.HasPrefix(s.Retrained, "drift")) {
			t.Errorf("stage %d brings new categories: drift %.2f, retrained %q; want a drift-triggered retrain", i+1, s.Drift, s.Retrained)
		}
	}
	// Stage 5 adds more of categories the index was just retrained on, and
	// grows the corpus by a third: neither trigger should fire.
	if s := r.Stages[4]; s.Retrained != "" || s.Drift >= DriftTrigger {
		t.Fatalf("stage 5: drift %.2f, retrained %q; want no retrain", s.Drift, s.Retrained)
	}
	if last := r.Stages[len(r.Stages)-1]; last.Retrains >= len(Stages) {
		t.Fatalf("%d retrains over %d stages; the triggers should skip some stages", last.Retrains, len(Stages))
	}
}

// The fix's costs: k-means training work the broken design mostly skips,
// approximate answers (recall below 1), and a centroid table in memory.
func TestCostOfFix(t *testing.T) {
	b, f := runs()
	if f.Trainings <= b.Trainings || f.TrainDistanceComputations <= b.TrainDistanceComputations {
		t.Fatalf("fixed trained %d times (%d distances), broken %d (%d); retraining should cost more",
			f.Trainings, f.TrainDistanceComputations, b.Trainings, b.TrainDistanceComputations)
	}
	approximate := false
	for _, s := range f.Stages {
		if s.RecallLaunch < 1 || (s.HasNew && s.RecallNew < 1) {
			approximate = true
		}
	}
	if !approximate {
		t.Fatal("fixed run should miss some true neighbours: IVF is approximate")
	}
	if f.CentroidBytes != NList*Dim*4 || f.CentroidBytes*20 > f.CorpusBytes {
		t.Fatalf("centroid table %d bytes for %d bytes of vectors, want under 5%%", f.CentroidBytes, f.CorpusBytes)
	}
	// Training is paid back by cheaper queries: the work of all trainings is
	// smaller than what flat search would cost over a modest query volume.
	last := f.Stages[len(f.Stages)-1]
	saved := float64(Stages[len(Stages)-1].Total()) - last.ScannedPerQuery - NList
	if queries := float64(f.TrainDistanceComputations) / saved; queries > 10_000 {
		t.Fatalf("training pays for itself only after %.0f queries", queries)
	}
}

// A second run gives the same report (seeded data, no map-order effects).
func TestDeterministic(t *testing.T) {
	_, f := runs()
	if !reflect.DeepEqual(f, Fixed()) {
		t.Fatal("fixed runs differ between calls")
	}
}

func TestModelledLatency(t *testing.T) {
	if got := ModelledLatency(1, 0); got != 153600 {
		t.Fatalf("one simulated vector = %v, want 153.6µs (400 products x 768 x 0.5ns)", got)
	}
	if got := ModelledLatency(0, 2); got != 768 {
		t.Fatalf("two centroids = %v, want 768ns", got)
	}
}

func TestPrint(t *testing.T) {
	b, f := runs()
	var sb strings.Builder
	b.Print(&sb)
	f.Print(&sb)
	out := sb.String()
	for _, want := range []string{"BROKEN", "FIXED", "violates", "drift ratio", "raising nprobe"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q", want)
		}
	}
}
