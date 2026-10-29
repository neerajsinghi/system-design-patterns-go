package chatbothallucinates

import (
	"reflect"
	"strings"
	"sync"
	"testing"

	ragretrieval "github.com/neerajsinghi/system-design-patterns-go/patterns/ai/rag-retrieval"
)

// The invariant: at least MinAccuracy of in-scope questions answered with the
// right fact, no ungrounded or uncited answer, and at least
// MinOutOfScopeRefusal of out-of-scope questions refused.
func TestBrokenViolatesInvariant(t *testing.T) {
	r := Broken()
	if len(r.Runs) != 2 {
		t.Fatalf("broken report has %d runs, want 2 (model memory, naive RAG)", len(r.Runs))
	}
	for _, run := range r.Runs {
		if len(run.Violations()) == 0 {
			t.Errorf("%s: should violate the invariant", run.Name)
		}
		if run.FalseRefusals+run.CorrectRefusals != 0 {
			t.Errorf("%s: the broken bot never says I don't know, but refused %d questions", run.Name, run.FalseRefusals+run.CorrectRefusals)
		}
		if run.Accuracy() >= MinAccuracy {
			t.Errorf("%s: accuracy %.2f should be below %.2f", run.Name, run.Accuracy(), MinAccuracy)
		}
	}
	mem, naive := r.Runs[0], r.Runs[1]
	if mem.Correct != 0 || mem.Uncited != mem.InScope+mem.OutOfScope {
		t.Errorf("model memory: %d right, %d uncited; want 0 right and every answer uncited", mem.Correct, mem.Uncited)
	}
	if !strings.Contains(mem.Spotlight[0], "14 days") || !strings.Contains(mem.Spotlight[2], "PEBBLE20") {
		t.Errorf("model memory should state the old 14-day window and the invented code: %q", mem.Spotlight)
	}
	if naive.RecallAt3 >= 0.5 {
		t.Errorf("naive RAG recall@3 = %.2f; tiny chunks should mostly miss the answer", naive.RecallAt3)
	}
}

func TestFixedHoldsInvariant(t *testing.T) {
	r := Fixed()
	if len(r.Runs) != 1 {
		t.Fatalf("fixed report has %d runs, want 1", len(r.Runs))
	}
	run := r.Runs[0]
	if v := run.Violations(); len(v) != 0 {
		t.Fatalf("fixed run violates the invariant: %v", v)
	}
	if run.Ungrounded != 0 || run.Uncited != 0 {
		t.Fatalf("fixed run shipped %d ungrounded and %d uncited answers", run.Ungrounded, run.Uncited)
	}
	if !strings.Contains(run.Spotlight[0], "30 days") || !strings.Contains(run.Spotlight[1], "$50") {
		t.Errorf("fixed run should quote the current policy: %q", run.Spotlight)
	}
	if run.Spotlight[2] != ragretrieval.RefusalText {
		t.Errorf("fixed run should refuse the invented-discount question, said %q", run.Spotlight[2])
	}
	if broken := Broken().Runs[1]; run.RecallAt3 <= broken.RecallAt3 {
		t.Errorf("fixed recall@3 %.2f should beat naive %.2f", run.RecallAt3, broken.RecallAt3)
	}
}

// The groundedness check is what turns the model's memory fallback into a
// refusal. The fixed pipeline's gates happen to stop every question that
// would trigger it, so test it directly: given a context that does not
// mention the question, the model answers from memory with no citation, and
// Grounded rejects that.
func TestGroundednessCheckRejectsMemoryFallback(t *testing.T) {
	hit := ragretrieval.Hit{Chunk: ragretrieval.Chunk{ID: "store-guide/9", Text: "Parking is free for the first 2 hours."}}
	p, err := ragretrieval.BuildPrompt("Do you offer a student discount?", []ragretrieval.Hit{hit}, TokenBudget)
	if err != nil {
		t.Fatal(err)
	}
	a := model(p)
	if a.Refused || !strings.Contains(a.Text, "PEBBLE20") {
		t.Fatalf("model should fall back to its invented discount, said %q", a.Text)
	}
	if ragretrieval.Grounded(a, p) == nil {
		t.Fatal("Grounded accepted an answer that cites nothing")
	}
	if naive := Broken().Runs[1]; naive.Ungrounded == 0 {
		t.Error("naive RAG should ship at least one memory answer that the check would have caught")
	}
}

// The cost of the fix: some in-scope paraphrases are refused, every prompt is
// much bigger than the bare question, and some answers are grounded, cited
// and still wrong.
func TestCostOfTheFix(t *testing.T) {
	mem, fixed := Broken().Runs[0], Fixed().Runs[0]
	if fixed.FalseRefusals == 0 || len(fixed.FalselyRefused) != fixed.FalseRefusals {
		t.Errorf("expected some false refusals of in-scope paraphrases, got %d (%v)", fixed.FalseRefusals, fixed.FalselyRefused)
	}
	if extra := fixed.PromptTokens - mem.PromptTokens; extra < 50 {
		t.Errorf("RAG should add a lot of prompt tokens; extra = %.0f", extra)
	}
	if fixed.GroundedButWrong == 0 {
		t.Error("expected residual grounded-but-wrong answers; the extractive model can quote the wrong sentence")
	}
	// The coverage gate buys out-of-scope refusals with in-scope ones.
	noCoverage := fixedRun(TopK, TokenBudget, MinRelevance, 0)
	if noCoverage.CorrectRefusals >= fixed.CorrectRefusals || noCoverage.FalseRefusals >= fixed.FalseRefusals {
		t.Errorf("coverage gate: refusals out-of-scope %d -> %d, in-scope %d -> %d; want both to rise",
			noCoverage.CorrectRefusals, fixed.CorrectRefusals, noCoverage.FalseRefusals, fixed.FalseRefusals)
	}
}

func TestDeterministicAndRaceFree(t *testing.T) {
	want := Fixed()
	var wg sync.WaitGroup
	got := make([]Report, 4)
	for i := range got {
		wg.Add(1)
		go func() { defer wg.Done(); got[i] = Fixed() }()
	}
	wg.Wait()
	for i, g := range got {
		if !reflect.DeepEqual(g, want) {
			t.Fatalf("run %d differs from the first", i)
		}
	}
}
