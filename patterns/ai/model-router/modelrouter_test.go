package modelrouter

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (f *fakeClock) Now() time.Time          { f.mu.Lock(); defer f.mu.Unlock(); return f.t }
func (f *fakeClock) Advance(d time.Duration) { f.mu.Lock(); f.t = f.t.Add(d); f.mu.Unlock() }

// scripted is a model with a fixed confidence and output size.
type scripted struct {
	name       string
	price      Pricing
	confidence float64
	latency    time.Duration
	mu         sync.Mutex
	calls      int
	lastInput  int
}

func (s *scripted) Name() string     { return s.name }
func (s *scripted) Pricing() Pricing { return s.price }
func (s *scripted) Complete(_ context.Context, _ Request, in int) (Response, error) {
	s.mu.Lock()
	s.calls++
	s.lastInput = in
	s.mu.Unlock()
	return Response{Text: s.name, OutputTokens: 1000, Latency: s.latency, Confidence: s.confidence, Correct: true}, nil
}

func (s *scripted) Calls() int { s.mu.Lock(); defer s.mu.Unlock(); return s.calls }

func tiers(confSmall, confMed float64) (*scripted, *scripted, *scripted) {
	return &scripted{name: "small", price: Pricing{0, 1}, confidence: confSmall, latency: 100 * time.Millisecond},
		&scripted{name: "medium", price: Pricing{0, 10}, confidence: confMed, latency: 300 * time.Millisecond},
		&scripted{name: "large", price: Pricing{0, 100}, confidence: 0.1, latency: time.Second}
}

func TestNewValidation(t *testing.T) {
	s, m, l := tiers(1, 1)
	three := []Model{s, m, l}
	tests := []struct {
		name   string
		models []Model
		th     []float64
		opts   []Option
	}{
		{"no models", nil, nil, nil},
		{"nil model", []Model{s, nil}, []float64{0.5}, nil},
		{"too few thresholds", three, []float64{0.5}, nil},
		{"too many thresholds", []Model{s}, []float64{0.5}, nil},
		{"zero threshold", three, []float64{0, 0.5}, nil},
		{"threshold above 1", three, []float64{0.5, 1.1}, nil},
		{"not ascending", three, []float64{0.6, 0.6}, nil},
		{"confidence above 1", three, []float64{0.3, 0.6}, []Option{WithEscalation(1.5)}},
		{"negative confidence", three, []float64{0.3, 0.6}, []Option{WithEscalation(-0.1)}},
		{"negative history limit", three, []float64{0.3, 0.6}, []Option{WithHistoryLimit(-2)}},
		{"bad scorer", three, []float64{0.3, 0.6}, []Option{WithScorer(Scorer{})}},
		{"negative scorer weight", three, []float64{0.3, 0.6}, []Option{WithScorer(func() Scorer {
			sc := DefaultScorer()
			sc.ToolsWeight = -1
			return sc
		}())}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.models, tt.th, tt.opts...); err == nil {
				t.Fatal("expected error")
			}
		})
	}
	if _, err := New([]Model{l}, nil); err != nil {
		t.Fatalf("single model should be valid: %v", err)
	}
}

func TestRoutingTableBoundaries(t *testing.T) {
	s, m, l := tiers(1, 1)
	r, err := New([]Model{s, m, l}, []float64{0.25, 0.55})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		score float64
		want  int
	}{
		{0, 0}, {0.2499, 0}, {0.25, 1}, {0.5499, 1}, {0.55, 2}, {1, 2},
	}
	for _, tt := range tests {
		if got := r.TierFor(tt.score); got != tt.want {
			t.Errorf("TierFor(%v) = %d, want %d", tt.score, got, tt.want)
		}
	}
}

func TestScorerFeatures(t *testing.T) {
	sc := DefaultScorer()
	tests := []struct {
		name string
		req  Request
		want float64
	}{
		{"greeting", Request{Prompt: "hi"}, 0.25 * 1.0 / 300},
		{"one keyword", Request{Prompt: "compare"}, 0.25*2.0/300 + 0.2},
		{"keyword cap", Request{Prompt: "explain why, compare, code, debug"}, 0.25*9.0/300 + 0.4},
		{"questions after the first", Request{Prompt: "a?b?c?"}, 0.25*2.0/300 + 0.2},
		{"tools", Request{Prompt: "", NeedsTools: true}, 0.25},
		{"depth beyond free turns", Request{History: make([]Turn, 7)}, 3 * 0.02},
		{"length capped", Request{Prompt: strings.Repeat("x", 4000)}, 0.25},
		{"total clamped", Request{Prompt: strings.Repeat("explain why compare? ", 200), NeedsTools: true,
			History: make([]Turn, 40)}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sc.Score(tt.req); math.Abs(got-tt.want) > 1e-9 {
				t.Fatalf("Score = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRoutesByScore(t *testing.T) {
	s, m, l := tiers(1, 1)
	r, _ := New([]Model{s, m, l}, []float64{0.25, 0.55})
	ctx := context.Background()
	for _, tt := range []struct {
		prompt string
		tools  bool
		want   string
	}{
		{"hello there", false, "small"},
		{"compare these two plans step by step", false, "medium"},
		{"explain why this code fails", true, "large"},
	} {
		res, err := r.Route(ctx, Request{Prompt: tt.prompt, NeedsTools: tt.tools})
		if err != nil {
			t.Fatal(err)
		}
		if res.Model != tt.want || res.Escalated {
			t.Errorf("%q: served by %s (escalated %v), want %s", tt.prompt, res.Model, res.Escalated, tt.want)
		}
	}
}

func TestEscalationPath(t *testing.T) {
	// small and medium are unsure, so a small-routed request climbs to large
	// and pays for all three calls.
	s, m, l := tiers(0.3, 0.5)
	r, _ := New([]Model{s, m, l}, []float64{0.25, 0.55}, WithEscalation(0.7))
	res, err := r.Route(context.Background(), Request{Prompt: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Model != "large" || res.Tier != 2 || res.RoutedTier != 0 || !res.Escalated {
		t.Fatalf("got model %s tier %d routed %d escalated %v", res.Model, res.Tier, res.RoutedTier, res.Escalated)
	}
	if len(res.Attempts) != 3 {
		t.Fatalf("attempts = %d, want 3", len(res.Attempts))
	}
	// 1000 output tokens each at $1, $10, $100 per 1K.
	if want := 111.0; math.Abs(res.Cost-want) > 1e-9 {
		t.Fatalf("cost = %v, want %v (every attempt is paid for)", res.Cost, want)
	}
	if want := 1400 * time.Millisecond; res.Latency != want {
		t.Fatalf("latency = %v, want %v (attempts are sequential)", res.Latency, want)
	}
	// The top tier's low confidence is accepted: there is nowhere to go.
	if l.Calls() != 1 {
		t.Fatalf("large called %d times", l.Calls())
	}

	// Confident small model: no escalation.
	s2, m2, l2 := tiers(0.9, 0.9)
	r2, _ := New([]Model{s2, m2, l2}, []float64{0.25, 0.55}, WithEscalation(0.7))
	res, _ = r2.Route(context.Background(), Request{Prompt: "hi"})
	if res.Model != "small" || res.Escalated || m2.Calls() != 0 {
		t.Fatalf("confident small should not escalate: %+v", res)
	}
	// Exactly at the threshold counts as confident.
	s3, m3, l3 := tiers(0.7, 0.9)
	r3, _ := New([]Model{s3, m3, l3}, []float64{0.25, 0.55}, WithEscalation(0.7))
	if res, _ = r3.Route(context.Background(), Request{Prompt: "hi"}); res.Escalated {
		t.Fatal("confidence equal to the threshold should not escalate")
	}
	if st := r.Stats(); st.Escalated != 1 || st.Requests != 1 || st.Served["large"] != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestBudgetDowngradeRejectAndReset(t *testing.T) {
	clk := &fakeClock{t: time.Date(2026, 1, 1, 23, 0, 0, 0, time.UTC)}
	b, err := NewBudget(150, 250, WithBudgetClock(clk.Now))
	if err != nil {
		t.Fatal(err)
	}
	s, m, l := tiers(0.3, 0.3)
	r, _ := New([]Model{s, m, l}, []float64{0.25, 0.55}, WithEscalation(0.7), WithBudget(b))
	ctx := context.Background()
	big := Request{Tenant: "acme", Prompt: "explain why this code fails", NeedsTools: true}

	res, err := r.Route(ctx, big) // large: $100
	if err != nil || res.Model != "large" || res.Downgraded {
		t.Fatalf("first call: %+v %v", res, err)
	}
	if b.Check("acme") != Allow {
		t.Fatal("$100 is under the soft limit")
	}
	r.Route(ctx, big) // $200: past soft
	if b.Check("acme") != Downgrade {
		t.Fatalf("want downgrade at $%v", b.Spent("acme"))
	}
	res, err = r.Route(ctx, big)
	if err != nil || res.Model != "small" || !res.Downgraded || res.Escalated {
		t.Fatalf("downgraded call should use small without escalation: %+v %v", res, err)
	}
	if b.Check("other") != Allow {
		t.Fatal("budgets are per tenant")
	}
	b.Charge("acme", 49) // $250: at the hard limit
	if _, err = r.Route(ctx, big); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("want ErrBudgetExceeded, got %v", err)
	}
	if st := r.Stats(); st.Rejected != 1 || st.Downgraded != 1 {
		t.Fatalf("stats = %+v", st)
	}

	clk.Advance(59 * time.Minute) // 23:59, same day
	if b.Check("acme") != Reject {
		t.Fatal("still the same day")
	}
	clk.Advance(time.Minute) // midnight UTC
	if b.Check("acme") != Allow || b.Spent("acme") != 0 {
		t.Fatalf("budget should reset at the day boundary, spent %v", b.Spent("acme"))
	}
	if res, err = r.Route(ctx, big); err != nil || res.Model != "large" {
		t.Fatalf("after reset: %+v %v", res, err)
	}
}

func TestBudgetValidation(t *testing.T) {
	for _, tt := range []struct{ soft, hard float64 }{{0, 1}, {1, 0}, {-1, 1}, {2, 1}} {
		if _, err := NewBudget(tt.soft, tt.hard); err == nil {
			t.Errorf("NewBudget(%v, %v): expected error", tt.soft, tt.hard)
		}
	}
	if _, err := NewBudget(1, 1); err != nil {
		t.Errorf("soft == hard should be valid: %v", err)
	}
}

func TestEstimateTokens(t *testing.T) {
	tests := []struct {
		in   string
		want int
	}{
		{"", 0}, {"a", 1}, {"abcd", 1}, {"abcde", 2}, {strings.Repeat("x", 400), 100},
		{"héllo", 2},  // 5 code points, not 6 bytes
		{"日本語の文章", 2}, // 6 code points
	}
	for _, tt := range tests {
		if got := EstimateTokens(tt.in); got != tt.want {
			t.Errorf("EstimateTokens(%q) = %d, want %d", tt.in, got, tt.want)
		}
	}
	req := Request{Prompt: "abcd", History: []Turn{{Content: "abcdefgh"}}}
	if got, want := EstimateRequestTokens(req), 1+4+2+4; got != want {
		t.Errorf("EstimateRequestTokens = %d, want %d", got, want)
	}
}

func TestTrimHistory(t *testing.T) {
	h := []Turn{{"user", "1"}, {"assistant", "2"}, {"user", "3"}, {"assistant", "4"}, {"user", "5"}}
	got := TrimHistory(h, 2)
	if len(got) != 3 || got[0].Role != "system" || !strings.Contains(got[0].Content, "3 earlier turns") ||
		got[1].Content != "4" || got[2].Content != "5" {
		t.Fatalf("TrimHistory = %+v", got)
	}
	if len(h) != 5 || h[0].Content != "1" {
		t.Fatal("input was modified")
	}
	if got = TrimHistory(h, 5); len(got) != 5 || got[0].Content != "1" {
		t.Fatalf("no trimming needed: %+v", got)
	}
	if got = TrimHistory(h, 0); len(got) != 1 || got[0].Role != "system" {
		t.Fatalf("keep 0: %+v", got)
	}
	if got = TrimHistory(nil, 3); len(got) != 0 {
		t.Fatalf("nil history: %+v", got)
	}

	// The router trims before estimating tokens but scores the full depth.
	s, m, l := tiers(1, 1)
	r, _ := New([]Model{s, m, l}, []float64{0.25, 0.55}, WithHistoryLimit(1))
	long := make([]Turn, 10)
	for i := range long {
		long[i] = Turn{Role: "user", Content: strings.Repeat("x", 400)}
	}
	res, err := r.Route(context.Background(), Request{Prompt: "hi", History: long})
	if err != nil {
		t.Fatal(err)
	}
	placeholder := EstimateTokens("[summary of 9 earlier turns]")
	if want := 1 + 4 + (placeholder + 4) + (100 + 4); res.InputTokens != want || s.lastInput != want {
		t.Fatalf("input tokens = %d (model saw %d), want %d", res.InputTokens, s.lastInput, want)
	}
	if res.TrimmedTurns != 9 {
		t.Fatalf("TrimmedTurns = %d, want 9", res.TrimmedTurns)
	}
	if want := DefaultScorer().Score(Request{Prompt: "hi", History: long}); res.Score != want {
		t.Fatalf("score %v should use the full history depth (%v)", res.Score, want)
	}
}

func TestFakeModel(t *testing.T) {
	if _, err := NewFakeModel(FakeConfig{}); err == nil {
		t.Fatal("expected error for missing name")
	}
	for _, cfg := range []FakeConfig{
		{Name: "x", Skill: 1.5},
		{Name: "x", Pricing: Pricing{InputPer1K: -1}},
		{Name: "x", BaseLatency: -1},
		{Name: "x", MaxAccuracy: 1.5},
	} {
		if _, err := NewFakeModel(cfg); err == nil {
			t.Errorf("NewFakeModel(%+v): expected error", cfg)
		}
	}
	weak, _ := NewFakeModel(FakeConfig{Name: "weak", Skill: 0.3, Seed: 7})
	strong, _ := NewFakeModel(FakeConfig{Name: "strong", Skill: 0.9, Seed: 7})
	if weak.PCorrect(0.8) >= strong.PCorrect(0.8) || weak.PCorrect(0.1) <= weak.PCorrect(0.8) {
		t.Fatal("accuracy should rise with skill and fall with difficulty")
	}
	ctx := context.Background()
	a, _ := weak.Complete(ctx, Request{ID: 42, Difficulty: 0.5}, 10)
	b, _ := weak.Complete(ctx, Request{ID: 42, Difficulty: 0.5}, 10)
	if a != b {
		t.Fatal("same request must give the same response")
	}
	// Empirical accuracy tracks PCorrect.
	n, correct := 20000, 0
	for i := range n {
		if r, _ := strong.Complete(ctx, Request{ID: uint64(i), Difficulty: 0.9}, 0); r.Correct {
			correct++
		}
	}
	if got, want := float64(correct)/float64(n), strong.PCorrect(0.9); math.Abs(got-want) > 0.02 {
		t.Fatalf("accuracy %.3f, want about %.3f", got, want)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := strong.Complete(cctx, Request{}, 0); err == nil {
		t.Fatal("expected context error")
	}
}

func TestConcurrentRouting(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	b, _ := NewBudget(1e9, 1e9, WithBudgetClock(clk.Now))
	s, m, l := tiers(0.3, 0.9)
	r, _ := New([]Model{s, m, l}, []float64{0.25, 0.55}, WithEscalation(0.7), WithBudget(b), WithHistoryLimit(2))
	var wg sync.WaitGroup
	const workers, each = 8, 200
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				tenant := []string{"a", "b"}[(w+i)%2]
				if _, err := r.Route(context.Background(), Request{Tenant: tenant, Prompt: "hi"}); err != nil {
					t.Error(err)
					return
				}
				if i%50 == 0 {
					clk.Advance(time.Second)
					_ = r.Stats()
				}
			}
		}()
	}
	wg.Wait()
	st := r.Stats()
	if st.Requests != workers*each || st.Escalated != workers*each || st.Served["medium"] != workers*each {
		t.Fatalf("stats = %+v", st)
	}
	// Each request costs $1 (small) + $10 (medium).
	if got, want := b.Spent("a")+b.Spent("b"), 11.0*workers*each; math.Abs(got-want) > 1e-6 {
		t.Fatalf("spent %v, want %v", got, want)
	}
}

func TestContextCanceled(t *testing.T) {
	s, m, l := tiers(1, 1)
	r, _ := New([]Model{s, m, l}, []float64{0.25, 0.55})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.Route(ctx, Request{Prompt: "hi"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}
