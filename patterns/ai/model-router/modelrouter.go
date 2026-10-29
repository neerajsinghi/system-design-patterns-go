// Package modelrouter implements an LLM model router: a cheap, local scorer
// decides how hard a request looks and sends it to the cheapest model tier
// that should answer it well, instead of sending every request to the
// largest, most expensive model.
//
// Most production LLM traffic is simple: greetings, FAQ lookups, intent
// classification, short rewrites. Paying large-model prices and latency for
// all of it makes cost grow linearly with traffic, while sending everything
// to a small model hurts quality on the minority of genuinely hard requests.
//
// The router combines four levers:
//
//   - Routing: a Scorer turns a request into a complexity score in [0, 1]
//     from cheap features (length, keywords such as "explain why" or
//     "compare", number of questions, whether tools are needed, conversation
//     depth). Ascending thresholds map the score to a tier.
//   - Escalation (a cascade): a model returns a confidence with its answer.
//     Below a minimum confidence the request is retried on the next tier, and
//     both calls are paid for.
//   - Budget caps: per tenant and per day, past a soft limit requests are
//     downgraded to the cheapest tier; past a hard limit they are rejected.
//   - Token reduction: conversation history is trimmed to the last N turns
//     plus a summary placeholder before the call, so fewer input tokens are
//     billed.
//
// Models here are fakes with deterministic, seeded behaviour (see FakeModel),
// so the trade-offs can be measured without a network.
package modelrouter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Turn is one message of earlier conversation.
type Turn struct {
	Role    string // "user", "assistant" or "system"
	Content string
}

// Request is one call to the language model.
type Request struct {
	// ID makes the fake models' outcomes deterministic per request.
	ID         uint64
	Tenant     string
	Prompt     string
	History    []Turn
	NeedsTools bool
	// Difficulty is the hidden ground truth in [0, 1] used only by fake
	// models to decide correctness. The router and scorer never read it.
	Difficulty float64
}

// Pricing is a model's list price in dollars per 1,000 tokens.
type Pricing struct {
	InputPer1K, OutputPer1K float64
}

// Cost returns the dollar cost of a call with the given token counts.
func (p Pricing) Cost(inputTokens, outputTokens int) float64 {
	return float64(inputTokens)/1000*p.InputPer1K + float64(outputTokens)/1000*p.OutputPer1K
}

// Response is what a model returns for one call.
type Response struct {
	Text         string
	OutputTokens int
	Latency      time.Duration
	// Confidence in [0, 1] is the model's own estimate that it is right.
	Confidence float64
	// Correct is ground truth, available only because the models are fake.
	Correct bool
}

// Model is one language model the router can call. Implementations must be
// safe for concurrent use.
type Model interface {
	Name() string
	Pricing() Pricing
	Complete(ctx context.Context, req Request, inputTokens int) (Response, error)
}

// ErrBudgetExceeded is returned when a tenant is past its hard daily limit.
var ErrBudgetExceeded = errors.New("modelrouter: tenant daily budget exceeded")

// Attempt records one model call made while serving a request.
type Attempt struct {
	Model      string
	Cost       float64
	Latency    time.Duration
	Confidence float64
}

// Result describes how a request was served.
type Result struct {
	Response
	Model        string  // the model whose answer was returned
	Tier         int     // index of that model, 0 = cheapest
	Score        float64 // complexity score from the Scorer
	RoutedTier   int     // tier the routing table picked, before escalation
	InputTokens  int     // after history trimming
	Cost         float64 // sum over all attempts
	Latency      time.Duration
	Attempts     []Attempt
	Escalated    bool // at least one retry on a bigger tier
	Downgraded   bool // forced to tier 0 by the soft budget limit
	TrimmedTurns int  // history turns replaced by the summary placeholder
}

// Stats counts what the router did.
type Stats struct {
	Requests, Escalated, Downgraded, Rejected uint64
	Cost                                      float64
	// Served counts answers returned per model name.
	Served map[string]uint64
}

// Router routes requests to model tiers. It is safe for concurrent use.
type Router struct {
	models        []Model
	thresholds    []float64
	scorer        Scorer
	minConfidence float64 // 0 disables escalation
	budget        *Budget
	keepTurns     int // <0 disables trimming

	mu    sync.Mutex
	stats Stats
}

// Option configures a Router.
type Option func(*Router)

// WithScorer replaces DefaultScorer.
func WithScorer(s Scorer) Option { return func(r *Router) { r.scorer = s } }

// WithEscalation retries on the next tier when a model's confidence is below
// minConfidence. The top tier's answer is always accepted.
func WithEscalation(minConfidence float64) Option {
	return func(r *Router) { r.minConfidence = minConfidence }
}

// WithBudget enforces a per-tenant daily budget.
func WithBudget(b *Budget) Option { return func(r *Router) { r.budget = b } }

// WithHistoryLimit keeps only the last keep turns of history (see
// TrimHistory) before estimating tokens and calling the model.
func WithHistoryLimit(keep int) Option { return func(r *Router) { r.keepTurns = keep } }

// New returns a router over models ordered from cheapest to most capable.
// thresholds must be strictly ascending, in (0, 1], with one fewer entry than
// models: a score below thresholds[0] goes to models[0], a score at or above
// thresholds[i-1] and below thresholds[i] goes to models[i], and a score at
// or above the last threshold goes to the last model. A single model with no
// thresholds is valid and gives an "always this model" baseline.
func New(models []Model, thresholds []float64, opts ...Option) (*Router, error) {
	if len(models) == 0 {
		return nil, errors.New("modelrouter: at least one model is required")
	}
	for i, m := range models {
		if m == nil {
			return nil, fmt.Errorf("modelrouter: model %d is nil", i)
		}
	}
	if len(thresholds) != len(models)-1 {
		return nil, fmt.Errorf("modelrouter: need %d thresholds for %d models, got %d",
			len(models)-1, len(models), len(thresholds))
	}
	for i, t := range thresholds {
		if t <= 0 || t > 1 {
			return nil, errors.New("modelrouter: thresholds must be in (0, 1]")
		}
		if i > 0 && t <= thresholds[i-1] {
			return nil, errors.New("modelrouter: thresholds must be strictly ascending")
		}
	}
	r := &Router{
		models:     append([]Model(nil), models...),
		thresholds: append([]float64(nil), thresholds...),
		scorer:     DefaultScorer(),
		keepTurns:  -1,
		stats:      Stats{Served: map[string]uint64{}},
	}
	for _, opt := range opts {
		opt(r)
	}
	if r.minConfidence < 0 || r.minConfidence > 1 {
		return nil, errors.New("modelrouter: escalation confidence must be in [0, 1]")
	}
	if err := r.scorer.validate(); err != nil {
		return nil, err
	}
	if r.keepTurns < -1 {
		return nil, errors.New("modelrouter: history limit must not be negative")
	}
	return r, nil
}

// TierFor returns the tier index the routing table gives a score.
func (r *Router) TierFor(score float64) int {
	tier := 0
	for _, t := range r.thresholds {
		if score >= t {
			tier++
		}
	}
	return tier
}

// Route serves req: check the budget, trim history, score, pick a tier, call
// the model and escalate while confidence is low.
func (r *Router) Route(ctx context.Context, req Request) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	decision := Allow
	if r.budget != nil {
		decision = r.budget.Check(req.Tenant)
	}
	if decision == Reject {
		r.mu.Lock()
		r.stats.Rejected++
		r.mu.Unlock()
		return Result{}, ErrBudgetExceeded
	}

	res := Result{Score: r.scorer.Score(req)} // score sees the full conversation depth
	if r.keepTurns >= 0 {
		trimmed := TrimHistory(req.History, r.keepTurns)
		res.TrimmedTurns = len(req.History) - min(len(req.History), r.keepTurns)
		req.History = trimmed
	}
	res.InputTokens = EstimateRequestTokens(req)

	tier := r.TierFor(res.Score)
	res.RoutedTier = tier
	escalate := r.minConfidence > 0
	if decision == Downgrade {
		tier, escalate, res.Downgraded = 0, false, true
	}

	for {
		m := r.models[tier]
		resp, err := m.Complete(ctx, req, res.InputTokens)
		if err != nil {
			return Result{}, fmt.Errorf("modelrouter: %s: %w", m.Name(), err)
		}
		cost := m.Pricing().Cost(res.InputTokens, resp.OutputTokens)
		res.Attempts = append(res.Attempts, Attempt{
			Model: m.Name(), Cost: cost, Latency: resp.Latency, Confidence: resp.Confidence,
		})
		res.Cost += cost
		res.Latency += resp.Latency // a cascade is sequential: latencies add up
		res.Response, res.Model, res.Tier = resp, m.Name(), tier
		if !escalate || tier == len(r.models)-1 || resp.Confidence >= r.minConfidence {
			break
		}
		tier++
		res.Escalated = true
	}
	res.Response.Latency = res.Latency

	if r.budget != nil {
		r.budget.Charge(req.Tenant, res.Cost)
	}
	r.mu.Lock()
	r.stats.Requests++
	r.stats.Cost += res.Cost
	r.stats.Served[res.Model]++
	if res.Escalated {
		r.stats.Escalated++
	}
	if res.Downgraded {
		r.stats.Downgraded++
	}
	r.mu.Unlock()
	return res, nil
}

// Stats returns a snapshot of the counters.
func (r *Router) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.stats
	s.Served = make(map[string]uint64, len(r.stats.Served))
	for k, v := range r.stats.Served {
		s.Served[k] = v
	}
	return s
}
