package modelrouter

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// Scorer turns a request into a complexity score in [0, 1] from cheap,
// local features. Each feature adds a capped amount; the total is clamped
// to 1. It costs microseconds, which matters: a router that calls a model to
// decide which model to call eats into the savings.
type Scorer struct {
	// LengthTokensForMax is the prompt length (estimated tokens) that earns
	// the full LengthWeight; shorter prompts earn a proportional share.
	LengthTokensForMax int
	LengthWeight       float64
	// Keywords that suggest reasoning work; each match adds KeywordWeight,
	// up to KeywordCap. Matching is case-insensitive substring.
	Keywords      []string
	KeywordWeight float64
	KeywordCap    float64
	// Each question mark after the first adds QuestionWeight, up to QuestionCap.
	QuestionWeight float64
	QuestionCap    float64
	// ToolsWeight is added when the request needs tool calls.
	ToolsWeight float64
	// Each history turn beyond DepthFree adds DepthWeight, up to DepthCap.
	DepthFree   int
	DepthWeight float64
	DepthCap    float64
}

// DefaultScorer returns the weights used by the example. They are a
// starting point; real weights come from labelled traffic.
func DefaultScorer() Scorer {
	return Scorer{
		LengthTokensForMax: 300, LengthWeight: 0.25,
		Keywords: []string{
			"explain why", "compare", "code", "debug", "prove", "step by step",
			"trade-off", "design", "analyze", "refactor",
		},
		KeywordWeight: 0.2, KeywordCap: 0.4,
		QuestionWeight: 0.1, QuestionCap: 0.2,
		ToolsWeight: 0.25,
		DepthFree:   4, DepthWeight: 0.02, DepthCap: 0.1,
	}
}

func (s Scorer) validate() error {
	if s.LengthTokensForMax <= 0 {
		return errors.New("modelrouter: scorer LengthTokensForMax must be positive")
	}
	if s.DepthFree < 0 {
		return errors.New("modelrouter: scorer DepthFree must not be negative")
	}
	for name, v := range map[string]float64{
		"LengthWeight": s.LengthWeight, "KeywordWeight": s.KeywordWeight, "KeywordCap": s.KeywordCap,
		"QuestionWeight": s.QuestionWeight, "QuestionCap": s.QuestionCap, "ToolsWeight": s.ToolsWeight,
		"DepthWeight": s.DepthWeight, "DepthCap": s.DepthCap,
	} {
		if v < 0 {
			return fmt.Errorf("modelrouter: scorer %s must not be negative", name)
		}
	}
	return nil
}

// Score returns the complexity score of req in [0, 1].
func (s Scorer) Score(req Request) float64 {
	score := 0.0

	tokens := EstimateTokens(req.Prompt)
	score += s.LengthWeight * min(1, float64(tokens)/float64(s.LengthTokensForMax))

	lower := strings.ToLower(req.Prompt)
	kw := 0.0
	for _, k := range s.Keywords {
		if strings.Contains(lower, k) {
			kw += s.KeywordWeight
		}
	}
	score += min(kw, s.KeywordCap)

	if q := strings.Count(req.Prompt, "?"); q > 1 {
		score += min(float64(q-1)*s.QuestionWeight, s.QuestionCap)
	}
	if req.NeedsTools {
		score += s.ToolsWeight
	}
	if d := len(req.History) - s.DepthFree; d > 0 {
		score += min(float64(d)*s.DepthWeight, s.DepthCap)
	}
	return min(score, 1)
}

// EstimateTokens approximates the token count of s as characters / 4,
// rounded up. Four characters per token is the usual rule of thumb for
// English text with BPE tokenizers; code, non-Latin scripts and numbers
// tokenize worse, so treat it as an estimate for routing and budgeting, not
// for billing. Characters are Unicode code points, not bytes.
func EstimateTokens(s string) int {
	return (utf8.RuneCountInString(s) + 3) / 4
}

// perMessageOverhead is the few tokens chat formats add per message for the
// role and separators.
const perMessageOverhead = 4

// EstimateRequestTokens estimates the input tokens of req: the prompt plus
// every history turn, each with a small per-message overhead.
func EstimateRequestTokens(req Request) int {
	n := EstimateTokens(req.Prompt) + perMessageOverhead
	for _, t := range req.History {
		n += EstimateTokens(t.Content) + perMessageOverhead
	}
	return n
}

// TrimHistory keeps the last keep turns. If any turns are dropped, a single
// system turn standing in for a summary of them is put first. In production
// that placeholder would be a real summary, produced once by a cheap model
// and cached; here it only records how many turns it replaces. The input
// slice is not modified.
func TrimHistory(history []Turn, keep int) []Turn {
	if keep < 0 {
		keep = 0
	}
	if len(history) <= keep {
		return append([]Turn(nil), history...)
	}
	dropped := len(history) - keep
	out := make([]Turn, 0, keep+1)
	out = append(out, Turn{Role: "system", Content: fmt.Sprintf("[summary of %d earlier turns]", dropped)})
	return append(out, history[dropped:]...)
}
