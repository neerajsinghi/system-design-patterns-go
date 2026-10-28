package modelrouter

import (
	"context"
	"errors"
	"hash/fnv"
	"math"
	"time"
)

// FakeConfig describes a FakeModel.
type FakeConfig struct {
	Name    string
	Pricing Pricing
	// Latency is BaseLatency plus PerOutputToken for every generated token.
	BaseLatency    time.Duration
	PerOutputToken time.Duration
	// Skill in [0, 1] is the difficulty at which the model is right about
	// half the time (scaled by MaxAccuracy). Bigger models have higher skill.
	Skill float64
	// MaxAccuracy caps the probability of a correct answer; even large
	// models are sometimes wrong on easy questions. Default 0.98.
	MaxAccuracy float64
	// Steepness controls how fast accuracy falls as difficulty passes Skill.
	// Default 10.
	Steepness float64
	// ConfidenceNoise is the half-width of uniform noise added to the true
	// probability of being right to produce the reported confidence.
	// Default 0.1.
	ConfidenceNoise float64
	Seed            uint64
}

// FakeModel is a deterministic stand-in for an LLM. For a request of
// difficulty d it is correct with probability
//
//	P(correct) = MaxAccuracy / (1 + exp(Steepness * (d - Skill)))
//
// decided by a hash of (Seed, request ID, model name), so the same request
// always gets the same outcome and no shared random state is needed. It
// reports a confidence equal to that probability plus bounded noise, which
// makes it a reasonably calibrated model: real models are less so.
// Output length grows with difficulty: 40 + 400*d tokens.
type FakeModel struct {
	cfg      FakeConfig
	nameHash uint64
}

// NewFakeModel validates cfg and applies defaults.
func NewFakeModel(cfg FakeConfig) (*FakeModel, error) {
	if cfg.Name == "" {
		return nil, errors.New("modelrouter: fake model needs a name")
	}
	if cfg.Pricing.InputPer1K < 0 || cfg.Pricing.OutputPer1K < 0 {
		return nil, errors.New("modelrouter: prices must not be negative")
	}
	if cfg.BaseLatency < 0 || cfg.PerOutputToken < 0 {
		return nil, errors.New("modelrouter: latencies must not be negative")
	}
	if cfg.Skill < 0 || cfg.Skill > 1 {
		return nil, errors.New("modelrouter: skill must be in [0, 1]")
	}
	if cfg.MaxAccuracy == 0 {
		cfg.MaxAccuracy = 0.98
	}
	if cfg.Steepness == 0 {
		cfg.Steepness = 10
	}
	if cfg.ConfidenceNoise == 0 {
		cfg.ConfidenceNoise = 0.1
	}
	if cfg.MaxAccuracy < 0 || cfg.MaxAccuracy > 1 || cfg.Steepness < 0 || cfg.ConfidenceNoise < 0 {
		return nil, errors.New("modelrouter: MaxAccuracy must be in [0, 1], Steepness and ConfidenceNoise non-negative")
	}
	h := fnv.New64a()
	h.Write([]byte(cfg.Name))
	return &FakeModel{cfg: cfg, nameHash: h.Sum64()}, nil
}

// Name returns the model name.
func (m *FakeModel) Name() string { return m.cfg.Name }

// Pricing returns the model's prices.
func (m *FakeModel) Pricing() Pricing { return m.cfg.Pricing }

// PCorrect is the model's probability of answering a request of the given
// difficulty correctly.
func (m *FakeModel) PCorrect(difficulty float64) float64 {
	return m.cfg.MaxAccuracy / (1 + math.Exp(m.cfg.Steepness*(difficulty-m.cfg.Skill)))
}

// Complete simulates a call. It returns immediately: Latency is simulated.
func (m *FakeModel) Complete(ctx context.Context, req Request, _ int) (Response, error) {
	if err := ctx.Err(); err != nil {
		return Response{}, err
	}
	p := m.PCorrect(req.Difficulty)
	correct := m.unit(req.ID, 1) < p
	noise := (m.unit(req.ID, 2)*2 - 1) * m.cfg.ConfidenceNoise
	conf := math.Min(1, math.Max(0, p+noise))
	out := 40 + int(400*req.Difficulty)
	text := "answer"
	if !correct {
		text = "plausible but wrong answer"
	}
	return Response{
		Text:         text,
		OutputTokens: out,
		Latency:      m.cfg.BaseLatency + time.Duration(out)*m.cfg.PerOutputToken,
		Confidence:   conf,
		Correct:      correct,
	}, nil
}

// unit returns a deterministic value in [0, 1) for (seed, model, id, salt).
func (m *FakeModel) unit(id, salt uint64) float64 {
	x := splitmix(m.cfg.Seed ^ splitmix(m.nameHash^splitmix(id^splitmix(salt))))
	return float64(x>>11) / (1 << 53)
}

func splitmix(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}
