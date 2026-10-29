// Command example compares four ways of serving 5,000 synthetic LLM requests
// (70% easy, 20% medium, 10% hard): always the large model, always the small
// model, routed by a complexity score, and routed with confidence-based
// escalation. It then adds history trimming, and finally runs one tenant
// against a daily budget to show the downgrade and reject phases and the
// reset at midnight.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	modelrouter "github.com/neerajsinghi/system-design-patterns-go/patterns/ai/model-router"
)

const (
	numRequests   = 5000
	keepTurns     = 4
	minConfidence = 0.7
)

var thresholds = []float64{0.25, 0.55}

type class int

const (
	easy class = iota
	medium
	hard
)

var words = strings.Fields("the order shipped late and the customer wants a refund for the second " +
	"item in the basket because the size was wrong and support already replied twice")

func filler(rng *rand.Rand, chars int) string {
	var b strings.Builder
	for b.Len() < chars {
		b.WriteString(words[rng.IntN(len(words))])
		b.WriteByte(' ')
	}
	return b.String()
}

func history(rng *rand.Rand, lo, hi int) []modelrouter.Turn {
	n := lo + rng.IntN(hi-lo+1)
	h := make([]modelrouter.Turn, n)
	for i := range h {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		h[i] = modelrouter.Turn{Role: role, Content: filler(rng, 150+rng.IntN(250))}
	}
	return h
}

// synthetic builds the request mix. Prompts carry the surface features a
// real prompt of that kind would, but not perfectly: some easy prompts
// mention "code", and 20% of hard prompts are short with no keywords, so
// the scorer will misroute them.
func synthetic(seed uint64) ([]modelrouter.Request, []class) {
	rng := rand.New(rand.NewPCG(seed, seed^0x5eed))
	reqs := make([]modelrouter.Request, numRequests)
	classes := make([]class, numRequests)
	for i := range reqs {
		r := modelrouter.Request{ID: uint64(i), Tenant: "acme"}
		u := rng.Float64()
		switch {
		case u < 0.70:
			classes[i] = easy
			r.Difficulty = rng.Float64() * 0.3
			switch rng.IntN(4) {
			case 0:
				r.Prompt = "hi! thanks for the help earlier"
			case 1:
				r.Prompt = "what are your opening hours on sunday?"
			case 2:
				r.Prompt = "classify this ticket as billing, shipping or other: " + filler(rng, 200)
			default:
				r.Prompt = "where do I enter my discount code?"
			}
			r.History = history(rng, 0, 4)
		case u < 0.90:
			classes[i] = medium
			r.Difficulty = 0.3 + rng.Float64()*0.35
			if rng.IntN(2) == 0 {
				r.Prompt = "compare the standard and premium plans for a family of four: " + filler(rng, 300)
			} else {
				r.Prompt = "summarise this thread for the account manager: " + filler(rng, 700+rng.IntN(500))
			}
			r.History = history(rng, 2, 8)
		default:
			classes[i] = hard
			r.Difficulty = 0.65 + rng.Float64()*0.35
			if rng.IntN(5) == 0 {
				r.Prompt = "is the refund rule in clause 7 consistent with clause 12?"
				r.History = history(rng, 0, 3)
			} else {
				r.Prompt = "explain why this code double-charges some customers and how to fix it? " +
					"which orders are affected? " + filler(rng, 600+rng.IntN(600))
				r.NeedsTools = rng.IntN(2) == 0
				r.History = history(rng, 4, 16)
			}
		}
		reqs[i] = r
	}
	return reqs, classes
}

func mustModel(cfg modelrouter.FakeConfig) modelrouter.Model {
	m, err := modelrouter.NewFakeModel(cfg)
	if err != nil {
		log.Fatal(err)
	}
	return m
}

type summary struct {
	name                 string
	cost                 float64
	latency              time.Duration
	correct, hardCorrect int
	hardTotal            int
	escalated            int
	inputTokens          int
	served               map[string]uint64
}

func run(name string, r *modelrouter.Router, reqs []modelrouter.Request, classes []class) summary {
	s := summary{name: name}
	ctx := context.Background()
	for i, req := range reqs {
		res, err := r.Route(ctx, req)
		if err != nil {
			log.Fatal(err)
		}
		s.cost += res.Cost
		s.latency += res.Latency
		s.inputTokens += res.InputTokens
		if res.Correct {
			s.correct++
		}
		if classes[i] == hard {
			s.hardTotal++
			if res.Correct {
				s.hardCorrect++
			}
		}
		if res.Escalated {
			s.escalated++
		}
	}
	s.served = r.Stats().Served
	return s
}

func pct(n, d int) float64 { return 100 * float64(n) / float64(d) }

func printTable(rows []summary) {
	fmt.Printf("   %-26s %9s %8s %9s %8s %9s %9s %10s  %s\n",
		"strategy", "cost $", "vs large", "mean lat", "correct", "hard ok", "escalated", "input tok", "answers small/medium/large")
	base := rows[0].cost
	for _, s := range rows {
		n := numRequests
		fmt.Printf("   %-26s %9.2f %7.0f%% %9v %7.1f%% %8.1f%% %8.1f%% %10d  %4d / %4d / %4d\n",
			s.name, s.cost, 100*s.cost/base, (s.latency / time.Duration(n)).Round(time.Millisecond),
			pct(s.correct, n), pct(s.hardCorrect, s.hardTotal), pct(s.escalated, n), s.inputTokens/n,
			s.served["small"], s.served["medium"], s.served["large"])
	}
	fmt.Println()
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func main() {
	small := mustModel(modelrouter.FakeConfig{Name: "small", Seed: 1,
		Pricing:     modelrouter.Pricing{InputPer1K: 0.00015, OutputPer1K: 0.0006},
		BaseLatency: 150 * time.Millisecond, PerOutputToken: 4 * time.Millisecond, Skill: 0.5})
	med := mustModel(modelrouter.FakeConfig{Name: "medium", Seed: 2,
		Pricing:     modelrouter.Pricing{InputPer1K: 0.001, OutputPer1K: 0.004},
		BaseLatency: 300 * time.Millisecond, PerOutputToken: 10 * time.Millisecond, Skill: 0.65})
	large := mustModel(modelrouter.FakeConfig{Name: "large", Seed: 3,
		Pricing:     modelrouter.Pricing{InputPer1K: 0.005, OutputPer1K: 0.015},
		BaseLatency: 600 * time.Millisecond, PerOutputToken: 25 * time.Millisecond, Skill: 0.95})
	tiers := []modelrouter.Model{small, med, large}

	reqs, classes := synthetic(42)
	counts := [3]int{}
	for _, c := range classes {
		counts[c]++
	}
	fmt.Printf("%d synthetic requests: %d easy, %d medium, %d hard (seed 42)\n", numRequests, counts[0], counts[1], counts[2])
	fmt.Println("models (per 1K tokens in/out): small $0.00015/$0.0006, medium $0.001/$0.004, large $0.005/$0.015")
	fmt.Printf("routing thresholds: score < %.2f small, < %.2f medium, else large; escalate below confidence %.1f\n\n",
		thresholds[0], thresholds[1], minConfidence)

	newRouter := func(models []modelrouter.Model, th []float64, opts ...modelrouter.Option) *modelrouter.Router {
		r, err := modelrouter.New(models, th, opts...)
		if err != nil {
			log.Fatal(err)
		}
		return r
	}

	fmt.Println("1) Four strategies on the same traffic")
	rows := []summary{
		run("always-large", newRouter([]modelrouter.Model{large}, nil), reqs, classes),
		run("always-small", newRouter([]modelrouter.Model{small}, nil), reqs, classes),
		run("routed", newRouter(tiers, thresholds), reqs, classes),
		run("routed+escalation", newRouter(tiers, thresholds, modelrouter.WithEscalation(minConfidence)), reqs, classes),
	}
	printTable(rows)

	fmt.Printf("2) Add token reduction: keep the last %d history turns plus a summary placeholder\n", keepTurns)
	rows = append(rows, run(fmt.Sprintf("routed+escalation+trim(%d)", keepTurns),
		newRouter(tiers, thresholds, modelrouter.WithEscalation(minConfidence), modelrouter.WithHistoryLimit(keepTurns)),
		reqs, classes))
	printTable(rows)
	fmt.Println("   (the fake models ignore history, so trimming costs no quality here; with real models")
	fmt.Println("    a poor summary can lose context the answer needed)")
	fmt.Println()

	budgetRun(tiers, reqs)
}

// budgetRun sends the same 5,000 requests from one tenant, evenly over two
// days, through routed+escalation with a daily budget.
func budgetRun(tiers []modelrouter.Model, reqs []modelrouter.Request) {
	const soft, hard = 1.20, 1.35
	clk := &clock{t: time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)}
	budget, err := modelrouter.NewBudget(soft, hard, modelrouter.WithBudgetClock(clk.Now))
	if err != nil {
		log.Fatal(err)
	}
	r, err := modelrouter.New(tiers, thresholds, modelrouter.WithEscalation(minConfidence), modelrouter.WithBudget(budget))
	if err != nil {
		log.Fatal(err)
	}
	step := 48 * time.Hour / numRequests

	fmt.Printf("3) Budget cap: tenant acme, soft $%.2f/day (downgrade to small), hard $%.2f/day (reject)\n", soft, hard)
	fmt.Printf("   the same %d requests spread over two days, one every %v\n", numRequests, step)
	type day struct {
		allowed, downgraded, rejected, correct int
		firstDowngrade, firstReject            string
		spent                                  float64
	}
	days := map[string]*day{}
	var order []string
	ctx := context.Background()
	for _, req := range reqs {
		now := clk.Now()
		key := now.Format("2006-01-02")
		d := days[key]
		if d == nil {
			d = &day{}
			days[key] = d
			order = append(order, key)
		}
		res, err := r.Route(ctx, req)
		switch {
		case errors.Is(err, modelrouter.ErrBudgetExceeded):
			d.rejected++
			if d.firstReject == "" {
				d.firstReject = now.Format("15:04")
			}
		case err != nil:
			log.Fatal(err)
		case res.Downgraded:
			d.downgraded++
			if d.firstDowngrade == "" {
				d.firstDowngrade = now.Format("15:04")
			}
		default:
			d.allowed++
		}
		if err == nil && res.Correct {
			d.correct++
		}
		d.spent = budget.Spent("acme")
		clk.Advance(step)
	}
	fmt.Printf("   %-10s %8s %10s %9s %13s %11s %8s %13s\n",
		"day", "routed", "downgraded", "rejected", "downgrade at", "reject at", "spent $", "correct/served")
	for _, k := range order {
		d := days[k]
		served := d.allowed + d.downgraded
		fmt.Printf("   %-10s %8d %10d %9d %13s %11s %8.2f %13.1f%%\n",
			k, d.allowed, d.downgraded, d.rejected, orDash(d.firstDowngrade), orDash(d.firstReject), d.spent, pct(d.correct, served))
	}
	fmt.Println("   the spend counter resets at UTC midnight, so day two starts routing normally again")
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
