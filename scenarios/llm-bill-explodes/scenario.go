// Package llmbill simulates the "AI platform suddenly becomes very
// expensive" incident. A support assistant launches, usage grows tenfold in
// a month, and the LLM bill grows faster than usage. Every request goes to
// the largest model; every turn resends a long system prompt plus the whole
// conversation so far, so a deep conversation costs more per turn the longer
// it runs; thousands of users ask the same few questions in different words
// and each phrasing pays for a fresh answer; and one tenant's integration
// gets stuck in a loop and spends a month of budget in a day. Finance asks
// for a 70% cut without making answers worse.
//
// Broken sends every request, with full history, to the large model, with no
// cache and no budget.
//
// Fixed puts a semantic cache in front (its similarity threshold picked from
// a labelled calibration set so that wrong answers from the cache stay under
// a target rate), routes cache misses to the cheapest model tier that should
// answer well and escalates on low confidence, trims history to the last few
// turns plus a running summary, and gives every tenant a daily budget: past
// the soft limit requests are downgraded to the small model, past the hard
// limit they are rejected.
//
// Both versions see the same seeded traffic and the same fake models.
package llmbill

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"slices"
	"sort"
	"strings"
	"time"

	modelrouter "github.com/neerajsinghi/system-design-patterns-go/patterns/ai/model-router"
	semanticcache "github.com/neerajsinghi/system-design-patterns-go/patterns/ai/semantic-cache"
)

const (
	// ConversationsPerTenant is how many support conversations each tenant
	// has in the simulated day.
	ConversationsPerTenant = 110
	// Runaway is the tenant whose integration loops.
	Runaway = "vandelay"
	// LoopRequests is how many requests the stuck integration sends, one
	// every LoopEvery from LoopStart.
	LoopRequests = 1500
	LoopEvery    = 3 * time.Second
	LoopStart    = 9 * time.Hour
	// LoopRestart is how many iterations the integration runs before it
	// crashes and starts over with an empty thread.
	LoopRestart = 40

	// KeepTurns is how many recent history turns the fixed version sends;
	// older turns are folded into a running summary of SummaryTokens.
	KeepTurns     = 4
	SummaryTokens = 60
	// MinConfidence: below it the router retries on the next tier.
	MinConfidence = 0.8
	// SoftBudget and HardBudget are each tenant's daily limits in dollars;
	// the hard limit is about a thirtieth of a $52 monthly allowance.
	SoftBudget = 1.50
	HardBudget = 1.75
	// FalseHitTarget is the highest share of cache answers allowed to be for
	// the wrong question when the threshold is chosen.
	FalseHitTarget = 0.01
	// EmbedLatency is paid on every fixed-version request for the cache
	// lookup; EmbedPer1K is the embedding model's price.
	EmbedLatency = 20 * time.Millisecond
	EmbedPer1K   = 0.00002
	// CalibrationSize is the number of labelled prompts used to pick the
	// cache threshold.
	CalibrationSize = 500

	embedDim = 256
)

// Tenants lists the five well-behaved tenants; Runaway is the sixth.
var Tenants = []string{"acme", "globex", "initech", "soylent", "umbrella"}

// Thresholds swept when picking the cache threshold.
var Thresholds = []float32{0.70, 0.75, 0.80, 0.85, 0.90, 0.95}

// Router tiers: score < 0.25 small, < 0.55 medium, otherwise large.
var routeThresholds = []float64{0.25, 0.55}

var day0 = time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC)

// TenantStats is one tenant's slice of a run.
type TenantStats struct {
	Name                           string
	Requests, Downgraded, Rejected int
	Cost                           float64
}

// SweepRow is one threshold tried on the calibration set.
type SweepRow struct {
	Threshold    float32
	HitRate      float64 // share of calibration prompts answered from cache
	FalseHitRate float64 // share of those answers that were for another question
}

// Report is what a run measured.
type Report struct {
	Name string

	Requests int // all requests, including the runaway loop
	Cost     float64
	// TokensSent counts input tokens sent to any model, including escalation
	// retries and summarisation calls.
	TokensSent int
	Tenants    []TenantStats

	// Quality is measured on the well-behaved tenants only: a request is
	// correct when the model's answer was correct or, for a cache hit, when
	// the cached answer was for the same question and was itself correct.
	// A rejected request counts as wrong.
	Regular, Correct int
	RegularCost      float64
	LatencyP50       time.Duration // well-behaved tenants' answered requests

	// Served counts answers by source: "cache", "small", "medium", "large".
	Served map[string]int
	// Escalated requests paid for more than one model call; EscalationCost
	// is what those extra calls cost.
	Escalated      int
	EscalationCost float64
	SummaryCost    float64

	CacheLookups, CacheHits, FalseHits int

	// Threshold is the cache threshold chosen from Sweep (fixed only).
	Threshold float32
	Sweep     []SweepRow
}

// Tenant returns the stats for name.
func (r Report) Tenant(name string) TenantStats {
	for _, t := range r.Tenants {
		if t.Name == name {
			return t
		}
	}
	return TenantStats{Name: name}
}

// QualityPct is the percentage of well-behaved requests answered correctly.
func (r Report) QualityPct() float64 { return 100 * float64(r.Correct) / float64(r.Regular) }

// CacheHitRate is hits / lookups.
func (r Report) CacheHitRate() float64 {
	if r.CacheLookups == 0 {
		return 0
	}
	return float64(r.CacheHits) / float64(r.CacheLookups)
}

// Print writes the report.
func (r Report) Print(w io.Writer) {
	fmt.Fprintf(w, "%s\n", r.Name)
	fmt.Fprintf(w, "%d requests in one simulated day: %d tenants × %d conversations, plus %d looping requests from %s\n\n",
		r.Requests, len(Tenants)+1, ConversationsPerTenant, LoopRequests, Runaway)

	if len(r.Sweep) > 0 {
		fmt.Fprintf(w, "  cache threshold, chosen on %d labelled calibration prompts (target: ≤ %.0f%% of cache answers wrong)\n",
			CalibrationSize, 100*FalseHitTarget)
		fmt.Fprintln(w, "    threshold  answered from cache  wrong question")
		for _, s := range r.Sweep {
			mark := ""
			if s.Threshold == r.Threshold {
				mark = "  <- chosen: lowest threshold within target"
			}
			fmt.Fprintf(w, "    %9.2f  %18.1f%%  %13.1f%%%s\n", s.Threshold, 100*s.HitRate, 100*s.FalseHitRate, mark)
		}
		fmt.Fprintln(w)
	}

	fmt.Fprintln(w, "  tenant      requests      cost $  downgraded  rejected")
	for _, t := range r.Tenants {
		fmt.Fprintf(w, "  %-10s  %8d  %10.2f  %10d  %8d\n", t.Name, t.Requests, t.Cost, t.Downgraded, t.Rejected)
	}
	fmt.Fprintf(w, "  %-10s  %8d  %10.2f\n\n", "total", r.Requests, r.Cost)

	answered := 0
	for _, n := range r.Served {
		answered += n
	}
	fmt.Fprintf(w, "  cost per request:        $%.5f (well-behaved tenants: $%.2f for %d requests, $%.5f each)\n",
		r.Cost/float64(r.Requests), r.RegularCost, r.Regular, r.RegularCost/float64(r.Regular))
	fmt.Fprintf(w, "  input tokens sent:       %d (%d per request)\n", r.TokensSent, r.TokensSent/r.Requests)
	fmt.Fprintf(w, "  answers by source:       cache %d, small %d, medium %d, large %d (of %d answered)\n",
		r.Served["cache"], r.Served["small"], r.Served["medium"], r.Served["large"], answered)
	if r.CacheLookups > 0 {
		fmt.Fprintf(w, "  cache:                   %.1f%% hit rate (%d of %d lookups; the loop's requests never hit), %d false hits (%.1f%% of cache answers were for a different question)\n",
			100*r.CacheHitRate(), r.CacheHits, r.CacheLookups, r.FalseHits, 100*float64(r.FalseHits)/float64(max(r.CacheHits, 1)))
	}
	if r.Escalated > 0 || r.SummaryCost > 0 {
		fmt.Fprintf(w, "  escalations:             %d requests paid twice, $%.2f extra; history summaries $%.2f\n",
			r.Escalated, r.EscalationCost, r.SummaryCost)
	}
	fmt.Fprintf(w, "  answer quality:          %.1f%% correct (%d of %d well-behaved requests; cache false hits and rejections count as wrong)\n",
		r.QualityPct(), r.Correct, r.Regular)
	fmt.Fprintf(w, "  p50 latency:             %v\n", r.LatencyP50.Round(time.Millisecond))
}

// ---------------------------------------------------------------- traffic

type kind int

const (
	faq     kind = iota // a common question, phrased many ways; cacheable
	account             // depends on the user's account; needs tools, not cacheable
	hard                // a long troubleshooting question
	loop                // the runaway integration's request
)

// call is one request in the day's traffic.
type call struct {
	id         uint64
	tenant     string
	conv       int // conversation number, unique across tenants
	at         time.Duration
	kind       kind
	prompt     string
	label      string
	difficulty float64
	needsTools bool
	history    []modelrouter.Turn // earlier turns of the conversation, no system prompt
}

type intent struct {
	name         string
	verbs, nouns []string
}

var intents = []intent{
	{"reset_password", []string{"reset", "recover"}, []string{"password", "pwd"}},
	{"change_email", []string{"change", "update"}, []string{"email", "email address"}},
	{"delete_account", []string{"delete", "close"}, []string{"account", "profile"}},
	{"enable_2fa", []string{"enable", "turn on"}, []string{"two factor auth", "2fa"}},
	{"disable_2fa", []string{"disable", "turn off"}, []string{"two factor auth", "2fa"}},
	{"cancel_order", []string{"cancel", "stop"}, []string{"order", "purchase"}},
	{"track_order", []string{"track", "find"}, []string{"order", "package"}},
	{"return_item", []string{"return", "send back"}, []string{"item", "product"}},
	{"apply_coupon", []string{"apply", "use"}, []string{"coupon", "promo code"}},
	{"invoice", []string{"download", "get"}, []string{"invoice", "receipt"}},
	{"cancel_subscription", []string{"cancel", "end"}, []string{"subscription", "membership"}},
	{"upgrade_plan", []string{"upgrade"}, []string{"plan", "subscription"}},
	{"downgrade_plan", []string{"downgrade"}, []string{"plan", "subscription"}},
	{"update_card", []string{"update", "change"}, []string{"card", "credit card"}},
	{"add_user", []string{"add", "invite"}, []string{"team member", "new user"}},
	{"remove_user", []string{"remove", "deactivate"}, []string{"team member", "user"}},
	{"export_data", []string{"export", "download"}, []string{"data", "reports"}},
	{"change_timezone", []string{"change", "set"}, []string{"timezone", "time zone"}},
	{"api_key", []string{"rotate", "regenerate"}, []string{"api key", "api token"}},
	{"webhook_setup", []string{"set up", "configure"}, []string{"webhook", "webhooks"}},
	{"notifications_off", []string{"turn off", "mute"}, []string{"notifications", "email alerts"}},
	{"contact_support", []string{"contact", "reach"}, []string{"support", "a human agent"}},
}

var (
	templates = []string{
		"how do i {v} my {n}", "how can i {v} my {n}", "i want to {v} my {n}",
		"i need to {v} my {n}", "help me {v} my {n}", "{v} {n}",
		"where do i go to {v} my {n}", "is it possible to {v} my {n}",
		"whats the way to {v} the {n}", "steps to {v} {n}",
	}
	prefixes  = []string{"", "", "hi, ", "hello ", "quick question: "}
	suffixes  = []string{"", "?", "?", " please", ", thanks!"}
	stopwords = []string{"i", "my", "me", "the", "a", "an", "to", "please", "hi", "hello", "thanks"}
	synonyms  = map[string]string{
		"pwd": "password", "recover": "reset", "membership": "subscription",
		"purchase": "order", "token": "key", "regenerate": "rotate", "webhooks": "webhook",
	}
	accountTemplates = []string{
		"where is my order #%d, it was meant to arrive yesterday",
		"why was i charged %d dollars twice this month",
		"order %d arrived damaged, can you send a replacement",
		"the refund for order %d still hasnt reached my card",
		"my invoice %d shows the wrong company name",
	}
	hardTemplates = []string{
		"explain why our sso login fails for users in one region after the certificate rotation and compare it with the other region? which settings should we check? ",
		"our webhook deliveries are retried and we see duplicates, explain why this happens and design a safe consumer? what about ordering? ",
		"compare the usage-based and seat-based plans for our team given these numbers and explain why the invoice went up? ",
	}
	shortHard = "is the refund rule in section 7 of our contract consistent with section 12?"
	words     = strings.Fields("the customer opened a ticket about the invoice and the agent replied with " +
		"steps for the dashboard but the export still failed after the retry so they asked again")
)

func filler(rng *rand.Rand, chars int) string {
	var b strings.Builder
	for b.Len() < chars {
		b.WriteString(words[rng.IntN(len(words))])
		b.WriteByte(' ')
	}
	return b.String()
}

// systemPrompt is the long instruction block resent with every call.
func systemPrompt(tenant string) string {
	rng := rand.New(rand.NewPCG(uint64(len(tenant)), 99))
	return "You are the support assistant for " + tenant + ". Follow the tone guide, the refund policy, " +
		"the escalation rules and the product notes below. " + filler(rng, 1900)
}

// replyChars matches the fake models' output length (40 + 400*d tokens).
func replyChars(d float64) int { return 4 * (40 + int(400*d)) }

func faqPrompt(rng *rand.Rand, in intent) string {
	t := templates[rng.IntN(len(templates))]
	t = strings.ReplaceAll(t, "{v}", in.verbs[rng.IntN(len(in.verbs))])
	t = strings.ReplaceAll(t, "{n}", in.nouns[rng.IntN(len(in.nouns))])
	return prefixes[rng.IntN(len(prefixes))] + t + suffixes[rng.IntN(len(suffixes))]
}

// question draws one user question: 60% common questions (a few intents
// dominate), 25% account-specific, 15% hard; 1 in 5 hard questions is short
// with no tell-tale keywords, so the router will under-route it.
func question(rng *rand.Rand, popularity []int, weights []float64, total float64, seq int) (kind, string, string, float64, bool) {
	switch u := rng.Float64(); {
	case u < 0.60:
		x := rng.Float64() * total
		i := 0
		for ; i < len(weights)-1; i++ {
			if x -= weights[i]; x < 0 {
				break
			}
		}
		in := intents[popularity[i]]
		return faq, faqPrompt(rng, in), in.name, 0.02 + rng.Float64()*0.18, false
	case u < 0.85:
		p := fmt.Sprintf(accountTemplates[rng.IntN(len(accountTemplates))], 1000+rng.IntN(90000))
		return account, p, fmt.Sprintf("account-%d", seq), 0.2 + rng.Float64()*0.25, true
	default:
		d := 0.6 + rng.Float64()*0.35
		if rng.IntN(5) == 0 {
			return hard, shortHard, fmt.Sprintf("hard-%d", seq), d, false
		}
		p := hardTemplates[rng.IntN(len(hardTemplates))] + filler(rng, 400+rng.IntN(500))
		return hard, p, fmt.Sprintf("hard-%d", seq), d, rng.IntN(2) == 0
	}
}

func zipf(n int) ([]float64, float64) {
	w := make([]float64, n)
	total := 0.0
	for i := range w {
		w[i] = 1 / math.Pow(float64(i+1), 0.9)
		total += w[i]
	}
	return w, total
}

// traffic builds the day's requests, sorted by time, with IDs assigned in
// that order. It is deterministic.
func traffic() []call {
	rng := rand.New(rand.NewPCG(2026, 10))
	weights, total := zipf(len(intents))
	var calls []call
	conv, seq := 0, 0
	for _, tenant := range append(slices.Clone(Tenants), Runaway) {
		popularity := rng.Perm(len(intents)) // each tenant's users favour different questions
		for range ConversationsPerTenant {
			depth := min(1+int(rng.ExpFloat64()*4), 20)
			start := time.Duration(rng.IntN(86400-depth*60)) * time.Second
			var turns []modelrouter.Turn
			for k := range depth {
				seq++
				kd, prompt, label, d, tools := question(rng, popularity, weights, total, seq)
				calls = append(calls, call{
					tenant: tenant, conv: conv, at: start + time.Duration(k)*45*time.Second,
					kind: kd, prompt: prompt, label: label, difficulty: d, needsTools: tools,
					history: turns[:len(turns):len(turns)],
				})
				turns = append(turns,
					modelrouter.Turn{Role: "user", Content: prompt},
					modelrouter.Turn{Role: "assistant", Content: filler(rng, replyChars(d))})
			}
			conv++
		}
	}
	// The stuck integration: it re-sends a triage request with every reply
	// appended to the thread, and starts over each time it crashes.
	var thread []modelrouter.Turn
	for i := range LoopRequests {
		if i%LoopRestart == 0 {
			thread, conv = nil, conv+1
		}
		seq++
		prompt := "triage ticket 77812: classify it, set a priority and draft a reply"
		d := 0.35
		calls = append(calls, call{
			tenant: Runaway, conv: conv, at: LoopStart + time.Duration(i)*LoopEvery,
			kind: loop, prompt: prompt, label: fmt.Sprintf("loop-%d", seq), difficulty: d, needsTools: true,
			history: thread[:len(thread):len(thread)],
		})
		thread = append(thread,
			modelrouter.Turn{Role: "user", Content: prompt + "\n" + filler(rng, 300)},
			modelrouter.Turn{Role: "assistant", Content: filler(rng, replyChars(d))})
	}
	sort.SliceStable(calls, func(i, j int) bool { return calls[i].at < calls[j].at })
	for i := range calls {
		calls[i].id = uint64(i)
	}
	return calls
}

// ---------------------------------------------------------------- models

type models struct{ small, medium, large *modelrouter.FakeModel }

func newModels() models {
	mk := func(cfg modelrouter.FakeConfig) *modelrouter.FakeModel {
		m, err := modelrouter.NewFakeModel(cfg)
		if err != nil {
			panic(err)
		}
		return m
	}
	return models{
		small: mk(modelrouter.FakeConfig{Name: "small", Seed: 1,
			Pricing:     modelrouter.Pricing{InputPer1K: 0.00015, OutputPer1K: 0.0006},
			BaseLatency: 150 * time.Millisecond, PerOutputToken: 4 * time.Millisecond, Skill: 0.5}),
		medium: mk(modelrouter.FakeConfig{Name: "medium", Seed: 2,
			Pricing:     modelrouter.Pricing{InputPer1K: 0.001, OutputPer1K: 0.004},
			BaseLatency: 300 * time.Millisecond, PerOutputToken: 10 * time.Millisecond, Skill: 0.65}),
		large: mk(modelrouter.FakeConfig{Name: "large", Seed: 3,
			Pricing:     modelrouter.Pricing{InputPer1K: 0.005, OutputPer1K: 0.015},
			BaseLatency: 600 * time.Millisecond, PerOutputToken: 25 * time.Millisecond, Skill: 0.95}),
	}
}

// ---------------------------------------------------------------- runs

type tally struct {
	r       *Report
	tenants map[string]*TenantStats
	lat     []time.Duration
}

func newTally(r *Report) *tally {
	r.Served = map[string]int{}
	t := &tally{r: r, tenants: map[string]*TenantStats{}}
	for _, name := range append(slices.Clone(Tenants), Runaway) {
		t.tenants[name] = &TenantStats{Name: name}
	}
	return t
}

func (t *tally) charge(c call, cost float64) {
	t.r.Cost += cost
	t.tenants[c.tenant].Cost += cost
	if c.kind != loop && c.tenant != Runaway {
		t.r.RegularCost += cost
	}
}

// answered records a served request.
func (t *tally) answered(c call, source string, correct bool, lat time.Duration) {
	t.r.Served[source]++
	if c.tenant == Runaway {
		return
	}
	t.r.Regular++
	if correct {
		t.r.Correct++
	}
	t.lat = append(t.lat, lat)
}

func (t *tally) finish() {
	for _, name := range append(slices.Clone(Tenants), Runaway) {
		t.r.Tenants = append(t.r.Tenants, *t.tenants[name])
	}
	slices.Sort(t.lat)
	if len(t.lat) > 0 {
		t.r.LatencyP50 = t.lat[len(t.lat)/2]
	}
}

var systemPrompts = func() map[string]string {
	m := map[string]string{}
	for _, t := range append(slices.Clone(Tenants), Runaway) {
		m[t] = systemPrompt(t)
	}
	return m
}()

func request(c call, history []modelrouter.Turn) modelrouter.Request {
	h := make([]modelrouter.Turn, 0, len(history)+1)
	h = append(h, modelrouter.Turn{Role: "system", Content: systemPrompts[c.tenant]})
	h = append(h, history...)
	return modelrouter.Request{ID: c.id, Tenant: c.tenant, Prompt: c.prompt, History: h,
		NeedsTools: c.needsTools, Difficulty: c.difficulty}
}

// Broken: the large model, the full history and the system prompt on every
// turn; no cache, no budget.
func Broken() Report {
	r := Report{Name: "BROKEN: every request to the large model with full history, no cache, no budgets"}
	t := newTally(&r)
	m := newModels()
	router, err := modelrouter.New([]modelrouter.Model{m.large}, nil)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	for _, c := range traffic() {
		r.Requests++
		t.tenants[c.tenant].Requests++
		res, err := router.Route(ctx, request(c, c.history))
		if err != nil {
			panic(err)
		}
		r.TokensSent += res.InputTokens * len(res.Attempts)
		t.charge(c, res.Cost)
		t.answered(c, res.Model, res.Correct, res.Latency)
	}
	t.finish()
	return r
}

func newEmbedder() *semanticcache.HashEmbedder {
	emb, err := semanticcache.NewHashEmbedder(embedDim,
		semanticcache.WithStopwords(stopwords...), semanticcache.WithSynonyms(synonyms))
	if err != nil {
		panic(err)
	}
	return emb
}

// Calibrate picks the cache threshold from a labelled calibration set drawn
// from the same question mix as production (a separate seed, one tenant).
// Each prompt is compared with the most similar earlier common question,
// which is what the cache would hold; at each threshold it counts how many
// prompts would be answered from cache and how many of those answers would
// be for a different question. It returns the lowest threshold whose wrong
// share is within FalseHitTarget, and the whole sweep.
func Calibrate(emb *semanticcache.HashEmbedder) (float32, []SweepRow) {
	rng := rand.New(rand.NewPCG(7, 7))
	weights, total := zipf(len(intents))
	popularity := rng.Perm(len(intents))
	type labelled struct {
		vec   []float32
		label string
		faq   bool
	}
	set := make([]labelled, CalibrationSize)
	best := make([]float32, CalibrationSize)
	bestLabel := make([]string, CalibrationSize)
	for i := range set {
		kd, p, label, _, _ := question(rng, popularity, weights, total, i)
		set[i] = labelled{vec: emb.Embed(p), label: label, faq: kd == faq}
		best[i] = -1
		for j := range i {
			if !set[j].faq {
				continue
			}
			if s := semanticcache.Cosine(set[i].vec, set[j].vec); s > best[i] {
				best[i], bestLabel[i] = s, set[j].label
			}
		}
	}
	chosen := Thresholds[len(Thresholds)-1]
	found := false
	var sweep []SweepRow
	for _, th := range Thresholds {
		hits, wrong := 0, 0
		for i := range set {
			if best[i] >= th {
				hits++
				if bestLabel[i] != set[i].label {
					wrong++
				}
			}
		}
		row := SweepRow{Threshold: th, HitRate: float64(hits) / CalibrationSize}
		if hits > 0 {
			row.FalseHitRate = float64(wrong) / float64(hits)
		}
		if !found && row.FalseHitRate <= FalseHitTarget {
			chosen, found = th, true
		}
		sweep = append(sweep, row)
	}
	return chosen, sweep
}

// Fixed: semantic cache, then a routed, escalating model call with trimmed
// history, under a per-tenant daily budget.
func Fixed() Report {
	r := Report{Name: "FIXED: semantic cache + model router with escalation + history trimming + per-tenant budgets"}
	t := newTally(&r)
	m := newModels()
	now := day0
	clock := func() time.Time { return now }

	emb := newEmbedder()
	r.Threshold, r.Sweep = Calibrate(emb)
	cache, err := semanticcache.New(emb.Embed, r.Threshold,
		semanticcache.WithExactKey(emb.Normalize), semanticcache.WithClock(clock))
	if err != nil {
		panic(err)
	}
	budget, err := modelrouter.NewBudget(SoftBudget, HardBudget, modelrouter.WithBudgetClock(clock))
	if err != nil {
		panic(err)
	}
	router, err := modelrouter.New([]modelrouter.Model{m.small, m.medium, m.large}, routeThresholds,
		modelrouter.WithEscalation(MinConfidence), modelrouter.WithBudget(budget))
	if err != nil {
		panic(err)
	}

	summarised := map[int]int{} // conversation -> turns already folded into its summary
	summary := strings.Repeat("summary ", SummaryTokens/2)
	ctx := context.Background()
	for _, c := range traffic() {
		now = day0.Add(c.at)
		r.Requests++
		ts := t.tenants[c.tenant]
		ts.Requests++

		// The gateway refuses a tenant past its hard limit before doing any
		// work; the router re-checks for the soft limit.
		if budget.Check(c.tenant) == modelrouter.Reject {
			ts.Rejected++
			if c.tenant != Runaway {
				r.Regular++ // a rejected well-behaved request counts as wrong
			}
			continue
		}

		// Every request pays for one embedding and the lookup latency.
		embedCost := float64(modelrouter.EstimateTokens(c.prompt)) / 1000 * EmbedPer1K
		t.charge(c, embedCost)
		budget.Charge(c.tenant, embedCost)

		var res modelrouter.Result
		hit, err := cache.GetOrCompute(ctx,
			semanticcache.Query{Tenant: c.tenant, Prompt: c.prompt, Label: c.label},
			func(ctx context.Context, _ string) (string, bool, error) {
				hist := c.history
				if len(hist) > KeepTurns {
					hist = modelrouter.TrimHistory(hist, KeepTurns)
					hist[0].Content = summary
				}
				var err error
				res, err = router.Route(ctx, request(c, hist))
				if err != nil {
					return "", false, err
				}
				// Fold turns that just left the window into the running
				// summary, once, with the small model.
				if drop := len(c.history) - KeepTurns; drop > summarised[c.conv] {
					in := SummaryTokens
					for _, turn := range c.history[summarised[c.conv]:drop] {
						in += modelrouter.EstimateTokens(turn.Content)
					}
					cost := m.small.Pricing().Cost(in, SummaryTokens)
					summarised[c.conv] = drop
					r.SummaryCost += cost
					r.TokensSent += in
					t.charge(c, cost)
					budget.Charge(c.tenant, cost)
				}
				answer := "wrong"
				if res.Correct {
					answer = "ok"
				}
				// Only common questions are reusable; anything that read the
				// user's account must not be served to someone else.
				return answer, c.kind == faq, nil
			})
		r.CacheLookups++
		if err != nil {
			panic(err) // the gateway check above means the router never rejects
		}
		if hit.Source != semanticcache.Miss {
			r.CacheHits++
			if hit.Label != c.label {
				r.FalseHits++
			}
			t.answered(c, "cache", hit.Label == c.label && hit.Response == "ok", EmbedLatency)
			continue
		}
		r.TokensSent += res.InputTokens * len(res.Attempts)
		t.charge(c, res.Cost)
		if res.Escalated {
			r.Escalated++
			r.EscalationCost += res.Cost - res.Attempts[len(res.Attempts)-1].Cost
		}
		if res.Downgraded {
			ts.Downgraded++
		}
		t.answered(c, res.Model, res.Correct, EmbedLatency+res.Latency)
	}
	t.finish()
	return r
}
