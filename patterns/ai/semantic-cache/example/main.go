// Command example simulates a customer-support bot answering 2,000 questions
// drawn from 50 intents, each asked in many paraphrased ways. It compares an
// exact-match cache with a semantic cache at several similarity thresholds:
// hit rate, LLM spend, and false hits (an answer for the wrong intent). Then
// it shows tenant isolation and an answer that must not be cached.
package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"strings"
	"time"

	semanticcache "github.com/neerajsinghi/system-design-patterns-go/patterns/ai/semantic-cache"
)

const (
	queries     = 2000
	costPerCall = 0.002 // dollars per LLM call
	llmLatency  = 900 * time.Millisecond
	lookupCost  = 5 * time.Millisecond // embedding + search, paid on every query
	gap         = 15 * time.Second     // simulated time between questions
)

// intent is one thing users want, with the words they use for it.
type intent struct {
	name  string
	verbs []string
	nouns []string
}

var intents = []intent{
	{"reset_password", []string{"reset", "recover"}, []string{"password", "pwd", "passcode"}},
	{"change_password", []string{"change", "update"}, []string{"password", "pwd"}},
	{"change_email", []string{"change", "update"}, []string{"email", "email address"}},
	{"verify_email", []string{"verify", "confirm"}, []string{"email", "email address"}},
	{"delete_account", []string{"delete", "close"}, []string{"account", "profile"}},
	{"create_account", []string{"create", "open"}, []string{"account", "new account"}},
	{"change_username", []string{"change", "rename"}, []string{"username", "user name"}},
	{"enable_2fa", []string{"enable", "turn on"}, []string{"two factor auth", "2fa"}},
	{"disable_2fa", []string{"disable", "turn off"}, []string{"two factor auth", "2fa"}},
	{"unlock_account", []string{"unlock", "unblock"}, []string{"account", "locked account"}},
	{"cancel_order", []string{"cancel", "stop"}, []string{"order", "purchase"}},
	{"track_order", []string{"track", "find"}, []string{"order", "package", "parcel"}},
	{"return_item", []string{"return", "send back"}, []string{"item", "product"}},
	{"refund_order", []string{"get a refund for", "refund"}, []string{"order", "purchase"}},
	{"change_address", []string{"change", "update"}, []string{"shipping address", "delivery address"}},
	{"modify_order", []string{"edit", "modify"}, []string{"order"}},
	{"reorder", []string{"repeat", "reorder"}, []string{"last order", "previous order"}},
	{"apply_coupon", []string{"apply", "use"}, []string{"coupon", "promo code", "discount code"}},
	{"gift_wrap", []string{"add", "request"}, []string{"gift wrap", "gift wrapping"}},
	{"invoice", []string{"download", "get"}, []string{"invoice", "receipt"}},
	{"cancel_subscription", []string{"cancel", "end"}, []string{"subscription", "membership"}},
	{"upgrade_plan", []string{"upgrade"}, []string{"plan", "subscription"}},
	{"downgrade_plan", []string{"downgrade"}, []string{"plan", "subscription"}},
	{"pause_subscription", []string{"pause", "freeze"}, []string{"subscription", "membership"}},
	{"renew_subscription", []string{"renew", "reactivate"}, []string{"subscription", "membership"}},
	{"update_card", []string{"update", "change"}, []string{"card", "credit card", "payment card"}},
	{"remove_card", []string{"remove", "delete"}, []string{"card", "saved card"}},
	{"add_payment", []string{"add", "set up"}, []string{"payment method", "paypal"}},
	{"billing_history", []string{"view", "see"}, []string{"billing history", "past charges"}},
	{"dispute_charge", []string{"dispute", "report"}, []string{"charge", "unknown charge"}},
	{"change_language", []string{"change", "switch"}, []string{"language"}},
	{"dark_mode", []string{"enable", "turn on"}, []string{"dark mode", "night mode"}},
	{"notifications_off", []string{"turn off", "disable"}, []string{"notifications", "push notifications"}},
	{"notifications_on", []string{"turn on", "enable"}, []string{"notifications", "alerts"}},
	{"export_data", []string{"export", "download"}, []string{"data", "personal data"}},
	{"change_timezone", []string{"change", "set"}, []string{"timezone", "time zone"}},
	{"upload_avatar", []string{"upload", "change"}, []string{"profile picture", "avatar"}},
	{"install_app", []string{"install", "download"}, []string{"app", "mobile app"}},
	{"update_app", []string{"update", "upgrade"}, []string{"app", "mobile app"}},
	{"app_crash", []string{"fix", "stop"}, []string{"app crashing", "crashes"}},
	{"contact_support", []string{"contact", "reach"}, []string{"support", "customer service", "an agent"}},
	{"store_hours", []string{"find", "check"}, []string{"store hours", "opening hours"}},
	{"store_location", []string{"find", "locate"}, []string{"store", "nearest store"}},
	{"shipping_cost", []string{"check", "calculate"}, []string{"shipping cost", "delivery fee"}},
	{"intl_shipping", []string{"get", "arrange"}, []string{"international shipping", "overseas delivery"}},
	{"warranty", []string{"claim", "check"}, []string{"warranty", "guarantee"}},
	{"loyalty_points", []string{"check", "redeem"}, []string{"loyalty points", "reward points"}},
	{"referral", []string{"share", "get"}, []string{"referral code", "invite link"}},
	{"newsletter_unsub", []string{"unsubscribe from", "stop"}, []string{"newsletter", "marketing emails"}},
	{"report_bug", []string{"report", "submit"}, []string{"bug", "issue"}},
}

var (
	templates = []string{
		"how do i {v} my {n}", "how can i {v} my {n}", "i want to {v} my {n}",
		"i need to {v} my {n}", "help me {v} my {n}", "can you {v} my {n}",
		"{v} {n}", "where do i go to {v} my {n}", "is it possible to {v} my {n}",
		"what's the process to {v} the {n}", "i can't figure out how to {v} my {n}",
		"steps to {v} {n}",
	}
	prefixes = []string{"", "", "hi, ", "hello ", "hey there, ", "quick question: "}
	suffixes = []string{"", "?", "?", " please", ", thanks!", " asap"}

	// A tiny normaliser for the toy embedding: a few filler words and a few
	// synonyms. A real embedding model learns these; here we spell them out.
	stopwords = []string{
		"i", "my", "me", "the", "a", "an", "to", "please", "hi", "hello", "hey", "there", "thanks",
	}
	synonyms = map[string]string{
		"pwd": "password", "passcode": "password", "recover": "reset",
		"membership": "subscription", "purchase": "order", "parcel": "package",
		"modify": "edit", "alerts": "notifications",
	}
)

// exactKey is the exact tier's light normalisation: lower-case, no
// punctuation, single spaces. "Track order?" and "track order" share a key;
// "track my order" does not.
func exactKey(s string) string {
	s = strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == ' ':
			return r
		case r >= 'A' && r <= 'Z':
			return r + 'a' - 'A'
		case r == '\'':
			return -1
		default:
			return ' '
		}
	}, s)
	return strings.Join(strings.Fields(s), " ")
}

type query struct{ prompt, intent string }

// workload draws n questions from the intents with a skewed popularity (a
// few intents dominate, as in real support traffic) and random phrasing.
func workload(n int) []query {
	r := rand.New(rand.NewPCG(7, 42))
	weights := make([]float64, len(intents))
	total := 0.0
	for i := range weights {
		weights[i] = 1 / math.Pow(float64(i+1), 0.9)
		total += weights[i]
	}
	pick := func() int {
		x := r.Float64() * total
		for i, w := range weights {
			if x -= w; x < 0 {
				return i
			}
		}
		return len(weights) - 1
	}
	order := r.Perm(len(intents)) // popularity rank is unrelated to list order
	out := make([]query, n)
	for i := range out {
		in := intents[order[pick()]]
		t := templates[r.IntN(len(templates))]
		t = strings.ReplaceAll(t, "{v}", in.verbs[r.IntN(len(in.verbs))])
		t = strings.ReplaceAll(t, "{n}", in.nouns[r.IntN(len(in.nouns))])
		p := prefixes[r.IntN(len(prefixes))] + t + suffixes[r.IntN(len(suffixes))]
		out[i] = query{prompt: p, intent: in.name}
	}
	return out
}

// llm is a fake model: it always knows the right intent and charges for it.
type llm struct {
	calls  int
	byText map[string]string
}

func (m *llm) answer(prompt string) string {
	m.calls++
	return "ANSWER[" + m.byText[prompt] + "]"
}

type outcome struct {
	name                    string
	exact, semantic, falseH int
	calls                   int
	latency                 time.Duration
}

func (o outcome) print() {
	hits := o.exact + o.semantic
	fmt.Printf("   %-24s %5.1f%%  %5d  %5d  %6d  %6d  $%5.2f  %7v\n", o.name,
		100*float64(hits)/queries, o.exact, o.semantic, o.falseH, o.calls,
		float64(o.calls)*costPerCall, (o.latency / queries).Round(time.Millisecond))
}

func header() {
	fmt.Printf("   %-24s %6s  %5s  %5s  %6s  %6s  %6s  %7s\n",
		"cache", "hits", "exact", "sem.", "wrong", "LLM", "cost", "avg lat")
}

func main() {
	qs := workload(queries)
	m := &llm{byText: map[string]string{}}
	distinct := map[string]bool{}
	for _, q := range qs {
		m.byText[q.prompt] = q.intent
		distinct[q.prompt] = true
	}
	emb, err := semanticcache.NewHashEmbedder(256,
		semanticcache.WithStopwords(stopwords...), semanticcache.WithSynonyms(synonyms))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Support bot: %d questions, %d intents, %d distinct phrasings; LLM call = $%.3f and %v\n\n",
		queries, len(intents), len(distinct), costPerCall, llmLatency)

	fmt.Println("1) No cache vs exact-match cache (keyed by the raw prompt text)")
	header()
	outcome{name: "no cache", calls: queries, latency: queries * llmLatency}.print()
	exact := map[string]string{}
	o := outcome{name: "exact match"}
	for _, q := range qs {
		if _, ok := exact[q.prompt]; ok {
			o.exact++
			continue
		}
		exact[q.prompt] = m.answer(q.prompt)
		o.calls++
		o.latency += llmLatency
	}
	o.print()
	fmt.Println("   Paraphrases rarely repeat character for character, so most questions still pay.")
	fmt.Println()

	fmt.Println("2) Semantic cache (exact tier on normalised text, then cosine similarity) at four thresholds")
	header()
	var worst []string
	for _, th := range []float32{0.95, 0.90, 0.80, 0.70} {
		now := time.Unix(0, 0)
		c, err := semanticcache.New(emb.Embed, th,
			semanticcache.WithExactKey(exactKey),
			semanticcache.WithClock(func() time.Time { return now }))
		if err != nil {
			log.Fatal(err)
		}
		calls := m.calls
		var lat time.Duration
		var examples []string
		for _, q := range qs {
			r, err := c.GetOrCompute(context.Background(),
				semanticcache.Query{Tenant: "acme", Prompt: q.prompt, Label: q.intent},
				func(_ context.Context, p string) (string, bool, error) {
					lat += llmLatency
					return m.answer(p), true, nil
				})
			if err != nil {
				log.Fatal(err)
			}
			lat += lookupCost
			if r.Source == semanticcache.Semantic && r.Label != q.intent && len(examples) < 3 {
				examples = append(examples, fmt.Sprintf("%q -> answered as %q (%s, sim %.2f)",
					q.prompt, r.MatchedPrompt, r.Label, r.Similarity))
			}
			now = now.Add(gap)
		}
		s := c.Stats()
		outcome{name: fmt.Sprintf("semantic @ %.2f", th), exact: int(s.ExactHits), semantic: int(s.SemanticHits),
			falseH: int(s.FalseHits), calls: m.calls - calls, latency: lat}.print()
		worst = examples
	}
	fmt.Println("   'wrong' = the user got the cached answer for a different intent. Examples at 0.70:")
	for _, e := range worst {
		fmt.Println("     -", e)
	}
	fmt.Println("   0.95 and 0.90 never answer the wrong intent but still miss most paraphrases; 0.80 and")
	fmt.Println("   0.70 cut the bill further by serving some users someone else's answer ('turn on' gets")
	fmt.Println("   the 'turn off' steps). Exact hits fall as the threshold drops because fewer misses means")
	fmt.Println("   fewer prompts get stored. With this toy embedding, 0.90 is the safe choice here.")
	fmt.Println()

	fmt.Println("3) Tenant isolation: two companies share one cache")
	c, err := semanticcache.New(emb.Embed, 0.80, semanticcache.WithExactKey(exactKey))
	if err != nil {
		log.Fatal(err)
	}
	_ = c.Put(semanticcache.Entry{Tenant: "acme", Prompt: "how do I reset my password",
		Response: "Acme: use the 'Forgot password' link on acme.example/login"})
	for _, tenant := range []string{"acme", "globex"} {
		r, ok := c.Lookup(semanticcache.Query{Tenant: tenant, Prompt: "How can I reset my pwd?"})
		if ok {
			fmt.Printf("   %-6s -> %s hit (sim %.2f): %s\n", tenant, r.Source, r.Similarity, r.Response)
		} else {
			fmt.Printf("   %-6s -> miss: asks its own model; it never sees another tenant's answer\n", tenant)
		}
	}
	fmt.Println()

	fmt.Println("4) Answers that depend on the user are not cached")
	personal := func(_ context.Context, p string) (string, bool, error) {
		if strings.Contains(p, "my order") && strings.Contains(p, "where") {
			return "Order #4417 for this user is out for delivery", false, nil // user-specific
		}
		return "Standard shipping takes 3-5 days", true, nil
	}
	for _, p := range []string{"where is my order", "where is my order?", "how long does shipping take", "how long does shipping take?"} {
		r, _ := c.GetOrCompute(context.Background(), semanticcache.Query{Tenant: "acme", Prompt: p}, personal)
		fmt.Printf("   %-30q -> %-8s %s\n", p, r.Source, r.Response)
	}
	s := c.Stats()
	fmt.Printf("   stored %d, refused to store %d user-specific answers\n", s.Stored, s.NotCacheable)
}
