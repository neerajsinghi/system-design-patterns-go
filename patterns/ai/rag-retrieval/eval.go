package ragretrieval

import (
	"embed"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
)

//go:embed corpus/*.md
var corpusFS embed.FS

// Corpus returns the embedded policy documents of Pebblebrook Home, a
// fictional retailer, sorted by ID. Document IDs are the file names without
// ".md".
func Corpus() []Document {
	entries, err := corpusFS.ReadDir("corpus")
	if err != nil {
		panic(err) // embedded at build time; cannot fail
	}
	var docs []Document
	for _, e := range entries {
		b, err := corpusFS.ReadFile("corpus/" + e.Name())
		if err != nil {
			panic(err)
		}
		text := string(b)
		title := ""
		if line, _, _ := strings.Cut(text, "\n"); strings.HasPrefix(line, "# ") {
			title = strings.TrimSpace(line[2:])
		}
		docs = append(docs, Document{ID: strings.TrimSuffix(e.Name(), path.Ext(e.Name())), Title: title, Text: text})
	}
	sort.Slice(docs, func(a, b int) bool { return docs[a].ID < docs[b].ID })
	return docs
}

// Question is one evaluation case: a user question, the section that
// answers it, and the exact phrase in that section that is the answer.
type Question struct {
	Q        string
	Passage  string // section ID, "docID#slug"
	Evidence string
}

// EvalSet returns 48 questions about Corpus, each phrased the way a
// customer might ask it rather than copied from the documents.
func EvalSet() []Question {
	return []Question{
		{"How many days do I have to send something back?", "returns#return-window", "within 30 days of delivery"},
		{"Is there a fee to return furniture after I put it together?", "returns#condition-of-returned-items", "15% restocking fee"},
		{"How long until my refund shows up on my card?", "returns#refund-timing", "within 5 business days"},
		{"Can I return a mattress I have opened?", "returns#items-that-cannot-be-returned", "Mattresses that have been unsealed"},
		{"Until when can I return a present bought in December?", "holiday-returns#extended-holiday-window", "until 31 January"},
		{"How is the refund paid when a gift is returned with a gift receipt?", "holiday-returns#gift-receipts", "issued as a Pebblebrook gift card"},
		{"How long does standard delivery take?", "shipping#standard-shipping", "3 to 5 business days"},
		{"What is the minimum order for free shipping?", "shipping#standard-shipping", "free on orders over $50"},
		{"What is the cutoff time for next-day express delivery?", "shipping#express-shipping", "before 2 pm local time"},
		{"Who pays import duties on international orders?", "shipping#international-shipping", "paid by the customer"},
		{"How many countries do you ship to?", "shipping#international-shipping", "14 countries"},
		{"When will I know the arrival time for my sofa delivery?", "large-delivery#delivery-window", "4-hour arrival window"},
		{"What does it cost if I reschedule my delivery at the last minute?", "large-delivery#rescheduling", "$30 redelivery fee"},
		{"How long is the warranty on a sofa or other furniture?", "warranty#what-the-warranty-covers", "2-year limited warranty"},
		{"What do I need to make a warranty claim?", "warranty#making-a-warranty-claim", "order number and photos of the defect"},
		{"Does the warranty cover fabric fading from the sun?", "warranty#exclusions", "Fabric fading caused by direct sunlight"},
		{"How soon must I report an item that arrived broken?", "damaged-items#reporting-damage", "within 48 hours of delivery"},
		{"Tracking says delivered but the parcel is not here. What should I do?", "damaged-items#missing-parcels", "wait one business day"},
		{"The price dropped after I bought it. Can I get the difference back?", "price-adjustment#our-own-price-drops", "within 14 days of your purchase"},
		{"Will you match a lower price from another store?", "price-adjustment#competitor-prices", "does not match competitor prices"},
		{"How many Pebble Points do I earn per dollar?", "loyalty#earning-points", "1 Pebble Point for every dollar spent"},
		{"What are 500 points worth?", "loyalty#redeeming-points", "$10 credit"},
		{"When do my loyalty points expire?", "loyalty#expiry", "18 months without any account activity"},
		{"Do gift cards expire?", "gift-cards#validity", "never expire"},
		{"I lost my gift card. Can it be replaced?", "gift-cards#lost-or-stolen-cards", "replace a lost or stolen gift card"},
		{"How long do I have to cancel my order?", "order-changes#cancelling-an-order", "within 1 hour of placing it"},
		{"Can I change the shipping address after I order?", "order-changes#changing-the-delivery-address", "until the order is packed"},
		{"Can I pay with bitcoin or other cryptocurrency?", "payment#accepted-payment-methods", "Cryptocurrency and cheques are not accepted"},
		{"Can I split the cost into instalments?", "payment#paying-in-instalments", "four interest-free instalments"},
		{"When is my card actually charged?", "payment#when-you-are-charged", "charged when the order ships"},
		{"How long do you keep my order history?", "privacy#what-we-keep", "kept for 7 years"},
		{"How long does it take to delete my data?", "privacy#deleting-your-data", "completed within 30 days"},
		{"How long is the password reset link good for?", "account-security#password-resets", "valid for 20 minutes"},
		{"How many wrong passwords before my account gets locked?", "account-security#locked-accounts", "After 5 failed sign-in attempts"},
		{"How much do you charge to assemble furniture?", "assembly#booking-assembly", "$79 per item"},
		{"How much does Pebble Plus cost?", "membership#price-and-benefits", "$49 per year"},
		{"Do I get my money back if I cancel Pebble Plus?", "membership#cancelling-membership", "Cancelling within the first 30 days gives a full refund"},
		{"What hours is customer support open?", "support#opening-hours", "8 am to 8 pm Eastern Time"},
		{"Is there a discount for buying in bulk?", "business-orders#bulk-discount", "8% discount"},
		{"How long will the store hold my click-and-collect order?", "store-pickup#how-long-we-hold-orders", "held for 7 days"},
		{"How much is it to take away my old sofa?", "haul-away#removing-old-furniture", "for $25"},
		{"How long before my product review is published?", "reviews#moderation", "within 72 hours"},
		{"What time does the showroom open on Sunday?", "store-guide#store-opening-hours", "from 11 am to 6 pm on Sunday"},
		{"Is parking free at the store?", "store-guide#parking", "free for the first 2 hours"},
		{"Can I bring my dog into the store?", "store-guide#pets-in-the-store", "Dogs on a short lead are welcome"},
		{"Can I borrow a wheelchair in the showroom?", "store-guide#accessibility", "Wheelchairs can be borrowed free of charge"},
		{"How many fabric swatches can I take for free?", "store-guide#fabric-swatches", "up to 5 free fabric swatches"},
		{"Can I return a floor model I bought at a discount?", "store-guide#floor-models", "so they cannot be returned"},
	}
}

// Metrics summarises retrieval quality over an evaluation set.
type Metrics struct {
	Questions int
	K         int
	// Recall is the fraction of questions with a relevant chunk in the top K.
	Recall float64
	// MRR is the mean of 1/rank of the first relevant chunk within the top
	// MRRDepth (0 when none is found that deep).
	MRR float64
	// AvgContextTokens is the mean token count of the top K chunks together:
	// what the model would have to read.
	AvgContextTokens float64
}

// MRRDepth is how deep MRR looks for the first relevant chunk.
const MRRDepth = 10

// Evaluator scores retrieval against a fixed question set.
type Evaluator struct {
	sections map[string]Section
	qs       []Question
}

// NewEvaluator validates the question set against docs: every passage must
// exist and contain its evidence phrase.
func NewEvaluator(docs []Document, qs []Question) (*Evaluator, error) {
	if len(qs) == 0 {
		return nil, errors.New("ragretrieval: evaluator needs at least one question")
	}
	text := map[string]string{}
	secs := map[string]Section{}
	for _, d := range docs {
		text[d.ID] = d.Text
		for _, s := range ParseSections(d) {
			secs[s.ID] = s
		}
	}
	for _, q := range qs {
		s, ok := secs[q.Passage]
		if !ok {
			return nil, fmt.Errorf("ragretrieval: question %q points at unknown passage %q", q.Q, q.Passage)
		}
		if q.Evidence == "" || !strings.Contains(text[s.DocID][s.Start:s.End], q.Evidence) {
			return nil, fmt.Errorf("ragretrieval: passage %q does not contain evidence %q", q.Passage, q.Evidence)
		}
	}
	return &Evaluator{sections: secs, qs: qs}, nil
}

// Relevant reports whether chunk c answers q: it overlaps q's passage and
// contains the whole evidence phrase. A chunk that holds only half of the
// answer, or the right section without the answer, does not count.
func (e *Evaluator) Relevant(c Chunk, q Question) bool {
	s := e.sections[q.Passage]
	return c.DocID == s.DocID && c.Start < s.End && s.Start < c.End && strings.Contains(c.Text, q.Evidence)
}

// Evaluate runs every question through ix under mode and returns recall@k
// and MRR.
func (e *Evaluator) Evaluate(ix *Index, mode Mode, k int) (Metrics, error) {
	if k <= 0 {
		return Metrics{}, errors.New("ragretrieval: k must be positive")
	}
	m := Metrics{Questions: len(e.qs), K: k}
	for _, q := range e.qs {
		hits, err := ix.Search(q.Q, mode, max(k, MRRDepth))
		if err != nil {
			return Metrics{}, err
		}
		for j := range min(k, len(hits)) {
			m.AvgContextTokens += float64(hits[j].Chunk.Tokens)
		}
		for j, h := range hits {
			if e.Relevant(h.Chunk, q) {
				if j < k {
					m.Recall++
				}
				if j < MRRDepth {
					m.MRR += 1 / float64(j+1)
				}
				break
			}
		}
	}
	n := float64(len(e.qs))
	m.Recall /= n
	m.MRR /= n
	m.AvgContextTokens /= n
	return m, nil
}

// AnswerAccuracy runs every question end to end (retrieve top k, build a
// prompt within budget, extractive answer) and returns the fraction whose
// answer contains the evidence phrase. Retrieval recall is an upper bound on
// it: the answerer can only quote what it was given, and it can still quote
// the wrong sentence when the right one is buried in long chunks.
func (e *Evaluator) AnswerAccuracy(ix *Index, mode Mode, k, budget int) (float64, error) {
	var correct int
	for _, q := range e.qs {
		hits, err := ix.Search(q.Q, mode, k)
		if err != nil {
			return 0, err
		}
		p, err := BuildPrompt(q.Q, hits, budget)
		if err != nil {
			return 0, err
		}
		if a := AnswerFrom(p); !a.Refused && strings.Contains(a.Text, q.Evidence) {
			correct++
		}
	}
	return float64(correct) / float64(len(e.qs)), nil
}
