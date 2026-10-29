// Package chatbothallucinates simulates the "support chatbot gives wrong
// answers" incident. Pebblebrook Home's chatbot (the fictional retailer whose
// policy documents ship with the rag-retrieval pattern) tells customers they
// have 14 days to return an item when the policy says 30, hands out a
// discount code that does not exist, and quotes an old free-shipping
// threshold. The team's first fix, retrieval-augmented generation over the
// help centre, still gives wrong answers: the documents were cut into tiny
// fixed-size chunks, so the chunk that ranks first rarely holds the answer,
// and the bot never says "I don't know".
//
// Every run uses the same model: a deterministic fake that quotes the
// best-matching sentence from whatever context it is given and, when nothing
// in the context matches, answers from its training memory instead (stale
// and partly invented facts, stated confidently). Only the system around it
// changes:
//
//   - ModelMemory: no retrieval at all.
//   - Broken: naive RAG. 12-word fixed chunks, dense-only retrieval, top 3,
//     no refusal gate and no check of what the model wrote.
//   - Fixed: heading-aware chunks, hybrid BM25 + dense retrieval fused with
//     reciprocal rank fusion, top 3 inside a token budget, a refusal gate on
//     retrieval similarity, and a groundedness check that rejects any answer
//     whose sentences are not copied from the passage they cite.
//
// All runs answer the same questions: the pattern's 48-question EvalSet,
// which the documents can answer, plus OutOfScope questions they cannot.
package chatbothallucinates

import (
	"fmt"
	"io"
	"strings"

	ragretrieval "github.com/neerajsinghi/system-design-patterns-go/patterns/ai/rag-retrieval"
)

const (
	// NaiveChunkWords is the fixed chunk size of the first RAG attempt.
	NaiveChunkWords = 12
	// HeadingMaxTokens caps a heading-aware chunk.
	HeadingMaxTokens = 120
	// TopK is how many chunks both RAG versions retrieve.
	TopK = 3
	// TokenBudget is the fixed version's prompt budget: instructions, question
	// and as many of the top chunks as fit, in rank order. The naive version
	// has none (its chunks are tiny, so it never needs one).
	TokenBudget = 150
	// MinRelevance is the first refusal gate: refuse when no retrieved chunk
	// has at least this dense cosine similarity to the question.
	MinRelevance = 0.25
	// MinCoverage is the second refusal gate: refuse when the best chunk's
	// BM25 score is below this fraction of the question's total keyword weight
	// (the sum of its terms' IDF), i.e. the passage leaves most of what the
	// question asks about unexplained.
	MinCoverage = 0.40

	// The invariant every shipped chatbot must hold.
	MinAccuracy          = 0.70 // in-scope questions answered with the right fact
	MinOutOfScopeRefusal = 0.75 // out-of-scope questions refused
)

// OutOfScope are questions the help centre does not answer. A correct bot
// says it does not know.
var OutOfScope = []string{
	"Do you offer a student discount?",
	"Is there a promo code for first-time customers?",
	"Do you sell electric cars?",
	"Who founded Pebblebrook Home?",
	"Can I rent furniture by the month?",
	"Will you buy back my old furniture for cash?",
	"What is the weather forecast for tomorrow?",
	"Do you have a store in Tokyo?",
}

// memory is what the model learned in training: the returns policy from
// before it changed, an old shipping threshold, and a discount code nobody
// ever issued. Keys are question terms (normalised with ragretrieval.Terms);
// the first matching term in the question wins.
var memory = map[string]string{
	"return":   "You can return any item within 14 days of delivery.",
	"back":     "You can return any item within 14 days of delivery.",
	"discount": "Yes! Use code PEBBLE20 at checkout for 20% off your first order.",
	"promo":    "Yes! Use code PEBBLE20 at checkout for 20% off your first order.",
	"ship":     "Shipping is free on orders over $35 and arrives in 2 business days.",
	"delivery": "Shipping is free on orders over $35 and arrives in 2 business days.",
}

// generic is what the model says when it remembers nothing relevant: a
// confident, unsupported yes.
const generic = "Yes, that is covered by our standard customer policy."

// fromMemory is the model answering with no usable context: no citation.
func fromMemory(question string) ragretrieval.Answer {
	for _, t := range ragretrieval.Terms(question) {
		if a, ok := memory[t]; ok {
			return ragretrieval.Answer{Text: a}
		}
	}
	return ragretrieval.Answer{Text: generic}
}

// model is the one language model all three runs share. It quotes the
// context when any sentence matches the question and otherwise falls back to
// memory; it never volunteers "I don't know".
func model(p ragretrieval.Prompt) ragretrieval.Answer {
	if a := ragretrieval.AnswerFrom(p); !a.Refused {
		return a
	}
	return fromMemory(p.Question)
}

// Spotlight questions from the incident, printed with each run's answer.
var Spotlight = []string{
	"How many days do I have to return an item?",
	"What is the minimum order for free shipping?",
	"Do you offer a student discount?",
}

// Run is what one chatbot design measured.
type Run struct {
	Name string
	// In-scope questions (EvalSet).
	InScope        int
	Correct        int // answered, and the answer holds the evidence phrase
	WrongConfident int // answered, but wrong
	FalseRefusals  int // refused although the documents answer it
	// FalselyRefused lists those questions.
	FalselyRefused []string
	// Out-of-scope questions.
	OutOfScope      int
	CorrectRefusals int
	OOSAnswered     int // answered anyway: wrong but confident
	// Over every shipped (non-refused) answer.
	Uncited          int // no citation at all
	Ungrounded       int // fails ragretrieval.Grounded (includes uncited)
	GroundedButWrong int // passes Grounded yet is wrong (in-scope wrong or out-of-scope answered)
	// RejectedByCheck counts answers the groundedness check turned into refusals.
	RejectedByCheck int
	// RecallAt3 is retrieval recall@3 on the EvalSet; -1 when nothing is retrieved.
	RecallAt3 float64
	// PromptTokens is the mean prompt size over all questions; refused-before-
	// prompt questions count the question alone.
	PromptTokens float64
	// Spotlight holds this run's answers to the Spotlight questions.
	Spotlight []string
}

// Accuracy is Correct / InScope.
func (r Run) Accuracy() float64 { return float64(r.Correct) / float64(r.InScope) }

// OOSRefusalRate is CorrectRefusals / OutOfScope.
func (r Run) OOSRefusalRate() float64 { return float64(r.CorrectRefusals) / float64(r.OutOfScope) }

// Violations lists every way the run breaks the invariant; empty means it holds.
func (r Run) Violations() []string {
	var v []string
	if r.Accuracy() < MinAccuracy {
		v = append(v, fmt.Sprintf("accuracy %.0f%% < %.0f%%", 100*r.Accuracy(), 100*MinAccuracy))
	}
	if r.Ungrounded > 0 {
		v = append(v, fmt.Sprintf("%d ungrounded answers", r.Ungrounded))
	}
	if r.Uncited > 0 {
		v = append(v, fmt.Sprintf("%d answers without a citation", r.Uncited))
	}
	if r.OOSRefusalRate() < MinOutOfScopeRefusal {
		v = append(v, fmt.Sprintf("out-of-scope refusals %.0f%% < %.0f%%", 100*r.OOSRefusalRate(), 100*MinOutOfScopeRefusal))
	}
	return v
}

// Report is the result of one or more chatbot designs over the same questions.
type Report struct {
	Name string
	Runs []Run
}

// Print writes each run's numbers and its spotlight answers.
func (r Report) Print(w io.Writer) {
	fmt.Fprintf(w, "%s\n", r.Name)
	for _, run := range r.Runs {
		fmt.Fprintf(w, "\n  %s\n", run.Name)
		for i, q := range Spotlight {
			fmt.Fprintf(w, "    Q: %s\n       %s\n", q, run.Spotlight[i])
		}
		fmt.Fprintf(w, "    in-scope (%d):     %2d right (%.0f%%), %2d wrong but confident, %2d refused\n",
			run.InScope, run.Correct, 100*run.Accuracy(), run.WrongConfident, run.FalseRefusals)
		for _, q := range run.FalselyRefused {
			fmt.Fprintf(w, "      refused in-scope: %q\n", q)
		}
		fmt.Fprintf(w, "    out-of-scope (%d):  %2d refused (%.0f%%), %2d answered anyway\n",
			run.OutOfScope, run.CorrectRefusals, 100*run.OOSRefusalRate(), run.OOSAnswered)
		fmt.Fprintf(w, "    shipped answers:   %2d uncited, %2d ungrounded, %2d grounded but wrong", run.Uncited, run.Ungrounded, run.GroundedButWrong)
		if run.RejectedByCheck > 0 {
			fmt.Fprintf(w, "; %d rejected by the groundedness check", run.RejectedByCheck)
		}
		fmt.Fprintln(w)
		if run.RecallAt3 >= 0 {
			fmt.Fprintf(w, "    retrieval recall@3: %.2f; mean prompt: %.0f tokens\n", run.RecallAt3, run.PromptTokens)
		} else {
			fmt.Fprintf(w, "    retrieval: none; mean prompt: %.0f tokens\n", run.PromptTokens)
		}
		if v := run.Violations(); len(v) > 0 {
			fmt.Fprintf(w, "    INVARIANT VIOLATED: %s\n", strings.Join(v, "; "))
		} else {
			fmt.Fprintln(w, "    invariant holds")
		}
	}
}

// outcome is one question's result.
type outcome struct {
	answer  ragretrieval.Answer
	prompt  ragretrieval.Prompt
	tokens  int
	checked bool // the groundedness check rejected the model's answer
}

type chatbot func(q string) outcome

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// measure runs every question through bot and scores it.
func measure(name string, bot chatbot, recall float64) Run {
	r := Run{Name: name, RecallAt3: recall}
	total := 0
	score := func(q string, evidence string, inScope bool) {
		o := bot(q)
		total += o.tokens
		if o.checked {
			r.RejectedByCheck++
		}
		a := o.answer
		if a.Refused {
			if inScope {
				r.FalseRefusals++
				r.FalselyRefused = append(r.FalselyRefused, q)
			} else {
				r.CorrectRefusals++
			}
			return
		}
		right := inScope && strings.Contains(a.Text, evidence)
		switch {
		case right:
			r.Correct++
		case inScope:
			r.WrongConfident++
		default:
			r.OOSAnswered++
		}
		if len(a.Sentences) == 0 {
			r.Uncited++
		}
		if ragretrieval.Grounded(a, o.prompt) != nil {
			r.Ungrounded++
		} else if !right {
			r.GroundedButWrong++
		}
	}
	for _, q := range ragretrieval.EvalSet() {
		r.InScope++
		score(q.Q, q.Evidence, true)
	}
	for _, q := range OutOfScope {
		r.OutOfScope++
		score(q, "", false)
	}
	r.PromptTokens = float64(total) / float64(r.InScope+r.OutOfScope)
	for _, q := range Spotlight {
		r.Spotlight = append(r.Spotlight, bot(q).answer.Text)
	}
	return r
}

func evaluator() *ragretrieval.Evaluator {
	return must(ragretrieval.NewEvaluator(ragretrieval.Corpus(), ragretrieval.EvalSet()))
}

// memoryRun is the model with no retrieval: its prompt is the question.
func memoryRun() Run {
	bot := func(q string) outcome {
		p := ragretrieval.Prompt{Question: q}
		return outcome{answer: model(p), prompt: p, tokens: ragretrieval.CountTokens(q)}
	}
	return measure("1. model memory only (no retrieval)", bot, -1)
}

// naiveRun is the first RAG attempt.
func naiveRun() Run {
	chunker := must(ragretrieval.NewFixedChunker(NaiveChunkWords))
	ix := must(ragretrieval.NewIndex(ragretrieval.ChunkAll(chunker, ragretrieval.Corpus())))
	bot := func(q string) outcome {
		hits := must(ix.Search(q, ragretrieval.Dense, TopK))
		p := must(ragretrieval.BuildPrompt(q, hits, 1<<20)) // no budget
		return outcome{answer: model(p), prompt: p, tokens: p.Tokens}
	}
	recall := must(evaluator().Evaluate(ix, ragretrieval.Dense, TopK)).Recall
	return measure(fmt.Sprintf("2. naive RAG: %s chunks, dense only, top-%d, no refusal gate, no groundedness check", chunker.Name(), TopK), bot, recall)
}

// Broken returns the incident: the model answering from memory, then the
// first RAG attempt. Both violate the invariant.
func Broken() Report {
	return Report{Name: "BROKEN: the support chatbot before and after a naive RAG attempt", Runs: []Run{memoryRun(), naiveRun()}}
}

// Fixed is heading-aware chunks, hybrid retrieval, a token budget, the
// refusal gate and the groundedness check, around the same model.
func Fixed() Report {
	return Report{Name: "FIXED: heading chunks + hybrid (BM25 + dense, RRF) + token budget + refusal gate + groundedness check",
		Runs: []Run{fixedRun(TopK, TokenBudget, MinRelevance, MinCoverage)}}
}

func fixedRun(k, budget int, minRelevance, minCoverage float64) Run {
	chunker := must(ragretrieval.NewHeadingChunker(HeadingMaxTokens))
	ix := must(ragretrieval.NewIndex(ragretrieval.ChunkAll(chunker, ragretrieval.Corpus())))
	pipe := must(ragretrieval.NewPipeline(ix, ragretrieval.WithMode(ragretrieval.Hybrid), ragretrieval.WithTopK(k),
		ragretrieval.WithTokenBudget(budget), ragretrieval.WithMinRelevance(minRelevance)))
	refuse := ragretrieval.Answer{Text: ragretrieval.RefusalText, Refused: true}
	bot := func(q string) outcome {
		resp := must(pipe.Ask(q))
		if len(resp.Prompt.Context) == 0 || coverage(ix, q, resp.Hits) < minCoverage {
			// A gate refused before the model was called.
			return outcome{answer: refuse, tokens: ragretrieval.CountTokens(q)}
		}
		o := outcome{answer: model(resp.Prompt), prompt: resp.Prompt, tokens: resp.Prompt.Tokens}
		if ragretrieval.Grounded(o.answer, o.prompt) != nil {
			o.answer, o.checked = refuse, true
		}
		return o
	}
	recall := must(evaluator().Evaluate(ix, ragretrieval.Hybrid, k)).Recall
	return measure(fmt.Sprintf("3. heading chunks, hybrid, top-%d within %d tokens, refusal gates (similarity %.2f, coverage %.2f), groundedness check",
		k, budget, minRelevance, minCoverage), bot, recall)
}

// coverage is the best hit's BM25 score as a fraction of the question's
// keyword weight: the sum of IDF over its distinct terms. A passage that
// matches only the common words of a question ("store", "furniture") scores
// low even when it ranks first.
func coverage(ix *ragretrieval.Index, q string, hits []ragretrieval.Hit) float64 {
	var weight, best float64
	seen := map[string]bool{}
	for _, t := range ragretrieval.Terms(q) {
		if !seen[t] {
			seen[t] = true
			weight += ix.IDF(t)
		}
	}
	for _, h := range hits {
		best = max(best, h.BM25)
	}
	if weight == 0 {
		return 0
	}
	return best / weight
}
