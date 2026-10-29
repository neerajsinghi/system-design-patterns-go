package ragretrieval

import (
	"errors"
	"fmt"
	"math"
	"strings"
)

// Instructions is the fixed preamble of every grounded prompt.
const Instructions = "You are a customer support assistant. Answer using only the numbered context passages below. " +
	"Cite every claim with its passage number, like [1]. If the context does not contain the answer, say you do not know."

// RefusalText is what the pipeline says when it has nothing to ground an answer in.
const RefusalText = "I don't know. I couldn't find this in our help documents, so I'd rather not guess. A support agent can help."

// Citation is one numbered context passage in a prompt.
type Citation struct {
	N       int // 1-based number used in [N]
	ChunkID string
	Heading string
	Text    string
}

// Prompt is an assembled grounded prompt.
type Prompt struct {
	Text     string
	Context  []Citation
	Tokens   int // CountTokens(Text), never above the budget
	Budget   int
	Dropped  int // retrieved chunks left out because they did not fit
	Question string
}

// BuildPrompt assembles instructions, numbered context and the question,
// adding hits in rank order while the whole prompt stays within budget
// tokens. A hit that does not fit is skipped (a smaller, lower-ranked one may
// still fit). Citation numbers are consecutive in the order included.
func BuildPrompt(question string, hits []Hit, budget int) (Prompt, error) {
	if strings.TrimSpace(question) == "" {
		return Prompt{}, errors.New("ragretrieval: question must not be empty")
	}
	head := Instructions + "\n\nContext:\n"
	tail := "\nQuestion: " + question + "\nAnswer:"
	used := CountTokens(head) + CountTokens(tail)
	if used > budget {
		return Prompt{}, fmt.Errorf("ragretrieval: token budget %d is below the %d tokens of instructions and question", budget, used)
	}
	p := Prompt{Budget: budget, Question: question}
	var b strings.Builder
	b.WriteString(head)
	for _, h := range hits {
		n := len(p.Context) + 1
		block := fmt.Sprintf("[%d] (source: %s)\n%s\n", n, h.Chunk.ID, h.Chunk.Text)
		t := CountTokens(block)
		if used+t > budget {
			p.Dropped++
			continue
		}
		used += t
		b.WriteString(block)
		p.Context = append(p.Context, Citation{N: n, ChunkID: h.Chunk.ID, Heading: h.Chunk.Heading, Text: h.Chunk.Text})
	}
	b.WriteString(tail)
	p.Text = b.String()
	p.Tokens = CountTokens(p.Text)
	return p, nil
}

// CitedSentence is one answer sentence and the passage it was copied from.
type CitedSentence struct {
	Text string
	Cite int
}

// Answer is a fake model's reply.
type Answer struct {
	Text      string
	Sentences []CitedSentence
	Refused   bool
}

// sentences splits a passage into candidate sentences, skipping Markdown
// heading lines and the heading path that structure-aware chunks start with.
func sentences(c Citation) []string {
	var out []string
	for _, line := range strings.Split(c.Text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || (c.Heading != "" && line == c.Heading) {
			continue
		}
		for len(line) > 0 {
			i := strings.Index(line, ". ")
			if i < 0 {
				out = append(out, line)
				break
			}
			out = append(out, line[:i+1])
			line = strings.TrimSpace(line[i+2:])
		}
	}
	return out
}

// AnswerFrom is the extractive fake model: it picks the context sentence
// that best matches the question and returns it verbatim with its citation.
// A sentence scores the sum, over distinct question terms it contains, of
// that term's rarity among the context sentences (ln(1 + S/s), with S the
// number of sentences and s those containing the term), so "duties" counts
// for more than "order". Earlier passages win ties. It never writes anything
// that is not in the context, so every answer can be checked by Grounded. If
// no sentence shares a term with the question, it refuses.
func AnswerFrom(p Prompt) Answer {
	q := map[string]bool{}
	for _, t := range Terms(p.Question) {
		q[t] = true
	}
	type cand struct {
		text  string
		cite  int
		terms map[string]bool
	}
	var cands []cand
	sf := map[string]int{}
	for _, c := range p.Context {
		for _, s := range sentences(c) {
			ts := map[string]bool{}
			for _, t := range Terms(s) {
				if q[t] {
					ts[t] = true
				}
			}
			for t := range ts {
				sf[t]++
			}
			cands = append(cands, cand{s, c.N, ts})
		}
	}
	var best *cand
	bestScore := 0.0
	for i := range cands {
		score := 0.0
		for t := range cands[i].terms {
			score += math.Log(1 + float64(len(cands))/float64(sf[t]))
		}
		if score > bestScore+1e-12 {
			best, bestScore = &cands[i], score
		}
	}
	if best == nil {
		return Answer{Text: RefusalText, Refused: true}
	}
	return Answer{
		Text:      fmt.Sprintf("%s [%d]", best.text, best.cite),
		Sentences: []CitedSentence{{Text: best.text, Cite: best.cite}},
	}
}

// Grounded checks that every sentence of a non-refused answer appears
// verbatim in the passage it cites. A nil error means the answer is grounded.
func Grounded(a Answer, p Prompt) error {
	if a.Refused {
		return nil
	}
	if len(a.Sentences) == 0 {
		return errors.New("ragretrieval: answer has no cited sentences")
	}
	for _, s := range a.Sentences {
		if s.Cite < 1 || s.Cite > len(p.Context) {
			return fmt.Errorf("ragretrieval: citation [%d] does not exist in the prompt", s.Cite)
		}
		if !strings.Contains(p.Context[s.Cite-1].Text, s.Text) {
			return fmt.Errorf("ragretrieval: sentence %q is not in passage [%d]", s.Text, s.Cite)
		}
	}
	return nil
}

// Pipeline is retrieve, gate, assemble prompt, answer. It is safe for
// concurrent use because the Index is immutable.
type Pipeline struct {
	ix          *Index
	mode        Mode
	k, budget   int
	minRelevant float64
}

// PipelineOption configures a Pipeline.
type PipelineOption func(*Pipeline)

// WithMode sets the retrieval mode (default Hybrid).
func WithMode(m Mode) PipelineOption { return func(p *Pipeline) { p.mode = m } }

// WithTopK sets how many chunks are retrieved (default 3).
func WithTopK(k int) PipelineOption { return func(p *Pipeline) { p.k = k } }

// WithTokenBudget sets the prompt token budget (default 400).
func WithTokenBudget(n int) PipelineOption { return func(p *Pipeline) { p.budget = n } }

// WithMinRelevance sets the refusal threshold: if no retrieved chunk has a
// dense cosine similarity of at least s with the question, the pipeline
// refuses instead of answering (default 0.25; 0 disables the gate).
func WithMinRelevance(s float64) PipelineOption { return func(p *Pipeline) { p.minRelevant = s } }

// NewPipeline returns a pipeline over ix.
func NewPipeline(ix *Index, opts ...PipelineOption) (*Pipeline, error) {
	if ix == nil {
		return nil, errors.New("ragretrieval: index must not be nil")
	}
	p := &Pipeline{ix: ix, mode: Hybrid, k: 3, budget: 400, minRelevant: 0.25}
	for _, o := range opts {
		o(p)
	}
	if p.mode < Dense || p.mode > Hybrid {
		return nil, fmt.Errorf("ragretrieval: unknown mode %v", p.mode)
	}
	if p.k <= 0 {
		return nil, errors.New("ragretrieval: top-k must be positive")
	}
	if p.budget <= 0 {
		return nil, errors.New("ragretrieval: token budget must be positive")
	}
	if p.minRelevant < 0 || p.minRelevant > 1 {
		return nil, errors.New("ragretrieval: min relevance must be in [0, 1]")
	}
	return p, nil
}

// Response is the full trace of one question.
type Response struct {
	Hits      []Hit
	BestDense float64 // highest dense similarity among Hits
	Prompt    Prompt  // empty when refused before prompt assembly
	Answer    Answer
	Reason    string // why it refused, if it did
}

// Ask answers question from the index or refuses.
func (p *Pipeline) Ask(question string) (Response, error) {
	hits, err := p.ix.Search(question, p.mode, p.k)
	if err != nil {
		return Response{}, err
	}
	r := Response{Hits: hits}
	for _, h := range hits {
		r.BestDense = max(r.BestDense, h.Dense)
	}
	if r.BestDense < p.minRelevant {
		r.Answer = Answer{Text: RefusalText, Refused: true}
		r.Reason = fmt.Sprintf("best similarity %.2f is below the %.2f threshold", r.BestDense, p.minRelevant)
		return r, nil
	}
	r.Prompt, err = BuildPrompt(question, hits, p.budget)
	if err != nil {
		return Response{}, err
	}
	r.Answer = AnswerFrom(r.Prompt)
	if r.Answer.Refused {
		r.Reason = "no context sentence shares a term with the question"
	}
	return r, nil
}
