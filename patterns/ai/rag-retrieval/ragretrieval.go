// Package ragretrieval implements the retrieval half of retrieval-augmented
// generation (RAG) with no model and no network, so that the part that
// decides whether RAG works can be measured.
//
// A support chatbot that answers from the model's memory will state the
// policy it saw in training, which may be out of date or simply invented.
// RAG fixes that by retrieving passages from the company's own documents and
// telling the model to answer only from them, with citations. But the model
// can only be as right as the passages it is handed: if chunking cuts the
// answer away from the words that describe it, or retrieval ranks the wrong
// passage first, the model is back to guessing, now with a citation that
// makes the guess look trustworthy.
//
// The package provides the pieces of that pipeline:
//
//   - Chunkers: fixed-size windows of words, fixed windows with overlap, and
//     a structure-aware chunker that splits Markdown by heading and keeps each
//     heading with its text.
//   - An Index with three retrieval modes: dense (a deterministic toy
//     embedding: hashed bag of words plus character trigrams, cosine
//     similarity), BM25 keyword scoring, and hybrid (reciprocal rank fusion of
//     the two rankings).
//   - BuildPrompt, which assembles a grounded prompt with numbered citations
//     inside a token budget, and Pipeline.Ask, which refuses when the best
//     retrieved passage is not similar enough to the question.
//   - Answer, a fake "model" that only answers with a sentence copied from
//     the retrieved context, so Grounded can check every answer against its
//     citation.
//   - An Evaluator that measures recall@k and MRR on a fixture of questions
//     whose answer passage is known, for any chunker and retrieval mode.
//
// Corpus returns 21 short policy documents for Pebblebrook Home, a fictional
// retailer, and EvalSet returns 48 questions about them.
package ragretrieval

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

// Document is one source document, in Markdown. A line starting with "# " is
// its title and lines starting with "## " start sections.
type Document struct {
	ID    string
	Title string
	Text  string
}

// Section is a headed part of a document. Its ID is "docID#slug", the unit
// the evaluation fixture points at.
type Section struct {
	ID         string
	DocID      string
	Heading    string
	Start, End int // byte offsets into Document.Text, End exclusive
}

// Chunk is a retrievable unit of text cut from one document.
type Chunk struct {
	ID    string
	DocID string
	// Heading is the "Title > Section" path for structure-aware chunks, empty
	// for fixed-size chunks.
	Heading string
	// Text is what gets indexed and shown to the model.
	Text string
	// Start and End are byte offsets of the covered span in the document.
	Start, End int
	// Tokens is the token count of Text (see CountTokens).
	Tokens int
}

// Chunker splits a document into chunks.
type Chunker interface {
	Name() string
	Chunk(doc Document) []Chunk
}

// CountTokens approximates a model's token count as the number of
// whitespace-separated words. Real tokenizers produce about 1.3 tokens per
// English word; a budget in words is enough to show the mechanism.
func CountTokens(s string) int { return len(strings.Fields(s)) }

// stopwords are dropped before scoring; they match everything and mean little.
var stopwords = map[string]bool{}

func init() {
	for _, w := range strings.Fields(`a an and are as at be been before by can could did do does
		for from get got had has have how i if in into is it its may me must my no not of on or our
		so than that the their them then there they this to up us was we were what when where which
		who why will with would you your yours after per am any all also`) {
		stopwords[w] = true
	}
}

// Terms normalises text into scoring terms: lowercase, split on anything that
// is not a letter or digit, stopwords removed, and a crude suffix strip so
// that "refunds", "refunded" and "refunding" all become "refund".
func Terms(s string) []string {
	fields := strings.FieldsFunc(strings.ToLower(s), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	out := fields[:0]
	for _, f := range fields {
		if stopwords[f] {
			continue
		}
		out = append(out, stem(f))
	}
	return out
}

func stem(w string) string {
	switch {
	case len(w) > 4 && strings.HasSuffix(w, "ies"):
		w = w[:len(w)-3] + "y"
	case len(w) > 3 && strings.HasSuffix(w, "s") && !strings.HasSuffix(w, "ss") && !strings.HasSuffix(w, "us"):
		w = w[:len(w)-1]
	}
	for _, suf := range []string{"ing", "ed"} {
		if len(w)-len(suf) >= 3 && strings.HasSuffix(w, suf) {
			w = w[:len(w)-len(suf)]
			// "shipping" -> "shipp" -> "ship", "cancelled" -> "cancel".
			n := len(w)
			double := n > 3 && w[n-1] == w[n-2]
			if double && (!strings.ContainsRune("aeiousl", rune(w[n-1])) || w[n-1] == 'l' && n > 5) {
				w = w[:n-1]
			}
			break
		}
	}
	// "assemble" and "assembled" both end up as "assembl".
	if len(w) > 3 && strings.HasSuffix(w, "e") {
		w = w[:len(w)-1]
	}
	return w
}

// Slug turns a heading into the lowercase-and-dashes form used in section IDs.
func Slug(heading string) string {
	var b strings.Builder
	dash := false
	for _, r := range strings.ToLower(heading) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			if dash && b.Len() > 0 {
				b.WriteByte('-')
			}
			b.WriteRune(r)
			dash = false
		} else {
			dash = true
		}
	}
	return b.String()
}

// ParseSections splits a document at its "## " headings. Text between the
// title and the first "## " heading becomes an "intro" section.
func ParseSections(doc Document) []Section {
	var secs []Section
	text := doc.Text
	cur := Section{ID: doc.ID + "#intro", DocID: doc.ID, Heading: doc.Title, Start: 0}
	for pos := 0; pos < len(text); {
		end := strings.IndexByte(text[pos:], '\n')
		if end < 0 {
			end = len(text)
		} else {
			end += pos
		}
		line := text[pos:end]
		if strings.HasPrefix(line, "## ") {
			cur.End = pos
			if hasBody(text[cur.Start:cur.End]) {
				secs = append(secs, cur)
			}
			h := strings.TrimSpace(line[3:])
			cur = Section{ID: doc.ID + "#" + Slug(h), DocID: doc.ID, Heading: h, Start: pos}
		}
		pos = end + 1
	}
	cur.End = len(text)
	if hasBody(text[cur.Start:cur.End]) {
		secs = append(secs, cur)
	}
	return secs
}

// hasBody reports whether a section span has text other than headings.
func hasBody(s string) bool {
	for _, line := range strings.Split(s, "\n") {
		t := strings.TrimSpace(line)
		if t != "" && !strings.HasPrefix(t, "#") {
			return true
		}
	}
	return false
}

// wordSpans returns the byte span of every whitespace-separated word in s.
func wordSpans(s string) [][2]int {
	var spans [][2]int
	start := -1
	for i, r := range s {
		if unicode.IsSpace(r) {
			if start >= 0 {
				spans = append(spans, [2]int{start, i})
				start = -1
			}
		} else if start < 0 {
			start = i
		}
	}
	if start >= 0 {
		spans = append(spans, [2]int{start, len(s)})
	}
	return spans
}

// FixedChunker cuts each document into windows of Size words. With Overlap >
// 0, each window starts Size-Overlap words after the previous one, so a
// sentence cut by one boundary appears whole in the neighbouring chunk.
type FixedChunker struct {
	size, overlap int
}

// NewFixedChunker returns a chunker of size-word windows with no overlap.
func NewFixedChunker(size int) (*FixedChunker, error) { return NewOverlapChunker(size, 0) }

// NewOverlapChunker returns a chunker of size-word windows where consecutive
// windows share overlap words.
func NewOverlapChunker(size, overlap int) (*FixedChunker, error) {
	if size <= 0 {
		return nil, errors.New("ragretrieval: chunk size must be positive")
	}
	if overlap < 0 || overlap >= size {
		return nil, errors.New("ragretrieval: overlap must be in [0, size)")
	}
	return &FixedChunker{size: size, overlap: overlap}, nil
}

// Name describes the chunker, e.g. "fixed-64" or "fixed-64/overlap-16".
func (c *FixedChunker) Name() string {
	if c.overlap == 0 {
		return fmt.Sprintf("fixed-%d", c.size)
	}
	return fmt.Sprintf("fixed-%d/overlap-%d", c.size, c.overlap)
}

// Chunk splits doc into word windows. Chunk text is the exact document
// substring the window covers, Markdown markers included.
func (c *FixedChunker) Chunk(doc Document) []Chunk {
	spans := wordSpans(doc.Text)
	var out []Chunk
	step := c.size - c.overlap
	for i := 0; i < len(spans); i += step {
		j := min(i+c.size, len(spans))
		start, end := spans[i][0], spans[j-1][1]
		out = append(out, Chunk{
			ID: fmt.Sprintf("%s/%d", doc.ID, len(out)), DocID: doc.ID,
			Text: doc.Text[start:end], Start: start, End: end, Tokens: j - i,
		})
		if j == len(spans) {
			break
		}
	}
	return out
}

// HeadingChunker is structure-aware: one chunk per Markdown section, with
// the "Title > Section" path prepended so the chunk still says what it is
// about when read on its own. A section longer than MaxTokens is split at
// paragraph boundaries, and every piece keeps the heading path.
type HeadingChunker struct {
	maxTokens int
}

// NewHeadingChunker returns a structure-aware chunker whose chunks are at
// most maxTokens words (heading path included), unless a single paragraph is
// longer, in which case that paragraph is cut into word windows.
func NewHeadingChunker(maxTokens int) (*HeadingChunker, error) {
	if maxTokens < 8 {
		return nil, errors.New("ragretrieval: heading chunker maxTokens must be at least 8")
	}
	return &HeadingChunker{maxTokens: maxTokens}, nil
}

// Name returns "heading".
func (c *HeadingChunker) Name() string { return "heading" }

// Chunk splits doc by section, then by paragraph if a section is too long.
func (c *HeadingChunker) Chunk(doc Document) []Chunk {
	var out []Chunk
	for _, sec := range ParseSections(doc) {
		path := doc.Title
		if !strings.HasSuffix(sec.ID, "#intro") {
			path = doc.Title + " > " + sec.Heading
		}
		budget := c.maxTokens - CountTokens(path)
		// Paragraphs as spans, skipping heading lines.
		type para struct{ start, end, tokens int }
		var paras []para
		body := doc.Text[sec.Start:sec.End]
		for off := 0; off < len(body); {
			n := strings.Index(body[off:], "\n\n")
			end := len(body)
			if n >= 0 {
				end = off + n
			}
			p := strings.TrimSpace(body[off:end])
			if p != "" && !strings.HasPrefix(p, "#") {
				lead := strings.Index(body[off:end], p)
				s := sec.Start + off + lead
				paras = append(paras, para{s, s + len(p), CountTokens(p)})
			}
			if n < 0 {
				break
			}
			off = end + 2
		}
		emit := func(start, end int) {
			text := path + "\n" + doc.Text[start:end]
			out = append(out, Chunk{
				ID: fmt.Sprintf("%s/%d", doc.ID, len(out)), DocID: doc.ID, Heading: path,
				Text: text, Start: start, End: end, Tokens: CountTokens(text),
			})
		}
		gStart, gEnd, gTok := -1, -1, 0
		flush := func() {
			if gStart >= 0 {
				emit(gStart, gEnd)
			}
			gStart, gEnd, gTok = -1, -1, 0
		}
		for _, p := range paras {
			if p.tokens > budget {
				flush()
				spans := wordSpans(doc.Text[p.start:p.end])
				for i := 0; i < len(spans); i += budget {
					j := min(i+budget, len(spans))
					emit(p.start+spans[i][0], p.start+spans[j-1][1])
				}
				continue
			}
			if gStart >= 0 && gTok+p.tokens > budget {
				flush()
			}
			if gStart < 0 {
				gStart = p.start
			}
			gEnd, gTok = p.end, gTok+p.tokens
		}
		flush()
	}
	return out
}

// ChunkAll applies c to every document.
func ChunkAll(c Chunker, docs []Document) []Chunk {
	var out []Chunk
	for _, d := range docs {
		out = append(out, c.Chunk(d)...)
	}
	return out
}
