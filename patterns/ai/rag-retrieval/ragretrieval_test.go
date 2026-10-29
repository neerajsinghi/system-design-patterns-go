package ragretrieval

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
)

func mustIndex(t *testing.T, c Chunker) *Index {
	t.Helper()
	ix, err := NewIndex(ChunkAll(c, Corpus()))
	if err != nil {
		t.Fatal(err)
	}
	return ix
}

func headingIndex(t *testing.T) *Index {
	t.Helper()
	h, err := NewHeadingChunker(120)
	if err != nil {
		t.Fatal(err)
	}
	return mustIndex(t, h)
}

func TestValidation(t *testing.T) {
	one := []Chunk{{ID: "a", DocID: "d", Text: "hello world"}}
	ix, err := NewIndex(one)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		err  func() error
	}{
		{"fixed size 0", func() error { _, err := NewFixedChunker(0); return err }},
		{"overlap negative", func() error { _, err := NewOverlapChunker(10, -1); return err }},
		{"overlap equals size", func() error { _, err := NewOverlapChunker(10, 10); return err }},
		{"heading max too small", func() error { _, err := NewHeadingChunker(4); return err }},
		{"empty index", func() error { _, err := NewIndex(nil); return err }},
		{"duplicate chunk IDs", func() error { _, err := NewIndex(append(one, one...)); return err }},
		{"zero dim", func() error { _, err := NewIndex(one, WithDim(0)); return err }},
		{"bad BM25 b", func() error { _, err := NewIndex(one, WithBM25Params(1.2, 1.5)); return err }},
		{"bad RRF k", func() error { _, err := NewIndex(one, WithRRFK(0)); return err }},
		{"search k 0", func() error { _, err := ix.Search("hello", Hybrid, 0); return err }},
		{"search unknown mode", func() error { _, err := ix.Search("hello", Mode(9), 1); return err }},
		{"nil pipeline index", func() error { _, err := NewPipeline(nil); return err }},
		{"pipeline top-k 0", func() error { _, err := NewPipeline(ix, WithTopK(0)); return err }},
		{"pipeline budget 0", func() error { _, err := NewPipeline(ix, WithTokenBudget(0)); return err }},
		{"pipeline relevance > 1", func() error { _, err := NewPipeline(ix, WithMinRelevance(1.5)); return err }},
		{"pipeline unknown mode", func() error { _, err := NewPipeline(ix, WithMode(Mode(-1))); return err }},
		{"prompt empty question", func() error { _, err := BuildPrompt(" ", nil, 100); return err }},
		{"prompt budget below instructions", func() error { _, err := BuildPrompt("hi?", nil, 10); return err }},
		{"evaluator no questions", func() error { _, err := NewEvaluator(Corpus(), nil); return err }},
		{"evaluator unknown passage", func() error {
			_, err := NewEvaluator(Corpus(), []Question{{"q", "returns#nope", "30 days"}})
			return err
		}},
		{"evaluator evidence not in passage", func() error {
			_, err := NewEvaluator(Corpus(), []Question{{"q", "returns#return-window", "90 days"}})
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err() == nil {
				t.Fatal("expected error")
			}
		})
	}
	ev, _ := NewEvaluator(Corpus(), EvalSet())
	if _, err := ev.Evaluate(ix, Dense, 0); err == nil {
		t.Fatal("Evaluate with k 0: expected error")
	}
}

func TestTermsNormalise(t *testing.T) {
	tests := []struct{ in, want string }{
		{"Refunds refunded refunding REFUND", "refund refund refund refund"},
		{"shipping ships shipped", "ship ship ship"},
		{"cancel cancelled cancelling", "cancel cancel cancel"},
		{"assemble assembled", "assembl assembl"},
		{"policies policy", "policy policy"},
		{"What is the $30 fee?", "30 fee"},
	}
	for _, tt := range tests {
		if got := strings.Join(Terms(tt.in), " "); got != tt.want {
			t.Errorf("Terms(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestFixedChunkBoundaries(t *testing.T) {
	doc := Document{ID: "d", Text: "w1 w2 w3 w4\nw5 w6  w7 w8 w9 w10"}
	c, _ := NewFixedChunker(4)
	got := c.Chunk(doc)
	want := []string{"w1 w2 w3 w4", "w5 w6  w7 w8", "w9 w10"}
	if len(got) != len(want) {
		t.Fatalf("got %d chunks, want %d", len(got), len(want))
	}
	for i, ch := range got {
		if ch.Text != want[i] || doc.Text[ch.Start:ch.End] != ch.Text {
			t.Errorf("chunk %d = %q [%d,%d), want %q", i, ch.Text, ch.Start, ch.End, want[i])
		}
		if ch.ID != fmt.Sprintf("d/%d", i) || ch.Tokens != CountTokens(want[i]) {
			t.Errorf("chunk %d: ID %q tokens %d", i, ch.ID, ch.Tokens)
		}
	}
}

func TestOverlapSharesWords(t *testing.T) {
	var words []string
	for i := range 11 {
		words = append(words, fmt.Sprintf("w%d", i))
	}
	doc := Document{ID: "d", Text: strings.Join(words, " ")}
	c, _ := NewOverlapChunker(4, 2)
	got := c.Chunk(doc)
	// Windows start every 2 words: 0-3, 2-5, 4-7, 6-9, 8-10.
	if len(got) != 5 {
		t.Fatalf("got %d chunks, want 5", len(got))
	}
	for i := 1; i < len(got); i++ {
		prev := strings.Fields(got[i-1].Text)
		cur := strings.Fields(got[i].Text)
		if strings.Join(prev[len(prev)-2:], " ") != strings.Join(cur[:2], " ") {
			t.Errorf("chunks %d and %d do not share 2 words: %q / %q", i-1, i, got[i-1].Text, got[i].Text)
		}
	}
	if last := got[len(got)-1]; last.End != len(doc.Text) {
		t.Errorf("last chunk ends at %d, want %d", last.End, len(doc.Text))
	}
}

func TestFixedChunksCoverEveryWord(t *testing.T) {
	for _, overlap := range []int{0, 7} {
		c, _ := NewOverlapChunker(20, overlap)
		for _, d := range Corpus() {
			covered := 0
			for _, ch := range c.Chunk(d) {
				if ch.Start < covered {
					covered = max(covered, ch.End) // overlap
					continue
				}
				if strings.TrimSpace(d.Text[covered:ch.Start]) != "" {
					t.Fatalf("%s %s: words in [%d,%d) are in no chunk", c.Name(), d.ID, covered, ch.Start)
				}
				covered = ch.End
			}
			if strings.TrimSpace(d.Text[covered:]) != "" {
				t.Fatalf("%s %s: trailing words are in no chunk", c.Name(), d.ID)
			}
		}
	}
}

func TestParseSections(t *testing.T) {
	doc := Document{ID: "d", Title: "Title", Text: "# Title\n\nIntro text.\n\n## First Part\n\nOne.\n\n## Second: part 2\n\nTwo.\n"}
	secs := ParseSections(doc)
	ids := []string{"d#intro", "d#first-part", "d#second-part-2"}
	if len(secs) != len(ids) {
		t.Fatalf("got %d sections, want %d: %+v", len(secs), len(ids), secs)
	}
	for i, s := range secs {
		if s.ID != ids[i] {
			t.Errorf("section %d ID %q, want %q", i, s.ID, ids[i])
		}
	}
	if got := strings.TrimSpace(doc.Text[secs[1].Start:secs[1].End]); got != "## First Part\n\nOne." {
		t.Errorf("first part span = %q", got)
	}
}

func TestHeadingChunkerKeepsHeadingsWithText(t *testing.T) {
	h, _ := NewHeadingChunker(40)
	for _, d := range Corpus() {
		secs := ParseSections(d)
		for _, ch := range h.Chunk(d) {
			if !strings.HasPrefix(ch.Text, ch.Heading+"\n") || !strings.HasPrefix(ch.Heading, d.Title) {
				t.Fatalf("%s: chunk does not start with its heading path %q: %q", ch.ID, ch.Heading, ch.Text)
			}
			if ch.Tokens > 40 {
				t.Fatalf("%s: %d tokens, max 40", ch.ID, ch.Tokens)
			}
			// The chunk lies inside exactly one section, whose heading it names.
			inside := 0
			for _, s := range secs {
				if ch.Start >= s.Start && ch.End <= s.End {
					inside++
					if !strings.HasSuffix(s.ID, "#intro") && !strings.HasSuffix(ch.Heading, " > "+s.Heading) {
						t.Fatalf("%s: heading %q, but text is from section %q", ch.ID, ch.Heading, s.Heading)
					}
				}
			}
			if inside != 1 {
				t.Fatalf("%s: chunk spans %d sections", ch.ID, inside)
			}
		}
	}
	// With a large limit, every section is one chunk.
	big, _ := NewHeadingChunker(500)
	d := Corpus()[0]
	if got, want := len(big.Chunk(d)), len(ParseSections(d)); got != want {
		t.Fatalf("got %d chunks, want one per section (%d)", got, want)
	}
}

func TestEmbedDeterministicAndNormalised(t *testing.T) {
	a, b := Embed("Refunds within 30 days", 256), Embed("Refunds within 30 days", 256)
	var norm float64
	for i := range a {
		if a[i] != b[i] {
			t.Fatal("embedding is not deterministic")
		}
		norm += float64(a[i]) * float64(a[i])
	}
	if math.Abs(norm-1) > 1e-5 {
		t.Fatalf("norm^2 = %v, want 1", norm)
	}
	related := dot(Embed("refund timing", 256), Embed("when are refunds paid", 256))
	unrelated := dot(Embed("refund timing", 256), Embed("dogs on a lead", 256))
	if related <= unrelated {
		t.Fatalf("related %.2f should beat unrelated %.2f", related, unrelated)
	}
}

func TestBM25Sanity(t *testing.T) {
	chunks := []Chunk{
		{ID: "once", Text: "refund policy for orders"},
		{ID: "twice", Text: "refund refund policy orders"},
		{ID: "long", Text: "refund policy orders plus many other unrelated words about sofas lamps beds tables chairs"},
		{ID: "none", Text: "delivery windows and parking"},
	}
	ix, _ := NewIndex(chunks)
	hits, _ := ix.Search("refund", BM25, 10)
	if len(hits) != 3 {
		t.Fatalf("got %d hits, want 3 (chunks without the term are not ranked)", len(hits))
	}
	order := []string{"twice", "once", "long"}
	for i, h := range hits {
		if h.Chunk.ID != order[i] {
			t.Fatalf("rank %d = %s, want %s (tf up, length down)", i+1, h.Chunk.ID, order[i])
		}
	}
	if ix.IDF("parking") <= ix.IDF("refund") {
		t.Fatalf("rare term idf %.2f should exceed common term idf %.2f", ix.IDF("parking"), ix.IDF("refund"))
	}
	// Saturation: a second occurrence adds less than the first.
	one := ix.bm25(0, map[string]bool{"refund": true})
	two := ix.bm25(1, map[string]bool{"refund": true})
	if two >= 2*one {
		t.Fatalf("tf=2 score %.3f should be less than twice tf=1 score %.3f", two, one)
	}
}

func TestFuseRRF(t *testing.T) {
	got := FuseRRF(60, []string{"a", "b", "c"}, []string{"c", "a", "d"})
	score := func(ranks ...int) float64 {
		var s float64
		for _, r := range ranks {
			s += 1 / (60 + float64(r))
		}
		return s
	}
	want := []Fused{{"a", score(1, 2)}, {"c", score(3, 1)}, {"b", score(2)}, {"d", score(3)}}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for i := range want {
		if got[i].ID != want[i].ID || math.Abs(got[i].Score-want[i].Score) > 1e-12 {
			t.Fatalf("rank %d = %+v, want %+v", i+1, got[i], want[i])
		}
	}
}

func TestHybridUsesBothRankings(t *testing.T) {
	ix := headingIndex(t)
	q := "Who pays import duties on international orders?"
	d, _ := ix.Search(q, Dense, 50)
	b, _ := ix.Search(q, BM25, 50)
	h, _ := ix.Search(q, Hybrid, 5)
	pos := func(hs []Hit, id string) int {
		for i, x := range hs {
			if x.Chunk.ID == id {
				return i + 1
			}
		}
		return 0
	}
	for _, x := range h {
		want := 0.0
		if r := pos(d, x.Chunk.ID); r > 0 {
			want += 1 / (60 + float64(r))
		}
		if r := pos(b, x.Chunk.ID); r > 0 {
			want += 1 / (60 + float64(r))
		}
		if math.Abs(x.Score-want) > 1e-12 {
			t.Fatalf("%s: hybrid score %.6f, want RRF %.6f", x.Chunk.ID, x.Score, want)
		}
	}
}

func TestRecallFloor(t *testing.T) {
	ev, err := NewEvaluator(Corpus(), EvalSet())
	if err != nil {
		t.Fatal(err)
	}
	ix := headingIndex(t)
	for _, mode := range Modes {
		m, err := ev.Evaluate(ix, mode, 3)
		if err != nil {
			t.Fatal(err)
		}
		if m.Recall < 0.9 || m.MRR < 0.8 {
			t.Errorf("heading/%s: recall@3 %.2f, MRR %.2f; want >= 0.90 and >= 0.80", mode, m.Recall, m.MRR)
		}
	}
	tiny, _ := NewFixedChunker(8)
	m, _ := ev.Evaluate(mustIndex(t, tiny), Hybrid, 3)
	if m.Recall > 0.5 {
		t.Errorf("8-word chunks: recall@3 %.2f; expected tiny chunks to lose the answer's context", m.Recall)
	}
}

func TestRefusePath(t *testing.T) {
	p, _ := NewPipeline(headingIndex(t))
	for _, q := range []string{"What is the capital of France?", "Do you sell electric cars?", "What is the weather tomorrow?"} {
		r, err := p.Ask(q)
		if err != nil {
			t.Fatal(err)
		}
		if !r.Answer.Refused || r.Answer.Text != RefusalText || r.Reason == "" || r.Prompt.Text != "" {
			t.Errorf("%q: want a refusal before prompt assembly, got %+v", q, r.Answer)
		}
		if r.BestDense >= 0.25 {
			t.Errorf("%q: best similarity %.2f should be below the gate", q, r.BestDense)
		}
	}
	r, _ := p.Ask("How long is the password reset link good for?")
	if r.Answer.Refused || !strings.Contains(r.Answer.Text, "valid for 20 minutes") {
		t.Fatalf("in-scope question: got %+v", r.Answer)
	}
	// With the gate off, the out-of-scope question gets a grounded but irrelevant answer.
	open, _ := NewPipeline(headingIndex(t), WithMinRelevance(0))
	r, _ = open.Ask("Do you sell electric cars?")
	if r.Answer.Refused || Grounded(r.Answer, r.Prompt) != nil {
		t.Fatalf("gate off: expected a grounded (but wrong) answer, got %+v", r.Answer)
	}
}

func TestCitationsMapToChunks(t *testing.T) {
	p, _ := NewPipeline(headingIndex(t), WithMinRelevance(0))
	for _, q := range EvalSet() {
		r, err := p.Ask(q.Q)
		if err != nil {
			t.Fatal(err)
		}
		if err := Grounded(r.Answer, r.Prompt); err != nil {
			t.Fatalf("%q: %v", q.Q, err)
		}
		for i, c := range r.Prompt.Context {
			if c.N != i+1 || c.ChunkID != r.Hits[i].Chunk.ID || c.Text != r.Hits[i].Chunk.Text {
				t.Fatalf("%q: citation %d does not map to hit %d", q.Q, c.N, i+1)
			}
			if !strings.Contains(r.Prompt.Text, fmt.Sprintf("[%d] (source: %s)\n%s", c.N, c.ChunkID, c.Text)) {
				t.Fatalf("%q: prompt is missing passage [%d]", q.Q, c.N)
			}
		}
		if !r.Answer.Refused && !strings.HasSuffix(r.Answer.Text, fmt.Sprintf("[%d]", r.Answer.Sentences[0].Cite)) {
			t.Fatalf("%q: answer text lacks its citation: %q", q.Q, r.Answer.Text)
		}
	}
}

func TestGroundedRejectsFabrication(t *testing.T) {
	p := Prompt{Context: []Citation{{N: 1, ChunkID: "x", Text: "Returns are accepted within 30 days."}}}
	tests := []struct {
		name string
		a    Answer
		ok   bool
	}{
		{"verbatim", Answer{Sentences: []CitedSentence{{"Returns are accepted within 30 days.", 1}}}, true},
		{"invented number", Answer{Sentences: []CitedSentence{{"Returns are accepted within 14 days.", 1}}}, false},
		{"citation out of range", Answer{Sentences: []CitedSentence{{"Returns are accepted within 30 days.", 2}}}, false},
		{"no citation at all", Answer{Text: "Within 14 days."}, false},
		{"refusal", Answer{Refused: true}, true},
	}
	for _, tt := range tests {
		if err := Grounded(tt.a, p); (err == nil) != tt.ok {
			t.Errorf("%s: Grounded = %v, want ok=%v", tt.name, err, tt.ok)
		}
	}
}

func TestTokenBudgetRespected(t *testing.T) {
	big, _ := NewFixedChunker(128)
	ix := mustIndex(t, big)
	q := "How long does standard delivery take?"
	hits, _ := ix.Search(q, Hybrid, 5)
	for _, budget := range []int{60, 100, 200, 300, 500, 2000} {
		p, err := BuildPrompt(q, hits, budget)
		if err != nil {
			t.Fatal(err)
		}
		if p.Tokens > budget || p.Tokens != CountTokens(p.Text) {
			t.Fatalf("budget %d: prompt has %d tokens", budget, p.Tokens)
		}
		if len(p.Context)+p.Dropped != len(hits) {
			t.Fatalf("budget %d: %d included + %d dropped != %d hits", budget, len(p.Context), p.Dropped, len(hits))
		}
	}
	// A 100-token budget skips the full 128-word chunks but can still take a
	// shorter, lower-ranked tail chunk.
	p, _ := BuildPrompt(q, hits, 100)
	if p.Dropped == 0 {
		t.Fatal("100-token budget should drop at least one 128-word chunk")
	}
	for _, c := range p.Context {
		if CountTokens(c.Text) >= 100 {
			t.Fatalf("included a %d-token chunk in a 100-token budget", CountTokens(c.Text))
		}
	}
	// Room for instructions and question only: no context, so the answerer refuses.
	base := CountTokens(Instructions+"\n\nContext:\n") + CountTokens("\nQuestion: "+q+"\nAnswer:")
	p, err := BuildPrompt(q, hits, base)
	if err != nil || len(p.Context) != 0 {
		t.Fatalf("budget %d: got %d passages, err %v", base, len(p.Context), err)
	}
	if a := AnswerFrom(p); !a.Refused {
		t.Fatalf("no context should mean a refusal, got %q", a.Text)
	}
}

func TestConcurrentAsk(t *testing.T) {
	p, _ := NewPipeline(headingIndex(t))
	qs := EvalSet()
	want := make([]string, len(qs))
	for i, q := range qs {
		r, _ := p.Ask(q.Q)
		want[i] = r.Answer.Text
	}
	var wg sync.WaitGroup
	errs := make(chan string, 8*len(qs))
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range qs {
				j := (i + g) % len(qs)
				r, err := p.Ask(qs[j].Q)
				if err != nil || r.Answer.Text != want[j] {
					errs <- qs[j].Q
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for q := range errs {
		t.Errorf("concurrent Ask gave a different answer for %q", q)
	}
}

func TestFixture(t *testing.T) {
	docs := Corpus()
	if len(docs) < 15 || len(docs) > 25 {
		t.Fatalf("corpus has %d documents", len(docs))
	}
	for _, d := range docs {
		if d.Title == "" || len(ParseSections(d)) < 2 {
			t.Fatalf("%s: needs a title and sections", d.ID)
		}
	}
	if n := len(EvalSet()); n < 40 {
		t.Fatalf("eval set has %d questions, want at least 40", n)
	}
	if _, err := NewEvaluator(docs, EvalSet()); err != nil {
		t.Fatal(err)
	}
}
