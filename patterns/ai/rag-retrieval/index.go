package ragretrieval

import (
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
)

// Mode selects how Search ranks chunks.
type Mode int

const (
	// Dense ranks by cosine similarity of toy embeddings.
	Dense Mode = iota
	// BM25 ranks by BM25 keyword relevance.
	BM25
	// Hybrid fuses the dense and BM25 rankings with reciprocal rank fusion.
	Hybrid
)

// Modes lists every retrieval mode, for tables and loops.
var Modes = []Mode{Dense, BM25, Hybrid}

func (m Mode) String() string {
	switch m {
	case Dense:
		return "dense"
	case BM25:
		return "bm25"
	case Hybrid:
		return "hybrid"
	}
	return fmt.Sprintf("Mode(%d)", int(m))
}

// Hit is one retrieved chunk.
type Hit struct {
	Chunk Chunk
	// Rank is 1-based.
	Rank int
	// Score is the score the mode ranked by (cosine, BM25 or RRF).
	Score float64
	// Dense and BM25 are the chunk's scores under each scorer, whatever the
	// mode. Dense is a cosine similarity, comparable across queries, which is
	// why the refusal gate uses it.
	Dense, BM25 float64
}

// Embed maps text to a deterministic, L2-normalised vector of length dim: a
// hashed bag of words. Each term adds ±1 to a hashed bucket and each of its
// character trigrams adds ±0.5, so "refund" and "refunded" end up close. It
// has no notion of meaning; "cancel" and "terminate" are unrelated to it.
func Embed(text string, dim int) []float32 {
	v := make([]float32, dim)
	add := func(feature string, w float32) {
		h := fnv.New64a()
		h.Write([]byte(feature))
		x := h.Sum64()
		if x>>63 == 1 {
			w = -w
		}
		v[x%uint64(dim)] += w
	}
	for _, t := range Terms(text) {
		add("w:"+t, 1)
		g := "^" + t + "$"
		for i := 0; i+3 <= len(g); i++ {
			add("g:"+g[i:i+3], 0.5)
		}
	}
	var norm float64
	for _, x := range v {
		norm += float64(x) * float64(x)
	}
	if norm > 0 {
		inv := float32(1 / math.Sqrt(norm))
		for i := range v {
			v[i] *= inv
		}
	}
	return v
}

func dot(a, b []float32) float64 {
	var s float64
	for i := range a {
		s += float64(a[i]) * float64(b[i])
	}
	return s
}

// Index holds chunks with their embeddings and BM25 statistics. It is
// immutable after NewIndex, so Search is safe for concurrent use.
type Index struct {
	chunks []Chunk
	vecs   [][]float32
	tf     []map[string]int
	dl     []float64
	avgdl  float64
	df     map[string]int
	dim    int
	k1, b  float64
	rrfK   float64
}

// IndexOption configures an Index.
type IndexOption func(*Index)

// WithDim sets the embedding dimension (default 512).
func WithDim(d int) IndexOption { return func(ix *Index) { ix.dim = d } }

// WithBM25Params sets BM25's term-saturation k1 (default 1.2) and length
// normalisation b (default 0.75).
func WithBM25Params(k1, b float64) IndexOption { return func(ix *Index) { ix.k1, ix.b = k1, b } }

// WithRRFK sets the reciprocal rank fusion constant (default 60).
func WithRRFK(k float64) IndexOption { return func(ix *Index) { ix.rrfK = k } }

// NewIndex embeds and indexes chunks.
func NewIndex(chunks []Chunk, opts ...IndexOption) (*Index, error) {
	if len(chunks) == 0 {
		return nil, errors.New("ragretrieval: index needs at least one chunk")
	}
	ix := &Index{dim: 512, k1: 1.2, b: 0.75, rrfK: 60, df: map[string]int{}}
	for _, o := range opts {
		o(ix)
	}
	if ix.dim <= 0 {
		return nil, errors.New("ragretrieval: embedding dimension must be positive")
	}
	if ix.k1 < 0 || ix.b < 0 || ix.b > 1 {
		return nil, errors.New("ragretrieval: BM25 needs k1 >= 0 and b in [0, 1]")
	}
	if ix.rrfK <= 0 {
		return nil, errors.New("ragretrieval: RRF k must be positive")
	}
	seen := map[string]bool{}
	ix.chunks = append([]Chunk(nil), chunks...)
	var total float64
	for _, c := range ix.chunks {
		if seen[c.ID] {
			return nil, fmt.Errorf("ragretrieval: duplicate chunk ID %q", c.ID)
		}
		seen[c.ID] = true
		terms := Terms(c.Text)
		tf := map[string]int{}
		for _, t := range terms {
			tf[t]++
		}
		for t := range tf {
			ix.df[t]++
		}
		ix.tf = append(ix.tf, tf)
		ix.dl = append(ix.dl, float64(len(terms)))
		total += float64(len(terms))
		ix.vecs = append(ix.vecs, Embed(c.Text, ix.dim))
	}
	ix.avgdl = total / float64(len(ix.chunks))
	return ix, nil
}

// Len returns the number of chunks.
func (ix *Index) Len() int { return len(ix.chunks) }

// IDF is BM25's inverse document frequency for an already normalised term:
// ln(1 + (N - n + 0.5) / (n + 0.5)), always positive.
func (ix *Index) IDF(term string) float64 {
	n := float64(ix.df[term])
	N := float64(len(ix.chunks))
	return math.Log(1 + (N-n+0.5)/(n+0.5))
}

// bm25 scores chunk i against query terms (each unique term counted once).
func (ix *Index) bm25(i int, q map[string]bool) float64 {
	var s float64
	norm := ix.k1 * (1 - ix.b + ix.b*ix.dl[i]/ix.avgdl)
	for t := range q {
		f := float64(ix.tf[i][t])
		if f == 0 {
			continue
		}
		s += ix.IDF(t) * f * (ix.k1 + 1) / (f + norm)
	}
	return s
}

type scored struct {
	i int
	s float64
}

// rank sorts by score descending, ties by chunk position, for determinism.
func rank(xs []scored) {
	sort.Slice(xs, func(a, b int) bool {
		if xs[a].s != xs[b].s {
			return xs[a].s > xs[b].s
		}
		return xs[a].i < xs[b].i
	})
}

// Search returns the top k chunks for query under mode.
func (ix *Index) Search(query string, mode Mode, k int) ([]Hit, error) {
	if k <= 0 {
		return nil, errors.New("ragretrieval: k must be positive")
	}
	if mode < Dense || mode > Hybrid {
		return nil, fmt.Errorf("ragretrieval: unknown mode %v", mode)
	}
	qv := Embed(query, ix.dim)
	qt := map[string]bool{}
	for _, t := range Terms(query) {
		qt[t] = true
	}
	n := len(ix.chunks)
	dense := make([]float64, n)
	bm := make([]float64, n)
	var dRank, bRank []scored
	for i := range n {
		dense[i] = dot(qv, ix.vecs[i])
		bm[i] = ix.bm25(i, qt)
		dRank = append(dRank, scored{i, dense[i]})
		if bm[i] > 0 {
			bRank = append(bRank, scored{i, bm[i]})
		}
	}
	rank(dRank)
	rank(bRank)

	var final []scored
	switch mode {
	case Dense:
		final = dRank
	case BM25:
		final = bRank
	case Hybrid:
		ids := func(xs []scored) []string {
			out := make([]string, len(xs))
			for j, x := range xs {
				out[j] = ix.chunks[x.i].ID
			}
			return out
		}
		pos := make(map[string]int, n)
		for i, c := range ix.chunks {
			pos[c.ID] = i
		}
		for _, f := range FuseRRF(ix.rrfK, ids(dRank), ids(bRank)) {
			final = append(final, scored{pos[f.ID], f.Score})
		}
	}
	if len(final) > k {
		final = final[:k]
	}
	hits := make([]Hit, len(final))
	for j, f := range final {
		hits[j] = Hit{Chunk: ix.chunks[f.i], Rank: j + 1, Score: f.s, Dense: dense[f.i], BM25: bm[f.i]}
	}
	return hits, nil
}

// Fused is one entry of a fused ranking.
type Fused struct {
	ID    string
	Score float64
}

// FuseRRF merges rankings (each a list of IDs, best first) with reciprocal
// rank fusion: an item's score is the sum over rankings of 1/(k + rank),
// with rank starting at 1. It uses only positions, never raw scores, so it
// can combine scorers whose scales are unrelated, like cosine and BM25. An
// item missing from a ranking gets nothing from it. Ties keep the order in
// which items were first seen.
func FuseRRF(k float64, rankings ...[]string) []Fused {
	score := map[string]float64{}
	var order []string
	for _, r := range rankings {
		for i, id := range r {
			if _, ok := score[id]; !ok {
				order = append(order, id)
			}
			score[id] += 1 / (k + float64(i+1))
		}
	}
	out := make([]Fused, len(order))
	for i, id := range order {
		out[i] = Fused{id, score[id]}
	}
	sort.SliceStable(out, func(a, b int) bool { return out[a].Score > out[b].Score })
	return out
}
