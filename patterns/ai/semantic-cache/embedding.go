package semanticcache

import (
	"errors"
	"math"
	"strings"
	"unicode"
)

// Embedder turns text into a vector. Texts that mean the same thing should
// get vectors pointing in roughly the same direction. The cache normalises
// whatever it is given, so an Embedder need not return unit vectors, but it
// must always return vectors of the same length.
type Embedder func(text string) []float32

// HashEmbedder is a deterministic, dependency-free toy embedding: a hashed
// bag of words plus character n-grams. It stands in for a real embedding
// model (OpenAI text-embedding-3, Cohere embed, a local sentence-transformer)
// so the cache can be demonstrated and tested with no model and no network.
//
// It only knows about surface forms: "reset" and "recover" are unrelated
// unless a synonym map says otherwise, and "enable" and "disable" look alike
// because they share most of their letters. A real model knows far more, but
// it fails in the same direction, which is why thresholds need tuning.
type HashEmbedder struct {
	dim         int
	ngram       int
	ngramWeight float32
	stopwords   map[string]bool
	synonyms    map[string]string
}

// EmbedOption configures a HashEmbedder.
type EmbedOption func(*HashEmbedder)

// WithStopwords drops these words (after lower-casing) before hashing.
func WithStopwords(words ...string) EmbedOption {
	return func(h *HashEmbedder) {
		for _, w := range words {
			h.stopwords[strings.ToLower(w)] = true
		}
	}
}

// WithSynonyms rewrites single words before hashing, e.g. "pwd" -> "password".
func WithSynonyms(m map[string]string) EmbedOption {
	return func(h *HashEmbedder) {
		for k, v := range m {
			h.synonyms[strings.ToLower(k)] = strings.ToLower(v)
		}
	}
}

// WithCharNGrams sets the character n-gram length (0 disables n-grams) and
// the weight of each n-gram relative to a whole word (weight 1). The default
// is trigrams at weight 0.5, which makes "password"/"passwords" or
// "cancel"/"cancelling" close without a stemmer.
func WithCharNGrams(n int, weight float32) EmbedOption {
	return func(h *HashEmbedder) { h.ngram, h.ngramWeight = n, weight }
}

// NewHashEmbedder returns an embedder producing dim-dimensional vectors.
func NewHashEmbedder(dim int, opts ...EmbedOption) (*HashEmbedder, error) {
	if dim < 16 {
		return nil, errors.New("semanticcache: embedding dimension must be at least 16")
	}
	h := &HashEmbedder{
		dim: dim, ngram: 3, ngramWeight: 0.5,
		stopwords: map[string]bool{}, synonyms: map[string]string{},
	}
	for _, opt := range opts {
		opt(h)
	}
	if h.ngram < 0 || h.ngram > 8 {
		return nil, errors.New("semanticcache: n-gram length must be between 0 and 8")
	}
	if h.ngramWeight < 0 {
		return nil, errors.New("semanticcache: n-gram weight must not be negative")
	}
	return h, nil
}

// Tokens returns the normalised words the embedding is built from:
// lower-cased, punctuation removed (apostrophes are dropped so "what's"
// becomes "whats"), synonyms applied, stopwords removed.
func (h *HashEmbedder) Tokens(text string) []string {
	var b strings.Builder
	for _, r := range strings.ToLower(text) {
		switch {
		case r == '\'' || r == '’':
			// "can't" -> "cant"
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			b.WriteRune(r)
		default:
			b.WriteByte(' ')
		}
	}
	var out []string
	for _, w := range strings.Fields(b.String()) {
		if s, ok := h.synonyms[w]; ok {
			w = s
		}
		if !h.stopwords[w] {
			out = append(out, w)
		}
	}
	return out
}

// Normalize returns the tokens joined by single spaces. It is a good exact-
// tier key: "How do I reset my password?" and "how do i reset my password"
// map to the same string.
func (h *HashEmbedder) Normalize(text string) string {
	return strings.Join(h.Tokens(text), " ")
}

// Embed returns the L2-normalised embedding of text. Text with no tokens left
// after normalisation returns the zero vector.
func (h *HashEmbedder) Embed(text string) []float32 {
	v := make([]float32, h.dim)
	for _, w := range h.Tokens(text) {
		h.add(v, "w:"+w, 1)
		if h.ngram == 0 || h.ngramWeight == 0 {
			continue
		}
		padded := []rune("<" + w + ">")
		for i := 0; i+h.ngram <= len(padded); i++ {
			h.add(v, "c:"+string(padded[i:i+h.ngram]), h.ngramWeight)
		}
	}
	normalize(v)
	return v
}

// add hashes a feature into a bucket with a hash-derived sign, the "hashing
// trick": the signs make collisions cancel out on average instead of always
// inflating similarity.
func (h *HashEmbedder) add(v []float32, feature string, weight float32) {
	x := fnv1a(feature)
	i := int(x % uint64(h.dim))
	if x>>63 == 1 {
		weight = -weight
	}
	v[i] += weight
}

func fnv1a(s string) uint64 {
	x := uint64(14695981039346656037)
	for i := 0; i < len(s); i++ {
		x ^= uint64(s[i])
		x *= 1099511628211
	}
	return x
}

// normalize scales v to unit length in place and reports whether it was
// non-zero.
func normalize(v []float32) bool {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	if sum == 0 {
		return false
	}
	inv := float32(1 / math.Sqrt(sum))
	for i := range v {
		v[i] *= inv
	}
	return true
}

// Cosine returns the cosine similarity of a and b (0 if either is zero or
// the lengths differ).
func Cosine(a, b []float32) float32 {
	if len(a) != len(b) {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return float32(dot / math.Sqrt(na*nb))
}

// dot is the cosine of two unit vectors.
func dot(a, b []float32) float32 {
	var s float32
	for i := range a {
		s += a[i] * b[i]
	}
	return s
}
