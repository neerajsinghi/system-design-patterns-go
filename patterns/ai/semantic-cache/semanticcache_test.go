package semanticcache

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (f *fakeClock) Now() time.Time          { f.mu.Lock(); defer f.mu.Unlock(); return f.t }
func (f *fakeClock) Advance(d time.Duration) { f.mu.Lock(); f.t = f.t.Add(d); f.mu.Unlock() }

func newEmbedder(t *testing.T) *HashEmbedder {
	t.Helper()
	e, err := NewHashEmbedder(256,
		WithStopwords("i", "my", "the", "a", "to", "please"),
		WithSynonyms(map[string]string{"pwd": "password"}))
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func newCache(t *testing.T, threshold float32, opts ...Option) (*Cache, *fakeClock) {
	t.Helper()
	clk := &fakeClock{t: time.Unix(0, 0)}
	c, err := New(newEmbedder(t).Embed, threshold, append([]Option{WithClock(clk.Now)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return c, clk
}

func put(t *testing.T, c *Cache, tenant, prompt, resp string) {
	t.Helper()
	if err := c.Put(Entry{Tenant: tenant, Prompt: prompt, Response: resp}); err != nil {
		t.Fatal(err)
	}
}

func TestNewValidation(t *testing.T) {
	emb := newEmbedder(t).Embed
	tests := []struct {
		name      string
		embed     Embedder
		threshold float32
		opts      []Option
	}{
		{"nil embedder", nil, 0.9, nil},
		{"zero threshold", emb, 0, nil},
		{"threshold above 1", emb, 1.1, nil},
		{"negative threshold", emb, -0.5, nil},
		{"zero ttl", emb, 0.9, []Option{WithTTL(0)}},
		{"zero max entries", emb, 0.9, []Option{WithMaxEntries(0)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.embed, tt.threshold, tt.opts...); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
	if _, err := New(emb, 1); err != nil {
		t.Fatalf("threshold 1 should be valid: %v", err)
	}
}

func TestEmbedderValidation(t *testing.T) {
	for name, fn := range map[string]func() (*HashEmbedder, error){
		"small dim":       func() (*HashEmbedder, error) { return NewHashEmbedder(8) },
		"negative n-gram": func() (*HashEmbedder, error) { return NewHashEmbedder(64, WithCharNGrams(-1, 1)) },
		"huge n-gram":     func() (*HashEmbedder, error) { return NewHashEmbedder(64, WithCharNGrams(9, 1)) },
		"negative weight": func() (*HashEmbedder, error) { return NewHashEmbedder(64, WithCharNGrams(3, -1)) },
	} {
		if _, err := fn(); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestPutValidation(t *testing.T) {
	c, _ := newCache(t, 0.9)
	if err := c.Put(Entry{Prompt: "x", Response: "y"}); err == nil {
		t.Error("empty tenant should be rejected")
	}
	if err := c.Put(Entry{Tenant: "t", Prompt: "x", TTL: -time.Second}); err == nil {
		t.Error("negative ttl should be rejected")
	}
	if _, err := c.GetOrCompute(context.Background(), Query{Prompt: "x"}, nil); err == nil {
		t.Error("empty tenant should be rejected")
	}
	if _, err := c.GetOrCompute(context.Background(), Query{Tenant: "t", Prompt: "x"}, nil); err == nil {
		t.Error("nil compute should be rejected")
	}
	var dim = 16
	odd, _ := New(func(string) []float32 { dim++; v := make([]float32, dim); v[0] = 1; return v }, 0.9)
	_ = odd.Put(Entry{Tenant: "t", Prompt: "a"})
	if err := odd.Put(Entry{Tenant: "t", Prompt: "b"}); err == nil {
		t.Error("changing vector length should be rejected")
	}
}

func TestEmbeddingIsDeterministicAndMeaningful(t *testing.T) {
	e := newEmbedder(t)
	a, b := e.Embed("How do I reset my password?"), e.Embed("How do I reset my password?")
	if !slices.Equal(a, b) {
		t.Fatal("same text gave different vectors")
	}
	e2 := newEmbedder(t) // a fresh embedder must agree: no per-process randomness
	if !slices.Equal(a, e2.Embed("How do I reset my password?")) {
		t.Fatal("two embedders disagree")
	}
	if s := Cosine(a, a); s < 0.9999 {
		t.Fatalf("self-similarity %v, want 1", s)
	}
	if s := Cosine(a, e.Embed("how do i reset my pwd")); s < 0.9999 {
		t.Fatalf("case, punctuation, stopwords and synonyms should not matter: %v", s)
	}
	similar := Cosine(a, e.Embed("how can I reset the password"))
	dissimilar := Cosine(a, e.Embed("what are your store opening hours"))
	if similar <= dissimilar {
		t.Fatalf("paraphrase %v should beat an unrelated question %v", similar, dissimilar)
	}
	if similar < 0.7 || dissimilar > 0.3 {
		t.Fatalf("unexpected similarities: paraphrase %v, unrelated %v", similar, dissimilar)
	}
	zero := e.Embed("the a my")
	for _, x := range zero {
		if x != 0 {
			t.Fatal("all-stopword text should embed to the zero vector")
		}
	}
	if got := e.Normalize("Hi, I can't RESET my pwd!"); got != "hi cant reset password" {
		t.Fatalf("Normalize = %q", got)
	}
}

func TestThreshold(t *testing.T) {
	e := newEmbedder(t)
	stored := "how do I reset my password"
	paraphrase := "how can I reset the password"
	sim := Cosine(e.Embed(stored), e.Embed(paraphrase))
	tests := []struct {
		name      string
		threshold float32
		want      bool
	}{
		{"well below similarity", sim - 0.1, true},
		{"just below similarity", sim - 0.001, true},
		{"just above similarity", sim + 0.001, false},
		{"exact only", 1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := newCache(t, tt.threshold)
			put(t, c, "t", stored, "use the forgot-password link")
			r, ok := c.Lookup(Query{Tenant: "t", Prompt: paraphrase})
			if ok != tt.want {
				t.Fatalf("threshold %v, similarity %v: hit=%v, want %v", tt.threshold, sim, ok, tt.want)
			}
			if ok && (r.Source != Semantic || r.MatchedPrompt != stored || r.Response != "use the forgot-password link") {
				t.Fatalf("unexpected result %+v", r)
			}
		})
	}
}

func TestNearestNeighbourWins(t *testing.T) {
	c, _ := newCache(t, 0.3)
	put(t, c, "t", "how do I reset my password", "reset")
	put(t, c, "t", "how do I change my email address", "email")
	put(t, c, "t", "what are your store opening hours", "hours")
	r, ok := c.Lookup(Query{Tenant: "t", Prompt: "when are store opening hours on sunday"})
	if !ok || r.Response != "hours" {
		t.Fatalf("got %+v, %v; want the hours answer", r, ok)
	}
}

func TestFalseHitsCounted(t *testing.T) {
	c, _ := newCache(t, 0.5)
	_ = c.Put(Entry{Tenant: "t", Prompt: "how do I turn off two factor auth", Response: "off", Label: "disable_2fa"})
	if _, ok := c.Lookup(Query{Tenant: "t", Prompt: "how do I turn on two factor auth", Label: "enable_2fa"}); !ok {
		t.Fatal("expected a (wrong) hit at a low threshold")
	}
	if _, ok := c.Lookup(Query{Tenant: "t", Prompt: "turn off two factor auth", Label: "disable_2fa"}); !ok {
		t.Fatal("expected a correct hit")
	}
	_, _ = c.Lookup(Query{Tenant: "t", Prompt: "turn off two factor auth"}) // unlabelled: not judged
	s := c.Stats()
	if s.SemanticHits != 3 || s.FalseHits != 1 {
		t.Fatalf("stats %+v, want 3 semantic hits and 1 false hit", s)
	}
}

func TestTenantIsolation(t *testing.T) {
	c, _ := newCache(t, 0.5)
	put(t, c, "acme", "how do I reset my password", "acme answer")
	put(t, c, "globex", "how do I export my data", "globex answer")
	if r, ok := c.Lookup(Query{Tenant: "globex", Prompt: "how do I reset my password"}); ok {
		t.Fatalf("globex got another tenant's answer: %+v", r)
	}
	if r, ok := c.Lookup(Query{Tenant: "acme", Prompt: "how do I reset my password"}); !ok || r.Response != "acme answer" || r.Source != Exact {
		t.Fatalf("acme lookup = %+v, %v", r, ok)
	}
	if _, ok := c.Lookup(Query{Tenant: "initech", Prompt: "how do I reset my password"}); ok {
		t.Fatal("unknown tenant should miss")
	}
	// Same prompt, different tenants: two separate entries.
	put(t, c, "globex", "how do I reset my password", "globex reset")
	if r, _ := c.Lookup(Query{Tenant: "acme", Prompt: "how do I reset my password"}); r.Response != "acme answer" {
		t.Fatalf("globex's put overwrote acme's entry: %+v", r)
	}
}

func TestTTL(t *testing.T) {
	c, clk := newCache(t, 0.8, WithTTL(time.Hour))
	put(t, c, "t", "how do I reset my password", "default ttl")
	_ = c.Put(Entry{Tenant: "t", Prompt: "what are your store opening hours", Response: "short", TTL: time.Minute})

	clk.Advance(59 * time.Second)
	if _, ok := c.Lookup(Query{Tenant: "t", Prompt: "what are your store opening hours"}); !ok {
		t.Fatal("entry expired early")
	}
	clk.Advance(time.Second)
	if _, ok := c.Lookup(Query{Tenant: "t", Prompt: "what are your store opening hours"}); ok {
		t.Fatal("per-entry TTL ignored")
	}
	if _, ok := c.Lookup(Query{Tenant: "t", Prompt: "how can I reset the password"}); !ok {
		t.Fatal("default-TTL entry should still be served semantically")
	}
	clk.Advance(time.Hour)
	if _, ok := c.Lookup(Query{Tenant: "t", Prompt: "how can I reset the password"}); ok {
		t.Fatal("expired entry served by the semantic tier")
	}
	if c.Len() != 0 {
		t.Fatalf("expired entries not removed: %d left", c.Len())
	}
	if s := c.Stats(); s.Expired != 2 {
		t.Fatalf("expired = %d, want 2", s.Expired)
	}
}

func TestLRUEviction(t *testing.T) {
	c, _ := newCache(t, 0.99, WithMaxEntries(2))
	put(t, c, "a", "how do I reset my password", "1")
	put(t, c, "b", "what are your store opening hours", "2")
	if _, ok := c.Lookup(Query{Tenant: "a", Prompt: "how do I reset my password"}); !ok { // "1" is now most recent
		t.Fatal("miss")
	}
	put(t, c, "a", "how do I export my data", "3") // evicts "2", the least recently used
	if _, ok := c.Lookup(Query{Tenant: "b", Prompt: "what are your store opening hours"}); ok {
		t.Fatal("least recently used entry not evicted")
	}
	for _, p := range []string{"how do I reset my password", "how do I export my data"} {
		if _, ok := c.Lookup(Query{Tenant: "a", Prompt: p}); !ok {
			t.Fatalf("%q evicted", p)
		}
	}
	if s := c.Stats(); s.Evicted != 1 || c.Len() != 2 {
		t.Fatalf("evicted %d, len %d", s.Evicted, c.Len())
	}
}

func TestExactTier(t *testing.T) {
	e := newEmbedder(t)
	c, err := New(e.Embed, 0.99, WithExactKey(e.Normalize))
	if err != nil {
		t.Fatal(err)
	}
	put(t, c, "t", "How do I reset my password?", "v1")
	r, ok := c.Lookup(Query{Tenant: "t", Prompt: "how do i reset my pwd"})
	if !ok || r.Source != Exact || r.Similarity != 1 {
		t.Fatalf("want an exact hit on the normalised key, got %+v %v", r, ok)
	}
	// A prompt with the same exact key replaces the entry instead of adding one.
	put(t, c, "t", "how do I reset my password", "v2")
	if r, _ := c.Lookup(Query{Tenant: "t", Prompt: "How do I reset my password?"}); r.Response != "v2" || c.Len() != 1 {
		t.Fatalf("got %q with %d entries, want v2 and 1", r.Response, c.Len())
	}
	// An all-stopword prompt has no embedding and here no exact key either:
	// storing it would be pointless, and it must not match anything.
	if err := c.Put(Entry{Tenant: "t", Prompt: "the a my", Response: "filler"}); !errors.Is(err, ErrEmptyPrompt) {
		t.Fatalf("Put(all stopwords) = %v, want ErrEmptyPrompt", err)
	}
	if _, ok := c.Lookup(Query{Tenant: "t", Prompt: "my"}); ok {
		t.Fatal("empty prompt must not match")
	}
	// With the default (identity) exact key it has a key, so it is stored and
	// reachable only by the same text.
	c2, _ := newCache(t, 0.5)
	put(t, c2, "t", "the a my", "filler")
	if r, ok := c2.Lookup(Query{Tenant: "t", Prompt: "the a my"}); !ok || r.Source != Exact {
		t.Fatalf("zero-vector prompt: %+v %v", r, ok)
	}
	if _, ok := c2.Lookup(Query{Tenant: "t", Prompt: "my"}); ok {
		t.Fatal("zero vector must not match semantically")
	}
	if s := c.Stats(); s.ExactHits != 2 || s.SemanticHits != 0 {
		t.Fatalf("stats %+v", s)
	}
}

func TestGetOrComputeAndCacheable(t *testing.T) {
	c, _ := newCache(t, 0.8)
	calls := 0
	compute := func(_ context.Context, p string) (string, bool, error) {
		calls++
		if p == "where is my order" {
			return "your order #42 ships today", false, nil
		}
		return "answer: " + p, true, nil
	}
	ctx := context.Background()
	for range 2 {
		r, err := c.GetOrCompute(ctx, Query{Tenant: "t", Prompt: "where is my order"}, compute)
		if err != nil || r.Source != Miss {
			t.Fatalf("user-specific answer should never come from cache: %+v %v", r, err)
		}
	}
	r1, _ := c.GetOrCompute(ctx, Query{Tenant: "t", Prompt: "how do I reset my password"}, compute)
	r2, _ := c.GetOrCompute(ctx, Query{Tenant: "t", Prompt: "how do I reset my pwd please"}, compute)
	if r1.Source != Miss || r2.Source == Miss || r2.Response != r1.Response {
		t.Fatalf("second phrasing should hit: %+v / %+v", r1, r2)
	}
	if calls != 3 {
		t.Fatalf("compute called %d times, want 3", calls)
	}
	if s := c.Stats(); s.NotCacheable != 2 || s.Stored != 1 {
		t.Fatalf("stats %+v", s)
	}
	boom := errors.New("model down")
	_, err := c.GetOrCompute(ctx, Query{Tenant: "t", Prompt: "export data"},
		func(context.Context, string) (string, bool, error) { return "", true, boom })
	if !errors.Is(err, boom) || c.Len() != 1 {
		t.Fatalf("errors must propagate and not be cached: %v, len %d", err, c.Len())
	}
}

func TestConcurrentUse(t *testing.T) {
	c, clk := newCache(t, 0.8, WithMaxEntries(50), WithTTL(time.Minute))
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tenant := fmt.Sprintf("tenant%d", g%3)
			for i := range 300 {
				p := fmt.Sprintf("how do I reset password number %d", i%40)
				r, err := c.GetOrCompute(context.Background(), Query{Tenant: tenant, Prompt: p},
					func(_ context.Context, p string) (string, bool, error) { return tenant + ":" + p, i%7 != 0, nil })
				if err != nil {
					t.Error(err)
					return
				}
				if r.Response[:len(tenant)] != tenant {
					t.Errorf("cross-tenant answer %q for %s", r.Response, tenant)
					return
				}
				if i%50 == 0 {
					clk.Advance(10 * time.Second)
				}
			}
		}()
	}
	wg.Wait()
	if n := c.Len(); n > 50 {
		t.Fatalf("len %d exceeds max entries", n)
	}
	s := c.Stats()
	if s.ExactHits+s.SemanticHits+s.Misses != 8*300 {
		t.Fatalf("lookups not all counted: %+v", s)
	}
}
