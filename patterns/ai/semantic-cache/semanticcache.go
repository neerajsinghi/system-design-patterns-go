// Package semanticcache implements a semantic cache for LLM responses: a
// cache looked up by meaning instead of by exact prompt text.
//
// Users ask the same question in many ways: "how do I reset my password",
// "forgot password, how to change it", "pwd reset pls". A cache keyed by the
// prompt string almost never hits, so each paraphrase pays for a full model
// call, in money and in seconds of latency. A semantic cache embeds each
// prompt into a vector, and on lookup returns the stored answer whose prompt
// vector is most similar (by cosine similarity) to the new one, provided the
// similarity is at least a threshold.
//
// The threshold is the whole game. Too high and paraphrases miss; too low and
// "enable two-factor auth" gets the cached answer for "disable two-factor
// auth". So this package also counts false hits when the caller supplies a
// ground-truth label, which is how a threshold should be tuned.
//
// The rest is what any shared cache needs, plus one rule specific to LLM
// answers: entries are isolated per tenant (one customer's answer is never
// returned to another), expire after a TTL, are evicted least-recently-used
// beyond a maximum count, and answers that depend on user-specific context
// can be marked not cacheable. An exact-match tier is checked first: it is a
// map lookup and needs no similarity search.
//
// Lookup is a brute-force scan over the tenant's entries, which is fine for
// thousands of entries. Beyond that, use an approximate nearest-neighbour
// index (see ../vector-ann-index).
package semanticcache

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"
)

// Source says how a lookup was answered.
type Source int

const (
	Miss     Source = iota // not found: the caller must ask the model
	Exact                  // the exact-match tier found the same (normalised) prompt
	Semantic               // a different prompt above the similarity threshold
)

func (s Source) String() string {
	switch s {
	case Exact:
		return "exact"
	case Semantic:
		return "semantic"
	default:
		return "miss"
	}
}

// Query is a lookup. Label is optional ground truth (for example the intent
// a test set says the prompt has); when both the query and the matched entry
// carry a label and they differ, the hit is counted as a false hit.
type Query struct {
	Tenant, Prompt, Label string
}

// Entry is a cached answer.
type Entry struct {
	Tenant, Prompt, Response string
	Label                    string        // optional ground truth, see Query
	TTL                      time.Duration // 0 uses the cache's default TTL
}

// Result is the outcome of a lookup.
type Result struct {
	Response      string
	Source        Source
	Similarity    float32 // 1 for exact hits
	MatchedPrompt string  // the cached prompt whose answer was returned
	Label         string  // the matched entry's label
}

// Stats counts how lookups were answered.
type Stats struct {
	ExactHits, SemanticHits, Misses uint64
	// FalseHits counts hits whose entry label differed from the query label.
	FalseHits uint64
	// Stored counts entries written; NotCacheable counts answers that
	// GetOrCompute deliberately did not store.
	Stored, NotCacheable uint64
	Evicted, Expired     uint64
}

// HitRate is (exact + semantic hits) / lookups.
func (s Stats) HitRate() float64 {
	total := s.ExactHits + s.SemanticHits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.ExactHits+s.SemanticHits) / float64(total)
}

type tenantKey struct{ tenant, key string }

type entry struct {
	tenant, prompt, key, response, label string
	vec                                  []float32
	expires                              time.Time
	elem                                 *list.Element // position in the LRU list
	idx                                  int           // position in its tenant's slice
}

// Cache is a concurrency-safe semantic cache.
type Cache struct {
	embed      Embedder
	threshold  float32
	ttl        time.Duration
	maxEntries int
	exactKey   func(string) string
	now        func() time.Time

	mu      sync.Mutex
	dim     int
	exact   map[tenantKey]*entry
	tenants map[string][]*entry // brute-force search space, per tenant
	lru     *list.List          // front = most recently used
	stats   Stats
}

// Option configures a Cache.
type Option func(*Cache)

// WithClock injects a clock, which makes TTL behaviour testable.
func WithClock(now func() time.Time) Option {
	return func(c *Cache) {
		if now != nil {
			c.now = now
		}
	}
}

// WithTTL sets the default time an entry lives (default 24h).
func WithTTL(d time.Duration) Option { return func(c *Cache) { c.ttl = d } }

// WithMaxEntries bounds the number of entries across all tenants (default
// 10,000). Beyond it the least recently used entry is evicted.
func WithMaxEntries(n int) Option { return func(c *Cache) { c.maxEntries = n } }

// WithExactKey sets how prompts are normalised for the exact-match tier,
// for example HashEmbedder.Normalize. The default uses the prompt as is.
func WithExactKey(fn func(string) string) Option {
	return func(c *Cache) {
		if fn != nil {
			c.exactKey = fn
		}
	}
}

// New returns a cache that embeds prompts with embed and serves a stored
// answer when cosine similarity is at least threshold (0 < threshold <= 1).
func New(embed Embedder, threshold float32, opts ...Option) (*Cache, error) {
	if embed == nil {
		return nil, errors.New("semanticcache: embedder must not be nil")
	}
	if !(threshold > 0 && threshold <= 1) {
		return nil, errors.New("semanticcache: threshold must be in (0, 1]")
	}
	c := &Cache{
		embed: embed, threshold: threshold, ttl: 24 * time.Hour, maxEntries: 10_000,
		exactKey: func(s string) string { return s }, now: time.Now,
		exact: map[tenantKey]*entry{}, tenants: map[string][]*entry{}, lru: list.New(),
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.ttl <= 0 {
		return nil, errors.New("semanticcache: ttl must be positive")
	}
	if c.maxEntries <= 0 {
		return nil, errors.New("semanticcache: max entries must be positive")
	}
	return c, nil
}

// Lookup returns the cached answer for q, if any: the exact tier first, then
// the most similar entry of the same tenant at or above the threshold.
func (c *Cache) Lookup(q Query) (Result, bool) {
	// Embed outside the lock: with a real model this is a network call.
	vec := c.vector(q.Prompt)
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lookupLocked(q, vec)
}

func (c *Cache) lookupLocked(q Query, vec []float32) (Result, bool) {
	now := c.now()
	if key := c.exactKey(q.Prompt); key == "" {
		// Nothing left after normalisation: no exact key to look up.
	} else if e, ok := c.exact[tenantKey{q.Tenant, key}]; ok {
		if now.Before(e.expires) {
			return c.hit(q, e, Exact, 1), true
		}
		c.remove(e)
		c.stats.Expired++
	}
	if vec == nil || len(vec) != c.dim {
		c.stats.Misses++
		return Result{}, false
	}
	var best *entry
	bestSim := float32(-2)
	var expired []*entry
	for _, e := range c.tenants[q.Tenant] {
		if !now.Before(e.expires) {
			expired = append(expired, e)
			continue
		}
		if s := dot(vec, e.vec); s > bestSim {
			best, bestSim = e, s
		}
	}
	for _, e := range expired {
		c.remove(e)
		c.stats.Expired++
	}
	if best == nil || bestSim < c.threshold {
		c.stats.Misses++
		return Result{}, false
	}
	return c.hit(q, best, Semantic, bestSim), true
}

func (c *Cache) hit(q Query, e *entry, src Source, sim float32) Result {
	if src == Exact {
		c.stats.ExactHits++
	} else {
		c.stats.SemanticHits++
	}
	if q.Label != "" && e.label != "" && q.Label != e.label {
		c.stats.FalseHits++
	}
	c.lru.MoveToFront(e.elem)
	return Result{Response: e.response, Source: src, Similarity: sim, MatchedPrompt: e.prompt, Label: e.label}
}

// ErrEmptyPrompt is returned by Put for a prompt with neither an exact key
// nor a non-zero embedding: nothing could ever look it up.
var ErrEmptyPrompt = errors.New("semanticcache: prompt is empty after normalisation")

// Put stores an answer. A prompt that normalises to an existing entry's exact
// key, for the same tenant, replaces it. A prompt with an empty embedding
// (nothing left after normalisation) is only reachable through the exact tier.
func (c *Cache) Put(e Entry) error {
	if e.Tenant == "" {
		return errors.New("semanticcache: tenant must not be empty")
	}
	if e.TTL < 0 {
		return errors.New("semanticcache: entry ttl must not be negative")
	}
	vec := c.vector(e.Prompt)
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.putLocked(e, vec)
}

func (c *Cache) putLocked(e Entry, vec []float32) error {
	if vec != nil {
		if c.dim == 0 {
			c.dim = len(vec)
		} else if len(vec) != c.dim {
			return errors.New("semanticcache: embedder returned a vector of a different length")
		}
	}
	ttl := e.TTL
	if ttl == 0 {
		ttl = c.ttl
	}
	key := c.exactKey(e.Prompt)
	if key == "" && vec == nil {
		return ErrEmptyPrompt
	}
	if old, ok := c.exact[tenantKey{e.Tenant, key}]; ok {
		c.remove(old)
	}
	n := &entry{
		tenant: e.Tenant, prompt: e.Prompt, key: key, response: e.Response, label: e.Label,
		vec: vec, expires: c.now().Add(ttl),
	}
	n.elem = c.lru.PushFront(n)
	if key != "" {
		c.exact[tenantKey{e.Tenant, key}] = n
	}
	if vec != nil {
		n.idx = len(c.tenants[e.Tenant])
		c.tenants[e.Tenant] = append(c.tenants[e.Tenant], n)
	} else {
		n.idx = -1
	}
	c.stats.Stored++
	for c.lru.Len() > c.maxEntries {
		c.remove(c.lru.Back().Value.(*entry))
		c.stats.Evicted++
	}
	return nil
}

// remove deletes e from every index. Caller holds mu.
func (c *Cache) remove(e *entry) {
	c.lru.Remove(e.elem)
	if e.key != "" {
		delete(c.exact, tenantKey{e.tenant, e.key})
	}
	if e.idx < 0 {
		return
	}
	s := c.tenants[e.tenant]
	last := s[len(s)-1]
	s[e.idx], last.idx = last, e.idx
	s[len(s)-1] = nil
	s = s[:len(s)-1]
	if len(s) == 0 {
		delete(c.tenants, e.tenant)
	} else {
		c.tenants[e.tenant] = s
	}
}

// Compute asks the model. cacheable is false when the answer depends on
// user-specific context ("where is MY order") and must not be reused.
type Compute func(ctx context.Context, prompt string) (response string, cacheable bool, err error)

// GetOrCompute returns a cached answer for q or calls compute and caches its
// answer if it is cacheable. Concurrent misses for similar prompts each call
// compute; there is no request coalescing.
func (c *Cache) GetOrCompute(ctx context.Context, q Query, compute Compute) (Result, error) {
	if q.Tenant == "" {
		return Result{}, errors.New("semanticcache: tenant must not be empty")
	}
	if compute == nil {
		return Result{}, errors.New("semanticcache: compute must not be nil")
	}
	vec := c.vector(q.Prompt)
	c.mu.Lock()
	r, ok := c.lookupLocked(q, vec)
	c.mu.Unlock()
	if ok {
		return r, nil
	}
	resp, cacheable, err := compute(ctx, q.Prompt)
	if err != nil {
		return Result{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !cacheable {
		c.stats.NotCacheable++
	} else if err := c.putLocked(Entry{Tenant: q.Tenant, Prompt: q.Prompt, Response: resp, Label: q.Label}, vec); errors.Is(err, ErrEmptyPrompt) {
		c.stats.NotCacheable++
	} else if err != nil {
		return Result{}, err
	}
	return Result{Response: resp, Source: Miss}, nil
}

// vector embeds and normalises prompt; nil if the embedding is empty or zero.
func (c *Cache) vector(prompt string) []float32 {
	v := c.embed(prompt)
	if len(v) == 0 {
		return nil
	}
	v = append([]float32(nil), v...) // never keep the embedder's slice
	if !normalize(v) {
		return nil
	}
	return v
}

// Len returns the number of entries across all tenants.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lru.Len()
}

// Stats returns a snapshot of the counters.
func (c *Cache) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}
