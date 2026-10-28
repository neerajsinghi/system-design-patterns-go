// Package vectorannindex implements approximate nearest neighbour (ANN)
// search over embedding vectors with an inverted-file (IVF) index, next to
// the exact brute-force (flat) index it approximates.
//
// Semantic search, RAG retrieval and recommendation all reduce to "find the k
// stored vectors most similar to this query vector". A flat index answers it
// exactly by comparing the query with every stored vector: O(N·d) work per
// query, so latency grows linearly with the corpus. At millions of vectors
// that is too slow for interactive search.
//
// IVF trades a little recall for a lot less work. It first runs k-means on a
// sample of the data to find nlist centroids (the coarse quantiser), then
// files every vector in the inverted list of its nearest centroid. A query
// compares itself with the nlist centroids, picks the nprobe nearest, and
// exact-scores only the members of those lists. nprobe is the dial: nprobe=1
// scans roughly N/nlist vectors and may miss neighbours that sit just across
// a cell boundary; nprobe=nlist scans everything and is exactly the flat
// answer.
//
// The centroids are only as good as the data they were trained on. Vectors
// added later are filed under the nearest existing centroid, so if new data
// drifts away from the training distribution the lists become a poor
// partition of it and recall drops; Retrain re-clusters everything stored.
package vectorannindex

import (
	"container/heap"
	"errors"
	"math"
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"
)

// Metric is the similarity used to rank vectors.
type Metric int

const (
	// Cosine ranks by cosine similarity. Vectors are normalised to unit
	// length when added, so scoring is a dot product. Score is in [-1, 1].
	Cosine Metric = iota
	// L2 ranks by Euclidean distance. Score is the negated squared
	// distance, so higher is still better.
	L2
)

func (m Metric) String() string {
	switch m {
	case Cosine:
		return "cosine"
	case L2:
		return "l2"
	}
	return "unknown"
}

// Errors returned by the indexes.
var (
	ErrDimension     = errors.New("vectorannindex: vector has the wrong dimension")
	ErrZeroVector    = errors.New("vectorannindex: zero vector has no direction under cosine")
	ErrNotFinite     = errors.New("vectorannindex: vector contains NaN or Inf")
	ErrDuplicateID   = errors.New("vectorannindex: id already present")
	ErrNotTrained    = errors.New("vectorannindex: index is not trained")
	ErrBadK          = errors.New("vectorannindex: k must be positive")
	ErrBadNProbe     = errors.New("vectorannindex: nprobe must be positive")
	ErrTooFewToTrain = errors.New("vectorannindex: need at least nlist training vectors")
)

// Result is one search hit. Higher Score is more similar.
type Result struct {
	ID    int
	Score float64
}

// better reports whether a outranks b: higher score, ties broken by lower ID
// so results are deterministic.
func better(a, b Result) bool {
	if a.Score != b.Score {
		return a.Score > b.Score
	}
	return a.ID < b.ID
}

// topK keeps the k best results seen so far in a min-heap whose root is the
// worst of them, so each candidate costs O(log k) at most.
type topK struct {
	k int
	h resultHeap
}

type resultHeap []Result

func (h resultHeap) Len() int           { return len(h) }
func (h resultHeap) Less(i, j int) bool { return better(h[j], h[i]) } // worst at the root
func (h resultHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *resultHeap) Push(x any)        { *h = append(*h, x.(Result)) }
func (h *resultHeap) Pop() any {
	old := *h
	r := old[len(old)-1]
	*h = old[:len(old)-1]
	return r
}

func newTopK(k int) *topK { return &topK{k: k, h: make(resultHeap, 0, k)} }

func (t *topK) offer(r Result) {
	if len(t.h) < t.k {
		heap.Push(&t.h, r)
		return
	}
	if better(r, t.h[0]) {
		t.h[0] = r
		heap.Fix(&t.h, 0)
	}
}

// sorted returns the kept results best first.
func (t *topK) sorted() []Result {
	out := make([]Result, len(t.h))
	copy(out, t.h)
	sort.Slice(out, func(i, j int) bool { return better(out[i], out[j]) })
	return out
}

// dot and sqDist accumulate in float32 with four independent sums, which is
// several times faster than a single float64 accumulator and plenty precise
// for embedding dimensions in the hundreds. Both require len(b) >= len(a).
func dot(a, b []float32) float64 {
	b = b[:len(a)]
	var s0, s1, s2, s3 float32
	i := 0
	for ; i+4 <= len(a); i += 4 {
		s0 += a[i] * b[i]
		s1 += a[i+1] * b[i+1]
		s2 += a[i+2] * b[i+2]
		s3 += a[i+3] * b[i+3]
	}
	for ; i < len(a); i++ {
		s0 += a[i] * b[i]
	}
	return float64((s0 + s1) + (s2 + s3))
}

func sqDist(a, b []float32) float64 {
	b = b[:len(a)]
	var s0, s1, s2, s3 float32
	i := 0
	for ; i+4 <= len(a); i += 4 {
		d0, d1, d2, d3 := a[i]-b[i], a[i+1]-b[i+1], a[i+2]-b[i+2], a[i+3]-b[i+3]
		s0 += d0 * d0
		s1 += d1 * d1
		s2 += d2 * d2
		s3 += d3 * d3
	}
	for ; i < len(a); i++ {
		d := a[i] - b[i]
		s0 += d * d
	}
	return float64((s0 + s1) + (s2 + s3))
}

// cdist is the coarse-quantiser distance: squared Euclidean distance. Under
// cosine every vector and centroid is unit length, so it equals 2 - 2·dot,
// which is cheaper to compute.
func cdist(m Metric, a, b []float32) float64 {
	if m == Cosine {
		return max(0, 2-2*dot(a, b))
	}
	return sqDist(a, b)
}

func score(m Metric, q, v []float32) float64 {
	if m == Cosine {
		return dot(q, v)
	}
	return -sqDist(q, v)
}

// prepare validates v and returns the copy the index stores: unit length
// under cosine, unchanged under L2.
func prepare(m Metric, dim int, v []float32) ([]float32, error) {
	if len(v) != dim {
		return nil, ErrDimension
	}
	out := make([]float32, dim)
	var norm float64
	for i, x := range v {
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			return nil, ErrNotFinite
		}
		out[i] = x
		norm += float64(x) * float64(x)
	}
	if m == Cosine {
		if norm == 0 {
			return nil, ErrZeroVector
		}
		inv := 1 / math.Sqrt(norm)
		for i := range out {
			out[i] = float32(float64(out[i]) * inv)
		}
	}
	return out, nil
}

func validMetric(m Metric) bool { return m == Cosine || m == L2 }

// ---------------------------------------------------------------- Flat ----

// Flat is an exact brute-force index: every query is scored against every
// stored vector. It is the ground truth that IVF's recall is measured
// against. Safe for concurrent use.
type Flat struct {
	dim    int
	metric Metric

	mu   sync.RWMutex
	ids  []int
	vecs [][]float32
	seen map[int]struct{}
}

// NewFlat returns an empty exact index for vectors of length dim.
func NewFlat(dim int, metric Metric) (*Flat, error) {
	if dim <= 0 {
		return nil, errors.New("vectorannindex: dim must be positive")
	}
	if !validMetric(metric) {
		return nil, errors.New("vectorannindex: unknown metric")
	}
	return &Flat{dim: dim, metric: metric, seen: map[int]struct{}{}}, nil
}

// Add stores v under id.
func (f *Flat) Add(id int, v []float32) error {
	p, err := prepare(f.metric, f.dim, v)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, dup := f.seen[id]; dup {
		return ErrDuplicateID
	}
	f.seen[id] = struct{}{}
	f.ids = append(f.ids, id)
	f.vecs = append(f.vecs, p)
	return nil
}

// Len returns the number of stored vectors.
func (f *Flat) Len() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.ids)
}

// Search returns the k most similar stored vectors, best first. It scans
// all Len() vectors.
func (f *Flat) Search(q []float32, k int) ([]Result, error) {
	if k <= 0 {
		return nil, ErrBadK
	}
	pq, err := prepare(f.metric, f.dim, q)
	if err != nil {
		return nil, err
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	t := newTopK(k)
	for i, v := range f.vecs {
		t.offer(Result{ID: f.ids[i], Score: score(f.metric, pq, v)})
	}
	return t.sorted(), nil
}

// ----------------------------------------------------------------- IVF ----

// Option configures an IVF index.
type Option func(*ivfOptions)

type ivfOptions struct {
	seed1, seed2 uint64
	maxIter      int
}

// WithSeed sets the seed for k-means++ initialisation. The same seed and
// training data always give the same centroids. Default (1, 2).
func WithSeed(s1, s2 uint64) Option {
	return func(o *ivfOptions) { o.seed1, o.seed2 = s1, s2 }
}

// WithMaxIterations caps Lloyd iterations per training run. Training also
// stops early once no vector changes cluster. Default 20.
func WithMaxIterations(n int) Option {
	return func(o *ivfOptions) { o.maxIter = n }
}

// TrainStats describes the cost of a Train or Retrain call.
type TrainStats struct {
	Samples    int  // vectors k-means ran on
	Iterations int  // Lloyd iterations run
	Converged  bool // stopped because no assignment changed
	// DistanceComputations counts vector-to-centroid distances, including
	// k-means++ seeding and re-filing every stored vector afterwards.
	DistanceComputations int64
	Reassigned           int // stored vectors re-filed into the new lists
}

// SearchStats describes the work one IVF query did.
type SearchStats struct {
	CentroidsCompared int // always nlist
	ListsProbed       int
	VectorsScanned    int // members exact-scored
}

// Stats are cumulative counters across all IVF queries.
type Stats struct {
	Queries        uint64
	VectorsScanned uint64
}

type invList struct {
	ids  []int
	vecs [][]float32
}

// IVF is an inverted-file ANN index with a k-means coarse quantiser. Safe for
// concurrent use: searches run in parallel; Add, Train and Retrain are
// exclusive.
type IVF struct {
	dim, nlist int
	metric     Metric
	opts       ivfOptions

	mu        sync.RWMutex
	trained   bool
	centroids [][]float32
	lists     []invList
	where     map[int]int // id -> list
	n         int

	queries, scanned atomic.Uint64
}

// NewIVF returns an untrained IVF index with nlist inverted lists.
func NewIVF(dim, nlist int, metric Metric, opts ...Option) (*IVF, error) {
	if dim <= 0 {
		return nil, errors.New("vectorannindex: dim must be positive")
	}
	if nlist <= 0 {
		return nil, errors.New("vectorannindex: nlist must be positive")
	}
	if !validMetric(metric) {
		return nil, errors.New("vectorannindex: unknown metric")
	}
	o := ivfOptions{seed1: 1, seed2: 2, maxIter: 20}
	for _, opt := range opts {
		opt(&o)
	}
	if o.maxIter <= 0 {
		return nil, errors.New("vectorannindex: max iterations must be positive")
	}
	return &IVF{dim: dim, nlist: nlist, metric: metric, opts: o, where: map[int]int{}}, nil
}

// NList returns the number of inverted lists.
func (ix *IVF) NList() int { return ix.nlist }

// Len returns the number of stored vectors.
func (ix *IVF) Len() int {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return ix.n
}

// Trained reports whether centroids exist.
func (ix *IVF) Trained() bool {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return ix.trained
}

// Train runs k-means on sample to learn the centroids, then re-files any
// vectors already stored. The sample is not added to the index.
func (ix *IVF) Train(sample [][]float32) (TrainStats, error) {
	prepped := make([][]float32, len(sample))
	for i, v := range sample {
		p, err := prepare(ix.metric, ix.dim, v)
		if err != nil {
			return TrainStats{}, err
		}
		prepped[i] = p
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	return ix.trainLocked(prepped)
}

// Retrain re-runs k-means on the vectors already stored and re-files them
// all. It is the fix when added data has drifted away from the original
// centroids. sampleSize caps how many stored vectors k-means runs on (an
// evenly strided, deterministic subset); 0 or more than Len() means all.
func (ix *IVF) Retrain(sampleSize int) (TrainStats, error) {
	if sampleSize < 0 {
		return TrainStats{}, errors.New("vectorannindex: sample size must not be negative")
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	all := make([][]float32, 0, ix.n)
	for _, l := range ix.lists {
		all = append(all, l.vecs...)
	}
	if sampleSize > 0 && sampleSize < len(all) {
		sub := make([][]float32, sampleSize)
		for i := range sub {
			sub[i] = all[i*len(all)/sampleSize]
		}
		all = sub
	}
	return ix.trainLocked(all)
}

func (ix *IVF) trainLocked(sample [][]float32) (TrainStats, error) {
	if len(sample) < ix.nlist {
		return TrainStats{}, ErrTooFewToTrain
	}
	cents, st := kmeans(sample, ix.nlist, ix.dim, ix.metric, ix.opts)

	// Re-file whatever is stored under the new centroids.
	old := ix.lists
	ix.centroids = cents
	ix.lists = make([]invList, ix.nlist)
	ix.trained = true
	for _, l := range old {
		for i, v := range l.vecs {
			c := nearest(ix.metric, cents, v)
			ix.lists[c].ids = append(ix.lists[c].ids, l.ids[i])
			ix.lists[c].vecs = append(ix.lists[c].vecs, v)
			ix.where[l.ids[i]] = c
			st.Reassigned++
		}
	}
	st.DistanceComputations += int64(st.Reassigned) * int64(ix.nlist)
	return st, nil
}

// nearest returns the index of the centroid closest to v. Ties go to the
// lower index.
func nearest(m Metric, cents [][]float32, v []float32) int {
	best, bestD := 0, math.Inf(1)
	for c, cv := range cents {
		if d := cdist(m, v, cv); d < bestD {
			best, bestD = c, d
		}
	}
	return best
}

// kmeans clusters sample into k groups: seeded k-means++ initialisation, then
// Lloyd iterations until no assignment changes or maxIter is reached. Under
// cosine the vectors are unit length and centroids are re-normalised after
// each update (spherical k-means), so nearest-by-L2 equals most-similar-by-
// cosine.
func kmeans(sample [][]float32, k, dim int, m Metric, o ivfOptions) ([][]float32, TrainStats) {
	rng := rand.New(rand.NewPCG(o.seed1, o.seed2))
	n := len(sample)
	st := TrainStats{Samples: n}

	// k-means++: first centre uniform, each next one with probability
	// proportional to squared distance from the nearest centre chosen so far.
	cents := make([][]float32, 0, k)
	cents = append(cents, clone(sample[rng.IntN(n)]))
	d2 := make([]float64, n)
	for i, v := range sample {
		d2[i] = cdist(m, v, cents[0])
	}
	st.DistanceComputations += int64(n)
	for len(cents) < k {
		var total float64
		for _, d := range d2 {
			total += d
		}
		pick := 0
		if total > 0 {
			r := rng.Float64() * total
			for i, d := range d2 {
				r -= d
				if r <= 0 {
					pick = i
					break
				}
				pick = i
			}
		} else {
			pick = rng.IntN(n) // every point coincides with a centre
		}
		c := clone(sample[pick])
		cents = append(cents, c)
		for i, v := range sample {
			if d := cdist(m, v, c); d < d2[i] {
				d2[i] = d
			}
		}
		st.DistanceComputations += int64(n)
	}

	assign := make([]int, n)
	for i := range assign {
		assign[i] = -1
	}
	sums := make([][]float64, k)
	for c := range sums {
		sums[c] = make([]float64, dim)
	}
	counts := make([]int, k)
	for st.Iterations < o.maxIter {
		st.Iterations++
		changed := 0
		for i, v := range sample {
			c := nearest(m, cents, v)
			if c != assign[i] {
				assign[i] = c
				changed++
			}
		}
		st.DistanceComputations += int64(n) * int64(k)
		if changed == 0 {
			st.Converged = true
			break
		}
		for c := range sums {
			clear(sums[c])
			counts[c] = 0
		}
		for i, v := range sample {
			c := assign[i]
			counts[c]++
			for j, x := range v {
				sums[c][j] += float64(x)
			}
		}
		for c := range cents {
			if counts[c] == 0 {
				// Empty cluster: move it onto the point farthest from its
				// own centre (deterministic: lowest index wins ties).
				far, farD := 0, -1.0
				for i, v := range sample {
					if d := cdist(m, v, cents[assign[i]]); d > farD {
						far, farD = i, d
					}
				}
				cents[c] = clone(sample[far])
				assign[far] = c
				continue
			}
			for j := range cents[c] {
				cents[c][j] = float32(sums[c][j] / float64(counts[c]))
			}
			if m == Cosine {
				normalise(cents[c])
			}
		}
	}
	return cents, st
}

func clone(v []float32) []float32 { return append([]float32(nil), v...) }

func normalise(v []float32) {
	var s float64
	for _, x := range v {
		s += float64(x) * float64(x)
	}
	if s == 0 {
		return
	}
	inv := 1 / math.Sqrt(s)
	for i := range v {
		v[i] = float32(float64(v[i]) * inv)
	}
}

// Add files v under the nearest trained centroid. The index must be trained.
func (ix *IVF) Add(id int, v []float32) error {
	p, err := prepare(ix.metric, ix.dim, v)
	if err != nil {
		return err
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if !ix.trained {
		return ErrNotTrained
	}
	if _, dup := ix.where[id]; dup {
		return ErrDuplicateID
	}
	c := nearest(ix.metric, ix.centroids, p)
	ix.lists[c].ids = append(ix.lists[c].ids, id)
	ix.lists[c].vecs = append(ix.lists[c].vecs, p)
	ix.where[id] = c
	ix.n++
	return nil
}

// ListOf returns the inverted list id is filed in.
func (ix *IVF) ListOf(id int) (int, bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	c, ok := ix.where[id]
	return c, ok
}

// NearestList returns the list a vector would be filed in.
func (ix *IVF) NearestList(v []float32) (int, error) {
	p, err := prepare(ix.metric, ix.dim, v)
	if err != nil {
		return 0, err
	}
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if !ix.trained {
		return 0, ErrNotTrained
	}
	return nearest(ix.metric, ix.centroids, p), nil
}

// ListSizes returns the number of vectors in each inverted list.
func (ix *IVF) ListSizes() []int {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	out := make([]int, len(ix.lists))
	for i, l := range ix.lists {
		out[i] = len(l.ids)
	}
	return out
}

// Search returns the k most similar vectors among the members of the nprobe
// lists whose centroids are closest to q, best first. nprobe larger than
// nlist is treated as nlist, which makes the answer exact.
func (ix *IVF) Search(q []float32, k, nprobe int) ([]Result, SearchStats, error) {
	if k <= 0 {
		return nil, SearchStats{}, ErrBadK
	}
	if nprobe <= 0 {
		return nil, SearchStats{}, ErrBadNProbe
	}
	pq, err := prepare(ix.metric, ix.dim, q)
	if err != nil {
		return nil, SearchStats{}, err
	}
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if !ix.trained {
		return nil, SearchStats{}, ErrNotTrained
	}
	nprobe = min(nprobe, ix.nlist)

	// Rank centroids by distance (ties to the lower list index) and keep
	// the nprobe nearest. A heap of size nprobe would be O(nlist log nprobe);
	// with nlist in the hundreds a sort is fine.
	type cd struct {
		c int
		d float64
	}
	order := make([]cd, ix.nlist)
	for c, cv := range ix.centroids {
		order[c] = cd{c, cdist(ix.metric, pq, cv)}
	}
	sort.Slice(order, func(i, j int) bool {
		if order[i].d != order[j].d {
			return order[i].d < order[j].d
		}
		return order[i].c < order[j].c
	})

	st := SearchStats{CentroidsCompared: ix.nlist, ListsProbed: nprobe}
	t := newTopK(k)
	for _, o := range order[:nprobe] {
		l := ix.lists[o.c]
		for i, v := range l.vecs {
			t.offer(Result{ID: l.ids[i], Score: score(ix.metric, pq, v)})
		}
		st.VectorsScanned += len(l.ids)
	}
	ix.queries.Add(1)
	ix.scanned.Add(uint64(st.VectorsScanned))
	return t.sorted(), st, nil
}

// Stats returns cumulative query counters.
func (ix *IVF) Stats() Stats {
	return Stats{Queries: ix.queries.Load(), VectorsScanned: ix.scanned.Load()}
}

// Recall returns the fraction of truth's IDs that also appear in got: the
// usual recall@k when both come from a k-nearest search.
func Recall(got, truth []Result) float64 {
	if len(truth) == 0 {
		return 1
	}
	in := make(map[int]struct{}, len(got))
	for _, r := range got {
		in[r.ID] = struct{}{}
	}
	hit := 0
	for _, r := range truth {
		if _, ok := in[r.ID]; ok {
			hit++
		}
	}
	return float64(hit) / float64(len(truth))
}
