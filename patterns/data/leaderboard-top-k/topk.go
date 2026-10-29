package leaderboard

import (
	"container/heap"
	"errors"
	"sort"
	"sync"
)

// ItemCount is an item with an exact count.
type ItemCount struct {
	Item  string
	Count int64
}

// before orders counts descending, then items ascending.
func (a ItemCount) before(b ItemCount) bool {
	if a.Count != b.Count {
		return a.Count > b.Count
	}
	return a.Item < b.Item
}

// worstFirst is a min-heap whose root is the weakest of the kept items.
type worstFirst []ItemCount

func (h worstFirst) Len() int           { return len(h) }
func (h worstFirst) Less(i, j int) bool { return h[j].before(h[i]) }
func (h worstFirst) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *worstFirst) Push(x any)        { *h = append(*h, x.(ItemCount)) }
func (h *worstFirst) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

// TopK returns the k items with the highest counts, highest first (ties by
// item name). It keeps a min-heap of size k, so it runs in O(n log k) time and
// O(k) extra space instead of sorting all n items. It needs exact counts, so
// it only fits a bounded key set.
func TopK(counts map[string]int64, k int) ([]ItemCount, error) {
	if k <= 0 {
		return nil, errors.New("leaderboard: k must be positive")
	}
	h := make(worstFirst, 0, min(k, len(counts)))
	for item, c := range counts {
		ic := ItemCount{Item: item, Count: c}
		if len(h) < k {
			heap.Push(&h, ic)
		} else if ic.before(h[0]) {
			h[0] = ic
			heap.Fix(&h, 0)
		}
	}
	out := []ItemCount(h)
	sort.Slice(out, func(i, j int) bool { return out[i].before(out[j]) })
	return out, nil
}

// Estimate is Space-Saving's view of one monitored item. The true count lies
// in [Count-Error, Count].
type Estimate struct {
	Item  string
	Count int64 // upper bound on the true count
	Error int64 // maximum overestimate
}

type counter struct {
	item       string
	count, err int64
	idx        int
}

type counterHeap []*counter

func (h counterHeap) Len() int { return len(h) }
func (h counterHeap) Less(i, j int) bool {
	if h[i].count != h[j].count {
		return h[i].count < h[j].count
	}
	return h[i].item > h[j].item
}
func (h counterHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].idx, h[j].idx = i, j
}
func (h *counterHeap) Push(x any) {
	c := x.(*counter)
	c.idx = len(*h)
	*h = append(*h, c)
}
func (h *counterHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

// SpaceSaving is the Space-Saving heavy-hitters summary of Metwally, Agrawal
// and El Abbadi (2005). It monitors at most k items. A new item arriving when
// all k counters are taken evicts the item with the smallest count m and
// inherits that count: its counter starts at m + delta with error m.
//
// Guarantees, for a stream of total weight N:
//   - every monitored item's true count is in [Count-Error, Count];
//   - Error <= MinCount() <= N/k;
//   - any item whose true count exceeds MinCount() (in particular, any item
//     with more than N/k occurrences) is monitored.
//
// This version keeps counters in an indexed min-heap, so Add is O(log k); the
// paper's "stream summary" linked list makes unit increments O(1).
// SpaceSaving is safe for concurrent use.
type SpaceSaving struct {
	mu    sync.Mutex
	k     int
	n     int64
	h     counterHeap
	index map[string]*counter
}

// NewSpaceSaving returns a summary with k counters.
func NewSpaceSaving(k int) (*SpaceSaving, error) {
	if k <= 0 {
		return nil, errors.New("leaderboard: space-saving needs k > 0 counters")
	}
	return &SpaceSaving{k: k, h: make(counterHeap, 0, k), index: make(map[string]*counter, k)}, nil
}

// Add records delta (> 0) occurrences of item.
func (s *SpaceSaving) Add(item string, delta int64) error {
	if delta <= 0 {
		return errors.New("leaderboard: space-saving delta must be positive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n += delta
	if c, ok := s.index[item]; ok {
		c.count += delta
		heap.Fix(&s.h, c.idx)
		return nil
	}
	if len(s.h) < s.k {
		c := &counter{item: item, count: delta}
		heap.Push(&s.h, c)
		s.index[item] = c
		return nil
	}
	c := s.h[0]
	delete(s.index, c.item)
	c.item, c.err = item, c.count
	c.count += delta
	s.index[item] = c
	heap.Fix(&s.h, 0)
	return nil
}

// Estimate returns the counter for item, if it is monitored.
func (s *SpaceSaving) Estimate(item string) (Estimate, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.index[item]
	if !ok {
		return Estimate{}, false
	}
	return Estimate{Item: c.item, Count: c.count, Error: c.err}, true
}

// Top returns up to n monitored items, highest estimated count first.
func (s *SpaceSaving) Top(n int) []Estimate {
	s.mu.Lock()
	out := make([]Estimate, len(s.h))
	for i, c := range s.h {
		out[i] = Estimate{Item: c.item, Count: c.count, Error: c.err}
	}
	s.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Item < out[j].Item
	})
	if n >= 0 && n < len(out) {
		out = out[:n]
	}
	return out
}

// MinCount returns the smallest counter once all k are in use, else 0. No
// unmonitored item can have a true count above it.
func (s *SpaceSaving) MinCount() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.h) < s.k {
		return 0
	}
	return s.h[0].count
}

// N returns the total weight added.
func (s *SpaceSaving) N() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n
}

// K returns the number of counters.
func (s *SpaceSaving) K() int { return s.k }
