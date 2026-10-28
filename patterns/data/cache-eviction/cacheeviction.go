// Package cacheeviction implements four eviction policies for a bounded
// cache: FIFO, LRU, LFU and W-TinyLFU.
//
// A bounded cache must throw something away on every insert once it is full.
// Which entry it picks decides the hit ratio:
//
//   - FIFO evicts the oldest insert, ignoring how often or how recently it was read.
//   - LRU evicts the least recently read entry. Great for recency, but a
//     single scan of one-off keys flushes the whole hot set.
//   - LFU evicts the least frequently read entry. Resists scans, but old
//     popular keys linger long after they go cold.
//   - W-TinyLFU puts new keys in a small LRU window, then only admits them to
//     the main cache if a count-min sketch says they are read more often than
//     the entry they would replace. It gets most of LRU's recency and LFU's
//     scan resistance in O(1) time, for a fixed amount of memory per entry.
//
// Every policy is safe for concurrent use and runs Get and Put in O(1).
package cacheeviction

import (
	"container/list"
	"errors"
	"fmt"
	"hash/maphash"
	"sync"
)

// Policy is a bounded key-value cache with a fixed eviction policy.
type Policy[K comparable, V any] interface {
	Get(key K) (V, bool)
	Put(key K, val V)
	Len() int
	Name() string
}

var errCapacity = errors.New("cacheeviction: capacity must be positive")

type item[K comparable, V any] struct {
	key  K
	val  V
	freq int // LFU only
}

// lru is an unsynchronised LRU list shared by LRU and W-TinyLFU.
// The front of ll is the most recently used entry.
type lru[K comparable, V any] struct {
	cap   int
	ll    *list.List
	items map[K]*list.Element
}

func newLRU[K comparable, V any](capacity int) *lru[K, V] {
	return &lru[K, V]{cap: capacity, ll: list.New(), items: make(map[K]*list.Element)}
}

func (l *lru[K, V]) get(key K) (*item[K, V], bool) {
	el, ok := l.items[key]
	if !ok {
		return nil, false
	}
	l.ll.MoveToFront(el)
	return el.Value.(*item[K, V]), true
}

// add inserts a key that is not present and returns the entry it evicted, if any.
func (l *lru[K, V]) add(key K, val V) (evicted *item[K, V]) {
	l.items[key] = l.ll.PushFront(&item[K, V]{key: key, val: val})
	if l.ll.Len() > l.cap {
		evicted = l.removeElement(l.ll.Back())
	}
	return evicted
}

func (l *lru[K, V]) victim() *item[K, V] {
	if el := l.ll.Back(); el != nil {
		return el.Value.(*item[K, V])
	}
	return nil
}

func (l *lru[K, V]) remove(key K) {
	if el, ok := l.items[key]; ok {
		l.removeElement(el)
	}
}

func (l *lru[K, V]) removeElement(el *list.Element) *item[K, V] {
	it := l.ll.Remove(el).(*item[K, V])
	delete(l.items, it.key)
	return it
}

// LRU evicts the least recently used entry.
type LRU[K comparable, V any] struct {
	mu sync.Mutex
	l  *lru[K, V]
}

// NewLRU returns an LRU cache holding at most capacity entries.
func NewLRU[K comparable, V any](capacity int) (*LRU[K, V], error) {
	if capacity <= 0 {
		return nil, errCapacity
	}
	return &LRU[K, V]{l: newLRU[K, V](capacity)}, nil
}

// Get returns the value for key and marks it most recently used.
func (c *LRU[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if it, ok := c.l.get(key); ok {
		return it.val, true
	}
	var zero V
	return zero, false
}

// Put inserts or updates key, evicting the least recently used entry if full.
func (c *LRU[K, V]) Put(key K, val V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if it, ok := c.l.get(key); ok {
		it.val = val
		return
	}
	c.l.add(key, val)
}

// Len returns the number of cached entries.
func (c *LRU[K, V]) Len() int { c.mu.Lock(); defer c.mu.Unlock(); return c.l.ll.Len() }

// Name returns "LRU".
func (c *LRU[K, V]) Name() string { return "LRU" }

// FIFO evicts the entry that was inserted first. Reads don't change the order.
type FIFO[K comparable, V any] struct {
	mu    sync.Mutex
	cap   int
	ll    *list.List // front = newest insert
	items map[K]*list.Element
}

// NewFIFO returns a FIFO cache holding at most capacity entries.
func NewFIFO[K comparable, V any](capacity int) (*FIFO[K, V], error) {
	if capacity <= 0 {
		return nil, errCapacity
	}
	return &FIFO[K, V]{cap: capacity, ll: list.New(), items: make(map[K]*list.Element)}, nil
}

// Get returns the value for key without changing its eviction order.
func (c *FIFO[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		return el.Value.(*item[K, V]).val, true
	}
	var zero V
	return zero, false
}

// Put inserts key, evicting the oldest insert if full. Updating an existing
// key keeps its original position.
func (c *FIFO[K, V]) Put(key K, val V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		el.Value.(*item[K, V]).val = val
		return
	}
	c.items[key] = c.ll.PushFront(&item[K, V]{key: key, val: val})
	if c.ll.Len() > c.cap {
		it := c.ll.Remove(c.ll.Back()).(*item[K, V])
		delete(c.items, it.key)
	}
}

// Len returns the number of cached entries.
func (c *FIFO[K, V]) Len() int { c.mu.Lock(); defer c.mu.Unlock(); return c.ll.Len() }

// Name returns "FIFO".
func (c *FIFO[K, V]) Name() string { return "FIFO" }

// LFU evicts the least frequently used entry, breaking ties by least recent
// use. Entries live in one list per access count, and minFreq points at the
// lowest non-empty list, so both Get and Put are O(1).
type LFU[K comparable, V any] struct {
	mu      sync.Mutex
	cap     int
	items   map[K]*list.Element
	freqs   map[int]*list.List // access count -> entries, front = most recent
	minFreq int
}

// NewLFU returns an LFU cache holding at most capacity entries.
func NewLFU[K comparable, V any](capacity int) (*LFU[K, V], error) {
	if capacity <= 0 {
		return nil, errCapacity
	}
	return &LFU[K, V]{cap: capacity, items: make(map[K]*list.Element), freqs: make(map[int]*list.List)}, nil
}

// touch moves an entry from its frequency list to the next one up.
func (c *LFU[K, V]) touch(el *list.Element) *item[K, V] {
	it := el.Value.(*item[K, V])
	old := c.freqs[it.freq]
	old.Remove(el)
	if old.Len() == 0 {
		delete(c.freqs, it.freq)
		if c.minFreq == it.freq {
			c.minFreq++
		}
	}
	it.freq++
	c.items[it.key] = c.bucket(it.freq).PushFront(it)
	return it
}

func (c *LFU[K, V]) bucket(freq int) *list.List {
	l, ok := c.freqs[freq]
	if !ok {
		l = list.New()
		c.freqs[freq] = l
	}
	return l
}

// Get returns the value for key and increments its access count.
func (c *LFU[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		return c.touch(el).val, true
	}
	var zero V
	return zero, false
}

// Put inserts or updates key. When full, it evicts from the lowest-frequency
// list, choosing that list's least recently used entry.
func (c *LFU[K, V]) Put(key K, val V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.touch(el).val = val
		return
	}
	if len(c.items) >= c.cap {
		l := c.freqs[c.minFreq]
		it := l.Remove(l.Back()).(*item[K, V])
		if l.Len() == 0 {
			delete(c.freqs, c.minFreq)
		}
		delete(c.items, it.key)
	}
	c.minFreq = 1
	c.items[key] = c.bucket(1).PushFront(&item[K, V]{key: key, val: val, freq: 1})
}

// Len returns the number of cached entries.
func (c *LFU[K, V]) Len() int { c.mu.Lock(); defer c.mu.Unlock(); return len(c.items) }

// Name returns "LFU".
func (c *LFU[K, V]) Name() string { return "LFU" }

// TinyLFU is a simplified W-TinyLFU: a small LRU window (1% of capacity) in
// front of a main LRU (the rest), with a count-min sketch acting as the
// admission filter between them.
//
// Every Get records one access in the sketch. A new key always enters the
// window. When the window overflows, its evicted candidate competes with the
// main cache's LRU victim, and only the one the sketch estimates as more
// frequently read stays. One-hit wonders from a scan therefore pass through
// the window without displacing the hot set.
//
// Caffeine's production version uses a segmented LRU for the main region and
// adapts the window size; this version keeps the core idea and nothing else.
type TinyLFU[K comparable, V any] struct {
	mu     sync.Mutex
	cap    int
	window *lru[K, V]
	main   *lru[K, V]
	sketch *Sketch
}

// NewTinyLFU returns a W-TinyLFU cache holding at most capacity entries.
// capacity must be at least 2 so the window and the main region each get one slot.
func NewTinyLFU[K comparable, V any](capacity int) (*TinyLFU[K, V], error) {
	if capacity < 2 {
		return nil, errors.New("cacheeviction: TinyLFU capacity must be at least 2")
	}
	windowCap := max(1, capacity/100)
	return &TinyLFU[K, V]{
		cap:    capacity,
		window: newLRU[K, V](windowCap),
		main:   newLRU[K, V](capacity - windowCap),
		sketch: NewSketch(capacity),
	}, nil
}

// Get returns the value for key and records the access in the sketch.
// Misses are recorded too, which is how a key earns admission before it is cached.
func (c *TinyLFU[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sketch.Increment(key)
	if it, ok := c.window.get(key); ok {
		return it.val, true
	}
	if it, ok := c.main.get(key); ok {
		return it.val, true
	}
	var zero V
	return zero, false
}

// Put inserts or updates key. New keys enter the window; a key leaving the
// window is admitted to the main region only if it beats the main victim.
func (c *TinyLFU[K, V]) Put(key K, val V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if it, ok := c.window.get(key); ok {
		it.val = val
		return
	}
	if it, ok := c.main.get(key); ok {
		it.val = val
		return
	}
	cand := c.window.add(key, val)
	if cand == nil {
		return
	}
	if c.main.ll.Len() < c.main.cap {
		c.main.add(cand.key, cand.val)
		return
	}
	victim := c.main.victim()
	if c.sketch.Estimate(cand.key) > c.sketch.Estimate(victim.key) {
		c.main.remove(victim.key)
		c.main.add(cand.key, cand.val)
	}
	// Otherwise the candidate is dropped and the hot victim stays.
}

// Len returns the number of cached entries.
func (c *TinyLFU[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.window.ll.Len() + c.main.ll.Len()
}

// Name returns "W-TinyLFU".
func (c *TinyLFU[K, V]) Name() string { return "W-TinyLFU" }

const (
	sketchDepth   = 4
	sketchMaxHits = 15 // 4-bit counters, as in Caffeine
)

// Sketch is a count-min sketch: an approximate frequency counter in fixed
// memory, here four rows of 8×capacity one-byte counters. Each key increments
// one counter in each row, and the estimate is the minimum of those counters.
// It can over-count (when a key collides with busier keys in every row) but
// never under-count. Counters saturate at 15, and every 10×capacity
// increments all counters are halved so old popularity fades.
type Sketch struct {
	rows       [sketchDepth][]uint8
	mask       uint64
	seed       maphash.Seed
	additions  int
	sampleSize int
}

// NewSketch returns a sketch sized for a cache of the given capacity.
func NewSketch(capacity int) *Sketch {
	width := 64
	for width < capacity*8 {
		width <<= 1
	}
	s := &Sketch{mask: uint64(width - 1), seed: maphash.MakeSeed(), sampleSize: 10 * max(capacity, 1)}
	for i := range s.rows {
		s.rows[i] = make([]uint8, width)
	}
	return s
}

// indexes derives one counter index per row from a single 64-bit hash using
// double hashing (h1 + i*h2).
func (s *Sketch) indexes(key any) [sketchDepth]uint64 {
	h := maphash.String(s.seed, fmt.Sprint(key))
	h1, h2 := h, (h>>32)|1
	var idx [sketchDepth]uint64
	for i := range idx {
		idx[i] = (h1 + uint64(i)*h2) & s.mask
	}
	return idx
}

// Increment records one access to key.
func (s *Sketch) Increment(key any) {
	for i, j := range s.indexes(key) {
		if s.rows[i][j] < sketchMaxHits {
			s.rows[i][j]++
		}
	}
	s.additions++
	if s.additions >= s.sampleSize {
		s.reset()
	}
}

// Estimate returns the approximate number of recent accesses to key.
func (s *Sketch) Estimate(key any) int {
	est := sketchMaxHits
	for i, j := range s.indexes(key) {
		est = min(est, int(s.rows[i][j]))
	}
	return est
}

// reset halves every counter (the "aging" step).
func (s *Sketch) reset() {
	for i := range s.rows {
		for j := range s.rows[i] {
			s.rows[i][j] >>= 1
		}
	}
	s.additions /= 2
}

// Replay runs a read-through access trace against p: every miss is followed
// by a Put of load(key). It returns the hit and miss counts.
func Replay[K comparable, V any](p Policy[K, V], trace []K, load func(K) V) (hits, misses int) {
	for _, k := range trace {
		if _, ok := p.Get(k); ok {
			hits++
			continue
		}
		misses++
		p.Put(k, load(k))
	}
	return hits, misses
}
