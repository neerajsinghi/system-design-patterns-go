// Package lsmtree implements a minimal log-structured merge tree.
//
// Writes go to an in-memory memtable. When it fills up, it is sorted and
// frozen into an immutable segment (an SSTable, in a real engine).
// Segments are never modified. A delete writes a tombstone that shadows
// older values. Reads check the memtable, then segments from newest to
// oldest, and the first hit wins. Compaction merges segments into one,
// keeping the newest version of each key and dropping tombstones, which
// bounds read amplification and reclaims space.
//
// A real engine also writes a WAL for the memtable (see ../write-ahead-log),
// stores segments on disk with sparse indexes, and adds a Bloom filter per
// segment (see ../bloom-filter). This package keeps everything in memory to
// show the data flow.
package lsmtree

import (
	"errors"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

type entry struct {
	key, val  string
	tombstone bool
}

// segment is an immutable sorted run.
type segment struct {
	entries []entry
}

func (s *segment) get(key string) (entry, bool) {
	if len(s.entries) == 0 || key < s.entries[0].key || key > s.entries[len(s.entries)-1].key {
		return entry{}, false // fence check: key outside this segment's range
	}
	i := sort.Search(len(s.entries), func(i int) bool { return s.entries[i].key >= key })
	if i < len(s.entries) && s.entries[i].key == key {
		return s.entries[i], true
	}
	return entry{}, false
}

// rangeOf returns the entries with lo <= key < hi (hi == "" means unbounded).
func rangeOf(run []entry, lo, hi string) []entry {
	i := sort.Search(len(run), func(i int) bool { return run[i].key >= lo })
	j := len(run)
	if hi != "" {
		j = sort.Search(len(run), func(i int) bool { return run[i].key >= hi })
	}
	if i >= j {
		return nil
	}
	return run[i:j]
}

// merge combines two sorted runs. On equal keys the entry from newer wins.
func merge(newer, older []entry) []entry {
	out := make([]entry, 0, len(newer)+len(older))
	i, j := 0, 0
	for i < len(newer) && j < len(older) {
		switch c := strings.Compare(newer[i].key, older[j].key); {
		case c < 0:
			out = append(out, newer[i])
			i++
		case c > 0:
			out = append(out, older[j])
			j++
		default:
			out = append(out, newer[i]) // shadow the older version
			i++
			j++
		}
	}
	out = append(out, newer[i:]...)
	return append(out, older[j:]...)
}

// Options configures a Tree.
type Options struct {
	// MemtableLimit is the number of keys that triggers a flush. Must be > 0.
	MemtableLimit int
	// MaxSegments triggers a full compaction when exceeded. 0 means compact only on demand.
	MaxSegments int
}

// Stats reports structure and read amplification.
type Stats struct {
	MemtableKeys   int
	Segments       int
	SegmentEntries int // includes shadowed versions and tombstones (space amplification)
	SegmentProbes  uint64
	Flushes        uint64
	Compactions    uint64
}

// Tree is a concurrency-safe in-memory LSM tree.
type Tree struct {
	opts Options

	mu       sync.RWMutex
	mem      map[string]entry
	segments []*segment // newest first

	probes, flushes, compactions atomic.Uint64
}

// New returns an empty tree.
func New(opts Options) (*Tree, error) {
	if opts.MemtableLimit <= 0 {
		return nil, errors.New("lsmtree: MemtableLimit must be positive")
	}
	if opts.MaxSegments < 0 {
		return nil, errors.New("lsmtree: MaxSegments must not be negative")
	}
	return &Tree{opts: opts, mem: make(map[string]entry)}, nil
}

// Put sets key to val.
func (t *Tree) Put(key, val string) { t.write(entry{key: key, val: val}) }

// Delete removes key by writing a tombstone.
func (t *Tree) Delete(key string) { t.write(entry{key: key, tombstone: true}) }

func (t *Tree) write(e entry) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mem[e.key] = e
	if len(t.mem) >= t.opts.MemtableLimit {
		t.flushLocked()
	}
}

// Get returns the newest value for key.
func (t *Tree) Get(key string) (string, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if e, ok := t.mem[key]; ok {
		return e.val, !e.tombstone
	}
	for _, s := range t.segments {
		t.probes.Add(1)
		if e, ok := s.get(key); ok {
			return e.val, !e.tombstone // a tombstone stops the search
		}
	}
	return "", false
}

// KV is a key/value pair returned by Scan.
type KV struct{ Key, Value string }

// Scan returns live pairs with lo <= key < hi in key order (hi == "" means unbounded).
func (t *Tree) Scan(lo, hi string) []KV {
	t.mu.RLock()
	run := rangeOf(t.sortedMem(), lo, hi)
	for _, s := range t.segments { // newest to oldest
		run = merge(run, rangeOf(s.entries, lo, hi))
	}
	t.mu.RUnlock()
	out := make([]KV, 0, len(run))
	for _, e := range run {
		if !e.tombstone {
			out = append(out, KV{e.key, e.val})
		}
	}
	return out
}

func (t *Tree) sortedMem() []entry {
	run := make([]entry, 0, len(t.mem))
	for _, e := range t.mem {
		run = append(run, e)
	}
	slices.SortFunc(run, func(a, b entry) int { return strings.Compare(a.key, b.key) })
	return run
}

// Flush freezes the memtable into a new segment.
func (t *Tree) Flush() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.flushLocked()
}

func (t *Tree) flushLocked() {
	if len(t.mem) == 0 {
		return
	}
	seg := &segment{entries: t.sortedMem()}
	t.segments = append([]*segment{seg}, t.segments...)
	t.mem = make(map[string]entry)
	t.flushes.Add(1)
	if t.opts.MaxSegments > 0 && len(t.segments) > t.opts.MaxSegments {
		t.compactLocked()
	}
}

// Compact merges all segments into one. Because the result includes the
// oldest data, tombstones have nothing left to shadow and are dropped.
func (t *Tree) Compact() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.compactLocked()
}

func (t *Tree) compactLocked() {
	if len(t.segments) == 0 {
		return
	}
	var run []entry
	for i := len(t.segments) - 1; i >= 0; i-- { // oldest first; each newer run wins
		run = merge(t.segments[i].entries, run)
	}
	live := run[:0]
	for _, e := range run {
		if !e.tombstone {
			live = append(live, e)
		}
	}
	t.segments = nil
	if len(live) > 0 {
		t.segments = []*segment{{entries: live}}
	}
	t.compactions.Add(1)
}

// Stats returns a snapshot of the tree's shape and counters.
func (t *Tree) Stats() Stats {
	t.mu.RLock()
	defer t.mu.RUnlock()
	s := Stats{
		MemtableKeys:  len(t.mem),
		Segments:      len(t.segments),
		SegmentProbes: t.probes.Load(),
		Flushes:       t.flushes.Load(),
		Compactions:   t.compactions.Load(),
	}
	for _, seg := range t.segments {
		s.SegmentEntries += len(seg.entries)
	}
	return s
}
