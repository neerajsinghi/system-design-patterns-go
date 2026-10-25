// Package bloomfilter implements a Bloom filter: a compact probabilistic set
// that answers "definitely not present" or "probably present".
//
// For n expected items and a target false-positive rate p, the optimal size
// and number of hash functions are
//
//	m = -n·ln(p) / (ln 2)²    bits
//	k = (m/n)·ln 2            hash functions
//
// The k bit positions come from double hashing (Kirsch–Mitzenmacher):
// g_i(x) = h1(x) + i·h2(x) mod m. This is as accurate as k independent hashes
// and much cheaper. There are no false negatives: an added item always tests
// true.
package bloomfilter

import (
	"errors"
	"hash/fnv"
	"math"
	"sync"
)

// Filter is a concurrency-safe Bloom filter.
type Filter struct {
	m, k uint64

	mu    sync.RWMutex
	bits  []uint64
	count uint64
}

// New sizes a filter for n items at false-positive rate p, with 0 < p < 1.
func New(n uint64, p float64) (*Filter, error) {
	if n == 0 {
		return nil, errors.New("bloomfilter: n must be positive")
	}
	if !(p > 0 && p < 1) {
		return nil, errors.New("bloomfilter: p must be in (0, 1)")
	}
	m, k := OptimalParams(n, p)
	return NewWithParams(m, k)
}

// OptimalParams returns the bit count m and hash count k for n items at rate p.
func OptimalParams(n uint64, p float64) (m, k uint64) {
	mf := math.Ceil(-float64(n) * math.Log(p) / (math.Ln2 * math.Ln2))
	m = max(uint64(mf), 1)
	k = max(uint64(math.Round(float64(m)/float64(n)*math.Ln2)), 1)
	return m, k
}

// NewWithParams builds a filter with explicit m bits and k hashes.
func NewWithParams(m, k uint64) (*Filter, error) {
	if m == 0 || k == 0 {
		return nil, errors.New("bloomfilter: m and k must be positive")
	}
	if m > 1<<40 { // 128 GiB of bits: refuse absurd sizes instead of OOMing
		return nil, errors.New("bloomfilter: m too large")
	}
	return &Filter{m: m, k: k, bits: make([]uint64, (m+63)/64)}, nil
}

// hashes derives two 64-bit hashes from one FNV-1a pass plus a splitmix64
// finaliser. h2 is forced odd so successive probes don't collapse onto a
// short cycle.
func hashes(data []byte) (h1, h2 uint64) {
	f := fnv.New64a()
	_, _ = f.Write(data)
	h1 = f.Sum64()
	z := h1 + 0x9e3779b97f4a7c15
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	h2 = (z ^ (z >> 31)) | 1
	return h1, h2
}

// Add inserts data.
func (f *Filter) Add(data []byte) {
	h1, h2 := hashes(data)
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.k {
		pos := (h1 + i*h2) % f.m
		f.bits[pos/64] |= 1 << (pos % 64)
	}
	f.count++
}

// Test reports whether data may be in the set. false means definitely not.
func (f *Filter) Test(data []byte) bool {
	h1, h2 := hashes(data)
	f.mu.RLock()
	defer f.mu.RUnlock()
	for i := range f.k {
		pos := (h1 + i*h2) % f.m
		if f.bits[pos/64]&(1<<(pos%64)) == 0 {
			return false
		}
	}
	return true
}

// AddString inserts s.
func (f *Filter) AddString(s string) { f.Add([]byte(s)) }

// TestString reports whether s may be in the set.
func (f *Filter) TestString(s string) bool { return f.Test([]byte(s)) }

// M returns the number of bits.
func (f *Filter) M() uint64 { return f.m }

// K returns the number of hash functions.
func (f *Filter) K() uint64 { return f.k }

// Count returns how many Add calls were made (duplicates included).
func (f *Filter) Count() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.count
}

// EstimatedFPR is the theoretical false-positive rate after Count() inserts:
// (1 - e^(-k·n/m))^k. It rises as the filter fills past its design capacity.
func (f *Filter) EstimatedFPR() float64 {
	n := float64(f.Count())
	k, m := float64(f.k), float64(f.m)
	return math.Pow(1-math.Exp(-k*n/m), k)
}
