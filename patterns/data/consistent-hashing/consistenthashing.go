// Package consistenthashing implements a consistent-hash ring with virtual
// nodes.
//
// Keys and nodes are hashed onto the same 64-bit ring. A key belongs to the
// first node clockwise from its hash. When a node joins or leaves, only the
// keys in the arcs it gains or loses move. That is roughly 1/N of the keys,
// compared with almost all of them under hash(key) % N.
//
// Each physical node is placed on the ring many times ("virtual nodes"),
// which evens out the arc lengths and so the load.
package consistenthashing

import (
	"errors"
	"hash/fnv"
	"slices"
	"strconv"
	"sync"
)

// Ring is a concurrency-safe consistent-hash ring.
type Ring struct {
	replicas int

	mu     sync.RWMutex
	hashes []uint64          // sorted virtual-node positions
	owner  map[uint64]string // position -> physical node
	nodes  map[string]struct{}
}

// New returns an empty ring that places each node at vnodes positions.
func New(vnodes int) (*Ring, error) {
	if vnodes <= 0 {
		return nil, errors.New("consistenthashing: vnodes must be positive")
	}
	return &Ring{
		replicas: vnodes,
		owner:    make(map[uint64]string),
		nodes:    make(map[string]struct{}),
	}, nil
}

// hash maps s onto the ring: FNV-1a followed by a splitmix64 finaliser,
// because raw FNV of similar strings ("node#1", "node#2") clusters badly.
func hash(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	x := h.Sum64()
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// Add places node on the ring. Adding an existing node is a no-op.
func (r *Ring) Add(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[node]; ok {
		return
	}
	r.nodes[node] = struct{}{}
	for i := range r.replicas {
		h := hash(node + "#" + strconv.Itoa(i))
		if _, taken := r.owner[h]; taken {
			continue // astronomically rare collision: first owner keeps it
		}
		r.owner[h] = node
		r.hashes = append(r.hashes, h)
	}
	slices.Sort(r.hashes)
}

// Remove takes node off the ring. Removing an unknown node is a no-op.
func (r *Ring) Remove(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[node]; !ok {
		return
	}
	delete(r.nodes, node)
	kept := r.hashes[:0]
	for _, h := range r.hashes {
		if r.owner[h] == node {
			delete(r.owner, h)
			continue
		}
		kept = append(kept, h)
	}
	r.hashes = kept
}

// search returns the index of the first virtual node at or after h, wrapping.
func (r *Ring) search(h uint64) int {
	i, _ := slices.BinarySearch(r.hashes, h)
	if i == len(r.hashes) {
		i = 0
	}
	return i
}

// Get returns the node that owns key, or false if the ring is empty.
func (r *Ring) Get(key string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.hashes) == 0 {
		return "", false
	}
	return r.owner[r.hashes[r.search(hash(key))]], true
}

// GetN returns up to n distinct nodes for key, walking clockwise. The first
// is the owner and the rest are natural replica placements. Fewer than n
// are returned if the ring has fewer nodes.
func (r *Ring) GetN(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if n <= 0 || len(r.hashes) == 0 {
		return nil
	}
	n = min(n, len(r.nodes))
	out := make([]string, 0, n)
	seen := make(map[string]bool, n)
	for i, start := 0, r.search(hash(key)); len(out) < n && i < len(r.hashes); i++ {
		node := r.owner[r.hashes[(start+i)%len(r.hashes)]]
		if !seen[node] {
			seen[node] = true
			out = append(out, node)
		}
	}
	return out
}

// Nodes returns the physical nodes on the ring, sorted.
func (r *Ring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.nodes))
	for n := range r.nodes {
		out = append(out, n)
	}
	slices.Sort(out)
	return out
}

// Assign maps every key to its owner. It's a helper for measuring
// distribution and movement.
func (r *Ring) Assign(keys []string) map[string]string {
	out := make(map[string]string, len(keys))
	for _, k := range keys {
		if n, ok := r.Get(k); ok {
			out[k] = n
		}
	}
	return out
}

// Moved returns how many keys have a different owner in after than in before.
func Moved(before, after map[string]string) int {
	moved := 0
	for k, owner := range before {
		if after[k] != owner {
			moved++
		}
	}
	return moved
}
