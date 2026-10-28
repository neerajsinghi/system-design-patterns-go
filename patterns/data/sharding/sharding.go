// Package sharding partitions a keyspace across shards. It provides three
// routing strategies and a stop-the-world resharding helper that counts how
// much data each strategy forces you to move.
//
//   - HashRouter: shard = hash(key) % N. Load spreads evenly, but range
//     scans must touch every shard, and changing N moves most keys.
//   - SlotRouter: Redis-Cluster-style hash slots. Keys hash to one of 16,384
//     fixed slots and a table maps slots to shards, so adding a shard moves
//     only the slots it takes over.
//   - RangeRouter: each shard owns a contiguous key range. Range scans
//     touch only the shards they overlap, and splitting a hot range moves
//     only that range's keys. Sequential keys all land on the last shard.
package sharding

import (
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"sort"
	"sync"
)

// Router maps keys to shard indexes in [0, NumShards()).
type Router interface {
	ShardFor(key string) int
	// ShardsForRange returns the shards that may hold keys in [lo, hi).
	ShardsForRange(lo, hi string) []int
	NumShards() int
}

// HashRouter is modulo-hash routing.
type HashRouter struct{ n int }

// NewHashRouter returns a router over n shards.
func NewHashRouter(n int) (HashRouter, error) {
	if n <= 0 {
		return HashRouter{}, errors.New("sharding: shard count must be positive")
	}
	return HashRouter{n: n}, nil
}

// ShardFor implements Router.
func (h HashRouter) ShardFor(key string) int {
	if h.n <= 0 {
		return 0 // zero-value router; NewCluster rejects it
	}
	f := fnv.New32a()
	_, _ = f.Write([]byte(key))
	return int(f.Sum32() % uint32(h.n))
}

// ShardsForRange implements Router. Hashing destroys key order, so every shard may hold keys in the range.
func (h HashRouter) ShardsForRange(_, _ string) []int {
	all := make([]int, h.n)
	for i := range all {
		all[i] = i
	}
	return all
}

// NumShards implements Router.
func (h HashRouter) NumShards() int { return h.n }

// RangeRouter assigns contiguous key ranges to shards. Range i covers
// [starts[i], starts[i+1]) and the last range is unbounded above.
type RangeRouter struct {
	starts []string // starts[0] == "", strictly increasing
	shards []int    // shards[i] owns range i
}

// NewRangeRouter builds a router from strictly increasing split points:
// NewRangeRouter("g", "p") gives shard 0 = [ "", g), 1 = [g, p), 2 = [p, ∞).
func NewRangeRouter(splits ...string) (RangeRouter, error) {
	starts := append([]string{""}, splits...)
	for i := 1; i < len(starts); i++ {
		if starts[i] <= starts[i-1] {
			return RangeRouter{}, fmt.Errorf("sharding: split points must be non-empty and strictly increasing, got %q after %q", starts[i], starts[i-1])
		}
	}
	shards := make([]int, len(starts))
	for i := range shards {
		shards[i] = i
	}
	return RangeRouter{starts: starts, shards: shards}, nil
}

// norm makes the zero RangeRouter behave as a single shard covering everything.
func (r RangeRouter) norm() RangeRouter {
	if len(r.starts) == 0 {
		return RangeRouter{starts: []string{""}, shards: []int{0}}
	}
	return r
}

func (r RangeRouter) rangeIndex(key string) int {
	// first start > key, minus one, is the range containing key
	return sort.SearchStrings(r.starts, key+"\x00") - 1
}

// ShardFor implements Router.
func (r RangeRouter) ShardFor(key string) int {
	r = r.norm()
	return r.shards[r.rangeIndex(key)]
}

// ShardsForRange implements Router: only the ranges overlapping [lo, hi).
func (r RangeRouter) ShardsForRange(lo, hi string) []int {
	if hi != "" && hi <= lo {
		return nil
	}
	r = r.norm()
	var out []int
	for i := r.rangeIndex(lo); i < len(r.starts); i++ {
		if hi != "" && r.starts[i] >= hi {
			break
		}
		out = append(out, r.shards[i])
	}
	return out
}

// NumShards implements Router.
func (r RangeRouter) NumShards() int { return slices.Max(r.norm().shards) + 1 }

// Split returns a new router where the range containing at is divided at
// at, and the upper half goes to a brand-new shard. Shard indexes of
// existing ranges are unchanged, so only the upper half's keys move.
func (r RangeRouter) Split(at string) (RangeRouter, error) {
	r = r.norm()
	i := r.rangeIndex(at)
	if r.starts[i] == at {
		return RangeRouter{}, fmt.Errorf("sharding: %q is already a split point", at)
	}
	starts := slices.Insert(slices.Clone(r.starts), i+1, at)
	shards := slices.Insert(slices.Clone(r.shards), i+1, r.NumShards())
	return RangeRouter{starts: starts, shards: shards}, nil
}

// Cluster is an in-process sharded key-value store.
type Cluster struct {
	mu     sync.RWMutex
	router Router
	shards []map[string]string
}

// NewCluster creates one empty shard per router shard.
func NewCluster(r Router) (*Cluster, error) {
	if r == nil || r.NumShards() <= 0 {
		return nil, errors.New("sharding: router must not be nil and must have shards")
	}
	c := &Cluster{router: r}
	c.grow(r.NumShards())
	return c, nil
}

func (c *Cluster) grow(n int) {
	for len(c.shards) < n {
		c.shards = append(c.shards, make(map[string]string))
	}
}

// Put stores key on its shard.
func (c *Cluster) Put(key, val string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.shards[c.router.ShardFor(key)][key] = val
}

// Get reads key from its shard.
func (c *Cluster) Get(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.shards[c.router.ShardFor(key)][key]
	return v, ok
}

// Scan returns the sorted keys in [lo, hi) (hi == "" means unbounded) and
// how many shards had to be queried.
func (c *Cluster) Scan(lo, hi string) (keys []string, shardsQueried int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	targets := c.router.ShardsForRange(lo, hi)
	for _, s := range targets {
		for k := range c.shards[s] {
			if k >= lo && (hi == "" || k < hi) {
				keys = append(keys, k)
			}
		}
	}
	slices.Sort(keys)
	return keys, len(targets)
}

// Counts returns the number of keys on each shard.
func (c *Cluster) Counts() []int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]int, len(c.shards))
	for i, s := range c.shards {
		out[i] = len(s)
	}
	return out
}

// Reshard switches to newRouter and moves every key whose shard changed,
// returning how many moved. It holds the write lock throughout. Real systems
// migrate online (copy, dual-write, cut over), but the number of keys moved is the same.
func (c *Cluster) Reshard(newRouter Router) (int, error) {
	if newRouter == nil || newRouter.NumShards() <= 0 {
		return 0, errors.New("sharding: router must not be nil and must have shards")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.grow(newRouter.NumShards())
	moved := 0
	for from, shard := range c.shards {
		for k, v := range shard {
			if to := newRouter.ShardFor(k); to != from {
				delete(shard, k)
				c.shards[to][k] = v
				moved++
			}
		}
	}
	c.shards = c.shards[:newRouter.NumShards()] // shrinking: emptied shards are dropped
	c.router = newRouter
	return moved, nil
}
