// Package twotier implements a two-tier cache: a small in-process L1 cache on
// every application server in front of a shared, sharded L2 cache cluster,
// with hot keys replicated across several L2 nodes.
//
// A shared cache spreads keys across nodes, but a single viral key always
// hashes to one node, and that node saturates long before the cluster does.
// Two fixes, usually combined:
//
//   - L1: each app server keeps hot values in its own memory for a short TTL,
//     so most reads of a hot key never leave the process.
//   - Hot-key replication: a known-hot key is stored under several L2 keys
//     that hash to different nodes, and each read picks one at random.
//
// The cost is consistency: after a write, other servers' L1 copies stay stale
// until their short TTL runs out.
package twotier

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"sync"
	"time"
)

type entry[V any] struct {
	val     V
	expires time.Time
}

// Shared simulates an L2 cache cluster such as Redis Cluster or a Memcached
// pool: keys are spread over nodes by hash, and each node counts the
// operations it serves.
type Shared[V any] struct {
	ttl time.Duration
	now func() time.Time

	mu    sync.Mutex
	nodes []map[string]entry[V]
	ops   []uint64
}

// NewShared returns a cluster of n nodes whose entries live for ttl.
// now may be nil to use the real clock.
func NewShared[V any](n int, ttl time.Duration, now func() time.Time) (*Shared[V], error) {
	if n <= 0 {
		return nil, errors.New("twotier: need at least one L2 node")
	}
	if ttl <= 0 {
		return nil, errors.New("twotier: L2 ttl must be positive")
	}
	if now == nil {
		now = time.Now
	}
	s := &Shared[V]{ttl: ttl, now: now, nodes: make([]map[string]entry[V], n), ops: make([]uint64, n)}
	for i := range s.nodes {
		s.nodes[i] = make(map[string]entry[V])
	}
	return s, nil
}

// NodeFor returns the node that owns key.
func (s *Shared[V]) NodeFor(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(len(s.nodes)))
}

// Get reads key from its node.
func (s *Shared[V]) Get(key string) (V, bool) {
	n := s.NodeFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops[n]++
	if e, ok := s.nodes[n][key]; ok && s.now().Before(e.expires) {
		return e.val, true
	}
	var zero V
	return zero, false
}

// Set writes key to its node.
func (s *Shared[V]) Set(key string, val V) {
	n := s.NodeFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops[n]++
	s.nodes[n][key] = entry[V]{val: val, expires: s.now().Add(s.ttl)}
}

// Delete removes key from its node.
func (s *Shared[V]) Delete(key string) {
	n := s.NodeFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops[n]++
	delete(s.nodes[n], key)
}

// Ops returns how many operations each node has served.
func (s *Shared[V]) Ops() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.ops...)
}

// Loader fetches a value from the source of truth.
type Loader[V any] func(ctx context.Context, key string) (V, error)

// Stats counts where reads were served from.
type Stats struct {
	L1Hits, L2Hits, Loads uint64
}

// Client is one application server's view of the cache: its private L1 plus
// the shared L2. It is safe for concurrent use.
type Client[V any] struct {
	shared   *Shared[V]
	load     Loader[V]
	l1TTL    time.Duration
	replicas int
	now      func() time.Time
	pick     func(n int) int

	mu    sync.Mutex
	l1    map[string]entry[V]
	hot   map[string][]string // hot key -> its L2 replica keys
	stats Stats
}

// Option configures a Client.
type Option func(*options)

type options struct {
	now      func() time.Time
	pick     func(n int) int
	hot      []string
	replicas int
}

// WithClock injects a clock for L1 expiry.
func WithClock(now func() time.Time) Option {
	return func(o *options) {
		if now != nil {
			o.now = now
		}
	}
}

// WithHotKeys marks keys to replicate across L2 nodes. In production the list
// comes from hot-key detection (sampling, redis-cli --hotkeys, client-side
// counters); here it is supplied up front.
func WithHotKeys(replicas int, keys ...string) Option {
	return func(o *options) { o.replicas, o.hot = replicas, append(o.hot, keys...) }
}

// WithPicker injects the function that chooses a replica (0..n-1) per read.
func WithPicker(pick func(n int) int) Option {
	return func(o *options) {
		if pick != nil {
			o.pick = pick
		}
	}
}

// NewClient returns a client over shared. l1TTL is how long this server
// keeps values in process memory; 0 disables L1.
func NewClient[V any](shared *Shared[V], load Loader[V], l1TTL time.Duration, opts ...Option) (*Client[V], error) {
	if shared == nil || load == nil {
		return nil, errors.New("twotier: shared cache and loader are required")
	}
	if l1TTL < 0 {
		return nil, errors.New("twotier: L1 ttl must not be negative")
	}
	o := options{now: time.Now, pick: rand.IntN, replicas: 1}
	for _, opt := range opts {
		opt(&o)
	}
	if o.replicas < 1 {
		return nil, errors.New("twotier: replicas must be at least 1")
	}
	c := &Client[V]{
		shared: shared, load: load, l1TTL: l1TTL, replicas: o.replicas,
		now: o.now, pick: o.pick, l1: make(map[string]entry[V]), hot: make(map[string][]string),
	}
	for _, k := range o.hot {
		c.hot[k] = c.replicaKeys(k)
	}
	return c, nil
}

// replicaKeys picks suffixed L2 keys for a hot key so that they land on as
// many distinct nodes as possible. Plain "key#0..key#N-1" can hash to the same
// node, which would defeat the point.
func (c *Client[V]) replicaKeys(key string) []string {
	want := min(c.replicas, len(c.shared.nodes))
	used := make(map[int]bool)
	var keys []string
	for i := 0; len(keys) < want && i < 64*want; i++ {
		k := fmt.Sprintf("%s#%d", key, i)
		if n := c.shared.NodeFor(k); !used[n] {
			used[n] = true
			keys = append(keys, k)
		}
	}
	return keys
}

// Get returns the value for key from L1, then L2, then the loader. A loaded
// value is written to L2 (every replica, for hot keys) and to L1.
func (c *Client[V]) Get(ctx context.Context, key string) (V, error) {
	c.mu.Lock()
	if e, ok := c.l1[key]; ok && c.now().Before(e.expires) {
		c.stats.L1Hits++
		c.mu.Unlock()
		return e.val, nil
	}
	l2key := key
	if reps, ok := c.hot[key]; ok {
		l2key = reps[c.pick(len(reps))]
	}
	c.mu.Unlock()

	if v, ok := c.shared.Get(l2key); ok {
		c.mu.Lock()
		c.stats.L2Hits++
		c.fillL1(key, v)
		c.mu.Unlock()
		return v, nil
	}

	v, err := c.load(ctx, key)
	if err != nil {
		var zero V
		return zero, err
	}
	for _, k := range c.l2Keys(key) {
		c.shared.Set(k, v)
	}
	c.mu.Lock()
	c.stats.Loads++
	c.fillL1(key, v)
	c.mu.Unlock()
	return v, nil
}

// Invalidate removes key from L2 (every replica) and from this server's L1.
// Call it after writing the source of truth. Other servers' L1 copies are not
// reachable from here and stay stale until their L1 TTL expires.
func (c *Client[V]) Invalidate(key string) {
	for _, k := range c.l2Keys(key) {
		c.shared.Delete(k)
	}
	c.mu.Lock()
	delete(c.l1, key)
	c.mu.Unlock()
}

func (c *Client[V]) l2Keys(key string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if reps, ok := c.hot[key]; ok {
		return reps
	}
	return []string{key}
}

// fillL1 stores v in L1. The caller must hold c.mu.
func (c *Client[V]) fillL1(key string, v V) {
	if c.l1TTL > 0 {
		c.l1[key] = entry[V]{val: v, expires: c.now().Add(c.l1TTL)}
	}
}

// Stats returns a snapshot of the counters.
func (c *Client[V]) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}
