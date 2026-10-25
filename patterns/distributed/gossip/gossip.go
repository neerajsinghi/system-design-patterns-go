// Package gossip simulates an epidemic (gossip) protocol that spreads both
// cluster membership and key/value state without any central server.
//
// Every round, each live node increments its own heartbeat and exchanges its
// full state with a few randomly chosen peers (push-pull). Merging keeps the
// newest version of every entry, so information spreads like an epidemic and
// reaches all N nodes in O(log N) rounds with high probability, even when
// messages are dropped. A member whose heartbeat stops increasing for
// SuspectAfter rounds is suspected to have failed.
//
// The simulation is synchronous and seeded, so runs are reproducible. Real
// implementations exchange digests rather than full state, run on timers, and
// use more careful failure detectors (e.g. SWIM's indirect probes or
// phi-accrual).
package gossip

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
)

// Versioned is a replicated value. Higher Version wins; ties are broken by
// Origin so every node picks the same winner.
type Versioned struct {
	Value   string
	Version uint64
	Origin  string
}

func (v Versioned) newerThan(o Versioned) bool {
	if v.Version != o.Version {
		return v.Version > o.Version
	}
	return v.Origin > o.Origin
}

type memberState struct {
	heartbeat  uint64
	lastChange int // local round when the heartbeat last increased
}

type node struct {
	id      string
	alive   bool
	data    map[string]Versioned
	members map[string]*memberState
}

// Config controls the simulation.
type Config struct {
	Nodes        int     // cluster size, >= 2
	Fanout       int     // peers contacted per node per round, >= 1
	DropRate     float64 // probability in [0,1) that an exchange is lost
	SuspectAfter int     // rounds without heartbeat progress before suspicion, >= 1
	Seed         uint64  // RNG seed for reproducible runs
}

// ErrBadConfig reports an invalid Config.
var ErrBadConfig = errors.New("gossip: invalid config")

// ErrUnknownNode is returned for IDs not in the cluster.
var ErrUnknownNode = errors.New("gossip: unknown node")

// Cluster is a set of simulated nodes plus a lossy network.
type Cluster struct {
	mu    sync.Mutex
	cfg   Config
	rng   *rand.Rand
	ids   []string
	nodes map[string]*node
	round int
	// Messages counts successful exchanges, a proxy for network cost.
	messages int
}

// NewCluster creates nodes n0..n(N-1). Each node initially knows only itself
// and the seed node n0; membership spreads by gossip.
func NewCluster(cfg Config) (*Cluster, error) {
	if cfg.Nodes < 2 || cfg.Fanout < 1 || cfg.DropRate < 0 || cfg.DropRate >= 1 || cfg.SuspectAfter < 1 {
		return nil, fmt.Errorf("%w: %+v", ErrBadConfig, cfg)
	}
	c := &Cluster{cfg: cfg, rng: rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15)), nodes: map[string]*node{}}
	for i := 0; i < cfg.Nodes; i++ {
		id := fmt.Sprintf("n%d", i)
		c.ids = append(c.ids, id)
		c.nodes[id] = &node{id: id, alive: true, data: map[string]Versioned{},
			members: map[string]*memberState{id: {}, "n0": {}}}
	}
	return c, nil
}

// Set writes key=value at node id with a version one higher than the
// node's current version of key.
func (c *Cluster) Set(id, key, value string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[id]
	if !ok || !n.alive {
		return ErrUnknownNode
	}
	n.data[key] = Versioned{Value: value, Version: n.data[key].Version + 1, Origin: id}
	return nil
}

// Get reads key as node id currently sees it.
func (c *Cluster) Get(id, key string) (Versioned, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[id]
	if !ok {
		return Versioned{}, false
	}
	v, ok := n.data[key]
	return v, ok
}

// Crash stops a node: it no longer gossips or answers.
func (c *Cluster) Crash(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[id]
	if !ok {
		return ErrUnknownNode
	}
	n.alive = false
	return nil
}

// Round runs one gossip round across the cluster and returns its number.
// Exchanges within a round use each node's state as of the start of the
// round, so information travels at most one hop per round, like a real
// network where all nodes gossip at roughly the same time.
func (c *Cluster) Round() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.round++
	for _, id := range c.ids {
		if n := c.nodes[id]; n.alive {
			self := n.members[id]
			self.heartbeat++
			self.lastChange = c.round
		}
	}
	snap := make(map[string]snapshot, len(c.ids))
	for _, id := range c.ids {
		snap[id] = takeSnapshot(c.nodes[id])
	}
	for _, id := range c.ids {
		n := c.nodes[id]
		if !n.alive {
			continue
		}
		for _, pid := range c.pickPeers(n) {
			p := c.nodes[pid]
			if !p.alive || c.rng.Float64() < c.cfg.DropRate {
				continue // peer down or message lost
			}
			c.messages++
			c.merge(p, snap[id])  // push
			c.merge(n, snap[pid]) // pull
		}
	}
	return c.round
}

type snapshot struct {
	data    map[string]Versioned
	members map[string]uint64 // heartbeats
}

func takeSnapshot(n *node) snapshot {
	s := snapshot{data: make(map[string]Versioned, len(n.data)), members: make(map[string]uint64, len(n.members))}
	for k, v := range n.data {
		s.data[k] = v
	}
	for id, m := range n.members {
		s.members[id] = m.heartbeat
	}
	return s
}

// pickPeers chooses up to Fanout known, non-suspected peers at random.
func (c *Cluster) pickPeers(n *node) []string {
	var cands []string
	for id, m := range n.members {
		if id != n.id && c.round-m.lastChange <= c.cfg.SuspectAfter {
			cands = append(cands, id)
		}
	}
	sort.Strings(cands) // map order is random; sort for reproducibility
	c.rng.Shuffle(len(cands), func(i, j int) { cands[i], cands[j] = cands[j], cands[i] })
	if len(cands) > c.cfg.Fanout {
		cands = cands[:c.cfg.Fanout]
	}
	return cands
}

// merge folds a peer's snapshot into dst.
func (c *Cluster) merge(dst *node, src snapshot) {
	for k, v := range src.data {
		if cur, ok := dst.data[k]; !ok || v.newerThan(cur) {
			dst.data[k] = v
		}
	}
	for id, hb := range src.members {
		cur, ok := dst.members[id]
		if !ok {
			dst.members[id] = &memberState{heartbeat: hb, lastChange: c.round}
			continue
		}
		if hb > cur.heartbeat {
			cur.heartbeat = hb
			cur.lastChange = c.round
		}
	}
}

// Converged reports whether every live node holds the same version of key
// and knows every node in the cluster.
func (c *Cluster) Converged(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	var want *Versioned
	for _, id := range c.ids {
		n := c.nodes[id]
		if !n.alive {
			continue
		}
		if len(n.members) != len(c.ids) {
			return false
		}
		v, ok := n.data[key]
		if !ok {
			return false
		}
		if want == nil {
			want = &v
		} else if v != *want {
			return false
		}
	}
	return want != nil
}

// RunUntilConverged runs rounds until Converged(key) or maxRounds; it returns
// the number of rounds run and whether convergence was reached.
func (c *Cluster) RunUntilConverged(key string, maxRounds int) (int, bool) {
	for r := 1; r <= maxRounds; r++ {
		c.Round()
		if c.Converged(key) {
			return r, true
		}
	}
	return maxRounds, false
}

// Suspects returns the members node id currently suspects have failed.
func (c *Cluster) Suspects(id string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[id]
	if !ok {
		return nil
	}
	var out []string
	for mid, m := range n.members {
		if mid != id && c.round-m.lastChange > c.cfg.SuspectAfter {
			out = append(out, mid)
		}
	}
	sort.Strings(out)
	return out
}

// Known returns how many members node id knows about (including itself).
func (c *Cluster) Known(id string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n, ok := c.nodes[id]; ok {
		return len(n.members)
	}
	return 0
}

// Messages returns the number of successful exchanges so far.
func (c *Cluster) Messages() int { c.mu.Lock(); defer c.mu.Unlock(); return c.messages }
