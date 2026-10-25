// Package leaderelection implements lease-based leader election with terms
// (epochs). It is a simplified teaching version, not Raft: the hard part,
// a linearizable and fault-tolerant lease store, is a single in-process
// object here. In production that store is etcd, ZooKeeper, Consul or a
// database row updated with compare-and-swap.
//
// Nodes repeatedly try to acquire or renew a lease with a TTL. Whoever holds
// an unexpired lease is the leader. Every change of holder increments the
// term, and the leader stamps the term on its writes so a resource can reject
// writes from a deposed leader that has not noticed yet (for example after a
// long GC pause).
package leaderelection

import (
	"errors"
	"sync"
	"time"
)

// Clock abstracts time so elections can be simulated deterministically.
type Clock interface{ Now() time.Time }

// ManualClock is a Clock that moves only when Advance is called.
type ManualClock struct {
	mu sync.Mutex
	t  time.Time
}

// NewManualClock returns a ManualClock set to t.
func NewManualClock(t time.Time) *ManualClock { return &ManualClock{t: t} }

// Now returns the fake time.
func (c *ManualClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }

// Advance moves the fake time forward by d.
func (c *ManualClock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

// Errors.
var (
	ErrHeld        = errors.New("leaderelection: lease held by another node")
	ErrUnreachable = errors.New("leaderelection: lease store unreachable")
	ErrStaleTerm   = errors.New("leaderelection: write from a stale term rejected")
	ErrBadTTL      = errors.New("leaderelection: ttl must be positive")
)

// Lease is the shared leadership record.
type Lease struct {
	Holder  string
	Term    uint64
	Expires time.Time
}

// LeaseStore is the single source of truth for the lease, standing in for a
// strongly consistent store. All operations are atomic.
type LeaseStore struct {
	mu          sync.Mutex
	clock       Clock
	lease       Lease
	unreachable map[string]bool
}

// NewLeaseStore returns a store with no leader.
func NewLeaseStore(clock Clock) *LeaseStore {
	return &LeaseStore{clock: clock, unreachable: map[string]bool{}}
}

// Partition cuts node off from the store (true) or heals it (false).
func (s *LeaseStore) Partition(node string, cut bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unreachable[node] = cut
}

// TryAcquire grants or renews the lease for node. Renewal keeps the term;
// taking over an expired or released lease increments it.
func (s *LeaseStore) TryAcquire(node string, ttl time.Duration) (Lease, error) {
	if ttl <= 0 {
		return Lease{}, ErrBadTTL
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unreachable[node] {
		return Lease{}, ErrUnreachable
	}
	now := s.clock.Now()
	live := s.lease.Holder != "" && now.Before(s.lease.Expires)
	switch {
	case live && s.lease.Holder == node:
		s.lease.Expires = now.Add(ttl)
	case live:
		return s.lease, ErrHeld
	default:
		s.lease = Lease{Holder: node, Term: s.lease.Term + 1, Expires: now.Add(ttl)}
	}
	return s.lease, nil
}

// Release gives up the lease if node holds it, allowing fast failover.
func (s *LeaseStore) Release(node string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unreachable[node] {
		return ErrUnreachable
	}
	if s.lease.Holder == node {
		s.lease.Holder = ""
		s.lease.Expires = s.clock.Now()
	}
	return nil
}

// Current returns the lease as the store sees it.
func (s *LeaseStore) Current() Lease { s.mu.Lock(); defer s.mu.Unlock(); return s.lease }

// Node is one election candidate.
type Node struct {
	ID string

	store  *LeaseStore
	clock  Clock
	ttl    time.Duration
	margin time.Duration

	mu   sync.Mutex
	view Lease // last lease this node obtained
}

// NewNode creates a candidate. margin makes the node stop considering itself
// leader slightly before its lease expires, to tolerate clock drift between
// the node and the store. It must be smaller than ttl.
func NewNode(id string, store *LeaseStore, clock Clock, ttl, margin time.Duration) (*Node, error) {
	if ttl <= 0 || margin < 0 || margin >= ttl {
		return nil, ErrBadTTL
	}
	return &Node{ID: id, store: store, clock: clock, ttl: ttl, margin: margin}, nil
}

// Tick is one round of the election loop: try to acquire or renew the lease.
// Call it periodically (well within ttl) from a ticker in production.
func (n *Node) Tick() error {
	l, err := n.store.TryAcquire(n.ID, n.ttl)
	if err == nil {
		n.mu.Lock()
		n.view = l
		n.mu.Unlock()
	}
	return err
}

// IsLeader reports whether this node may act as leader right now, judged
// only from its own last successful renewal and its own clock.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.view.Holder == n.ID && n.clock.Now().Before(n.view.Expires.Add(-n.margin))
}

// Term returns the term of this node's last acquired lease.
func (n *Node) Term() uint64 { n.mu.Lock(); defer n.mu.Unlock(); return n.view.Term }

// Resign releases leadership.
func (n *Node) Resign() error {
	n.mu.Lock()
	n.view = Lease{}
	n.mu.Unlock()
	return n.store.Release(n.ID)
}

// Resource is something only the leader should write to. It remembers the
// highest term it has seen and rejects writes from lower terms.
type Resource struct {
	mu      sync.Mutex
	maxTerm uint64
	log     []string
}

// Write appends entry if term is not older than any term seen before.
func (r *Resource) Write(term uint64, entry string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if term < r.maxTerm {
		return ErrStaleTerm
	}
	r.maxTerm = term
	r.log = append(r.log, entry)
	return nil
}

// Log returns the accepted entries.
func (r *Resource) Log() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.log...)
}
