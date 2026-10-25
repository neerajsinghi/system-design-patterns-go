// Package lamportvectorclocks implements Lamport clocks and vector clocks,
// the two classic logical clocks for ordering events without synchronised
// physical time.
//
// A Lamport clock gives every event a number such that if a happened before
// b then L(a) < L(b). The converse does not hold: L(a) < L(b) says nothing
// about whether a could have influenced b. Lamport timestamps plus a node ID
// tie-breaker give a total order consistent with causality.
//
// A vector clock keeps one counter per node. Comparing two vectors tells you
// exactly whether one event happened before the other, after it, or whether
// they are concurrent, which is what replicated stores need to detect
// conflicting writes.
package lamportvectorclocks

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// LamportClock is a concurrency-safe Lamport clock.
type LamportClock struct {
	mu sync.Mutex
	t  uint64
}

// Tick records a local or send event and returns its timestamp.
func (c *LamportClock) Tick() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t++
	return c.t
}

// Observe records receiving a message stamped remote: the clock jumps past
// both its own value and the sender's.
func (c *LamportClock) Observe(remote uint64) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = max(c.t, remote) + 1
	return c.t
}

// Time returns the current value without advancing it.
func (c *LamportClock) Time() uint64 { c.mu.Lock(); defer c.mu.Unlock(); return c.t }

// LamportStamp is a Lamport time with the node ID used to break ties.
type LamportStamp struct {
	Time uint64
	Node string
}

// Less is a total order over stamps that respects happens-before.
func (a LamportStamp) Less(b LamportStamp) bool {
	if a.Time != b.Time {
		return a.Time < b.Time
	}
	return a.Node < b.Node
}

// Ordering is the result of comparing two vector clocks.
type Ordering int

const (
	// Equal: the same event (or identical knowledge).
	Equal Ordering = iota
	// Before: the receiver happened before the argument.
	Before
	// After: the receiver happened after the argument.
	After
	// Concurrent: neither happened before the other.
	Concurrent
)

// String returns the ordering name.
func (o Ordering) String() string {
	if o < Equal || o > Concurrent {
		return "unknown"
	}
	return [...]string{"equal", "before", "after", "concurrent"}[o]
}

// VectorClock maps node IDs to event counters. Missing entries are zero.
// Values are treated as immutable: methods return new clocks.
type VectorClock map[string]uint64

// Copy returns an independent copy.
func (v VectorClock) Copy() VectorClock {
	out := make(VectorClock, len(v))
	for k, n := range v {
		out[k] = n
	}
	return out
}

// Increment returns a copy with node's counter increased by one.
func (v VectorClock) Increment(node string) VectorClock {
	out := v.Copy()
	out[node]++
	return out
}

// Merge returns the element-wise maximum of v and o.
func (v VectorClock) Merge(o VectorClock) VectorClock {
	out := v.Copy()
	for k, n := range o {
		if n > out[k] {
			out[k] = n
		}
	}
	return out
}

// Compare reports how v relates to o in the happens-before order.
func (v VectorClock) Compare(o VectorClock) Ordering {
	less, greater := false, false
	for k := range union(v, o) {
		switch a, b := v[k], o[k]; {
		case a < b:
			less = true
		case a > b:
			greater = true
		}
	}
	switch {
	case less && greater:
		return Concurrent
	case less:
		return Before
	case greater:
		return After
	default:
		return Equal
	}
}

func union(a, b VectorClock) map[string]struct{} {
	keys := make(map[string]struct{}, len(a)+len(b))
	for k := range a {
		keys[k] = struct{}{}
	}
	for k := range b {
		keys[k] = struct{}{}
	}
	return keys
}

// String renders the clock with sorted keys, e.g. "{A:2 B:1}".
func (v VectorClock) String() string {
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s:%d", k, v[k])
	}
	return "{" + strings.Join(parts, " ") + "}"
}

// Stamp is the pair of timestamps attached to one event.
type Stamp struct {
	Lamport LamportStamp
	Vector  VectorClock
}

// Message carries the sender's timestamps.
type Message struct {
	From  string
	Body  string
	Stamp Stamp
}

// Process is a node that maintains both clocks.
type Process struct {
	ID string

	mu      sync.Mutex
	lamport LamportClock
	vector  VectorClock
}

// NewProcess returns a process with zeroed clocks.
func NewProcess(id string) *Process { return &Process{ID: id, vector: VectorClock{}} }

// Local records an internal event.
func (p *Process) Local() Stamp {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.vector = p.vector.Increment(p.ID)
	return Stamp{LamportStamp{p.lamport.Tick(), p.ID}, p.vector.Copy()}
}

// Send records a send event and returns the message to deliver.
func (p *Process) Send(body string) Message {
	return Message{From: p.ID, Body: body, Stamp: p.Local()}
}

// Receive records delivery of m: merge the sender's knowledge, then count
// the receive as a new local event.
func (p *Process) Receive(m Message) Stamp {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.vector = p.vector.Merge(m.Stamp.Vector).Increment(p.ID)
	return Stamp{LamportStamp{p.lamport.Observe(m.Stamp.Lamport.Time), p.ID}, p.vector.Copy()}
}
