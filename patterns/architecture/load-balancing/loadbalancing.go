// Package loadbalancing implements classic backend-selection strategies
// behind one interface: round robin, smooth weighted round robin, least
// connections, power of two random choices, least response time (EWMA
// latency × in-flight), and IP-hash client affinity (modulo or rendezvous
// hashing).
package loadbalancing

import (
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
)

// ErrNoBackends is returned when a balancer has nothing to pick from.
var ErrNoBackends = errors.New("loadbalancing: no backends")

// Backend is a destination. Its in-flight counter is maintained by the
// balancers through the done callback returned from Pick.
type Backend struct {
	Name   string
	Weight int // used by WeightedRoundRobin. Values < 1 count as 1
	active atomic.Int64
}

// Active reports the number of requests currently in flight.
func (b *Backend) Active() int64 { return b.active.Load() }

// Balancer picks a backend for one request. The caller must call done once
// the request finishes, so connection-aware strategies see accurate load.
// Every Balancer here is safe for concurrent use.
type Balancer interface {
	Pick() (b *Backend, done func(), err error)
}

func acquire(b *Backend) (*Backend, func(), error) {
	b.active.Add(1)
	var once sync.Once
	return b, func() { once.Do(func() { b.active.Add(-1) }) }, nil
}

// RoundRobin cycles through backends in order. It is simple and fair when
// backends and requests are uniform.
type RoundRobin struct {
	backends []*Backend
	next     atomic.Uint64
}

// NewRoundRobin returns a round-robin balancer.
func NewRoundRobin(backends ...*Backend) *RoundRobin { return &RoundRobin{backends: backends} }

// Pick implements Balancer.
func (r *RoundRobin) Pick() (*Backend, func(), error) {
	if len(r.backends) == 0 {
		return nil, nil, ErrNoBackends
	}
	n := r.next.Add(1) - 1
	return acquire(r.backends[n%uint64(len(r.backends))])
}

// WeightedRoundRobin is the "smooth" weighted round robin used by NGINX.
// Weights 5,1,1 yield a,a,b,a,c,a,a rather than a,a,a,a,a,b,c, so heavy
// backends are not hit in bursts.
type WeightedRoundRobin struct {
	mu       sync.Mutex
	backends []*Backend
	current  []int
	total    int
}

// NewWeightedRoundRobin returns a smooth weighted round-robin balancer.
func NewWeightedRoundRobin(backends ...*Backend) *WeightedRoundRobin {
	w := &WeightedRoundRobin{backends: backends, current: make([]int, len(backends))}
	for _, b := range backends {
		w.total += weight(b)
	}
	return w
}

func weight(b *Backend) int { return max(b.Weight, 1) }

// Pick implements Balancer. Each pick adds every backend's weight to its
// running score, chooses the highest score, and subtracts the total from
// the winner.
func (w *WeightedRoundRobin) Pick() (*Backend, func(), error) {
	if len(w.backends) == 0 {
		return nil, nil, ErrNoBackends
	}
	w.mu.Lock()
	best := 0
	for i, b := range w.backends {
		w.current[i] += weight(b)
		if w.current[i] > w.current[best] {
			best = i
		}
	}
	w.current[best] -= w.total
	w.mu.Unlock()
	return acquire(w.backends[best])
}

// LeastConnections sends each request to the backend with the fewest
// in-flight requests. It adapts to slow backends and uneven request costs,
// but it scans every backend on every pick. Ties are broken by rotating the
// scan's starting point. Otherwise an idle pool would send everything to the
// first backend.
type LeastConnections struct {
	mu       sync.Mutex // makes "find min and increment" atomic
	backends []*Backend
	start    int
}

// NewLeastConnections returns a least-connections balancer.
func NewLeastConnections(backends ...*Backend) *LeastConnections {
	return &LeastConnections{backends: backends}
}

// Pick implements Balancer.
func (l *LeastConnections) Pick() (*Backend, func(), error) {
	n := len(l.backends)
	if n == 0 {
		return nil, nil, ErrNoBackends
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	best := l.backends[l.start]
	for k := 1; k < n; k++ {
		if b := l.backends[(l.start+k)%n]; b.Active() < best.Active() {
			best = b
		}
	}
	l.start = (l.start + 1) % n
	return acquire(best)
}

// PowerOfTwo samples two distinct backends at random and takes the less
// loaded one. It gets close to least-connections balance in O(1), needs no
// global lock, and avoids the herding you get when many balancers with
// slightly stale load data all choose the same "least loaded" node.
type PowerOfTwo struct {
	backends []*Backend
	mu       sync.Mutex // guards rng. *rand.Rand is not goroutine-safe
	rng      *rand.Rand
}

// NewPowerOfTwo returns a P2C balancer. A nil rng uses a randomly seeded one.
// Pass a seeded rng for reproducible tests.
func NewPowerOfTwo(rng *rand.Rand, backends ...*Backend) *PowerOfTwo {
	if rng == nil {
		rng = rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	}
	return &PowerOfTwo{backends: backends, rng: rng}
}

// Pick implements Balancer.
func (p *PowerOfTwo) Pick() (*Backend, func(), error) {
	n := len(p.backends)
	switch n {
	case 0:
		return nil, nil, ErrNoBackends
	case 1:
		return acquire(p.backends[0])
	}
	p.mu.Lock()
	i := p.rng.IntN(n)
	j := p.rng.IntN(n - 1)
	p.mu.Unlock()
	if j >= i { // pick j from the other n-1 slots so i != j
		j++
	}
	a, b := p.backends[i], p.backends[j]
	if b.Active() < a.Active() {
		a = b
	}
	return acquire(a)
}
