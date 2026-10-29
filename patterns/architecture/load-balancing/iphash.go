package loadbalancing

import (
	"errors"
	"hash/fnv"
	"net"
	"sync"
)

// HashMode chooses how IPHash maps a client key to a backend.
type HashMode int

const (
	// HashModulo is hash(key) % n, as in a classic "ip_hash". Removing or
	// adding one backend changes n, so almost every client moves.
	HashModulo HashMode = iota
	// HashRendezvous is rendezvous (highest-random-weight) hashing: each
	// client goes to the backend with the highest hash(key, backend).
	// Removing a backend moves only the clients that were on it. This is the
	// same minimal-movement property a consistent-hash ring gives.
	HashRendezvous
)

// IPHash gives client affinity: the same client key (usually the source IP)
// always reaches the same backend while the pool is unchanged. That allows
// local session state or warm per-client caches, at the cost of balance.
// Load follows how clients are distributed, not live load, and a NAT that
// puts thousands of users behind one IP lands them all on one backend.
//
// IPHash implements Balancer. Pick, without a key, falls back to round robin
// for requests that carry no client identity.
type IPHash struct {
	mode HashMode

	mu       sync.RWMutex
	backends []*Backend
	rr       uint64
}

// NewIPHash returns a client-affinity balancer.
func NewIPHash(mode HashMode, backends ...*Backend) (*IPHash, error) {
	if mode != HashModulo && mode != HashRendezvous {
		return nil, errors.New("loadbalancing: unknown hash mode")
	}
	seen := map[string]bool{}
	for _, b := range backends {
		if b == nil || b.Name == "" || seen[b.Name] {
			return nil, errors.New("loadbalancing: IPHash needs non-nil backends with unique names")
		}
		seen[b.Name] = true
	}
	return &IPHash{mode: mode, backends: append([]*Backend(nil), backends...)}, nil
}

// PickFor routes the request of the client identified by key.
func (h *IPHash) PickFor(key string) (*Backend, func(), error) {
	h.mu.RLock()
	b := h.lookup(key)
	h.mu.RUnlock()
	if b == nil {
		return nil, nil, ErrNoBackends
	}
	return acquire(b)
}

// Pick implements Balancer for requests with no client key (round robin).
func (h *IPHash) Pick() (*Backend, func(), error) {
	h.mu.Lock()
	if len(h.backends) == 0 {
		h.mu.Unlock()
		return nil, nil, ErrNoBackends
	}
	b := h.backends[h.rr%uint64(len(h.backends))]
	h.rr++
	h.mu.Unlock()
	return acquire(b)
}

func (h *IPHash) lookup(key string) *Backend {
	n := len(h.backends)
	if n == 0 {
		return nil
	}
	if h.mode == HashModulo {
		return h.backends[hash64(key, "")%uint64(n)]
	}
	var best *Backend
	var bestW uint64
	for _, b := range h.backends {
		if w := hash64(key, b.Name); best == nil || w > bestW {
			best, bestW = b, w
		}
	}
	return best
}

// Add puts a backend into the pool. It returns false if the name is taken.
func (h *IPHash) Add(b *Backend) bool {
	if b == nil || b.Name == "" {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, x := range h.backends {
		if x.Name == b.Name {
			return false
		}
	}
	h.backends = append(h.backends, b)
	return true
}

// Remove takes the named backend out of the pool (scale-in or failure).
func (h *IPHash) Remove(name string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, x := range h.backends {
		if x.Name == name {
			h.backends = append(h.backends[:i:i], h.backends[i+1:]...)
			return true
		}
	}
	return false
}

// hash64 is FNV-1a over key and backend name, followed by a 64-bit
// finalizer so similar inputs (10.0.0.1, 10.0.0.2) spread well.
func hash64(key, backend string) uint64 {
	f := fnv.New64a()
	f.Write([]byte(key))
	f.Write([]byte{0})
	f.Write([]byte(backend))
	x := f.Sum64()
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}

// ClientKey turns an http.Request.RemoteAddr ("ip:port") into a client key
// (just the IP), so a client's different source ports map to one backend.
// Behind another proxy, derive the key from a trusted X-Forwarded-For hop
// instead.
func ClientKey(remoteAddr string) string {
	if host, _, err := net.SplitHostPort(remoteAddr); err == nil {
		return host
	}
	return remoteAddr
}
