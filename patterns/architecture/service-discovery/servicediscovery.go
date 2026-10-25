// Package servicediscovery implements a service registry with TTL-based
// leases and a client-side resolver. Instances register themselves and must
// heartbeat before their lease expires. Clients look up healthy instances
// and pick one themselves, with no load balancer in the request path.
package servicediscovery

import (
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Errors returned by the registry and resolver.
var (
	ErrUnknownInstance = errors.New("servicediscovery: unknown or expired instance")
	ErrNoInstances     = errors.New("servicediscovery: no healthy instances")
	ErrInvalid         = errors.New("servicediscovery: service, id and addr are required and ttl must be > 0")
)

// Instance is one running copy of a service.
type Instance struct {
	Service string
	ID      string
	Addr    string
	Meta    map[string]string // copied on Register. Treat it as read-only after Lookup
}

type lease struct {
	inst      Instance
	ttl       time.Duration
	expiresAt time.Time
}

// Registry stores instances under a lease. It is safe for concurrent use.
// Expired instances are invisible to Lookup at once, and Sweep reclaims
// their memory.
type Registry struct {
	mu       sync.RWMutex
	now      func() time.Time
	services map[string]map[string]*lease // service -> id -> lease
}

// NewRegistry returns an empty registry. A nil now uses time.Now.
func NewRegistry(now func() time.Time) *Registry {
	if now == nil {
		now = time.Now
	}
	return &Registry{now: now, services: make(map[string]map[string]*lease)}
}

// Register adds or replaces an instance with a lease of ttl.
func (r *Registry) Register(inst Instance, ttl time.Duration) error {
	if inst.Service == "" || inst.ID == "" || inst.Addr == "" || ttl <= 0 {
		return ErrInvalid
	}
	meta := make(map[string]string, len(inst.Meta))
	for k, v := range inst.Meta {
		meta[k] = v
	}
	inst.Meta = meta
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := r.services[inst.Service]
	if ids == nil {
		ids = make(map[string]*lease)
		r.services[inst.Service] = ids
	}
	ids[inst.ID] = &lease{inst: inst, ttl: ttl, expiresAt: r.now().Add(ttl)}
	return nil
}

// Heartbeat renews an instance's lease. Once a lease has expired, the
// instance must Register again. This forces a restarted process to announce
// its (possibly new) address instead of silently reviving a stale entry.
func (r *Registry) Heartbeat(service, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	l, ok := r.services[service][id]
	now := r.now()
	if !ok || !now.Before(l.expiresAt) {
		return ErrUnknownInstance
	}
	l.expiresAt = now.Add(l.ttl)
	return nil
}

// Deregister removes an instance. Call it on graceful shutdown so clients
// stop routing to it before the lease runs out.
func (r *Registry) Deregister(service, id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.services[service], id)
	if len(r.services[service]) == 0 {
		delete(r.services, service)
	}
}

// Lookup returns the live instances of service, sorted by ID.
func (r *Registry) Lookup(service string) []Instance {
	r.mu.RLock()
	defer r.mu.RUnlock()
	now := r.now()
	var out []Instance
	for _, l := range r.services[service] {
		if now.Before(l.expiresAt) {
			out = append(out, l.inst)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Sweep deletes expired leases and returns how many it removed. Run it
// periodically. Correctness does not depend on it.
func (r *Registry) Sweep() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	removed := 0
	for svc, ids := range r.services {
		for id, l := range ids {
			if !now.Before(l.expiresAt) {
				delete(ids, id)
				removed++
			}
		}
		if len(ids) == 0 {
			delete(r.services, svc)
		}
	}
	return removed
}

// Lookuper is what a Resolver needs from a registry. In production this is
// usually a network client for Consul, etcd, or a DNS SRV lookup.
type Lookuper interface {
	Lookup(service string) []Instance
}

// Resolver does client-side discovery. It caches lookups for CacheTTL, so
// the registry is not queried on every request, and rotates through the
// cached instances round-robin.
type Resolver struct {
	reg      Lookuper
	cacheTTL time.Duration
	now      func() time.Time

	mu    sync.Mutex
	cache map[string]cached
	next  atomic.Uint64
}

type cached struct {
	instances []Instance
	fetchedAt time.Time
}

// NewResolver wraps reg. A cacheTTL of 0 disables caching. A nil now uses
// time.Now.
func NewResolver(reg Lookuper, cacheTTL time.Duration, now func() time.Time) *Resolver {
	if now == nil {
		now = time.Now
	}
	return &Resolver{reg: reg, cacheTTL: cacheTTL, now: now, cache: make(map[string]cached)}
}

// Pick returns one instance of service. Clients see changes in the registry
// at most CacheTTL late. That staleness is the price of not querying the
// registry on every call.
func (c *Resolver) Pick(service string) (Instance, error) {
	instances := c.instances(service)
	if len(instances) == 0 {
		return Instance{}, ErrNoInstances
	}
	n := c.next.Add(1) - 1
	return instances[n%uint64(len(instances))], nil
}

// Invalidate drops the cached entry, for example after a connection error
// to a picked instance.
func (c *Resolver) Invalidate(service string) {
	c.mu.Lock()
	delete(c.cache, service)
	c.mu.Unlock()
}

func (c *Resolver) instances(service string) []Instance {
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.cache[service]; ok && now.Sub(e.fetchedAt) < c.cacheTTL {
		return e.instances
	}
	list := c.reg.Lookup(service)
	// Do not cache empty results. Retry quickly while a service starts up.
	if len(list) > 0 && c.cacheTTL > 0 {
		c.cache[service] = cached{instances: list, fetchedAt: now}
	}
	return list
}
