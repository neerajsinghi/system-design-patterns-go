package servicediscovery

// This file adds CachingResolver, a client-side resolver that behaves like a
// DNS stub resolver: it caches each answer for a TTL. That brings in the
// stale-endpoint problem. When an instance is removed or crashes, a client
// that resolved before the change keeps sending traffic to the dead address
// until its cached answer expires. A client that ignores TTL (the JVM's old
// "cache forever" default, many connection pools, a hard-coded IP) keeps
// sending to it indefinitely.
//
// The mitigations modelled here:
//   - a short TTL, which bounds the stale window but costs more lookups;
//   - negative feedback (WithFailureEviction): when a call to an endpoint
//     fails, the client evicts that endpoint from its cached answer,
//     quarantines it for a while, and re-resolves. One failed call then
//     cleans up the cache instead of a TTL's worth of failed calls;
//   - health-aware resolution: the source (Registry) only returns instances
//     whose lease is live, unlike a plain DNS zone that keeps serving a
//     record until someone deletes it.

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// CachingStats counts resolver activity.
type CachingStats struct {
	Lookups   uint64 // queries sent to the source (registry / DNS server)
	CacheHits uint64 // answers served from the cache
	Evictions uint64 // endpoints evicted after a reported failure
}

// CachingOption configures a CachingResolver.
type CachingOption func(*CachingResolver)

// WithResolverClock injects the clock used for TTL and quarantine expiry.
func WithResolverClock(now func() time.Time) CachingOption {
	return func(r *CachingResolver) {
		if now != nil {
			r.now = now
		}
	}
}

// WithIgnoreTTL makes the resolver keep the first answer it gets for each
// service forever, like a JVM with networkaddress.cache.ttl=-1. It exists to
// demonstrate the failure mode. With WithFailureEviction, a reported failure
// still forces a re-resolve.
func WithIgnoreTTL() CachingOption {
	return func(r *CachingResolver) { r.ignoreTTL = true }
}

// WithFailureEviction turns on negative feedback. ReportFailure removes the
// endpoint from the cached answer, quarantines its address for the given
// duration (so a source that still lists it, for example a registry whose
// lease hasn't expired yet, can't put it straight back), and forces the next
// Pick to re-resolve.
func WithFailureEviction(quarantine time.Duration) CachingOption {
	return func(r *CachingResolver) { r.evict, r.quarantine = true, quarantine }
}

// CachingResolver is a DNS-style client-side resolver. It is safe for
// concurrent use.
type CachingResolver struct {
	src        Lookuper
	ttl        time.Duration
	now        func() time.Time
	ignoreTTL  bool
	evict      bool
	quarantine time.Duration

	mu     sync.Mutex
	cache  map[string]*cachedAnswer
	banned map[string]map[string]time.Time // service -> addr -> quarantined until
	next   atomic.Uint64

	lookups, hits, evictions atomic.Uint64
}

type cachedAnswer struct {
	instances []Instance
	expiresAt time.Time
	forceNext bool // a failure was reported: re-resolve on next use
}

// NewCachingResolver returns a resolver over src that caches answers for
// ttl. A ttl of 0 disables caching, so every Pick queries src.
func NewCachingResolver(src Lookuper, ttl time.Duration, opts ...CachingOption) (*CachingResolver, error) {
	if src == nil {
		return nil, errors.New("servicediscovery: nil source")
	}
	if ttl < 0 {
		return nil, errors.New("servicediscovery: ttl must be >= 0")
	}
	r := &CachingResolver{
		src:    src,
		ttl:    ttl,
		now:    time.Now,
		cache:  make(map[string]*cachedAnswer),
		banned: make(map[string]map[string]time.Time),
	}
	for _, o := range opts {
		o(r)
	}
	if r.evict && r.quarantine <= 0 {
		return nil, errors.New("servicediscovery: failure eviction needs a positive quarantine")
	}
	return r, nil
}

// Stats returns a snapshot of the counters.
func (r *CachingResolver) Stats() CachingStats {
	return CachingStats{Lookups: r.lookups.Load(), CacheHits: r.hits.Load(), Evictions: r.evictions.Load()}
}

// Resolve returns the (possibly cached) endpoints of service. The slice is
// shared, so don't modify it.
func (r *CachingResolver) Resolve(service string) ([]Instance, error) {
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.cache[service]; ok && !e.forceNext && (r.ignoreTTL || now.Before(e.expiresAt)) {
		r.hits.Add(1)
		return e.instances, nil
	}
	r.lookups.Add(1)
	list := r.filterBanned(service, r.src.Lookup(service), now)
	if len(list) == 0 {
		// Like the existing Resolver, don't cache an empty answer, so a
		// service that is just starting becomes reachable quickly. (Real DNS
		// caches NXDOMAIN too, for the zone's negative TTL.)
		delete(r.cache, service)
		return nil, ErrNoInstances
	}
	if r.ttl > 0 || r.ignoreTTL {
		r.cache[service] = &cachedAnswer{instances: list, expiresAt: now.Add(r.ttl)}
	}
	return list, nil
}

// filterBanned drops quarantined addresses. If that would leave nothing, it
// returns the full list: trying a possibly-dead endpoint beats failing every
// call outright (Envoy calls this panic mode).
func (r *CachingResolver) filterBanned(service string, list []Instance, now time.Time) []Instance {
	bans := r.banned[service]
	if len(bans) == 0 {
		return list
	}
	out := make([]Instance, 0, len(list))
	for _, in := range list {
		if until, ok := bans[in.Addr]; ok && now.Before(until) {
			continue
		}
		out = append(out, in)
	}
	for addr, until := range bans {
		if !now.Before(until) {
			delete(bans, addr)
		}
	}
	if len(out) == 0 {
		return list
	}
	return out
}

// Pick resolves service and returns one endpoint, round robin.
func (r *CachingResolver) Pick(service string) (Instance, error) {
	list, err := r.Resolve(service)
	if err != nil {
		return Instance{}, err
	}
	n := r.next.Add(1) - 1
	return list[n%uint64(len(list))], nil
}

// ReportFailure tells the resolver that a call to inst failed at the
// connection level (refused, reset, timeout). It does nothing unless
// WithFailureEviction is set. Don't report application errors such as a
// 500: they don't mean the address is gone.
func (r *CachingResolver) ReportFailure(service string, inst Instance) {
	if !r.evict {
		return
	}
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	bans := r.banned[service]
	if bans == nil {
		bans = make(map[string]time.Time)
		r.banned[service] = bans
	}
	bans[inst.Addr] = now.Add(r.quarantine)
	if e, ok := r.cache[service]; ok {
		for _, in := range e.instances {
			if in.Addr == inst.Addr {
				r.evictions.Add(1)
				break
			}
		}
		// The next Resolve re-queries the source and filters out the
		// quarantined address, so the cached answer is replaced as a whole.
		e.forceNext = true
	}
}
