// Package loadshedding rejects excess work early, before it consumes
// resources, so that a server under overload keeps serving the requests it
// does accept with good latency instead of serving everyone slowly.
//
// The Shedder combines three ideas:
//   - Queue-depth limit: at most Limit requests are in flight.
//   - Adaptive limit (AIMD): Limit grows slowly while latency stays under the
//     target and shrinks multiplicatively when it does not, tracking the
//     server's real capacity without manual tuning.
//   - Priority shedding: lower priorities may only use part of the limit, so
//     they are shed first and critical traffic keeps headroom.
package loadshedding

import (
	"errors"
	"sync"
	"time"
)

// ErrShed is returned when a request is rejected to protect the server.
var ErrShed = errors.New("loadshedding: request shed")

// Priority classifies a request. Lower values are more important.
type Priority int

const (
	Critical  Priority = iota // e.g. checkout, health checks from the LB
	Normal                    // ordinary user traffic
	Sheddable                 // prefetch, analytics, batch, retries
)

// Config tunes a Shedder. Zero values get defaults.
type Config struct {
	InitialLimit  int           // starting concurrency limit; default 20
	MinLimit      int           // floor for the adaptive limit; default 1
	MaxLimit      int           // ceiling for the adaptive limit; default 1000
	TargetLatency time.Duration // latency above which the limit shrinks; 0 disables adaptation
	Backoff       float64       // multiplicative decrease factor in (0,1); default 0.9
	// Share is the fraction of Limit each priority may occupy. Default
	// Critical 1.0, Normal 0.8, Sheddable 0.5.
	Share map[Priority]float64
	Now   func() time.Time // default time.Now
}

// Shedder admits or rejects requests. Safe for concurrent use.
type Shedder struct {
	cfg Config

	mu       sync.Mutex
	limit    float64
	inFlight int
	shed     map[Priority]int64
	lastCut  time.Time // when the limit was last decreased
}

// New returns a Shedder.
func New(cfg Config) *Shedder {
	if cfg.MinLimit <= 0 {
		cfg.MinLimit = 1
	}
	if cfg.MaxLimit <= 0 {
		cfg.MaxLimit = 1000
	}
	if cfg.MaxLimit < cfg.MinLimit {
		cfg.MaxLimit = cfg.MinLimit
	}
	if cfg.InitialLimit <= 0 {
		cfg.InitialLimit = 20
	}
	cfg.InitialLimit = min(max(cfg.InitialLimit, cfg.MinLimit), cfg.MaxLimit)
	if !(cfg.Backoff > 0 && cfg.Backoff < 1) {
		cfg.Backoff = 0.9
	}
	share := map[Priority]float64{Critical: 1, Normal: 0.8, Sheddable: 0.5}
	for p, f := range cfg.Share {
		share[p] = min(max(f, 0), 1)
	}
	cfg.Share = share
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Shedder{cfg: cfg, limit: float64(cfg.InitialLimit), shed: map[Priority]int64{}}
}

// capacityFor returns how many in-flight requests priority p may join. mu held.
func (s *Shedder) capacityFor(p Priority) int {
	share, ok := s.cfg.Share[p]
	if !ok {
		share = s.cfg.Share[Sheddable] // unknown priorities are least important
	}
	return int(s.limit * share)
}

// Acquire admits a request of priority p or returns ErrShed. On success the
// caller must call done exactly once when the work finishes; extra calls are
// ignored. done feeds the observed latency into the adaptive limit.
func (s *Shedder) Acquire(p Priority) (done func(), err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inFlight >= s.capacityFor(p) {
		s.shed[p]++
		return nil, ErrShed
	}
	s.inFlight++
	start := s.cfg.Now()
	var once sync.Once
	return func() { once.Do(func() { s.release(start) }) }, nil
}

func (s *Shedder) release(start time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inFlight--
	if s.cfg.TargetLatency <= 0 {
		return
	}
	now := s.cfg.Now()
	if now.Sub(start) > s.cfg.TargetLatency {
		// Multiplicative decrease, at most once per congestion episode: like
		// TCP, ignore slow responses from requests admitted before the last
		// cut, since they reflect the old, larger limit. Without this a single
		// slow batch of N requests would cut the limit N times.
		if !start.Before(s.lastCut) {
			s.limit *= s.cfg.Backoff
			s.lastCut = now
		}
	} else if float64(s.inFlight+1) >= s.limit*0.5 {
		// Additive increase, about +1 per "limit" fast completions. Only grow
		// when the limit is actually being used; otherwise an idle server
		// would inflate its limit without evidence that it can cope.
		s.limit += 1 / s.limit
	}
	s.limit = min(max(s.limit, float64(s.cfg.MinLimit)), float64(s.cfg.MaxLimit))
}

// Stats is a snapshot for metrics.
type Stats struct {
	Limit    int
	InFlight int
	Shed     map[Priority]int64
}

// Stats returns the current limit, in-flight count and shed counters.
func (s *Shedder) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	shed := make(map[Priority]int64, len(s.shed))
	for p, n := range s.shed {
		shed[p] = n
	}
	return Stats{Limit: int(s.limit), InFlight: s.inFlight, Shed: shed}
}
