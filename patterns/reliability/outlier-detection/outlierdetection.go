// Package outlierdetection implements passive outlier detection inside a
// client-side load balancer: it watches the latency and errors of real
// requests to each host and temporarily ejects hosts that are much worse than
// their peers.
//
// Active health checks answer "is this process up?". A host that is thrashing
// in garbage collection, sharing a machine with a noisy neighbour, or waiting
// on a failing disk still answers /healthz with 200, so it stays in rotation
// and quietly ruins the tail latency of every request routed to it. A circuit
// breaker per dependency does not help either: the service as a whole is
// fine, only one host is bad, and tripping the breaker would punish the
// healthy hosts too.
//
// A Pool round-robins over its hosts and is told how each request went. Per
// host it keeps an exponentially weighted moving average (EWMA) of successful
// latencies and a counter of consecutive errors. A host is ejected when its
// EWMA exceeds k times the median EWMA of the pool, or when it fails N times
// in a row. Ejection lasts base × (number of ejections), so a host that keeps
// coming back bad is kept out longer each time. A cap on the share of hosts
// that may be ejected at once stops a pool-wide problem from ejecting
// everything. When the ejection expires the host is re-admitted with a clean
// slate and has to prove itself again on live traffic.
package outlierdetection

import (
	"errors"
	"slices"
	"sync"
	"time"
)

// ErrAllEjected is returned by Pick when every host is ejected. With the
// default max-ejection percentage this cannot happen for pools of two or
// more hosts.
var ErrAllEjected = errors.New("outlierdetection: all hosts are ejected")

// Reason says why a host was ejected.
type Reason int

const (
	// ConsecutiveErrors: the host failed ConsecutiveErrors requests in a row.
	ConsecutiveErrors Reason = iota + 1
	// SlowLatency: the host's latency EWMA exceeded k × the pool median.
	SlowLatency
)

func (r Reason) String() string {
	switch r {
	case ConsecutiveErrors:
		return "consecutive errors"
	case SlowLatency:
		return "slow latency"
	default:
		return "none"
	}
}

// EventKind classifies an Event.
type EventKind int

const (
	// Ejected: the host was taken out of rotation.
	Ejected EventKind = iota + 1
	// Readmitted: the host's ejection expired and it is back in rotation.
	Readmitted
	// EjectionCapped: the host qualified for ejection but the max-ejection
	// percentage was already reached, so it stays in rotation.
	EjectionCapped
)

func (k EventKind) String() string {
	switch k {
	case Ejected:
		return "ejected"
	case Readmitted:
		return "readmitted"
	case EjectionCapped:
		return "ejection capped"
	default:
		return "unknown"
	}
}

// Event describes an ejection decision. Events are delivered to the
// WithOnEvent callback after the pool's lock is released.
type Event struct {
	At     time.Time
	Host   string
	Kind   EventKind
	Reason Reason // for Ejected and EjectionCapped
	// Duration is how long the host is ejected for (Ejected only).
	Duration time.Duration
	// Ejections is the ejection multiplier after this event (Ejected only).
	Ejections int
	// EWMA and Median are the host's latency average and the pool median at
	// the time of a SlowLatency decision.
	EWMA, Median time.Duration
}

// HostStatus is a snapshot of one host.
type HostStatus struct {
	Name              string
	Ejected           bool
	EjectedUntil      time.Time
	Ejections         int // current multiplier for the next ejection's duration
	TotalEjections    int
	EWMA              time.Duration
	Samples           int // successful samples since (re)admission
	ConsecutiveErrors int
}

type host struct {
	name           string
	ewma           float64 // nanoseconds
	samples        int
	consecErrs     int
	ejected        bool
	ejectedUntil   time.Time
	ejections      int
	totalEjections int
	admittedAt     time.Time // last re-admission, used to decay ejections
	capReported    bool      // EjectionCapped already reported for this streak
}

// Pool is a round-robin load balancer with passive outlier detection. It is
// safe for concurrent use.
type Pool struct {
	cfg config

	mu      sync.Mutex
	hosts   []*host
	byName  map[string]*host
	next    int
	ejected int
}

type config struct {
	consecutiveErrors int
	latencyFactor     float64
	minLatency        time.Duration
	alpha             float64
	minRequests       int
	minHosts          int
	baseEjection      time.Duration
	maxEjection       time.Duration
	maxEjectPercent   int
	now               func() time.Time
	onEvent           func(Event)
}

// Option configures a Pool.
type Option func(*config)

// WithConsecutiveErrors ejects a host after n consecutive failed requests.
// 0 disables error-based ejection. Default 5 (Envoy's consecutive_5xx).
func WithConsecutiveErrors(n int) Option { return func(c *config) { c.consecutiveErrors = n } }

// WithLatencyFactor ejects a host whose latency EWMA exceeds k × the median
// EWMA of the pool. k must be greater than 1; 0 disables latency-based
// ejection. Default 3.
func WithLatencyFactor(k float64) Option { return func(c *config) { c.latencyFactor = k } }

// WithMinLatency only allows latency ejection when the host's EWMA is also
// above d, so a 3 ms host is not ejected from a pool whose median is 1 ms.
// Default 0.
func WithMinLatency(d time.Duration) Option { return func(c *config) { c.minLatency = d } }

// WithEWMAAlpha sets the weight of each new latency sample, in (0, 1].
// Higher reacts faster but is noisier. Default 0.3.
func WithEWMAAlpha(a float64) Option { return func(c *config) { c.alpha = a } }

// WithMinRequests is how many successful samples a host needs, since it was
// (re)admitted, before its latency is judged. Default 5.
func WithMinRequests(n int) Option { return func(c *config) { c.minRequests = n } }

// WithMinHosts is how many in-rotation hosts with enough samples are needed
// before a median is trusted. Default 3.
func WithMinHosts(n int) Option { return func(c *config) { c.minHosts = n } }

// WithEjectionTime sets the base ejection time (multiplied by the number of
// ejections) and its upper bound. Defaults 30s and 300s, as in Envoy.
func WithEjectionTime(base, maxTime time.Duration) Option {
	return func(c *config) { c.baseEjection, c.maxEjection = base, maxTime }
}

// WithMaxEjectionPercent caps the share of hosts ejected at the same time,
// in 1..100. At least one host may always be ejected. Default 10.
func WithMaxEjectionPercent(p int) Option { return func(c *config) { c.maxEjectPercent = p } }

// WithClock injects the clock used for ejection timing.
func WithClock(now func() time.Time) Option {
	return func(c *config) {
		if now != nil {
			c.now = now
		}
	}
}

// WithOnEvent registers a callback for ejections, re-admissions and capped
// ejections. It runs outside the pool's lock.
func WithOnEvent(f func(Event)) Option { return func(c *config) { c.onEvent = f } }

// New returns a Pool over the named hosts. With both detectors disabled
// (WithConsecutiveErrors(0), WithLatencyFactor(0)) it is a plain round robin.
func New(hosts []string, opts ...Option) (*Pool, error) {
	if len(hosts) == 0 {
		return nil, errors.New("outlierdetection: at least one host is required")
	}
	c := config{
		consecutiveErrors: 5, latencyFactor: 3, alpha: 0.3, minRequests: 5, minHosts: 3,
		baseEjection: 30 * time.Second, maxEjection: 300 * time.Second, maxEjectPercent: 10,
		now: time.Now,
	}
	for _, opt := range opts {
		opt(&c)
	}
	switch {
	case c.consecutiveErrors < 0:
		return nil, errors.New("outlierdetection: consecutive errors must not be negative")
	case c.latencyFactor != 0 && c.latencyFactor <= 1:
		return nil, errors.New("outlierdetection: latency factor must be > 1, or 0 to disable")
	case c.minLatency < 0:
		return nil, errors.New("outlierdetection: min latency must not be negative")
	case c.alpha <= 0 || c.alpha > 1:
		return nil, errors.New("outlierdetection: EWMA alpha must be in (0, 1]")
	case c.minRequests < 1:
		return nil, errors.New("outlierdetection: min requests must be at least 1")
	case c.minHosts < 1:
		return nil, errors.New("outlierdetection: min hosts must be at least 1")
	case c.baseEjection <= 0:
		return nil, errors.New("outlierdetection: base ejection time must be positive")
	case c.maxEjection < c.baseEjection:
		return nil, errors.New("outlierdetection: max ejection time must be >= base ejection time")
	case c.maxEjectPercent < 1 || c.maxEjectPercent > 100:
		return nil, errors.New("outlierdetection: max ejection percent must be in 1..100")
	}
	p := &Pool{cfg: c, byName: make(map[string]*host, len(hosts))}
	for _, name := range hosts {
		if name == "" {
			return nil, errors.New("outlierdetection: host name must not be empty")
		}
		if _, dup := p.byName[name]; dup {
			return nil, errors.New("outlierdetection: duplicate host " + name)
		}
		h := &host{name: name}
		p.hosts = append(p.hosts, h)
		p.byName[name] = h
	}
	return p, nil
}

// Pick returns the next in-rotation host in round-robin order.
func (p *Pool) Pick() (string, error) {
	p.mu.Lock()
	events := p.readmitExpired(p.cfg.now())
	var name string
	for range len(p.hosts) {
		h := p.hosts[p.next]
		p.next = (p.next + 1) % len(p.hosts)
		if !h.ejected {
			name = h.name
			break
		}
	}
	p.mu.Unlock()
	p.emit(events)
	if name == "" {
		return "", ErrAllEjected
	}
	return name, nil
}

// Report records the outcome of one request to host. err != nil counts as a
// failure (a 5xx, a reset, a timeout); only successful requests feed the
// latency average, so a host that fails fast cannot look fast. Reports for
// unknown or currently ejected hosts (late in-flight responses) are ignored.
func (p *Pool) Report(name string, latency time.Duration, err error) {
	p.mu.Lock()
	now := p.cfg.now()
	events := p.readmitExpired(now)
	h := p.byName[name]
	if h != nil && !h.ejected {
		if err != nil {
			h.consecErrs++
			if p.cfg.consecutiveErrors > 0 && h.consecErrs >= p.cfg.consecutiveErrors {
				events = p.eject(h, now, Event{Reason: ConsecutiveErrors}, events)
			}
		} else {
			h.consecErrs = 0
			h.capReported = false
			if h.samples == 0 {
				h.ewma = float64(latency)
			} else {
				h.ewma += p.cfg.alpha * (float64(latency) - h.ewma)
			}
			h.samples++
			events = p.checkLatency(h, now, events)
		}
	}
	p.mu.Unlock()
	p.emit(events)
}

// checkLatency ejects h if it is a latency outlier. Caller holds p.mu.
func (p *Pool) checkLatency(h *host, now time.Time, events []Event) []Event {
	if p.cfg.latencyFactor == 0 || h.samples < p.cfg.minRequests {
		return events
	}
	if time.Duration(h.ewma) <= p.cfg.minLatency {
		return events
	}
	var ewmas []float64
	for _, o := range p.hosts {
		if !o.ejected && o.samples >= p.cfg.minRequests {
			ewmas = append(ewmas, o.ewma)
		}
	}
	if len(ewmas) < p.cfg.minHosts {
		return events
	}
	median := medianOf(ewmas)
	if h.ewma <= p.cfg.latencyFactor*median {
		return events
	}
	return p.eject(h, now, Event{Reason: SlowLatency, EWMA: time.Duration(h.ewma), Median: time.Duration(median)}, events)
}

func medianOf(v []float64) float64 {
	slices.Sort(v)
	n := len(v)
	if n%2 == 1 {
		return v[n/2]
	}
	return (v[n/2-1] + v[n/2]) / 2
}

// maxEjected is how many hosts may be out at once. Caller holds p.mu.
func (p *Pool) maxEjected() int {
	return max(1, len(p.hosts)*p.cfg.maxEjectPercent/100)
}

// eject takes h out of rotation unless the cap is reached. ev carries the
// reason and latency details. Caller holds p.mu.
func (p *Pool) eject(h *host, now time.Time, ev Event, events []Event) []Event {
	ev.At, ev.Host = now, h.name
	if p.ejected >= p.maxEjected() {
		if !h.capReported {
			h.capReported = true
			ev.Kind = EjectionCapped
			events = append(events, ev)
		}
		return events
	}
	// The multiplier decays by one for every base ejection time the host has
	// stayed healthy since it was last re-admitted, so an old ejection does
	// not haunt a host forever.
	if h.ejections > 0 && !h.admittedAt.IsZero() {
		h.ejections = max(0, h.ejections-int(now.Sub(h.admittedAt)/p.cfg.baseEjection))
	}
	h.ejections++
	h.totalEjections++
	d := min(p.cfg.baseEjection*time.Duration(h.ejections), p.cfg.maxEjection)
	h.ejected = true
	h.ejectedUntil = now.Add(d)
	h.capReported = false
	p.ejected++
	ev.Kind, ev.Duration, ev.Ejections = Ejected, d, h.ejections
	return append(events, ev)
}

// readmitExpired returns hosts whose ejection has expired to rotation with a
// clean slate: no samples, no error streak. Caller holds p.mu.
func (p *Pool) readmitExpired(now time.Time) []Event {
	var events []Event
	for _, h := range p.hosts {
		if h.ejected && !now.Before(h.ejectedUntil) {
			h.ejected = false
			h.ewma, h.samples, h.consecErrs = 0, 0, 0
			h.admittedAt = now
			p.ejected--
			events = append(events, Event{At: now, Host: h.name, Kind: Readmitted, Ejections: h.ejections})
		}
	}
	return events
}

func (p *Pool) emit(events []Event) {
	if p.cfg.onEvent == nil {
		return
	}
	for _, e := range events {
		p.cfg.onEvent(e)
	}
}

// Hosts returns a snapshot of every host, in the order given to New.
func (p *Pool) Hosts() []HostStatus {
	p.mu.Lock()
	events := p.readmitExpired(p.cfg.now())
	out := make([]HostStatus, len(p.hosts))
	for i, h := range p.hosts {
		out[i] = HostStatus{
			Name: h.name, Ejected: h.ejected, EjectedUntil: h.ejectedUntil,
			Ejections: h.ejections, TotalEjections: h.totalEjections,
			EWMA: time.Duration(h.ewma), Samples: h.samples, ConsecutiveErrors: h.consecErrs,
		}
	}
	p.mu.Unlock()
	p.emit(events)
	return out
}

// Ejected reports how many hosts are currently out of rotation.
func (p *Pool) Ejected() int {
	p.mu.Lock()
	events := p.readmitExpired(p.cfg.now())
	n := p.ejected
	p.mu.Unlock()
	p.emit(events)
	return n
}
