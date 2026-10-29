package healthcheck

import (
	"errors"
	"sort"
	"sync"
	"time"
)

// ErrHeartbeatConfig is returned by NewHeartbeatMonitor for a non-positive
// interval or maxMisses below 1.
var ErrHeartbeatConfig = errors.New("healthcheck: heartbeat interval must be > 0 and maxMisses >= 1")

// DeadConn is a connection that Sweep declared dead.
type DeadConn struct {
	ID       string
	LastBeat time.Time
	Missed   int // whole intervals that passed without a beat
}

// HeartbeatOption configures a HeartbeatMonitor.
type HeartbeatOption func(*HeartbeatMonitor)

// WithHeartbeatClock injects the clock (default time.Now), so tests and
// simulations can move time by hand.
func WithHeartbeatClock(now func() time.Time) HeartbeatOption {
	return func(m *HeartbeatMonitor) {
		if now != nil {
			m.now = now
		}
	}
}

// WithOnDead registers a callback run by Sweep, outside the monitor's lock,
// for every connection it declares dead. Typically it closes the socket and
// unregisters the connection from its hub.
func WithOnDead(fn func(DeadConn)) HeartbeatOption {
	return func(m *HeartbeatMonitor) { m.onDead = fn }
}

// WithOnLate registers a callback run by Sweep for every connection that has
// missed at least one interval but is not dead yet. A server that pings
// clients can use it to send an extra ping; a dashboard can show it as
// "suspect".
func WithOnLate(fn func(id string, missed int)) HeartbeatOption {
	return func(m *HeartbeatMonitor) { m.onLate = fn }
}

// HeartbeatMonitor tracks liveness of many long-lived connections (WebSocket,
// SSE, TCP sessions) from the server's side. Each connection is expected to
// produce a beat every interval: a client heartbeat message, or the pong to a
// server ping. A connection that misses maxMisses consecutive intervals is
// declared dead by the next Sweep and removed.
//
// Unlike Checker, which answers an orchestrator's pull ("is this instance
// healthy?"), the monitor judges each connection by what it pushes. Unlike
// Detector, which reports a state for cluster peers it keeps forever, the
// monitor reaps: dead connections are handed to OnDead and forgotten, so a
// server with churning clients does not grow without bound.
//
// Nothing runs in the background: call Sweep from a ticker. Safe for
// concurrent use.
type HeartbeatMonitor struct {
	interval  time.Duration
	maxMisses int
	now       func() time.Time
	onDead    func(DeadConn)
	onLate    func(string, int)

	mu   sync.Mutex
	last map[string]time.Time
}

// NewHeartbeatMonitor returns a monitor that expects a beat every interval
// and declares a connection dead after maxMisses intervals without one.
func NewHeartbeatMonitor(interval time.Duration, maxMisses int, opts ...HeartbeatOption) (*HeartbeatMonitor, error) {
	if interval <= 0 || maxMisses < 1 {
		return nil, ErrHeartbeatConfig
	}
	m := &HeartbeatMonitor{interval: interval, maxMisses: maxMisses, now: time.Now, last: map[string]time.Time{}}
	for _, o := range opts {
		o(m)
	}
	return m, nil
}

// Beat records a heartbeat (or pong) from connID, registering it if new.
func (m *HeartbeatMonitor) Beat(connID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.last[connID] = m.now()
}

// Remove forgets connID, e.g. after a clean close. It reports whether it was tracked.
func (m *HeartbeatMonitor) Remove(connID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.last[connID]
	delete(m.last, connID)
	return ok
}

// Missed returns how many whole intervals connID has gone without a beat,
// and false if it is not tracked.
func (m *HeartbeatMonitor) Missed(connID string) (int, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.last[connID]
	if !ok {
		return 0, false
	}
	return m.missed(t, m.now()), true
}

// Len returns the number of tracked connections.
func (m *HeartbeatMonitor) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.last)
}

func (m *HeartbeatMonitor) missed(last, now time.Time) int {
	return int(now.Sub(last) / m.interval)
}

// Sweep removes and returns, sorted by ID, every connection that has missed
// maxMisses or more intervals, calling OnDead for each and OnLate for the
// connections that are behind but still within their allowance.
func (m *HeartbeatMonitor) Sweep() []DeadConn {
	type late struct {
		id     string
		missed int
	}
	var dead []DeadConn
	var lates []late
	m.mu.Lock()
	now := m.now()
	for id, t := range m.last {
		switch n := m.missed(t, now); {
		case n >= m.maxMisses:
			dead = append(dead, DeadConn{ID: id, LastBeat: t, Missed: n})
			delete(m.last, id)
		case n > 0:
			lates = append(lates, late{id, n})
		}
	}
	m.mu.Unlock()

	sort.Slice(dead, func(i, j int) bool { return dead[i].ID < dead[j].ID })
	sort.Slice(lates, func(i, j int) bool { return lates[i].id < lates[j].id })
	if m.onLate != nil {
		for _, l := range lates {
			m.onLate(l.id, l.missed)
		}
	}
	if m.onDead != nil {
		for _, d := range dead {
			m.onDead(d)
		}
	}
	return dead
}
