// Package readreplicas simulates leader-follower replication with
// replication lag, and the read-your-writes guarantee built on top of it.
//
// All writes go to the leader, which appends them to a replication log.
// Each follower applies the log asynchronously and trails the leader by its
// own lag. Reads spread across followers to scale out, so a read may be
// stale. A Session remembers the version of its own last write and only
// reads from replicas that have caught up to it, falling back to the
// leader. That gives each client read-your-writes without making every read
// go to the leader.
package readreplicas

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// Version is a position in the leader's replication log (1 = first write).
type Version uint64

type logEntry struct {
	at       time.Time
	key, val string
}

type follower struct {
	name    string
	lag     time.Duration
	applied Version // log entries [0, applied) are applied
	data    map[string]string
}

// Stats counts where reads were served.
type Stats struct {
	FollowerReads, LeaderReads uint64
}

// Cluster is one leader plus followers. It is safe for concurrent use.
type Cluster struct {
	now func() time.Time

	mu        sync.Mutex
	log       []logEntry // the leader's log. A real system truncates it once every follower has it
	leader    map[string]string
	followers []*follower
	next      int // round-robin cursor
	stats     Stats
}

// Option configures a Cluster.
type Option func(*Cluster)

// WithClock injects the clock used to timestamp writes and measure lag.
func WithClock(now func() time.Time) Option {
	return func(c *Cluster) {
		if now != nil {
			c.now = now
		}
	}
}

// New creates a leader and one follower per entry in lags. Each lag is how
// far that follower trails the leader.
func New(lags []time.Duration, opts ...Option) (*Cluster, error) {
	if len(lags) == 0 {
		return nil, errors.New("readreplicas: need at least one follower")
	}
	c := &Cluster{now: time.Now, leader: make(map[string]string)}
	for _, o := range opts {
		o(c)
	}
	for i, lag := range lags {
		if lag < 0 {
			return nil, fmt.Errorf("readreplicas: follower %d has negative lag", i)
		}
		c.followers = append(c.followers, &follower{
			name: fmt.Sprintf("replica-%d", i+1), lag: lag, data: make(map[string]string),
		})
	}
	return c, nil
}

// Write applies a write on the leader and returns its log version.
func (c *Cluster) Write(key, val string) Version {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leader[key] = val
	c.log = append(c.log, logEntry{at: c.now(), key: key, val: val})
	return Version(len(c.log))
}

// catchUp applies every log entry that is at least f.lag old. The caller holds mu.
func (c *Cluster) catchUp(f *follower) {
	cutoff := c.now().Add(-f.lag)
	for int(f.applied) < len(c.log) {
		e := c.log[f.applied]
		if e.at.After(cutoff) {
			break
		}
		f.data[e.key] = e.val
		f.applied++
	}
}

// Read serves an eventually consistent read from the next follower in
// round-robin order. from names the node that answered.
func (c *Cluster) Read(key string) (val string, ok bool, from string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	f := c.followers[c.next%len(c.followers)]
	c.next++
	c.catchUp(f)
	c.stats.FollowerReads++
	val, ok = f.data[key]
	return val, ok, f.name
}

// ReadAtLeast serves a read from a follower that has applied at least
// version minVersion, trying followers round-robin. If none has caught up, the
// leader answers.
func (c *Cluster) ReadAtLeast(key string, minVersion Version) (val string, ok bool, from string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for range c.followers {
		f := c.followers[c.next%len(c.followers)]
		c.next++
		c.catchUp(f)
		if f.applied >= minVersion {
			c.stats.FollowerReads++
			val, ok = f.data[key]
			return val, ok, f.name
		}
	}
	c.stats.LeaderReads++
	val, ok = c.leader[key]
	return val, ok, "leader"
}

// ReplicaVersions returns how far each follower has applied the log right now.
func (c *Cluster) ReplicaVersions() map[string]Version {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]Version, len(c.followers))
	for _, f := range c.followers {
		c.catchUp(f)
		out[f.name] = f.applied
	}
	return out
}

// Stats returns a snapshot of the read counters.
func (c *Cluster) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

// Session gives one client read-your-writes consistency by tracking the
// version of its latest write. It is safe for concurrent use. In a web app
// the version would travel in a cookie or token.
type Session struct {
	c         *Cluster
	mu        sync.Mutex
	lastWrite Version
}

// NewSession starts a client session on c.
func (c *Cluster) NewSession() *Session { return &Session{c: c} }

// Write writes through the leader and remembers the version.
func (s *Session) Write(key, val string) Version {
	v := s.c.Write(key, val)
	s.mu.Lock()
	s.lastWrite = max(s.lastWrite, v)
	s.mu.Unlock()
	return v
}

// Read never returns data older than this session's own last write.
func (s *Session) Read(key string) (val string, ok bool, from string) {
	s.mu.Lock()
	minVersion := s.lastWrite
	s.mu.Unlock()
	return s.c.ReadAtLeast(key, minVersion)
}
