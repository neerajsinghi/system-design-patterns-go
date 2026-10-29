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
//
// The session state is small (two log positions and a timestamp), so it can
// be serialised into an opaque, optionally HMAC-signed token with
// (*Session).Token and restored on any stateless app server with
// (*Cluster).ResumeSession. A session can also opt into monotonic reads
// (never read an older log prefix than one it has already seen), or use the
// simpler "read from the leader for a window after a write" strategy instead
// of version tracking.
package readreplicas

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
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

	tokenKey []byte // HMAC key for session tokens. nil means unsigned tokens
	keySet   bool
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

// WithTokenKey makes session tokens HMAC-SHA256 signed with key, so a client
// cannot forge or edit its own consistency state. Every app server that
// resumes tokens must be configured with the same key. The key must be at
// least 16 bytes.
func WithTokenKey(key []byte) Option {
	return func(c *Cluster) {
		c.tokenKey = append([]byte(nil), key...)
		c.keySet = true
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
	if c.keySet && len(c.tokenKey) < 16 {
		return nil, errors.New("readreplicas: token key must be at least 16 bytes")
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
	v, _ := c.write(key, val)
	return v
}

func (c *Cluster) write(key, val string) (Version, time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	at := c.now()
	c.leader[key] = val
	c.log = append(c.log, logEntry{at: at, key: key, val: val})
	return Version(len(c.log)), at
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
	val, ok, from, _ = c.readAtLeast(key, minVersion)
	return val, ok, from
}

// readAtLeast is ReadAtLeast that also reports the log prefix the answering
// node had applied, which a session needs for monotonic reads.
func (c *Cluster) readAtLeast(key string, minVersion Version) (val string, ok bool, from string, served Version) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for range c.followers {
		f := c.followers[c.next%len(c.followers)]
		c.next++
		c.catchUp(f)
		if f.applied >= minVersion {
			c.stats.FollowerReads++
			val, ok = f.data[key]
			return val, ok, f.name, f.applied
		}
	}
	return c.readLeaderLocked(key)
}

func (c *Cluster) readLeader(key string) (val string, ok bool, from string, served Version) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readLeaderLocked(key)
}

func (c *Cluster) readLeaderLocked(key string) (val string, ok bool, from string, served Version) {
	c.stats.LeaderReads++
	val, ok = c.leader[key]
	return val, ok, "leader", Version(len(c.log))
}

// Head returns the leader's latest log version.
func (c *Cluster) Head() Version {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Version(len(c.log))
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
// version of its latest write. It is safe for concurrent use. Token and
// ResumeSession move that state between stateless app servers.
type Session struct {
	c *Cluster

	monotonic   bool
	stickyFor   time.Duration // > 0: leader-after-write strategy instead of version tracking
	mu          sync.Mutex
	lastWrite   Version
	lastWriteAt time.Time // meaningful only once lastWrite > 0
	lastRead    Version   // highest log prefix this session has read from
}

// SessionOption configures a Session.
type SessionOption func(*Session)

// WithMonotonicReads makes the session remember the highest log version it
// has read and never read from a replica that is behind it, so consecutive
// reads cannot go back in time.
func WithMonotonicReads() SessionOption {
	return func(s *Session) { s.monotonic = true }
}

// WithLeaderAfterWrite replaces version tracking with a simpler time-based
// strategy: for window after the session's last write every read goes to the
// leader, and afterwards reads go to any replica. It needs no versions, but
// it loads the leader even when replicas have caught up, and it breaks if a
// replica lags by more than window. A window <= 0 leaves version tracking on.
func WithLeaderAfterWrite(window time.Duration) SessionOption {
	return func(s *Session) {
		if window > 0 {
			s.stickyFor = window
		}
	}
}

// NewSession starts a client session on c.
func (c *Cluster) NewSession(opts ...SessionOption) *Session {
	s := &Session{c: c}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Write writes through the leader and remembers the version.
func (s *Session) Write(key, val string) Version {
	v, at := s.c.write(key, val)
	s.mu.Lock()
	if v > s.lastWrite {
		s.lastWrite, s.lastWriteAt = v, at
	}
	s.mu.Unlock()
	return v
}

// Read never returns data older than this session's own last write. With
// WithMonotonicReads it also never returns an older log prefix than an
// earlier read did. With WithLeaderAfterWrite the read-your-writes guarantee
// only holds while replica lag stays under the window.
func (s *Session) Read(key string) (val string, ok bool, from string) {
	s.mu.Lock()
	var minVersion Version
	if s.monotonic {
		minVersion = s.lastRead
	}
	useLeader := false
	if s.stickyFor > 0 {
		useLeader = s.lastWrite > 0 && s.c.now().Sub(s.lastWriteAt) < s.stickyFor
	} else {
		minVersion = max(minVersion, s.lastWrite)
	}
	s.mu.Unlock()

	var served Version
	if useLeader {
		val, ok, from, served = s.c.readLeader(key)
	} else {
		val, ok, from, served = s.c.readAtLeast(key, minVersion)
	}
	s.mu.Lock()
	s.lastRead = max(s.lastRead, served)
	s.mu.Unlock()
	return val, ok, from
}

// State reports the session's consistency state: the version of its last
// write and the highest log version it has read.
func (s *Session) State() (lastWrite, lastRead Version) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastWrite, s.lastRead
}

// Token layout (before base64url): format byte, lastWrite, lastRead,
// lastWriteAt in Unix nanoseconds (ignored when lastWrite is 0), then a 32-byte
// HMAC-SHA256 of everything before it when the cluster has a token key.
const (
	tokenFormat  byte = 1
	tokenBodyLen      = 1 + 8 + 8 + 8
	tokenMACLen       = sha256.Size
)

// ErrInvalidToken is returned by ResumeSession for a token it cannot trust.
var ErrInvalidToken = errors.New("readreplicas: invalid session token")

// Token serialises the session's consistency state into an opaque string
// that can travel in a cookie or header. It holds log positions only, never
// data. Hand it to ResumeSession on any app server to continue the session.
func (s *Session) Token() string {
	s.mu.Lock()
	b := make([]byte, tokenBodyLen, tokenBodyLen+tokenMACLen)
	b[0] = tokenFormat
	binary.BigEndian.PutUint64(b[1:], uint64(s.lastWrite))
	binary.BigEndian.PutUint64(b[9:], uint64(s.lastRead))
	if s.lastWrite > 0 {
		binary.BigEndian.PutUint64(b[17:], uint64(s.lastWriteAt.UnixNano()))
	}
	s.mu.Unlock()
	if s.c.tokenKey != nil {
		b = append(b, s.c.mac(b)...)
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func (c *Cluster) mac(body []byte) []byte {
	h := hmac.New(sha256.New, c.tokenKey)
	h.Write(body)
	return h.Sum(nil)
}

// ResumeSession rebuilds a session from a token produced by Token, typically
// on a different app server than the one that issued it. Options are the
// resuming server's read policy and are not carried in the token. It rejects
// tokens that are malformed, carry a bad signature, or name versions the
// leader has never written.
func (c *Cluster) ResumeSession(token string, opts ...SessionOption) (*Session, error) {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, fmt.Errorf("%w: not base64url", ErrInvalidToken)
	}
	wantLen := tokenBodyLen
	if c.tokenKey != nil {
		wantLen += tokenMACLen
	}
	if len(b) != wantLen {
		return nil, fmt.Errorf("%w: length %d, want %d", ErrInvalidToken, len(b), wantLen)
	}
	if b[0] != tokenFormat {
		return nil, fmt.Errorf("%w: unknown format %d", ErrInvalidToken, b[0])
	}
	if c.tokenKey != nil && !hmac.Equal(b[tokenBodyLen:], c.mac(b[:tokenBodyLen])) {
		return nil, fmt.Errorf("%w: bad signature", ErrInvalidToken)
	}
	lastWrite := Version(binary.BigEndian.Uint64(b[1:]))
	lastRead := Version(binary.BigEndian.Uint64(b[9:]))
	atNanos := int64(binary.BigEndian.Uint64(b[17:]))
	if head := c.Head(); lastWrite > head || lastRead > head {
		return nil, fmt.Errorf("%w: version beyond leader head %d", ErrInvalidToken, head)
	}
	s := c.NewSession(opts...)
	s.lastWrite, s.lastRead = lastWrite, lastRead
	if lastWrite > 0 {
		s.lastWriteAt = time.Unix(0, atNanos)
	}
	return s, nil
}
