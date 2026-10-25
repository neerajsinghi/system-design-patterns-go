// Package distributedlockfencing implements a lease-based distributed lock
// that issues monotonically increasing fencing tokens, and a storage service
// that rejects writes carrying a token older than one it has already seen.
//
// A lease lock alone is unsafe: a client can acquire the lock, pause (GC,
// page fault, network delay) past the lease, and then write while another
// client holds the lock. The lock service cannot stop a paused client, but
// the storage can: the stale client's token is lower than the new holder's,
// so its write is rejected.
//
// The lock service here is a single in-process object. In production use a
// consensus-backed store (etcd, ZooKeeper, Consul); etcd revisions and
// ZooKeeper zxids make natural fencing tokens.
package distributedlockfencing

import (
	"errors"
	"sync"
	"time"
)

// Clock abstracts time for deterministic tests.
type Clock interface{ Now() time.Time }

// ManualClock is a Clock that moves only when Advance is called.
type ManualClock struct {
	mu sync.Mutex
	t  time.Time
}

// NewManualClock returns a ManualClock set to t.
func NewManualClock(t time.Time) *ManualClock { return &ManualClock{t: t} }

// Now returns the fake time.
func (c *ManualClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }

// Advance moves the fake time forward by d.
func (c *ManualClock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

// Errors.
var (
	ErrLockHeld     = errors.New("distributedlockfencing: lock held by another owner")
	ErrNotHolder    = errors.New("distributedlockfencing: not the current lock holder")
	ErrStaleToken   = errors.New("distributedlockfencing: fencing token is stale")
	ErrInvalidInput = errors.New("distributedlockfencing: invalid argument")
)

// Lock is a granted lease.
type Lock struct {
	Name    string
	Owner   string
	Token   uint64 // strictly increasing across all grants of this lock
	Expires time.Time
}

type lockState struct {
	owner   string
	token   uint64
	expires time.Time
}

// LockService grants named lease locks with fencing tokens.
type LockService struct {
	mu    sync.Mutex
	clock Clock
	locks map[string]*lockState
	last  map[string]uint64 // last token issued per lock, never reused
}

// NewLockService returns an empty lock service.
func NewLockService(clock Clock) *LockService {
	return &LockService{clock: clock, locks: map[string]*lockState{}, last: map[string]uint64{}}
}

// Acquire grants the lock to owner for ttl if it is free or expired. Every
// grant gets a new, higher token.
func (s *LockService) Acquire(name, owner string, ttl time.Duration) (Lock, error) {
	if name == "" || owner == "" || ttl <= 0 {
		return Lock{}, ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	if st, ok := s.locks[name]; ok && now.Before(st.expires) {
		return Lock{}, ErrLockHeld
	}
	s.last[name]++
	st := &lockState{owner: owner, token: s.last[name], expires: now.Add(ttl)}
	s.locks[name] = st
	return Lock{Name: name, Owner: owner, Token: st.token, Expires: st.expires}, nil
}

// Renew extends a lease that is still held. A lease that already expired
// cannot be renewed (someone else may have taken it); acquire again instead.
func (s *LockService) Renew(l Lock, ttl time.Duration) (Lock, error) {
	if ttl <= 0 {
		return Lock{}, ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.locks[l.Name]
	if !ok || st.token != l.Token || !s.clock.Now().Before(st.expires) {
		return Lock{}, ErrNotHolder
	}
	st.expires = s.clock.Now().Add(ttl)
	l.Expires = st.expires
	return l, nil
}

// Release frees the lock if l is still the current grant.
func (s *LockService) Release(l Lock) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.locks[l.Name]
	if !ok || st.token != l.Token {
		return ErrNotHolder
	}
	delete(s.locks, l.Name)
	return nil
}

// Storage is a resource protected by the lock. It remembers the highest
// fencing token seen per key and rejects anything lower. Equal tokens are
// accepted, so the current holder can write many times.
type Storage struct {
	mu      sync.Mutex
	data    map[string]string
	highest map[string]uint64
}

// NewStorage returns empty storage.
func NewStorage() *Storage {
	return &Storage{data: map[string]string{}, highest: map[string]uint64{}}
}

// Write stores value under key if token is not stale.
func (s *Storage) Write(key, value string, token uint64) error {
	if token == 0 {
		return ErrInvalidInput // zero means "no lock held"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if token < s.highest[key] {
		return ErrStaleToken
	}
	s.highest[key] = token
	s.data[key] = value
	return nil
}

// Read returns the current value of key.
func (s *Storage) Read(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	return v, ok
}
