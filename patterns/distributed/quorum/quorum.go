// Package quorum implements Dynamo-style quorum replication: every key is
// stored on N replicas, a write succeeds once W replicas acknowledge it, and
// a read consults R replicas and returns the newest version it sees. When
// R + W > N every read quorum overlaps every write quorum, so a read always
// includes at least one replica with the latest successful write. Reads also
// perform read repair: replicas that returned a stale version are updated.
//
// Versions are (counter, writer) pairs ordered lexicographically, which is
// last-writer-wins: concurrent writes are resolved deterministically but one
// of them is silently discarded. Systems that must keep both use vector
// clocks (see the lamport-vector-clocks pattern).
package quorum

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// Errors.
var (
	ErrBadConfig   = errors.New("quorum: invalid N/R/W")
	ErrUnavailable = errors.New("quorum: replica unavailable")
	ErrNoQuorum    = errors.New("quorum: not enough replicas responded")
	ErrNotFound    = errors.New("quorum: key not found")
)

// Version orders writes. Counter is the logical time; Writer breaks ties.
type Version struct {
	Counter uint64
	Writer  string
}

// Less reports whether v is older than o.
func (v Version) Less(o Version) bool {
	if v.Counter != o.Counter {
		return v.Counter < o.Counter
	}
	return v.Writer < o.Writer
}

// Value is a versioned value stored on a replica.
type Value struct {
	Data    string
	Version Version
}

// Replica is one storage node that can be taken down.
type Replica struct {
	ID string

	mu   sync.Mutex
	data map[string]Value
	down bool
}

// NewReplica returns an empty, healthy replica.
func NewReplica(id string) *Replica { return &Replica{ID: id, data: map[string]Value{}} }

// SetDown simulates a crash or partition. Data survives.
func (r *Replica) SetDown(down bool) { r.mu.Lock(); r.down = down; r.mu.Unlock() }

// Get returns the replica's local copy of key (zero Value if absent).
func (r *Replica) Get(key string) (Value, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.down {
		return Value{}, false, ErrUnavailable
	}
	v, ok := r.data[key]
	return v, ok, nil
}

// Put stores v unless the replica already has a newer version, so retries
// and out-of-order repairs never move a key backwards.
func (r *Replica) Put(key string, v Value) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.down {
		return ErrUnavailable
	}
	if cur, ok := r.data[key]; ok && !cur.Version.Less(v.Version) {
		return nil
	}
	r.data[key] = v
	return nil
}

// Config holds the replication parameters.
type Config struct{ N, R, W int }

// Validate checks 1 <= R, W <= N.
func (c Config) Validate() error {
	if c.N < 1 || c.R < 1 || c.W < 1 || c.R > c.N || c.W > c.N {
		return fmt.Errorf("%w: %+v", ErrBadConfig, c)
	}
	return nil
}

// Strong reports whether read and write quorums always overlap.
func (c Config) Strong() bool { return c.R+c.W > c.N }

// Store coordinates reads and writes across N replicas.
type Store struct {
	cfg      Config
	replicas []*Replica
	next     atomic.Uint64 // rotates read preference, like a load balancer

	// Repairs counts replicas fixed by read repair.
	repairs atomic.Int64
}

// NewStore validates cfg and requires exactly N replicas.
func NewStore(cfg Config, replicas []*Replica) (*Store, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if len(replicas) != cfg.N {
		return nil, fmt.Errorf("%w: need %d replicas, got %d", ErrBadConfig, cfg.N, len(replicas))
	}
	return &Store{cfg: cfg, replicas: replicas}, nil
}

type response struct {
	replica *Replica
	val     Value
	found   bool
}

// Put writes data for key on behalf of writer. It first reads the current
// version from every reachable replica (at least R must answer) to pick a
// higher one, then sends the write to
// all N replicas in parallel and succeeds once W acknowledge. On ErrNoQuorum
// the write may still have landed on some replicas (there is no rollback),
// so a later read can return it.
func (s *Store) Put(writer, key, data string) (Version, error) {
	resps, err := s.readQuorum(key, 0, s.cfg.N) // ask everyone reachable for the latest version
	if err != nil {
		return Version{}, err
	}
	var latest Version
	for _, r := range resps {
		if r.found && latest.Less(r.val.Version) {
			latest = r.val.Version
		}
	}
	v := Value{Data: data, Version: Version{Counter: latest.Counter + 1, Writer: writer}}

	var wg sync.WaitGroup
	var acks atomic.Int32
	for _, rep := range s.replicas {
		wg.Add(1)
		go func(rep *Replica) {
			defer wg.Done()
			if rep.Put(key, v) == nil {
				acks.Add(1)
			}
		}(rep)
	}
	wg.Wait()
	if int(acks.Load()) < s.cfg.W {
		return v.Version, fmt.Errorf("%w: %d/%d acks for write", ErrNoQuorum, acks.Load(), s.cfg.W)
	}
	return v.Version, nil
}

// Get reads key from R replicas, starting at a rotating offset.
func (s *Store) Get(key string) (Value, error) {
	start := int(s.next.Add(1)-1) % s.cfg.N
	return s.GetFrom(key, start)
}

// GetFrom reads key from the first R reachable replicas in preference order
// starting at replica index start, returns the newest version, and repairs
// contacted replicas that were stale.
func (s *Store) GetFrom(key string, start int) (Value, error) {
	if start < 0 {
		start = 0
	}
	resps, err := s.readQuorum(key, start%s.cfg.N, s.cfg.R)
	if err != nil {
		return Value{}, err
	}
	var newest response
	for _, r := range resps {
		if r.found && (!newest.found || newest.val.Version.Less(r.val.Version)) {
			newest = r
		}
	}
	if !newest.found {
		return Value{}, ErrNotFound
	}
	for _, r := range resps { // read repair
		if !r.found || r.val.Version.Less(newest.val.Version) {
			if r.replica.Put(key, newest.val) == nil {
				s.repairs.Add(1)
			}
		}
	}
	return newest.val, nil
}

// readQuorum asks replicas in order from start until want have answered.
// It fails if fewer than R replicas are reachable.
func (s *Store) readQuorum(key string, start, want int) ([]response, error) {
	var out []response
	for i := 0; i < s.cfg.N && len(out) < want; i++ {
		rep := s.replicas[(start+i)%s.cfg.N]
		v, ok, err := rep.Get(key)
		if err != nil {
			continue
		}
		out = append(out, response{replica: rep, val: v, found: ok})
	}
	if len(out) < s.cfg.R {
		return nil, fmt.Errorf("%w: %d/%d replicas answered read", ErrNoQuorum, len(out), s.cfg.R)
	}
	return out, nil
}

// Repairs returns how many replica copies read repair has fixed.
func (s *Store) Repairs() int64 { return s.repairs.Load() }
