// Package shardedcounters implements counters that absorb very high write
// rates by splitting one logical value across N shards. Writes go to a random
// shard, so they don't contend. Reads sum every shard.
//
// Two flavours are provided:
//   - Counter: in-process, with shards padded to separate CPU cache lines,
//     so concurrent goroutines don't bounce one cache line between cores.
//   - Distributed: the storage version. One logical counter becomes N rows in
//     a key-value store, because a single row can only take so many writes
//     per second (row locks, hot partitions, per-document write limits).
package shardedcounters

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Counter is an in-memory sharded counter. The zero value is not usable.
// Call New.
type Counter struct {
	shards []paddedInt
}

// paddedInt keeps each shard on its own 64-byte cache line to avoid false
// sharing.
type paddedInt struct {
	n atomic.Int64
	_ [56]byte
}

// New returns a counter with n shards. n < 1 uses GOMAXPROCS.
func New(n int) *Counter {
	if n < 1 {
		n = runtime.GOMAXPROCS(0)
	}
	return &Counter{shards: make([]paddedInt, n)}
}

// Add adds delta to a random shard. math/rand/v2's top-level functions are
// safe for concurrent use and don't share a lock.
func (c *Counter) Add(delta int64) { c.shards[rand.IntN(len(c.shards))].n.Add(delta) }

// Inc adds one.
func (c *Counter) Inc() { c.Add(1) }

// Value sums all shards. Under concurrent writes it is not a point-in-time
// snapshot: increments that land during the sum may or may not be included.
// Once writers stop, it is exact.
func (c *Counter) Value() int64 {
	var total int64
	for i := range c.shards {
		total += c.shards[i].n.Load()
	}
	return total
}

// Shards reports the shard count.
func (c *Counter) Shards() int { return len(c.shards) }

// RowStore is the minimal key-value API the distributed counter needs: an
// atomic per-row increment and a read.
type RowStore interface {
	Increment(row string, delta int64) error
	Get(row string) (int64, error)
}

// ErrInvalid is returned for an empty name or a shard count < 1.
var ErrInvalid = errors.New("shardedcounters: name required and shards must be >= 1")

// Distributed spreads one logical counter over Shards rows named
// "<name>#<i>".
type Distributed struct {
	store  RowStore
	name   string
	shards int
	pick   func(n int) int
}

// NewDistributed returns a distributed counter. pick chooses a shard in
// [0, n). nil uses rand.IntN. Inject a deterministic picker in tests.
func NewDistributed(store RowStore, name string, shards int, pick func(n int) int) (*Distributed, error) {
	if name == "" || shards < 1 || store == nil {
		return nil, ErrInvalid
	}
	if pick == nil {
		pick = rand.IntN
	}
	return &Distributed{store: store, name: name, shards: shards, pick: pick}, nil
}

func (d *Distributed) row(i int) string { return fmt.Sprintf("%s#%d", d.name, i) }

// Add increments one randomly chosen shard row.
func (d *Distributed) Add(delta int64) error {
	return d.store.Increment(d.row(d.pick(d.shards)), delta)
}

// Value reads every shard row and sums them, so a read costs N row reads.
// That is the price of cheap writes. Cache the result, or keep a periodically
// rolled-up total, if reads are frequent.
func (d *Distributed) Value() (int64, error) {
	var total int64
	for i := 0; i < d.shards; i++ {
		n, err := d.store.Get(d.row(i))
		if err != nil {
			return 0, fmt.Errorf("shard %d: %w", i, err)
		}
		total += n
	}
	return total, nil
}

// MemStore is an in-memory RowStore that models per-row write contention.
// Each row has its own lock, and each write holds it for WriteCost. That
// mimics a database where updates to the same row serialise.
type MemStore struct {
	WriteCost time.Duration

	mu   sync.Mutex // guards rows map only
	rows map[string]*memRow
}

type memRow struct {
	mu sync.Mutex
	n  int64
}

// NewMemStore returns a store whose row writes each take writeCost.
func NewMemStore(writeCost time.Duration) *MemStore {
	return &MemStore{WriteCost: writeCost, rows: make(map[string]*memRow)}
}

func (s *MemStore) rowFor(key string) *memRow {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[key]
	if !ok {
		r = &memRow{}
		s.rows[key] = r
	}
	return r
}

// Increment serialises on the row, which is the bottleneck sharding removes.
func (s *MemStore) Increment(key string, delta int64) error {
	r := s.rowFor(key)
	r.mu.Lock()
	defer r.mu.Unlock()
	if s.WriteCost > 0 {
		time.Sleep(s.WriteCost)
	}
	r.n += delta
	return nil
}

// Get returns a row's value. Missing rows read as 0.
func (s *MemStore) Get(key string) (int64, error) {
	r := s.rowFor(key)
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n, nil
}
