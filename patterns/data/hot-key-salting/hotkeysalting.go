// Package hotkeysalting implements write sharding ("salting") of hot partition
// keys, with adaptive detection so only the keys that need it pay for it.
//
// Partitioned stores such as DynamoDB, Cassandra and Bigtable hash each
// partition key to one partition, and each partition can absorb only so many
// writes per second (DynamoDB documents roughly 1,000 write units per second
// per partition). When one logical key gets more writes than that (a
// celebrity's post, a flash-sale product, a key that is today's date) those
// writes are throttled, even though the table as a whole is mostly idle.
// Adding partitions doesn't help: the key still hashes to exactly one.
//
// Salting splits the hot logical key into K physical keys, key, key#1 ...
// key#K-1, which hash to different partitions. Each write goes to one suffix
// (random or round-robin), so the key's write rate is divided by about K.
// Reads scatter-gather all K physical keys and combine them with a merge
// function: sum for counters, append or union for item lists.
//
// Salting every key would make every read cost K lookups, so the Salter can
// run adaptively: a per-key write-rate counter watches each key over a short
// window, and only a key that crosses a threshold is salted. Cold keys stay
// single-lookup.
//
// The transition is safe by construction: suffix 0 is the original, unsalted
// key, so when a key is promoted, the writes it took before promotion are
// already one of the K physical keys every read fetches. Nothing is moved and
// no write is lost.
package hotkeysalting

import (
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Separator joins a logical key and its salt suffix. Logical keys must not
// contain it, so "post#3" can never be mistaken for suffix 3 of "post".
const Separator = "#"

// ErrThrottled is returned when a write lands on a partition that has already
// used its write capacity for the current second.
var ErrThrottled = errors.New("hotkeysalting: partition write capacity exceeded")

// ErrInvalidKey is returned for an empty key or one containing Separator.
var ErrInvalidKey = errors.New("hotkeysalting: key must be non-empty and must not contain " + Separator)

// PartitionStats counts what one partition did.
type PartitionStats struct {
	Writes    uint64 // accepted writes
	Throttled uint64 // writes rejected because the partition was at capacity
	Reads     uint64 // lookups served
}

type partition[V any] struct {
	items  map[string]V
	second time.Time // the simulated second `used` refers to
	used   int
	stats  PartitionStats
}

// Table simulates a partitioned key-value table: keys are hashed (FNV-1a) to
// one of N partitions, and each partition accepts at most a fixed number of
// writes per second. It is safe for concurrent use.
type Table[V any] struct {
	capacity int
	now      func() time.Time

	mu    sync.Mutex
	parts []partition[V]
}

// NewTable returns a table of n partitions, each accepting writesPerSecond
// writes per clock second. now may be nil to use the real clock.
func NewTable[V any](n, writesPerSecond int, now func() time.Time) (*Table[V], error) {
	if n < 1 {
		return nil, errors.New("hotkeysalting: need at least one partition")
	}
	if writesPerSecond < 1 {
		return nil, errors.New("hotkeysalting: partition write capacity must be at least 1 per second")
	}
	if now == nil {
		now = time.Now
	}
	t := &Table[V]{capacity: writesPerSecond, now: now, parts: make([]partition[V], n)}
	for i := range t.parts {
		t.parts[i].items = make(map[string]V)
	}
	return t, nil
}

// Partitions reports the partition count.
func (t *Table[V]) Partitions() int { return len(t.parts) }

// Capacity reports each partition's writes per second.
func (t *Table[V]) Capacity() int { return t.capacity }

// PartitionFor returns the partition that owns a physical key.
func (t *Table[V]) PartitionFor(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(len(t.parts)))
}

// Write applies update to the value stored under key (ok is false if there is
// none) and stores the result. If the key's partition has already accepted
// Capacity writes in the current second, nothing is stored and ErrThrottled
// is returned.
func (t *Table[V]) Write(key string, update func(old V, ok bool) V) error {
	p := &t.parts[t.PartitionFor(key)]
	sec := t.now().Truncate(time.Second)
	t.mu.Lock()
	defer t.mu.Unlock()
	if !p.second.Equal(sec) {
		p.second, p.used = sec, 0
	}
	if p.used >= t.capacity {
		p.stats.Throttled++
		return ErrThrottled
	}
	p.used++
	p.stats.Writes++
	old, ok := p.items[key]
	p.items[key] = update(old, ok)
	return nil
}

// Read returns the value stored under a physical key. Reads are counted but
// not throttled: in DynamoDB they draw on separate read capacity.
func (t *Table[V]) Read(key string) (V, bool) {
	p := &t.parts[t.PartitionFor(key)]
	t.mu.Lock()
	defer t.mu.Unlock()
	p.stats.Reads++
	v, ok := p.items[key]
	return v, ok
}

// Stats returns a copy of every partition's counters.
func (t *Table[V]) Stats() []PartitionStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]PartitionStats, len(t.parts))
	for i := range t.parts {
		out[i] = t.parts[i].stats
	}
	return out
}

// Mode chooses which keys a Salter salts.
type Mode int

const (
	// Unsalted salts only the keys listed with WithHotKeys (none by default).
	Unsalted Mode = iota
	// SaltAll salts every key with the same K: writes spread, every read fans out.
	SaltAll
	// Adaptive salts keys listed with WithHotKeys plus any key whose write
	// rate crosses the detector threshold. This is the default.
	Adaptive
)

func (m Mode) String() string {
	switch m {
	case Unsalted:
		return "unsalted"
	case SaltAll:
		return "salt-all"
	case Adaptive:
		return "adaptive"
	}
	return "Mode(" + strconv.Itoa(int(m)) + ")"
}

// Stats counts what a Salter did.
type Stats struct {
	Writes     uint64 // logical writes attempted
	Throttled  uint64 // of those, rejected by the table
	Reads      uint64 // logical reads
	Lookups    uint64 // physical reads issued by those logical reads
	Promotions uint64 // keys the detector turned from cold to hot
}

// Option configures a Salter.
type Option func(*options)

type options struct {
	mode      Mode
	shards    int
	threshold int
	window    time.Duration
	hot       []string
	pick      func(n int) int
	now       func() time.Time
}

// WithMode sets which keys get salted. The default is Adaptive.
func WithMode(m Mode) Option { return func(o *options) { o.mode = m } }

// WithShards sets K, the number of physical keys a salted key is split into.
// The default is 8. It must be at least 2.
func WithShards(k int) Option { return func(o *options) { o.shards = k } }

// WithDetector sets the adaptive hot-key detector: a key is promoted once it
// receives threshold write attempts within one window. The default is half a
// partition's per-second capacity over a one-second window, so a key is
// salted well before it can saturate its partition on its own.
func WithDetector(threshold int, window time.Duration) Option {
	return func(o *options) { o.threshold, o.window = threshold, window }
}

// WithHotKeys salts these keys from the start, in every mode: the "we know
// the flash sale starts at noon" case.
func WithHotKeys(keys ...string) Option {
	return func(o *options) { o.hot = append(o.hot, keys...) }
}

// WithPicker sets how a write chooses a suffix in [0, n). The default is
// rand.IntN; pass RoundRobin() or a seeded generator for determinism. The
// picker is called under the Salter's lock, so it need not be thread-safe.
func WithPicker(pick func(n int) int) Option { return func(o *options) { o.pick = pick } }

// WithClock sets the clock the detector uses for its windows.
func WithClock(now func() time.Time) Option { return func(o *options) { o.now = now } }

// RoundRobin returns a picker that cycles 0, 1, ..., n-1 across all calls.
// DynamoDB's guidance calls this a "calculated" suffix when it is derived from
// the item; a counter gives a perfectly even spread.
func RoundRobin() func(n int) int {
	var c atomic.Uint64
	return func(n int) int { return int((c.Add(1) - 1) % uint64(n)) }
}

// Salter writes and reads logical keys over a Table, salting hot keys. It is
// safe for concurrent use.
type Salter[V any] struct {
	table     *Table[V]
	merge     func(a, b V) V
	mode      Mode
	shards    int
	threshold int
	window    time.Duration
	pick      func(n int) int
	now       func() time.Time

	mu          sync.Mutex
	hot         map[string]bool
	windowStart time.Time
	counts      map[string]int // write attempts per cold key in the current window

	writes, throttled, reads, lookups, promotions atomic.Uint64
}

// New returns a Salter over table. merge combines two values of the same
// logical key; it is used both to apply a write to the stored value and to
// combine the K physical values on read, so it must be associative, and the
// order suffixes are merged in must not matter to you (sum, max, set union,
// or append when item order is irrelevant).
func New[V any](table *Table[V], merge func(a, b V) V, opts ...Option) (*Salter[V], error) {
	if table == nil || merge == nil {
		return nil, errors.New("hotkeysalting: table and merge func are required")
	}
	o := options{
		mode:      Adaptive,
		shards:    8,
		threshold: max(1, table.Capacity()/2),
		window:    time.Second,
		pick:      rand.IntN,
		now:       time.Now,
	}
	for _, opt := range opts {
		opt(&o)
	}
	switch {
	case o.mode < Unsalted || o.mode > Adaptive:
		return nil, fmt.Errorf("hotkeysalting: unknown mode %d", int(o.mode))
	case o.shards < 2:
		return nil, errors.New("hotkeysalting: shards must be at least 2")
	case o.threshold < 1 || o.window <= 0:
		return nil, errors.New("hotkeysalting: detector threshold must be >= 1 and window > 0")
	case o.pick == nil || o.now == nil:
		return nil, errors.New("hotkeysalting: picker and clock must not be nil")
	}
	s := &Salter[V]{
		table: table, merge: merge, mode: o.mode, shards: o.shards,
		threshold: o.threshold, window: o.window, pick: o.pick, now: o.now,
		hot: make(map[string]bool), counts: make(map[string]int),
	}
	for _, k := range o.hot {
		if err := validKey(k); err != nil {
			return nil, err
		}
		s.hot[k] = true
	}
	return s, nil
}

func validKey(key string) error {
	if key == "" || strings.Contains(key, Separator) {
		return ErrInvalidKey
	}
	return nil
}

// physical returns the stored key for suffix i. Suffix 0 is the logical key
// itself, which is what makes promotion lossless.
func physical(key string, i int) string {
	if i == 0 {
		return key
	}
	return key + Separator + strconv.Itoa(i)
}

func (s *Salter[V]) fanoutLocked(key string) int {
	if s.mode == SaltAll || s.hot[key] {
		return s.shards
	}
	return 1
}

// observeLocked counts a write attempt for a cold key and promotes it once it
// crosses the threshold within the current window. The window is global and
// the counts are cleared at each rollover, so memory is bounded by the number
// of distinct keys written per window.
func (s *Salter[V]) observeLocked(key string) {
	if s.hot[key] {
		return
	}
	now := s.now()
	if s.windowStart.IsZero() || !now.Before(s.windowStart.Add(s.window)) {
		s.windowStart = now.Truncate(s.window)
		clear(s.counts)
	}
	s.counts[key]++
	if s.counts[key] >= s.threshold {
		s.hot[key] = true
		delete(s.counts, key)
		s.promotions.Add(1)
	}
}

// Write merges delta into the logical key. A salted key's write goes to one
// suffix chosen by the picker. It returns ErrThrottled if that physical key's
// partition is out of capacity this second; the write is then not applied and
// the caller may retry.
func (s *Salter[V]) Write(key string, delta V) error {
	if err := validKey(key); err != nil {
		return err
	}
	s.mu.Lock()
	if s.mode == Adaptive {
		s.observeLocked(key)
	}
	k := s.fanoutLocked(key)
	i := 0
	if k > 1 {
		i = s.pick(k)
	}
	s.mu.Unlock()
	if i < 0 || i >= k {
		return fmt.Errorf("hotkeysalting: picker returned %d for %d shards", i, k)
	}

	s.writes.Add(1)
	err := s.table.Write(physical(key, i), func(old V, ok bool) V {
		if !ok {
			return delta
		}
		return s.merge(old, delta)
	})
	if errors.Is(err, ErrThrottled) {
		s.throttled.Add(1)
	}
	return err
}

// Read scatter-gathers every physical key of the logical key and merges what
// it finds. found is false if none of them exist. A cold key costs one
// lookup; a salted key costs K. Under concurrent writes the result is not a
// point-in-time snapshot, but every write that completed before Read started
// is included.
func (s *Salter[V]) Read(key string) (v V, found bool, err error) {
	if err := validKey(key); err != nil {
		return v, false, err
	}
	s.mu.Lock()
	k := s.fanoutLocked(key)
	s.mu.Unlock()
	s.reads.Add(1)
	s.lookups.Add(uint64(k))
	for i := range k {
		part, ok := s.table.Read(physical(key, i))
		if !ok {
			continue
		}
		if !found {
			v, found = part, true
			continue
		}
		v = s.merge(v, part)
	}
	return v, found, nil
}

// Fanout reports how many physical keys a read of key currently touches.
func (s *Salter[V]) Fanout(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fanoutLocked(key)
}

// PhysicalKeys lists the stored keys a read of key currently touches.
func (s *Salter[V]) PhysicalKeys(key string) []string {
	k := s.Fanout(key)
	out := make([]string, k)
	for i := range k {
		out[i] = physical(key, i)
	}
	return out
}

// HotKeys returns the keys salted individually (listed or detected), sorted.
// In SaltAll mode every key is salted, but only these are listed.
func (s *Salter[V]) HotKeys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.hot))
	for k := range s.hot {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// Stats returns the Salter's counters.
func (s *Salter[V]) Stats() Stats {
	return Stats{
		Writes:     s.writes.Load(),
		Throttled:  s.throttled.Load(),
		Reads:      s.reads.Load(),
		Lookups:    s.lookups.Load(),
		Promotions: s.promotions.Load(),
	}
}

// Sum is a merge func for counters.
func Sum(a, b int64) int64 { return a + b }

// Concat is a merge func for item lists where order across suffixes doesn't
// matter. It never aliases its inputs.
func Concat[T any](a, b []T) []T {
	out := make([]T, 0, len(a)+len(b))
	return append(append(out, a...), b...)
}
