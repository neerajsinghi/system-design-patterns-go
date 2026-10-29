// Package cdcinvalidation keeps a cache consistent with a database by tailing
// the database's change log (change data capture) instead of trusting every
// writer to invalidate the cache itself, and by versioning cache entries so
// that a slow reader can never put an old value back.
//
// The usual cache-aside rule, "update the database, then delete the cache
// key", has two holes:
//
//   - It only covers writers that know about the cache. An admin script, a
//     batch job, another service or a data migration that writes the table
//     directly leaves its keys stale until they expire.
//   - It races. A reader misses, reads version 1 from the database and is
//     delayed. A writer commits version 2 and deletes the key. The reader then
//     stores version 1, and that stale value is served until its TTL runs out.
//
// The fix has two halves. First, the database already records every committed
// change in order (MySQL's binlog, Postgres's WAL): an [Invalidator] tails
// that log and deletes (or overwrites) the affected keys, so no writer can be
// forgotten. Second, every cache entry carries the row version it was read at
// (here, the commit's log sequence number), and [Cache.Set] refuses a version
// older than the one it already holds. An invalidation leaves a tombstone
// carrying the version that caused it, so a late set of an older version is
// rejected even after the key was deleted. Versions also make at-least-once
// delivery safe: a duplicate or replayed change can never move an entry
// backwards.
//
// Everything is synchronous and driven by an injectable clock, so the races
// are reproduced by calling steps in a chosen order (see [Scheduler]), not by
// hoping goroutines interleave badly.
package cdcinvalidation

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// Errors.
var (
	ErrEmptyKey   = errors.New("cdcinvalidation: empty key")
	ErrNotFound   = errors.New("cdcinvalidation: row not found")
	ErrBadRewind  = errors.New("cdcinvalidation: cannot rewind past the current position")
	errNilDB      = errors.New("cdcinvalidation: nil db")
	errNilCache   = errors.New("cdcinvalidation: nil cache")
	errTTL        = errors.New("cdcinvalidation: ttl must be positive")
	errTombstone  = errors.New("cdcinvalidation: tombstone ttl must be positive")
	errBatch      = errors.New("cdcinvalidation: batch size must be positive")
	errMode       = errors.New("cdcinvalidation: unknown invalidation mode")
	errNilStepFun = errors.New("cdcinvalidation: step has no function")
)

// Option configures a DB, Cache or Invalidator. Options that do not apply to
// a constructor are ignored by it.
type Option func(*options)

type options struct {
	now          func() time.Time
	tombstoneTTL time.Duration
	unversioned  bool
	mode         Mode
	batch        int
}

// WithClock injects a clock. The DB stamps commits with it, the Cache uses it
// for TTLs and the Invalidator for its lag in time.
func WithClock(now func() time.Time) Option {
	return func(o *options) {
		if now != nil {
			o.now = now
		}
	}
}

// WithTombstoneTTL sets how long a Cache keeps an invalidation tombstone
// (default: the cache TTL). It must be longer than the slowest
// read-from-database-then-set, or a very late stale set can slip in after the
// tombstone is gone.
func WithTombstoneTTL(d time.Duration) Option { return func(o *options) { o.tombstoneTTL = d } }

// WithoutVersionCheck turns a Cache into a plain cache-aside cache: Set
// always overwrites and Invalidate just deletes. It exists to show the race
// that versioned sets fix.
func WithoutVersionCheck() Option { return func(o *options) { o.unversioned = true } }

// WithMode chooses whether an Invalidator deletes keys (default) or writes the
// new value into the cache.
func WithMode(m Mode) Option { return func(o *options) { o.mode = m } }

// WithBatchSize caps how many changes one Invalidator.Poll applies
// (default 100). A small batch models a consumer slower than the write rate.
func WithBatchSize(n int) Option { return func(o *options) { o.batch = n } }

func buildOptions(opts []Option) options {
	o := options{now: time.Now, mode: ModeDelete, batch: 100}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// ---------------------------------------------------------------------------
// Database with a change log.

// Row is a committed row and the version (LSN of the commit) that wrote it.
type Row struct {
	Value   string
	Version uint64
}

// Change is one committed change, as a CDC connector would publish it.
// Version is the commit's log sequence number: it increases with every commit,
// so it orders changes to the same row.
type Change struct {
	Version     uint64
	Key         string
	Value       string
	Deleted     bool
	CommittedAt time.Time
}

// DB is an in-memory key-value table that appends every committed change to
// an ordered change log, like a binlog or WAL. It is safe for concurrent use.
type DB struct {
	now func() time.Time

	mu   sync.RWMutex
	rows map[string]Row
	log  []Change // log[i].Version == i+1
}

// NewDB returns an empty database. WithClock is the relevant option.
func NewDB(opts ...Option) *DB {
	o := buildOptions(opts)
	return &DB{now: o.now, rows: make(map[string]Row)}
}

func (d *DB) commit(key, value string, deleted bool) Change {
	c := Change{Version: uint64(len(d.log)) + 1, Key: key, Value: value, Deleted: deleted, CommittedAt: d.now()}
	d.log = append(d.log, c)
	if deleted {
		delete(d.rows, key)
	} else {
		d.rows[key] = Row{Value: value, Version: c.Version}
	}
	return c
}

// Write inserts or updates a row. The row change and its log entry are made
// atomically, so the log is exactly the commit order.
func (d *DB) Write(key, value string) (Change, error) {
	if key == "" {
		return Change{}, ErrEmptyKey
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.commit(key, value, false), nil
}

// Delete removes a row and logs the deletion.
func (d *DB) Delete(key string) (Change, error) {
	if key == "" {
		return Change{}, ErrEmptyKey
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.rows[key]; !ok {
		return Change{}, fmt.Errorf("%w: %s", ErrNotFound, key)
	}
	return d.commit(key, "", true), nil
}

// Read returns the current row.
func (d *DB) Read(key string) (Row, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	r, ok := d.rows[key]
	return r, ok
}

// ChangesSince returns up to limit changes with Version > after, oldest first.
// limit <= 0 means no limit.
func (d *DB) ChangesSince(after uint64, limit int) []Change {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if after >= uint64(len(d.log)) {
		return nil
	}
	rest := d.log[after:]
	if limit > 0 && len(rest) > limit {
		rest = rest[:limit]
	}
	return append([]Change(nil), rest...)
}

// LastVersion returns the version of the latest commit (0 if none).
func (d *DB) LastVersion() uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return uint64(len(d.log))
}

func (d *DB) changeAt(version uint64) (Change, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if version == 0 || version > uint64(len(d.log)) {
		return Change{}, false
	}
	return d.log[version-1], true
}

// ---------------------------------------------------------------------------
// Versioned cache.

// CacheStats counts cache activity.
type CacheStats struct {
	Hits, Misses uint64
	// Sets counts accepted sets; RejectedSets counts sets refused because the
	// cache already held a newer (or equal) version or a newer tombstone.
	Sets, RejectedSets uint64
	// Invalidations counts entries deleted or tombstoned; SkippedInvalidations
	// counts invalidations ignored because the cache already held that version
	// or a newer one (duplicates and replays land here).
	Invalidations, SkippedInvalidations uint64
}

type entry struct {
	value     string
	version   uint64
	tombstone bool
	expires   time.Time
}

// Cache is a TTL cache whose entries carry row versions. It is safe for
// concurrent use; each Set and Invalidate is an atomic compare-and-set on the
// version.
type Cache struct {
	ttl, tombstoneTTL time.Duration
	unversioned       bool
	now               func() time.Time

	mu      sync.Mutex
	entries map[string]entry
	stats   CacheStats
}

// NewCache returns a cache. Options: WithClock, WithTombstoneTTL,
// WithoutVersionCheck.
func NewCache(ttl time.Duration, opts ...Option) (*Cache, error) {
	if ttl <= 0 {
		return nil, errTTL
	}
	o := buildOptions(opts)
	if o.tombstoneTTL == 0 {
		o.tombstoneTTL = ttl
	}
	if o.tombstoneTTL < 0 {
		return nil, errTombstone
	}
	return &Cache{ttl: ttl, tombstoneTTL: o.tombstoneTTL, unversioned: o.unversioned, now: o.now, entries: make(map[string]entry)}, nil
}

// live returns the unexpired entry for key; expired ones are dropped. Caller
// holds c.mu.
func (c *Cache) live(key string) (entry, bool) {
	e, ok := c.entries[key]
	if !ok {
		return entry{}, false
	}
	if !c.now().Before(e.expires) {
		delete(c.entries, key)
		return entry{}, false
	}
	return e, true
}

// Get returns the cached row. A tombstone is a miss.
func (c *Cache) Get(key string) (Row, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.live(key)
	if !ok || e.tombstone {
		c.stats.Misses++
		return Row{}, false
	}
	c.stats.Hits++
	return Row{Value: e.value, Version: e.version}, true
}

// Set stores value at version and reports whether it was accepted. With
// version checks on, it is accepted only if the cache holds nothing for the
// key, an older value, or a tombstone whose version is not newer than this
// one. A tombstone at version v means "anything older than v is stale", so the
// row as of v itself may be stored.
func (c *Cache) Set(key, value string, version uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.unversioned {
		if e, ok := c.live(key); ok {
			if (e.tombstone && version < e.version) || (!e.tombstone && version <= e.version) {
				c.stats.RejectedSets++
				return false
			}
		}
	}
	c.entries[key] = entry{value: value, version: version, expires: c.now().Add(c.ttl)}
	c.stats.Sets++
	return true
}

// Invalidate removes key because the row changed at version. With version
// checks on, it leaves a tombstone at version (blocking later sets of older
// versions) unless the cache already holds that version or a newer one, in
// which case it does nothing: that is what makes duplicate and replayed
// change events harmless. It reports whether anything changed.
func (c *Cache) Invalidate(key string, version uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.unversioned {
		_, ok := c.entries[key]
		delete(c.entries, key)
		if ok {
			c.stats.Invalidations++
		}
		return ok
	}
	if e, ok := c.live(key); ok && e.version >= version {
		c.stats.SkippedInvalidations++
		return false
	}
	c.entries[key] = entry{version: version, tombstone: true, expires: c.now().Add(c.tombstoneTTL)}
	c.stats.Invalidations++
	return true
}

// ReadThrough is the cache-aside read path in one call: Get, and on a miss
// read the database and Set the row at the version read. It returns false if
// the row does not exist.
func (c *Cache) ReadThrough(db *DB, key string) (Row, bool) {
	if r, ok := c.Get(key); ok {
		return r, true
	}
	r, ok := db.Read(key)
	if !ok {
		return Row{}, false
	}
	c.Set(key, r.Value, r.Version)
	return r, true
}

// Stats returns a snapshot of the counters.
func (c *Cache) Stats() CacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

// ---------------------------------------------------------------------------
// CDC invalidator.

// Mode is what an Invalidator does with a change.
type Mode int

const (
	// ModeDelete invalidates the key (with a tombstone); the next read
	// refills it from the database.
	ModeDelete Mode = iota
	// ModeUpdate writes the changed value into the cache at its version, so
	// readers keep hitting. Deletions still invalidate.
	ModeUpdate
)

// Lag is how far the invalidator is behind the database.
type Lag struct {
	// Events is the number of committed changes not yet applied.
	Events uint64
	// Time is how long ago the oldest unapplied change committed: the
	// longest any reader can currently be served a value that change made
	// stale. Zero when caught up.
	Time time.Duration
}

// InvalidatorStats counts applied changes.
type InvalidatorStats struct {
	// Applied counts every change handed to the cache, including duplicates.
	Applied uint64
	// Redelivered counts changes at or below the highest version applied
	// before, i.e. at-least-once duplicates after a Rewind.
	Redelivered uint64
}

// Invalidator tails a DB's change log and applies each change to a Cache.
// Its position is the version of the last change applied. It is safe for
// concurrent use; polls are serialized.
type Invalidator struct {
	db    *DB
	cache *Cache
	mode  Mode
	batch int
	now   func() time.Time

	mu        sync.Mutex
	position  uint64
	highWater uint64
	stats     InvalidatorStats
}

// NewInvalidator returns an invalidator starting at the beginning of the log.
// Options: WithMode, WithBatchSize, WithClock.
func NewInvalidator(db *DB, cache *Cache, opts ...Option) (*Invalidator, error) {
	if db == nil {
		return nil, errNilDB
	}
	if cache == nil {
		return nil, errNilCache
	}
	o := buildOptions(opts)
	if o.batch <= 0 {
		return nil, errBatch
	}
	if o.mode != ModeDelete && o.mode != ModeUpdate {
		return nil, errMode
	}
	return &Invalidator{db: db, cache: cache, mode: o.mode, batch: o.batch, now: o.now}, nil
}

// Apply hands one change to the cache. It is idempotent: applying the same
// change twice, or an older change after a newer one, never moves a cache
// entry back to an older version.
func (inv *Invalidator) Apply(c Change) {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	inv.apply(c)
}

func (inv *Invalidator) apply(c Change) {
	inv.stats.Applied++
	if c.Version <= inv.highWater {
		inv.stats.Redelivered++
	}
	inv.highWater = max(inv.highWater, c.Version)
	if inv.mode == ModeUpdate && !c.Deleted {
		inv.cache.Set(c.Key, c.Value, c.Version)
		return
	}
	inv.cache.Invalidate(c.Key, c.Version)
}

// Poll applies up to the batch size of changes after the current position and
// returns how many it applied.
func (inv *Invalidator) Poll() int {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	changes := inv.db.ChangesSince(inv.position, inv.batch)
	for _, c := range changes {
		inv.apply(c)
		inv.position = c.Version
	}
	return len(changes)
}

// CatchUp polls until the invalidator has applied every committed change and
// returns how many it applied.
func (inv *Invalidator) CatchUp() int {
	total := 0
	for {
		n := inv.Poll()
		if n == 0 {
			return total
		}
		total += n
	}
}

// Rewind moves the position back to version, as when a consumer restarts from
// its last checkpointed offset. The changes after it are delivered again.
func (inv *Invalidator) Rewind(version uint64) error {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	if version > inv.position {
		return ErrBadRewind
	}
	inv.position = version
	return nil
}

// Position returns the version of the last change applied by Poll.
func (inv *Invalidator) Position() uint64 {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	return inv.position
}

// Lag reports how far behind the database the invalidator is.
func (inv *Invalidator) Lag() Lag {
	inv.mu.Lock()
	pos := inv.position
	inv.mu.Unlock()
	last := inv.db.LastVersion()
	if last <= pos {
		return Lag{}
	}
	l := Lag{Events: last - pos}
	if c, ok := inv.db.changeAt(pos + 1); ok {
		l.Time = max(0, inv.now().Sub(c.CommittedAt))
	}
	return l
}

// Stats returns a snapshot of the counters.
func (inv *Invalidator) Stats() InvalidatorStats {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	return inv.stats
}

// ---------------------------------------------------------------------------
// Deterministic interleavings.

// Step is one action by one actor in a scripted interleaving.
type Step struct {
	Actor, Action string
	Do            func() string // returns a short note for the trace (may be "")
}

// Scheduler runs steps strictly in the order they were added. Concurrency
// bugs are reproduced by choosing the order, so every run is identical.
type Scheduler struct {
	steps []Step
}

// Add appends a step.
func (s *Scheduler) Add(actor, action string, do func() string) *Scheduler {
	s.steps = append(s.steps, Step{Actor: actor, Action: action, Do: do})
	return s
}

// Run executes every step in order and writes a numbered trace to w (which
// may be nil).
func (s *Scheduler) Run(w io.Writer) error {
	for i, st := range s.steps {
		if st.Do == nil {
			return fmt.Errorf("%w: step %d", errNilStepFun, i+1)
		}
	}
	for i, st := range s.steps {
		note := st.Do()
		if w != nil {
			if note != "" {
				note = "  -> " + note
			}
			fmt.Fprintf(w, "   %2d. %-8s %-34s%s\n", i+1, st.Actor, st.Action, note)
		}
	}
	return nil
}
