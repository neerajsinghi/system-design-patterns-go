// Package snowflakeid generates unique, roughly time-ordered 64-bit IDs on
// many machines at once, without asking a central service for each one.
//
// A single database sequence hands out perfectly ordered IDs, but every
// insert on every server must reach that one counter: it caps throughput and
// takes ID generation down with it when it fails. Random UUIDv4s need no
// coordination, but they are 128 bits and completely unordered, so every
// insert lands on a random page of a B-tree index and "newest first" needs a
// separate timestamp column.
//
// A Snowflake ID packs three things into one int64:
//
//	 0 | 41 bits: ms since a custom epoch | 10 bits: node | 12 bits: sequence
//	sign                                   (0..1023)      (0..4095 per ms)
//
// The node ID makes two machines' IDs different even in the same
// millisecond, the sequence makes one machine's IDs different within a
// millisecond, and putting the timestamp in the high bits makes IDs sort by
// creation time. The only coordination needed is handing each running
// generator a distinct node ID, once, at start-up.
//
// Two edge cases need a policy. If a node asks for more than 4096 IDs in one
// millisecond, the generator waits for the next millisecond. If the clock
// steps backwards (an NTP correction, a VM migration), reusing old
// timestamps could repeat IDs, so the generator waits out small regressions
// and refuses with ErrClockMovedBackwards beyond a tolerance.
package snowflakeid

import (
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"time"
)

// Bit layout.
const (
	TimestampBits = 41
	NodeBits      = 10
	SequenceBits  = 12

	MaxNode      = 1<<NodeBits - 1      // 1023
	MaxSequence  = 1<<SequenceBits - 1  // 4095
	MaxTimestamp = 1<<TimestampBits - 1 // ~69.7 years of milliseconds

	nodeShift = SequenceBits
	timeShift = SequenceBits + NodeBits
)

// DefaultEpoch is the epoch used when WithEpoch is not given. A recent epoch
// leaves most of the 41-bit range for the future.
var DefaultEpoch = time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)

// DefaultClockTolerance is how far the clock may step backwards before Next
// refuses instead of waiting.
const DefaultClockTolerance = 10 * time.Millisecond

var (
	// ErrClockMovedBackwards is returned when the clock is behind the last
	// issued timestamp by more than the tolerance.
	ErrClockMovedBackwards = errors.New("snowflakeid: clock moved backwards")
	// ErrBeforeEpoch is returned when the clock reads earlier than the epoch.
	ErrBeforeEpoch = errors.New("snowflakeid: clock is before the epoch")
	// ErrTimestampOverflow is returned once the 41-bit timestamp is used up.
	ErrTimestampOverflow = errors.New("snowflakeid: timestamp exceeds 41 bits, epoch exhausted")
)

// Lifetime is how long an epoch lasts: 2^41 milliseconds, about 69.7 years.
const Lifetime = time.Duration(MaxTimestamp+1) * time.Millisecond

// ID is a Snowflake ID. Sorting IDs numerically sorts them by timestamp, then
// node, then sequence.
type ID int64

// Compose builds an ID from its fields, which must be within their bit
// widths.
func Compose(millis, node, seq int64) (ID, error) {
	switch {
	case millis < 0 || millis > MaxTimestamp:
		return 0, fmt.Errorf("snowflakeid: timestamp %d out of range 0..%d", millis, int64(MaxTimestamp))
	case node < 0 || node > MaxNode:
		return 0, fmt.Errorf("snowflakeid: node %d out of range 0..%d", node, MaxNode)
	case seq < 0 || seq > MaxSequence:
		return 0, fmt.Errorf("snowflakeid: sequence %d out of range 0..%d", seq, MaxSequence)
	}
	return ID(millis<<timeShift | node<<nodeShift | seq), nil
}

// Millis is the timestamp field: milliseconds since the generator's epoch.
func (id ID) Millis() int64 { return int64(id) >> timeShift }

// Node is the node ID field.
func (id ID) Node() int64 { return int64(id) >> nodeShift & MaxNode }

// Seq is the sequence field.
func (id ID) Seq() int64 { return int64(id) & MaxSequence }

// Time decodes the creation time assuming DefaultEpoch. Use TimeFrom for IDs
// made with a custom epoch: the epoch is not stored in the ID.
func (id ID) Time() time.Time { return id.TimeFrom(DefaultEpoch) }

// TimeFrom decodes the creation time relative to epoch.
func (id ID) TimeFrom(epoch time.Time) time.Time {
	return epoch.Add(time.Duration(id.Millis()) * time.Millisecond)
}

// String formats the ID in decimal. Systems that hand IDs to JavaScript send
// them as strings, because a float64 only holds 53 bits exactly.
func (id ID) String() string { return strconv.FormatInt(int64(id), 10) }

// Parse reads a decimal ID as produced by String.
func Parse(s string) (ID, error) {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("snowflakeid: parse %q: %w", s, err)
	}
	if v < 0 {
		return 0, fmt.Errorf("snowflakeid: parse %q: negative ID", s)
	}
	return ID(v), nil
}

// Stats counts what the generator had to do.
type Stats struct {
	Generated       uint64 // IDs returned
	OverflowWaits   uint64 // times the 4096 sequence ran out and Next waited for the next ms
	BackwardsWaits  uint64 // clock regressions within tolerance that Next waited out
	BackwardsErrors uint64 // calls refused with ErrClockMovedBackwards
}

// Generator issues IDs for one node. It is safe for concurrent use.
type Generator struct {
	node      int64
	epoch     time.Time
	epochMs   int64
	now       func() time.Time
	tolerance time.Duration
	yield     func()

	mu     sync.Mutex
	lastMs int64 // timestamp of the last issued ID, -1 before the first
	seq    int64
	stats  Stats
}

// Option configures a Generator.
type Option func(*options)

type options struct {
	epoch     time.Time
	now       func() time.Time
	tolerance time.Duration
	fakeClock bool
}

// WithEpoch sets the custom epoch. It must not be in the future, and every
// generator whose IDs are compared must use the same one.
func WithEpoch(epoch time.Time) Option { return func(o *options) { o.epoch = epoch } }

// WithClock injects the clock; only its millisecond precision is used. While
// waiting (sequence overflow or a small regression) Next calls it in a loop,
// so a fake clock must advance as it is polled or Next will never return.
func WithClock(now func() time.Time) Option {
	return func(o *options) {
		if now != nil {
			o.now = now
			o.fakeClock = true
		}
	}
}

// WithClockTolerance sets how far backwards the clock may step before Next
// returns ErrClockMovedBackwards instead of waiting. Zero refuses every
// regression, as Twitter's original Snowflake did.
func WithClockTolerance(d time.Duration) Option { return func(o *options) { o.tolerance = d } }

// New returns a generator for nodeID (0..1023).
func New(nodeID int64, opts ...Option) (*Generator, error) {
	if nodeID < 0 || nodeID > MaxNode {
		return nil, fmt.Errorf("snowflakeid: node ID %d out of range 0..%d", nodeID, MaxNode)
	}
	o := options{epoch: DefaultEpoch, now: time.Now, tolerance: DefaultClockTolerance}
	for _, opt := range opts {
		opt(&o)
	}
	if o.tolerance < 0 {
		return nil, errors.New("snowflakeid: clock tolerance must not be negative")
	}
	if o.epoch.After(o.now()) {
		return nil, errors.New("snowflakeid: epoch is in the future")
	}
	g := &Generator{
		node:      nodeID,
		epoch:     o.epoch,
		epochMs:   o.epoch.UnixMilli(),
		now:       o.now,
		tolerance: o.tolerance,
		yield:     func() {},
		lastMs:    -1,
	}
	// With the real clock, let other goroutines run while spinning. A fake
	// clock advances only when polled, so yielding there would just be slower.
	if !o.fakeClock {
		g.yield = runtime.Gosched
	}
	return g, nil
}

// Node returns the generator's node ID.
func (g *Generator) Node() int64 { return g.node }

// Epoch returns the generator's epoch.
func (g *Generator) Epoch() time.Time { return g.epoch }

// EndOfLife is when this epoch's 41-bit timestamp runs out.
func (g *Generator) EndOfLife() time.Time { return g.epoch.Add(Lifetime) }

// Stats returns a snapshot of the counters.
func (g *Generator) Stats() Stats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.stats
}

func (g *Generator) millis() int64 { return g.now().UnixMilli() - g.epochMs }

// waitUntil polls the clock until it reaches target ms.
func (g *Generator) waitUntil(target int64) int64 {
	for {
		if now := g.millis(); now >= target {
			return now
		}
		g.yield()
	}
}

// Next returns a new ID. IDs from one generator strictly increase.
//
// The lock is held while waiting for the next millisecond, so concurrent
// callers queue behind the waiter, which is what they would have to do
// anyway: the node has no IDs left for the current millisecond.
func (g *Generator) Next() (ID, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.millis()
	if now < 0 {
		return 0, ErrBeforeEpoch
	}
	if now < g.lastMs {
		behind := time.Duration(g.lastMs-now) * time.Millisecond
		if behind > g.tolerance {
			g.stats.BackwardsErrors++
			return 0, fmt.Errorf("%w by %v (tolerance %v)", ErrClockMovedBackwards, behind, g.tolerance)
		}
		g.stats.BackwardsWaits++
		now = g.waitUntil(g.lastMs)
	}

	if now == g.lastMs {
		g.seq = (g.seq + 1) & MaxSequence
		if g.seq == 0 { // 4096 IDs used in this ms
			g.stats.OverflowWaits++
			now = g.waitUntil(g.lastMs + 1)
		}
	} else {
		g.seq = 0
	}
	if now > MaxTimestamp {
		return 0, ErrTimestampOverflow
	}
	g.lastMs = now
	g.stats.Generated++
	return ID(now<<timeShift | g.node<<nodeShift | g.seq), nil
}
