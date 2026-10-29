// Package idempotentconsumer shows why at-least-once delivery produces
// duplicates, how the timing of an offset commit trades lost messages for
// duplicated side effects, and how an inbox (a "processed message IDs" table
// written in the same transaction as the side effect) makes redelivery
// harmless.
//
// A consumer reading from a partitioned log does two things per message: it
// applies a side effect (credit an account, store a chat message) and it
// commits its offset so a restart resumes after that message. Those are two
// writes to two systems, and the process can crash between them:
//
//   - Commit before the side effect, crash in between: on restart the log says
//     the message was handled, but it never was. The message is lost
//     (at-most-once).
//   - Commit after the side effect, crash in between: on restart the message
//     is delivered again and the side effect runs twice (at-least-once).
//
// Duplicates also come from the sender: a producer whose ack was lost cannot
// tell "the broker never got it" from "the broker got it and the reply was
// lost", so it resends and the log holds the same message twice. Rebalances
// and offset resets replay messages too.
//
// The fix is to accept at-least-once delivery and make the consumer
// idempotent. The producer gives every message a unique ID (producer ID plus
// sequence number) that survives retries. The consumer commits the offset
// after processing, and processing is one local transaction that applies
// the side effect and inserts the message ID into an inbox table. A
// redelivered message finds its ID already there and is skipped, so each
// message's effect lands exactly once even though it may be delivered many
// times ("effectively-once").
//
// The inbox cannot grow forever, so IDs are pruned after a retention window.
// A duplicate that arrives after its ID was pruned is applied again: the
// window must be longer than the longest plausible redelivery delay.
//
// Everything here is in-process and deterministic: a Log with partitions,
// offsets and per-group committed offsets; a Store with atomic transactions;
// a Consumer with injectable crashes at exact points; and a Producer with
// injectable lost requests and lost acks.
package idempotentconsumer

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"time"
)

var (
	// ErrCrashed is returned by Consumer.Poll when an injected crash fires.
	// The consumer forgets its in-memory positions, exactly like a restarted
	// process, and resumes from the committed offsets on the next Poll.
	ErrCrashed = errors.New("idempotentconsumer: consumer crashed (injected)")
	// ErrNotAcked is returned by Producer.Send when no attempt was
	// acknowledged. The message may still be in the log (if only acks were
	// lost).
	ErrNotAcked = errors.New("idempotentconsumer: send not acknowledged after max attempts")
)

// Message is one record in the log. ID is assigned by the producer and is
// the same on every retransmission; Partition and Offset are assigned by the
// log on append, so a retransmitted message gets a new offset.
type Message struct {
	ID        string
	Key       string
	Value     int64
	Partition int
	Offset    int64
}

// ---------------------------------------------------------------------------
// Log

type groupPartition struct {
	group     string
	partition int
}

// Log is an in-process partitioned, append-only log with per-group committed
// offsets, a stand-in for a Kafka topic plus its __consumer_offsets.
type Log struct {
	mu        sync.Mutex
	parts     [][]Message
	committed map[groupPartition]int64
}

// NewLog returns a log with the given number of partitions.
func NewLog(partitions int) (*Log, error) {
	if partitions < 1 {
		return nil, errors.New("idempotentconsumer: partitions must be at least 1")
	}
	return &Log{
		parts:     make([][]Message, partitions),
		committed: make(map[groupPartition]int64),
	}, nil
}

// Partitions returns the number of partitions.
func (l *Log) Partitions() int { return len(l.parts) }

// PartitionFor maps a key to a partition (FNV-1a hash), so all messages for
// one key land in one partition.
func (l *Log) PartitionFor(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(len(l.parts)))
}

// Append writes m to the partition for its key and returns it with Partition
// and Offset filled in. The log does not deduplicate: appending the same ID
// twice stores it twice.
func (l *Log) Append(m Message) (Message, error) {
	if m.ID == "" {
		return Message{}, errors.New("idempotentconsumer: message ID is required")
	}
	m.Partition = l.PartitionFor(m.Key)
	l.mu.Lock()
	defer l.mu.Unlock()
	m.Offset = int64(len(l.parts[m.Partition]))
	l.parts[m.Partition] = append(l.parts[m.Partition], m)
	return m, nil
}

func (l *Log) checkPartition(p int) error {
	if p < 0 || p >= len(l.parts) {
		return fmt.Errorf("idempotentconsumer: partition %d out of range [0,%d)", p, len(l.parts))
	}
	return nil
}

// Read returns up to max messages of a partition starting at offset from.
func (l *Log) Read(partition int, from int64, max int) ([]Message, error) {
	if err := l.checkPartition(partition); err != nil {
		return nil, err
	}
	if from < 0 || max < 1 {
		return nil, errors.New("idempotentconsumer: from must be >= 0 and max >= 1")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	p := l.parts[partition]
	if from >= int64(len(p)) {
		return nil, nil
	}
	end := min(int64(len(p)), from+int64(max))
	return append([]Message(nil), p[from:end]...), nil
}

// End returns the offset the next appended message in a partition will get.
func (l *Log) End(partition int) int64 {
	if l.checkPartition(partition) != nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return int64(len(l.parts[partition]))
}

// Commit records next as the offset group will resume from in partition
// (Kafka convention: the offset of the next message to read, not the last
// one processed). Committing a lower offset rewinds the group, which is how
// an offset reset or replay is simulated.
func (l *Log) Commit(group string, partition int, next int64) error {
	if group == "" {
		return errors.New("idempotentconsumer: group is required")
	}
	if err := l.checkPartition(partition); err != nil {
		return err
	}
	if next < 0 {
		return errors.New("idempotentconsumer: offset must be >= 0")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.committed[groupPartition{group, partition}] = next
	return nil
}

// Committed returns the committed offset of group in partition (0 if none).
func (l *Log) Committed(group string, partition int) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.committed[groupPartition{group, partition}]
}

// ---------------------------------------------------------------------------
// Store

// Store is an in-process transactional database holding account balances
// (the side effects) and the inbox of processed message IDs. Transactions
// are serialised by one lock, so each Apply is atomic and isolated.
//
// The store also records how many times each message ID's transaction
// committed. That is instrumentation for the tests and example (it is how
// "lost" and "duplicated" are measured), not part of the pattern.
type Store struct {
	mu           sync.Mutex
	now          func() time.Time
	retention    time.Duration
	balances     map[string]int64
	inbox        map[string]time.Time
	inboxOrder   []inboxEntry // insertion order, for O(1) amortised pruning
	applications map[string]int
	pruned       uint64
}

type inboxEntry struct {
	id string
	at time.Time
}

// StoreOption configures a Store.
type StoreOption func(*Store)

// WithClock injects the store's clock, used to timestamp inbox entries and
// decide when they expire.
func WithClock(now func() time.Time) StoreOption {
	return func(s *Store) {
		if now != nil {
			s.now = now
		}
	}
}

// WithRetention sets the dedup window: inbox entries older than d are
// pruned. Zero (the default) keeps them forever.
func WithRetention(d time.Duration) StoreOption {
	return func(s *Store) { s.retention = d }
}

// NewStore returns an empty store.
func NewStore(opts ...StoreOption) (*Store, error) {
	s := &Store{
		now:          time.Now,
		balances:     make(map[string]int64),
		inbox:        make(map[string]time.Time),
		applications: make(map[string]int),
	}
	for _, o := range opts {
		o(s)
	}
	if s.retention < 0 {
		return nil, errors.New("idempotentconsumer: retention must be >= 0")
	}
	return s, nil
}

// Tx stages writes; they become visible only if the transaction commits.
type Tx struct {
	s      *Store
	deltas map[string]int64
}

// Add stages a change to an account's balance.
func (tx *Tx) Add(account string, delta int64) {
	tx.deltas[account] += delta
}

// Balance reads an account's balance including this transaction's writes.
func (tx *Tx) Balance(account string) int64 {
	return tx.s.balances[account] + tx.deltas[account]
}

// Handler applies one message's side effect inside a transaction. Returning
// an error rolls the transaction back.
type Handler func(tx *Tx, m Message) error

// Apply runs h for m in one transaction. With dedup set, it is the inbox
// pattern: if m.ID is already in the inbox nothing happens and applied is
// false; otherwise the handler's writes and the inbox insert commit together,
// so there is no moment at which one exists without the other.
func (s *Store) Apply(m Message, dedup bool, h Handler) (applied bool, err error) {
	if h == nil {
		return false, errors.New("idempotentconsumer: handler is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if dedup {
		s.pruneLocked(now)
		if _, seen := s.inbox[m.ID]; seen {
			return false, nil
		}
	}
	tx := &Tx{s: s, deltas: make(map[string]int64)}
	if err := h(tx, m); err != nil {
		return false, err // staged writes are dropped: rollback
	}
	// Commit: side effects, inbox row and instrumentation in one step.
	for k, d := range tx.deltas {
		s.balances[k] += d
	}
	if dedup {
		s.inbox[m.ID] = now
		s.inboxOrder = append(s.inboxOrder, inboxEntry{m.ID, now})
	}
	s.applications[m.ID]++
	return true, nil
}

// Prune removes inbox entries older than the retention window and returns
// how many were removed. Apply also prunes, so calling it is optional.
func (s *Store) Prune() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pruneLocked(s.now())
}

func (s *Store) pruneLocked(now time.Time) int {
	if s.retention == 0 {
		return 0
	}
	cutoff := now.Add(-s.retention)
	n := 0
	for len(s.inboxOrder) > 0 && s.inboxOrder[0].at.Before(cutoff) {
		e := s.inboxOrder[0]
		s.inboxOrder = s.inboxOrder[1:]
		if at, ok := s.inbox[e.id]; ok && at.Equal(e.at) {
			delete(s.inbox, e.id)
			n++
		}
	}
	s.pruned += uint64(n)
	return n
}

// Balance returns an account's committed balance.
func (s *Store) Balance(account string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.balances[account]
}

// Applications returns how many times a transaction for message ID committed.
func (s *Store) Applications(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applications[id]
}

// InboxSize returns the number of message IDs currently in the inbox.
func (s *Store) InboxSize() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.inbox)
}

// Pruned returns how many inbox entries have expired so far.
func (s *Store) Pruned() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pruned
}

// Audit compares what was applied with what should have been: each of ids
// exactly once.
type Audit struct {
	Expected   int // distinct message IDs that should have been applied once
	Exact      int // applied exactly once
	Lost       int // never applied
	Duplicated int // applied more than once
	Applied    int // total applications across ids
}

// Audit checks every distinct ID in ids.
func (s *Store) Audit(ids []string) Audit {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := make(map[string]bool, len(ids))
	var a Audit
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		a.Expected++
		n := s.applications[id]
		a.Applied += n
		switch {
		case n == 0:
			a.Lost++
		case n == 1:
			a.Exact++
		default:
			a.Duplicated++
		}
	}
	return a
}

// ---------------------------------------------------------------------------
// Consumer

// Strategy is how the consumer orders its offset commit and side effect.
type Strategy int

const (
	// CommitBefore commits the offset, then applies the side effect.
	// A crash in between loses the message (at-most-once).
	CommitBefore Strategy = iota + 1
	// CommitAfter applies the side effect, then commits. A crash in between
	// applies the side effect again on redelivery (at-least-once).
	CommitAfter
	// Inbox applies the side effect and records the message ID in one
	// transaction, then commits. Redeliveries are skipped (effectively-once).
	Inbox
)

func (s Strategy) String() string {
	switch s {
	case CommitBefore:
		return "commit-before"
	case CommitAfter:
		return "commit-after"
	case Inbox:
		return "inbox"
	}
	return fmt.Sprintf("Strategy(%d)", int(s))
}

// CrashPoint is a moment in the handling of one message. For CommitBefore
// the order is BeforeCommit, commit, AfterCommit, BeforeEffect, effect,
// AfterEffect (so AfterCommit and BeforeEffect are the same moment). For
// CommitAfter and Inbox it is BeforeEffect, effect, AfterEffect,
// BeforeCommit, commit, AfterCommit.
type CrashPoint int

const (
	BeforeEffect CrashPoint = iota + 1
	AfterEffect
	BeforeCommit
	AfterCommit
)

func (p CrashPoint) String() string {
	switch p {
	case BeforeEffect:
		return "before-effect"
	case AfterEffect:
		return "after-effect"
	case BeforeCommit:
		return "before-commit"
	case AfterCommit:
		return "after-commit"
	}
	return fmt.Sprintf("CrashPoint(%d)", int(p))
}

// Crash makes the consumer crash once, at Point, while handling the message
// at Partition/Offset.
type Crash struct {
	Partition int
	Offset    int64
	Point     CrashPoint
}

// ConsumerStats counts what a consumer did.
type ConsumerStats struct {
	Delivered uint64 // messages handed to the processing pipeline
	Applied   uint64 // side-effect transactions committed
	Skipped   uint64 // duplicates found in the inbox
	Commits   uint64 // offset commits
	Crashes   uint64 // injected crashes (each followed by a restart)
}

// Consumer reads assigned partitions for a consumer group and handles each
// message with the chosen strategy, committing after every message.
type Consumer struct {
	log      *Log
	store    *Store
	group    string
	strategy Strategy
	handler  Handler
	parts    []int

	mu      sync.Mutex
	pos     map[int]int64 // in-memory positions; empty after a (re)start
	crashes map[Crash]bool
	stats   ConsumerStats
}

type consumerConfig struct {
	parts    []int
	partsSet bool
	crashes  []Crash
}

// ConsumerOption configures a Consumer.
type ConsumerOption func(*consumerConfig)

// WithPartitions assigns partitions to the consumer (default: all).
func WithPartitions(ps ...int) ConsumerOption {
	return func(c *consumerConfig) { c.parts, c.partsSet = append([]int(nil), ps...), true }
}

// WithCrashes schedules injected crashes. Each fires at most once.
func WithCrashes(cs ...Crash) ConsumerOption {
	return func(c *consumerConfig) { c.crashes = append(c.crashes, cs...) }
}

// NewConsumer returns a consumer for group using strategy s and handler h.
func NewConsumer(log *Log, store *Store, group string, s Strategy, h Handler, opts ...ConsumerOption) (*Consumer, error) {
	if log == nil || store == nil || h == nil {
		return nil, errors.New("idempotentconsumer: log, store and handler are required")
	}
	if group == "" {
		return nil, errors.New("idempotentconsumer: group is required")
	}
	if s < CommitBefore || s > Inbox {
		return nil, fmt.Errorf("idempotentconsumer: unknown strategy %d", int(s))
	}
	var cfg consumerConfig
	for _, o := range opts {
		o(&cfg)
	}
	if !cfg.partsSet {
		for p := range log.Partitions() {
			cfg.parts = append(cfg.parts, p)
		}
	}
	if len(cfg.parts) == 0 {
		return nil, errors.New("idempotentconsumer: at least one partition is required")
	}
	seen := map[int]bool{}
	for _, p := range cfg.parts {
		if err := log.checkPartition(p); err != nil {
			return nil, err
		}
		if seen[p] {
			return nil, fmt.Errorf("idempotentconsumer: partition %d assigned twice", p)
		}
		seen[p] = true
	}
	sort.Ints(cfg.parts)
	crashes := make(map[Crash]bool, len(cfg.crashes))
	for _, c := range cfg.crashes {
		if c.Point < BeforeEffect || c.Point > AfterCommit {
			return nil, fmt.Errorf("idempotentconsumer: unknown crash point %d", int(c.Point))
		}
		crashes[c] = true
	}
	return &Consumer{
		log: log, store: store, group: group, strategy: s, handler: h,
		parts: cfg.parts, pos: make(map[int]int64), crashes: crashes,
	}, nil
}

// Poll handles up to max messages across the assigned partitions. If an
// injected crash fires it returns ErrCrashed; the consumer has then
// "restarted" and the next Poll resumes from the committed offsets. A
// handler error is returned as is; the message is retried on the next Poll.
func (c *Consumer) Poll(max int) (int, error) {
	if max < 1 {
		return 0, errors.New("idempotentconsumer: max must be at least 1")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, p := range c.parts {
		if n == max {
			break
		}
		from, ok := c.pos[p]
		if !ok {
			from = c.log.Committed(c.group, p)
		}
		msgs, err := c.log.Read(p, from, max-n)
		if err != nil {
			return n, err
		}
		for _, m := range msgs {
			c.stats.Delivered++
			if err := c.handle(m); err != nil {
				if errors.Is(err, ErrCrashed) {
					c.stats.Crashes++
					c.pos = make(map[int]int64) // restart: forget everything in memory
				}
				return n, err
			}
			c.pos[p] = m.Offset + 1
			n++
		}
	}
	return n, nil
}

// Drain polls until every assigned partition is caught up, restarting after
// each injected crash. It returns the first non-crash error.
func (c *Consumer) Drain() error {
	for {
		n, err := c.Poll(256)
		if errors.Is(err, ErrCrashed) {
			continue
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
	}
}

// Stats returns a snapshot of the consumer's counters.
func (c *Consumer) Stats() ConsumerStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

func (c *Consumer) crashAt(m Message, p CrashPoint) bool {
	k := Crash{m.Partition, m.Offset, p}
	if c.crashes[k] {
		delete(c.crashes, k)
		return true
	}
	return false
}

func (c *Consumer) commit(m Message) error {
	if err := c.log.Commit(c.group, m.Partition, m.Offset+1); err != nil {
		return err
	}
	c.stats.Commits++
	return nil
}

func (c *Consumer) effect(m Message) error {
	applied, err := c.store.Apply(m, c.strategy == Inbox, c.handler)
	if err != nil {
		return err
	}
	if applied {
		c.stats.Applied++
	} else {
		c.stats.Skipped++
	}
	return nil
}

func (c *Consumer) handle(m Message) error {
	if c.strategy == CommitBefore {
		if c.crashAt(m, BeforeCommit) {
			return ErrCrashed
		}
		if err := c.commit(m); err != nil {
			return err
		}
		if c.crashAt(m, AfterCommit) || c.crashAt(m, BeforeEffect) {
			return ErrCrashed // offset says "done", effect never ran: lost
		}
		if err := c.effect(m); err != nil {
			return err
		}
		if c.crashAt(m, AfterEffect) {
			return ErrCrashed
		}
		return nil
	}
	if c.crashAt(m, BeforeEffect) {
		return ErrCrashed
	}
	if err := c.effect(m); err != nil {
		return err
	}
	if c.crashAt(m, AfterEffect) || c.crashAt(m, BeforeCommit) {
		return ErrCrashed // effect done, offset not committed: redelivered
	}
	if err := c.commit(m); err != nil {
		return err
	}
	if c.crashAt(m, AfterCommit) {
		return ErrCrashed
	}
	return nil
}

// ---------------------------------------------------------------------------
// Producer

// Fault is what the simulated network does to one send attempt.
type Fault int

const (
	// Delivered: the append happens and the ack arrives.
	Delivered Fault = iota
	// LoseRequest: the request never reaches the log.
	LoseRequest
	// LoseAck: the log appends the message but the ack is lost, so the
	// producer cannot tell this apart from LoseRequest and resends.
	LoseAck
)

// ProducerStats counts what a producer did.
type ProducerStats struct {
	Sent        uint64 // Send calls
	Attempts    uint64 // append requests made, including retries
	Retransmits uint64 // attempts after the first, per message
	Failed      uint64 // Send calls that returned ErrNotAcked
}

// Producer appends messages to a log and retries until acknowledged. Every
// message gets an ID once, before the first attempt, and keeps it on every
// retry, so downstream deduplication can recognise retransmissions.
type Producer struct {
	log         *Log
	nextID      func() string
	maxAttempts int
	fault       func(m Message, attempt int) Fault

	mu    sync.Mutex
	seq   uint64
	id    string
	stats ProducerStats
}

// ProducerOption configures a Producer.
type ProducerOption func(*Producer)

// WithMaxAttempts sets how many times Send tries before giving up (default 5).
func WithMaxAttempts(n int) ProducerOption {
	return func(p *Producer) { p.maxAttempts = n }
}

// WithFaults injects network faults per attempt (attempt counts from 1).
func WithFaults(f func(m Message, attempt int) Fault) ProducerOption {
	return func(p *Producer) { p.fault = f }
}

// WithIDs replaces the default "<producerID>-<sequence>" message IDs, for
// example with Snowflake IDs or UUIDs.
func WithIDs(next func() string) ProducerOption {
	return func(p *Producer) { p.nextID = next }
}

// NewProducer returns a producer identified by producerID.
func NewProducer(log *Log, producerID string, opts ...ProducerOption) (*Producer, error) {
	if log == nil {
		return nil, errors.New("idempotentconsumer: log is required")
	}
	if producerID == "" {
		return nil, errors.New("idempotentconsumer: producer ID is required")
	}
	p := &Producer{log: log, id: producerID, maxAttempts: 5}
	for _, o := range opts {
		o(p)
	}
	if p.maxAttempts < 1 {
		return nil, errors.New("idempotentconsumer: max attempts must be at least 1")
	}
	return p, nil
}

func (p *Producer) assignID() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seq++
	p.stats.Sent++
	if p.nextID != nil {
		return p.nextID()
	}
	return fmt.Sprintf("%s-%d", p.id, p.seq)
}

// Send appends a message for key, retrying lost requests and lost acks. It
// returns the message as stored by the acknowledged attempt.
func (p *Producer) Send(key string, value int64) (Message, error) {
	m := Message{ID: p.assignID(), Key: key, Value: value}
	if m.ID == "" {
		return Message{}, errors.New("idempotentconsumer: ID generator returned an empty ID")
	}
	for attempt := 1; attempt <= p.maxAttempts; attempt++ {
		p.mu.Lock()
		p.stats.Attempts++
		if attempt > 1 {
			p.stats.Retransmits++
		}
		p.mu.Unlock()
		f := Delivered
		if p.fault != nil {
			f = p.fault(m, attempt)
		}
		if f == LoseRequest {
			continue
		}
		stored, err := p.log.Append(m)
		if err != nil {
			return Message{}, err
		}
		if f == LoseAck {
			continue
		}
		return stored, nil
	}
	p.mu.Lock()
	p.stats.Failed++
	p.mu.Unlock()
	return m, ErrNotAcked
}

// Stats returns a snapshot of the producer's counters.
func (p *Producer) Stats() ProducerStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stats
}
