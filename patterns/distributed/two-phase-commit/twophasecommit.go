// Package twophasecommit implements the two-phase commit (2PC) protocol
// between a coordinator and in-process participants.
//
// Phase 1 (prepare): the coordinator asks every participant to prepare. A
// participant that votes yes has durably staged the writes, locked the keys,
// and promised it can commit; it may no longer decide on its own.
// Phase 2 (commit/abort): if every participant voted yes, the coordinator
// logs COMMIT and tells everyone to commit; otherwise it logs ABORT.
//
// The protocol's weakness is shown explicitly: if the coordinator crashes
// after participants have prepared but before it logs a decision, those
// participants are "in doubt". They keep their locks and cannot commit or
// abort until the coordinator recovers, blocking every other transaction on
// those keys.
package twophasecommit

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// Errors.
var (
	ErrUnavailable        = errors.New("twophasecommit: participant unavailable")
	ErrLocked             = errors.New("twophasecommit: key locked by an in-doubt transaction")
	ErrCoordinatorCrashed = errors.New("twophasecommit: coordinator crashed")
	ErrUnknownParticipant = errors.New("twophasecommit: unknown participant")
	ErrUnknownTx          = errors.New("twophasecommit: unknown transaction")
)

// Participant is the coordinator's view of a resource manager.
type Participant interface {
	Prepare(ctx context.Context, txID string, writes map[string]int) (yes bool, err error)
	Commit(ctx context.Context, txID string) error
	Abort(ctx context.Context, txID string) error
}

type txState int

const (
	prepared txState = iota + 1
	committed
	aborted
)

// Store is an in-memory key/value participant that can be taken offline.
// Values must be non-negative (think account balances); a negative write
// makes it vote no.
type Store struct {
	Name string

	mu     sync.Mutex
	data   map[string]int
	locks  map[string]string         // key -> txID holding it
	staged map[string]map[string]int // txID -> writes
	state  map[string]txState
	down   bool
}

// NewStore returns a participant with the given initial data.
func NewStore(name string, initial map[string]int) *Store {
	s := &Store{Name: name, data: map[string]int{}, locks: map[string]string{},
		staged: map[string]map[string]int{}, state: map[string]txState{}}
	for k, v := range initial {
		s.data[k] = v
	}
	return s
}

// SetDown simulates a crash/partition (true) or recovery (false). Prepared
// state survives, as it would on disk.
func (s *Store) SetDown(down bool) { s.mu.Lock(); s.down = down; s.mu.Unlock() }

// Prepare implements Participant.
func (s *Store) Prepare(_ context.Context, txID string, writes map[string]int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down {
		return false, ErrUnavailable
	}
	switch s.state[txID] {
	case prepared, committed:
		return true, nil // duplicate prepare
	case aborted:
		return false, nil // already aborted (e.g. presumed abort after timeout)
	}
	for k, v := range writes {
		if v < 0 {
			return false, nil // constraint violation
		}
		if holder, ok := s.locks[k]; ok && holder != txID {
			return false, nil // conflicting transaction holds the lock
		}
	}
	staged := make(map[string]int, len(writes))
	for k, v := range writes {
		s.locks[k] = txID
		staged[k] = v
	}
	s.staged[txID] = staged
	s.state[txID] = prepared
	return true, nil
}

// Commit implements Participant. It is idempotent.
func (s *Store) Commit(_ context.Context, txID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down {
		return ErrUnavailable
	}
	switch s.state[txID] {
	case committed:
		return nil
	case prepared:
	default:
		return fmt.Errorf("%w: %s cannot commit %q (not prepared)", ErrUnknownTx, s.Name, txID)
	}
	for k, v := range s.staged[txID] {
		s.data[k] = v
	}
	s.releaseLocked(txID)
	s.state[txID] = committed
	return nil
}

// Abort implements Participant. It is idempotent and also records aborts
// for transactions it never saw, so a late Prepare votes no.
func (s *Store) Abort(_ context.Context, txID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down {
		return ErrUnavailable
	}
	if s.state[txID] == committed {
		return fmt.Errorf("twophasecommit: %s already committed %q", s.Name, txID)
	}
	s.releaseLocked(txID)
	s.state[txID] = aborted
	return nil
}

func (s *Store) releaseLocked(txID string) {
	for k := range s.staged[txID] {
		if s.locks[k] == txID {
			delete(s.locks, k)
		}
	}
	delete(s.staged, txID)
}

// Get reads a committed value. Reads of keys held by a prepared transaction
// fail with ErrLocked: this is the blocking that in-doubt transactions cause.
func (s *Store) Get(key string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down {
		return 0, ErrUnavailable
	}
	if tx, ok := s.locks[key]; ok {
		return 0, fmt.Errorf("%w (%s)", ErrLocked, tx)
	}
	return s.data[key], nil
}

// InDoubt returns the transactions prepared but not yet decided here.
func (s *Store) InDoubt() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for tx, st := range s.state {
		if st == prepared {
			out = append(out, tx)
		}
	}
	sort.Strings(out)
	return out
}

// Decision is the coordinator's outcome for a transaction.
type Decision string

// Decisions. Undecided means the coordinator crashed before deciding.
const (
	Undecided Decision = ""
	Commit    Decision = "commit"
	Abort     Decision = "abort"
)

type logEntry struct {
	writes   map[string]map[string]int
	decision Decision
	acked    map[string]bool // participants that applied the decision
}

// Coordinator drives 2PC. Its log stands in for a durable write-ahead log.
type Coordinator struct {
	mu    sync.Mutex
	parts map[string]Participant
	log   map[string]*logEntry
	order []string

	// CrashAfterPrepare, if set and returning true, makes Execute stop after
	// phase 1 without logging a decision, simulating a coordinator crash at
	// the worst moment.
	CrashAfterPrepare func(txID string) bool
}

// NewCoordinator returns a coordinator for the named participants.
func NewCoordinator(parts map[string]Participant) *Coordinator {
	return &Coordinator{parts: parts, log: map[string]*logEntry{}}
}

// Execute runs 2PC for writes (participant name -> key -> value). It returns
// the decision. A non-nil error with Decision Commit means the decision is
// final but some participants have not applied it yet; Recover finishes it.
func (c *Coordinator) Execute(ctx context.Context, txID string, writes map[string]map[string]int) (Decision, error) {
	if len(writes) == 0 {
		return Abort, fmt.Errorf("twophasecommit: transaction %q has no participants", txID)
	}
	for name := range writes {
		if c.parts[name] == nil {
			return Abort, fmt.Errorf("%w: %q", ErrUnknownParticipant, name)
		}
	}
	c.mu.Lock()
	if _, dup := c.log[txID]; dup {
		c.mu.Unlock()
		return Abort, fmt.Errorf("twophasecommit: duplicate transaction id %q", txID)
	}
	entry := &logEntry{writes: writes, acked: map[string]bool{}}
	c.log[txID] = entry // "begin" record
	c.order = append(c.order, txID)
	c.mu.Unlock()

	// Phase 1: prepare in parallel.
	var wg sync.WaitGroup
	var vmu sync.Mutex
	allYes := true
	for name, w := range writes {
		wg.Add(1)
		go func(p Participant, w map[string]int) {
			defer wg.Done()
			yes, err := p.Prepare(ctx, txID, w)
			if err != nil || !yes {
				vmu.Lock()
				allYes = false
				vmu.Unlock()
			}
		}(c.parts[name], w)
	}
	wg.Wait()

	if c.CrashAfterPrepare != nil && c.CrashAfterPrepare(txID) {
		return Undecided, ErrCoordinatorCrashed
	}

	decision := Abort
	if allYes && ctx.Err() == nil {
		decision = Commit
	}
	c.mu.Lock()
	entry.decision = decision // the commit point: once logged, it is final
	c.mu.Unlock()

	return decision, c.deliver(ctx, txID)
}

// deliver sends the logged decision to every participant that has not yet
// acknowledged it.
func (c *Coordinator) deliver(ctx context.Context, txID string) error {
	c.mu.Lock()
	entry := c.log[txID]
	decision := entry.decision
	var pending []string
	for name := range entry.writes {
		if !entry.acked[name] {
			pending = append(pending, name)
		}
	}
	c.mu.Unlock()
	sort.Strings(pending)

	var errs []error
	for _, name := range pending {
		p := c.parts[name]
		var err error
		if decision == Commit {
			err = p.Commit(ctx, txID)
		} else {
			err = p.Abort(ctx, txID)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("%s %s: %w", decision, name, err))
			continue
		}
		c.mu.Lock()
		entry.acked[name] = true
		c.mu.Unlock()
	}
	return errors.Join(errs...)
}

// Recover is what a restarted coordinator does: transactions with no logged
// decision are aborted (presumed abort, safe because nobody can have
// committed without a logged COMMIT), and every decision is re-sent to
// participants that have not acknowledged it.
func (c *Coordinator) Recover(ctx context.Context) error {
	c.mu.Lock()
	txs := append([]string(nil), c.order...)
	for _, tx := range txs {
		if c.log[tx].decision == Undecided {
			c.log[tx].decision = Abort
		}
	}
	c.mu.Unlock()
	var errs []error
	for _, tx := range txs {
		if err := c.deliver(ctx, tx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Decision returns the logged decision for txID.
func (c *Coordinator) Decision(txID string) Decision {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.log[txID]; ok {
		return e.decision
	}
	return Undecided
}
