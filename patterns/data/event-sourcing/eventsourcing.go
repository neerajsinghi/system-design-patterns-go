// Package eventsourcing stores an aggregate's state as the sequence of events
// that produced it, instead of overwriting a row.
//
// Current state is a left fold over the events. That gives a complete audit
// trail, lets you answer "what was the balance at version N?", and lets new
// projections be built from history. Snapshots cap the replay cost for
// long-lived streams. Optimistic concurrency (expected version on append)
// stops two writers from both acting on the same stale state.
package eventsourcing

import (
	"errors"
	"fmt"
	"sync"
)

// Event is a fact about a bank account.
type Event interface{ isEvent() }

// Account events.
type (
	Opened    struct{ Owner string }
	Deposited struct{ Amount int64 }
	Withdrawn struct{ Amount int64 }
	Closed    struct{}
)

func (Opened) isEvent()    {}
func (Deposited) isEvent() {}
func (Withdrawn) isEvent() {}
func (Closed) isEvent()    {}

// Errors.
var (
	ErrConcurrency  = errors.New("eventsourcing: stream was modified concurrently")
	ErrNotFound     = errors.New("eventsourcing: stream not found")
	ErrInvalid      = errors.New("eventsourcing: invalid command")
	ErrInsufficient = errors.New("eventsourcing: insufficient funds")
)

// Store is an in-memory, append-only event store.
type Store struct {
	mu         sync.Mutex
	streams    map[string][]Event
	eventsRead uint64
}

// NewStore returns an empty store.
func NewStore() *Store { return &Store{streams: make(map[string][]Event)} }

// Append adds events to stream only if the stream is currently at
// expectedVersion (the number of events in it), and returns the new version.
func (s *Store) Append(stream string, expectedVersion int, events ...Event) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := len(s.streams[stream])
	if cur != expectedVersion {
		return cur, fmt.Errorf("%w: %s at version %d, expected %d", ErrConcurrency, stream, cur, expectedVersion)
	}
	s.streams[stream] = append(s.streams[stream], events...)
	return cur + len(events), nil
}

// Load returns the events of stream with versions in (after, upto]. If upto <= 0, it returns all events after after.
func (s *Store) Load(stream string, after, upto int) []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	evs := s.streams[stream]
	if upto <= 0 || upto > len(evs) {
		upto = len(evs)
	}
	after = max(after, 0)
	if after >= upto {
		return nil
	}
	out := append([]Event(nil), evs[after:upto]...)
	s.eventsRead += uint64(len(out))
	return out
}

// EventsRead counts events returned by Load, which shows how much replay snapshots save.
func (s *Store) EventsRead() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.eventsRead
}

// State is the account's materialised state, and what a snapshot stores.
type State struct {
	ID      string
	Owner   string
	Balance int64
	Opened  bool
	Closed  bool
	Version int // number of events applied
}

// Account is the aggregate: it validates commands and records new events.
type Account struct {
	State
	pending []Event
}

// NewAccount starts an empty aggregate. Call Open before anything else.
func NewAccount(id string) *Account { return &Account{State: State{ID: id}} }

// apply mutates state for one event. It never fails, because events are facts.
func (s *State) apply(e Event) {
	switch e := e.(type) {
	case Opened:
		s.Owner, s.Opened = e.Owner, true
	case Deposited:
		s.Balance += e.Amount
	case Withdrawn:
		s.Balance -= e.Amount
	case Closed:
		s.Closed = true
	}
	s.Version++
}

func (a *Account) raise(e Event) {
	a.State.apply(e)
	a.pending = append(a.pending, e)
}

// Open is a command.
func (a *Account) Open(owner string) error {
	if a.Opened || owner == "" {
		return fmt.Errorf("%w: open", ErrInvalid)
	}
	a.raise(Opened{Owner: owner})
	return nil
}

// Deposit is a command.
func (a *Account) Deposit(amount int64) error {
	if !a.Opened || a.Closed || amount <= 0 {
		return fmt.Errorf("%w: deposit %d", ErrInvalid, amount)
	}
	a.raise(Deposited{Amount: amount})
	return nil
}

// Withdraw is a command that refuses to overdraw.
func (a *Account) Withdraw(amount int64) error {
	if !a.Opened || a.Closed || amount <= 0 {
		return fmt.Errorf("%w: withdraw %d", ErrInvalid, amount)
	}
	if amount > a.Balance {
		return fmt.Errorf("%w: balance %d, withdraw %d", ErrInsufficient, a.Balance, amount)
	}
	a.raise(Withdrawn{Amount: amount})
	return nil
}

// Close is a command. Only an empty, open account can be closed.
func (a *Account) Close() error {
	if !a.Opened || a.Closed || a.Balance != 0 {
		return fmt.Errorf("%w: close", ErrInvalid)
	}
	a.raise(Closed{})
	return nil
}

// Pending returns events raised but not yet saved.
func (a *Account) Pending() []Event { return append([]Event(nil), a.pending...) }

// Repository loads and saves Accounts, taking a snapshot every N events.
type Repository struct {
	store *Store
	every int

	mu    sync.Mutex
	snaps map[string]State
}

// NewRepository returns a repository over store. snapshotEvery <= 0 disables snapshots.
func NewRepository(store *Store, snapshotEvery int) (*Repository, error) {
	if store == nil {
		return nil, errors.New("eventsourcing: store must not be nil")
	}
	return &Repository{store: store, every: snapshotEvery, snaps: make(map[string]State)}, nil
}

// Load rebuilds an account from its latest snapshot plus newer events.
func (r *Repository) Load(id string) (*Account, error) {
	r.mu.Lock()
	snap, ok := r.snaps[id]
	r.mu.Unlock()
	if !ok {
		snap = State{ID: id}
	}
	a := &Account{State: snap}
	for _, e := range r.store.Load(id, snap.Version, 0) {
		a.State.apply(e)
	}
	if a.Version == 0 {
		return nil, ErrNotFound
	}
	return a, nil
}

// LoadAt rebuilds an account as it was at version (a temporal query). It
// replays from the start and ignores snapshots, which may be newer than version.
func (r *Repository) LoadAt(id string, version int) (*Account, error) {
	if version <= 0 {
		return nil, fmt.Errorf("%w: version must be positive", ErrInvalid)
	}
	a := NewAccount(id)
	for _, e := range r.store.Load(id, 0, version) {
		a.State.apply(e)
	}
	if a.Version == 0 {
		return nil, ErrNotFound
	}
	return a, nil
}

// Save appends the account's pending events, expecting the stream to be at
// the version the account was loaded at. On ErrConcurrency the caller should
// reload and retry the command.
func (r *Repository) Save(a *Account) error {
	if len(a.pending) == 0 {
		return nil
	}
	expected := a.Version - len(a.pending)
	if _, err := r.store.Append(a.ID, expected, a.pending...); err != nil {
		return err
	}
	a.pending = nil
	if r.every > 0 && a.Version/r.every > expected/r.every { // crossed a snapshot boundary
		r.mu.Lock()
		if a.Version > r.snaps[a.ID].Version {
			r.snaps[a.ID] = a.State
		}
		r.mu.Unlock()
	}
	return nil
}
