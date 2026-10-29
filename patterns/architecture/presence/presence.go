// Package presence tracks who is online, on which connection server, and
// when they were last seen, without writing to a database on every
// heartbeat.
//
// Chat apps show "online" and "last seen 5 min ago", message routers need to
// know which gateway node holds a user's socket, and live-tracking screens
// need a rider's latest position together with how old it is. All three
// are the same problem: a large amount of short-lived state that changes on
// every heartbeat and is worthless a minute later. Writing it to the primary
// database would turn heartbeat interval × users into a write load that
// dwarfs the real traffic.
//
// This package keeps that state in memory as heartbeat-renewed TTL leases,
// one per (user, connection), each recording the gateway node that holds the
// connection. A user is online if any connection is live and recently
// active, away if connected but idle, and offline once every lease is gone.
// Phones drop connections all the time, so the offline announcement waits a
// grace period: a reconnect inside it produces no offline/online events at
// all. Watchers subscribe to the users they can see and receive coalesced
// state changes. An optional latest-position payload carries its own
// timestamp so readers can refuse positions that are too old.
//
// Expired leases are invisible to Lookup and Status at once (lazy expiry).
// Sweep reconciles every user, emitting the events for crashed nodes,
// expired grace periods and idle-to-away transitions.
package presence

import (
	"errors"
	"sort"
	"sync"
	"time"
)

// Errors returned by the tracker.
var (
	ErrInvalid           = errors.New("presence: user, connection id and node are required")
	ErrUnknownConnection = errors.New("presence: unknown or expired connection")
	ErrStalePosition     = errors.New("presence: position is older than the one already stored or than maxAge")
	ErrNoPosition        = errors.New("presence: no position recorded")
)

// State is a user's presence as announced to watchers.
type State int

// Presence states.
const (
	Offline State = iota
	Away          // connected, but no activity for AwayAfter
	Online
)

func (s State) String() string {
	switch s {
	case Online:
		return "online"
	case Away:
		return "away"
	default:
		return "offline"
	}
}

// Connection is one live socket of a user, held by one gateway node.
type Connection struct {
	User   string
	ID     string // connection or device id, unique per user
	Device string // optional: "phone", "web", ...
	Node   string // gateway node that holds the socket, used for routing
}

// Position is a latest-location payload. At is when the device measured it,
// not when the server received it.
type Position struct {
	Lat, Lng float64
	At       time.Time
}

// Presence is the read view of one user.
type Presence struct {
	User        string
	State       State
	LastSeen    time.Time // last heartbeat, activity or disconnect seen from any device
	Connections int       // live connections
}

// Event is a state change delivered to a watcher. Prev is the state this
// watcher last received for the user, so coalesced changes still read as
// one transition.
type Event struct {
	User     string
	Prev     State
	State    State
	At       time.Time
	LastSeen time.Time
}

// Stats counts tracker activity.
type Stats struct {
	Heartbeats   uint64 // Heartbeat, Activity and UpdatePosition calls that renewed a lease
	StateChanges uint64 // announced state changes (before per-watcher coalescing)
	Delivered    uint64 // events handed to watchers by Drain
}

type conn struct {
	c          Connection
	lastBeat   time.Time
	lastActive time.Time
	expiresAt  time.Time
}

type user struct {
	conns     map[string]*conn
	announced State
	goneAt    time.Time // when the last connection went away
	lastSeen  time.Time
	pos       *Position
	watchers  map[*Watcher]struct{}
}

// Tracker holds presence for all users. It is safe for concurrent use.
type Tracker struct {
	now       func() time.Time
	ttl       time.Duration
	awayAfter time.Duration
	grace     time.Duration

	mu    sync.Mutex
	users map[string]*user
	stats Stats
}

// Option configures a Tracker.
type Option func(*Tracker)

// WithClock injects the clock. Tests and the example use simulated time.
func WithClock(now func() time.Time) Option { return func(t *Tracker) { t.now = now } }

// WithTTL sets the lease length. A connection that misses heartbeats for ttl
// is treated as gone. Default 30s.
func WithTTL(ttl time.Duration) Option { return func(t *Tracker) { t.ttl = ttl } }

// WithAwayAfter sets how long a connected user may be idle before turning
// away. 0 disables the away state. Default 5m.
func WithAwayAfter(d time.Duration) Option { return func(t *Tracker) { t.awayAfter = d } }

// WithOfflineGrace sets how long to wait after the last connection is gone
// before announcing offline. A reconnect inside the grace period produces no
// events. 0 announces offline immediately. Default 15s.
func WithOfflineGrace(d time.Duration) Option { return func(t *Tracker) { t.grace = d } }

// New returns an empty tracker.
func New(opts ...Option) (*Tracker, error) {
	t := &Tracker{
		now:       time.Now,
		ttl:       30 * time.Second,
		awayAfter: 5 * time.Minute,
		grace:     15 * time.Second,
		users:     make(map[string]*user),
	}
	for _, o := range opts {
		o(t)
	}
	switch {
	case t.now == nil:
		return nil, errors.New("presence: clock must not be nil")
	case t.ttl <= 0:
		return nil, errors.New("presence: ttl must be > 0")
	case t.awayAfter < 0:
		return nil, errors.New("presence: awayAfter must be >= 0")
	case t.grace < 0:
		return nil, errors.New("presence: offline grace must be >= 0")
	}
	return t, nil
}

func (t *Tracker) get(name string) *user {
	u := t.users[name]
	if u == nil {
		u = &user{conns: make(map[string]*conn)}
		t.users[name] = u
	}
	return u
}

// Connect registers (or replaces) a connection with a fresh lease. It counts
// as activity. A user's first live connection is announced at once: only
// offline is delayed.
func (t *Tracker) Connect(c Connection) error {
	if c.User == "" || c.ID == "" || c.Node == "" {
		return ErrInvalid
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	u := t.get(c.User)
	u.conns[c.ID] = &conn{c: c, lastBeat: now, lastActive: now, expiresAt: now.Add(t.ttl)}
	t.reconcile(c.User, u, now)
	return nil
}

// Heartbeat renews a connection's lease without marking the user active (a
// backgrounded app still heartbeats). After expiry the client must Connect
// again, so a dead socket cannot be revived by a late heartbeat.
func (t *Tracker) Heartbeat(userID, connID string) error {
	return t.renew(userID, connID, false)
}

// Activity renews the lease and marks the user active, moving away back to
// online.
func (t *Tracker) Activity(userID, connID string) error {
	return t.renew(userID, connID, true)
}

func (t *Tracker) renew(userID, connID string, active bool) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	u := t.users[userID]
	if u == nil {
		return ErrUnknownConnection
	}
	c := u.conns[connID]
	if c == nil || !now.Before(c.expiresAt) {
		return ErrUnknownConnection
	}
	c.lastBeat = now
	c.expiresAt = now.Add(t.ttl)
	if active {
		c.lastActive = now
	}
	t.stats.Heartbeats++
	t.reconcile(userID, u, now)
	return nil
}

// Disconnect removes a connection on a clean socket close. If it was the
// user's last one, the offline grace period starts now.
func (t *Tracker) Disconnect(userID, connID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	u := t.users[userID]
	if u == nil {
		return
	}
	if _, ok := u.conns[connID]; !ok {
		return
	}
	now := t.now()
	delete(u.conns, connID)
	u.lastSeen = now
	u.goneAt = now
	t.reconcile(userID, u, now)
}

// Lookup returns the user's live connections sorted by ID, for routing a
// message to the right gateway node. Expired leases are filtered out even if
// Sweep has not run. During the offline grace period Lookup is already
// empty while Status still says online: routing must fail fast (fall back to
// push notification) while the UI stays stable.
func (t *Tracker) Lookup(userID string) []Connection {
	t.mu.Lock()
	defer t.mu.Unlock()
	u := t.users[userID]
	if u == nil {
		return nil
	}
	now := t.now()
	var out []Connection
	for _, c := range u.conns {
		if now.Before(c.expiresAt) {
			out = append(out, c.c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Status returns the user's presence as of now, applying lease expiry,
// away and the offline grace lazily.
func (t *Tracker) Status(userID string) Presence {
	t.mu.Lock()
	defer t.mu.Unlock()
	u := t.users[userID]
	if u == nil {
		return Presence{User: userID}
	}
	st, live, seen := t.view(u, t.now())
	return Presence{User: userID, State: st, LastSeen: seen, Connections: live}
}

// Count returns how many known users are in state s right now.
func (t *Tracker) Count(s State) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	n := 0
	for _, u := range t.users {
		if st, _, _ := t.view(u, now); st == s {
			n++
		}
	}
	return n
}

// Sweep removes expired leases and emits pending state changes for every
// user (crashed nodes, ended grace periods, idle users turning away). It
// returns the number of state changes. Run it every second or so: reads are
// correct without it, but watchers only hear about time-driven changes when
// something reconciles the user.
func (t *Tracker) Sweep() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	before := t.stats.StateChanges
	for name, u := range t.users {
		t.reconcile(name, u, now)
	}
	return int(t.stats.StateChanges - before)
}

// Stats returns a snapshot of the counters.
func (t *Tracker) Stats() Stats {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stats
}

// UpdatePosition stores a latest-position payload for the user, renewing
// the connection's lease. Positions that are not newer than the stored one
// (GPS fixes arriving out of order over a flaky network) are rejected with
// ErrStalePosition, so an old fix can never overwrite a newer one.
func (t *Tracker) UpdatePosition(userID, connID string, p Position) error {
	if p.At.IsZero() {
		return errors.New("presence: position timestamp is required")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	u := t.users[userID]
	if u == nil {
		return ErrUnknownConnection
	}
	c := u.conns[connID]
	if c == nil || !now.Before(c.expiresAt) {
		return ErrUnknownConnection
	}
	c.lastBeat = now
	c.expiresAt = now.Add(t.ttl)
	t.stats.Heartbeats++
	if u.pos != nil && !p.At.After(u.pos.At) {
		return ErrStalePosition
	}
	u.pos = &p
	return nil
}

// LatestPosition returns the user's latest position. If maxAge > 0 and the
// position was measured more than maxAge ago, it returns the position with
// ErrStalePosition so the reader can show "location unavailable" instead of
// a rider frozen in the wrong place.
func (t *Tracker) LatestPosition(userID string, maxAge time.Duration) (Position, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	u := t.users[userID]
	if u == nil || u.pos == nil {
		return Position{}, ErrNoPosition
	}
	p := *u.pos
	if maxAge > 0 && t.now().Sub(p.At) > maxAge {
		return p, ErrStalePosition
	}
	return p, nil
}

// view computes the user's state without mutating anything.
func (t *Tracker) view(u *user, now time.Time) (st State, live int, lastSeen time.Time) {
	gone, lastSeen := u.goneAt, u.lastSeen
	active := false
	for _, c := range u.conns {
		if c.lastBeat.After(lastSeen) {
			lastSeen = c.lastBeat
		}
		if now.Before(c.expiresAt) {
			live++
			if t.awayAfter == 0 || now.Sub(c.lastActive) < t.awayAfter {
				active = true
			}
		} else if c.expiresAt.After(gone) {
			gone = c.expiresAt
		}
	}
	switch {
	case live > 0 && active:
		return Online, live, lastSeen
	case live > 0:
		return Away, live, lastSeen
	case u.announced != Offline && now.Before(gone.Add(t.grace)):
		return u.announced, 0, lastSeen // inside the grace period: hold the last state
	default:
		return Offline, 0, lastSeen
	}
}

// reconcile drops expired leases and announces a state change if there is
// one. Callers hold t.mu.
func (t *Tracker) reconcile(name string, u *user, now time.Time) {
	for id, c := range u.conns {
		if !now.Before(c.expiresAt) {
			if c.expiresAt.After(u.goneAt) {
				u.goneAt = c.expiresAt
			}
			if c.lastBeat.After(u.lastSeen) {
				u.lastSeen = c.lastBeat
			}
			delete(u.conns, id)
		}
	}
	st, _, seen := t.view(u, now)
	if st == u.announced {
		return
	}
	u.announced = st
	t.stats.StateChanges++
	ev := Event{User: name, State: st, At: now, LastSeen: seen}
	for w := range u.watchers {
		w.push(ev)
	}
}

// Watcher receives coalesced presence changes for a set of users. Subscribe
// only to contacts that are actually visible (an open chat list, a map
// viewport): presence fan-out, not heartbeats, is the expensive part.
type Watcher struct {
	t         *Tracker
	users     map[string]struct{}
	pending   map[string]Event
	delivered map[string]State
	notify    chan struct{}
	closed    bool
}

// Watch subscribes to the given users. The first Drain includes the current
// state of every watched user who is not offline.
func (t *Tracker) Watch(users ...string) (*Watcher, error) {
	w := &Watcher{
		t:         t,
		users:     make(map[string]struct{}),
		pending:   make(map[string]Event),
		delivered: make(map[string]State),
		notify:    make(chan struct{}, 1),
	}
	for _, u := range users {
		if err := w.Add(u); err != nil {
			return nil, err
		}
	}
	return w, nil
}

// Add starts watching a user.
func (w *Watcher) Add(userID string) error {
	if userID == "" {
		return errors.New("presence: user to watch must not be empty")
	}
	t := w.t
	t.mu.Lock()
	defer t.mu.Unlock()
	if w.closed {
		return errors.New("presence: watcher is closed")
	}
	if _, ok := w.users[userID]; ok {
		return nil
	}
	now := t.now()
	u := t.get(userID)
	t.reconcile(userID, u, now)
	if u.watchers == nil {
		u.watchers = make(map[*Watcher]struct{})
	}
	u.watchers[w] = struct{}{}
	w.users[userID] = struct{}{}
	w.delivered[userID] = Offline
	if u.announced != Offline {
		_, _, seen := t.view(u, now)
		w.push(Event{User: userID, State: u.announced, At: now, LastSeen: seen})
	}
	return nil
}

// Remove stops watching a user and drops any pending event for them.
func (w *Watcher) Remove(userID string) {
	t := w.t
	t.mu.Lock()
	defer t.mu.Unlock()
	w.remove(userID)
}

func (w *Watcher) remove(userID string) {
	if u := w.t.users[userID]; u != nil {
		delete(u.watchers, w)
	}
	delete(w.users, userID)
	delete(w.pending, userID)
	delete(w.delivered, userID)
}

// Close unsubscribes from every user.
func (w *Watcher) Close() {
	t := w.t
	t.mu.Lock()
	defer t.mu.Unlock()
	for u := range w.users {
		w.remove(u)
	}
	w.closed = true
}

// Notify returns a channel that receives a value when events are pending.
// It never blocks the tracker: several pushes collapse into one signal.
func (w *Watcher) Notify() <-chan struct{} { return w.notify }

// Drain returns the pending events, at most one per user (the latest), sorted
// by user. A user who changed and changed back since the last Drain (online,
// offline, online) produces no event at all.
func (w *Watcher) Drain() []Event {
	t := w.t
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(w.pending) == 0 {
		return nil
	}
	out := make([]Event, 0, len(w.pending))
	for _, ev := range w.pending {
		ev.Prev = w.delivered[ev.User]
		w.delivered[ev.User] = ev.State
		out = append(out, ev)
	}
	clear(w.pending)
	sort.Slice(out, func(i, j int) bool { return out[i].User < out[j].User })
	t.stats.Delivered += uint64(len(out))
	return out
}

// push records ev for later delivery. Callers hold t.mu.
func (w *Watcher) push(ev Event) {
	if w.delivered[ev.User] == ev.State {
		delete(w.pending, ev.User) // changed back before the watcher looked
		return
	}
	w.pending[ev.User] = ev
	select {
	case w.notify <- struct{}{}:
	default:
	}
}
