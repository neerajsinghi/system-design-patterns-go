// Package connectionpool implements a bounded, generic connection pool: a
// set of already-open connections (database sessions, TCP/TLS sockets, gRPC
// channels) that requests borrow and give back instead of dialing their own.
//
// Opening a connection per request is expensive and dangerous at scale. Each
// open pays a TCP handshake, usually a TLS handshake and often an
// authentication round trip before any useful work happens. Every close
// leaves a socket in TIME_WAIT on the side that closed, so at high request
// rates the client runs out of ephemeral ports. And the backend has a hard
// connection limit (Postgres max_connections, a proxy's file descriptor
// limit): enough concurrent requests and new connections are refused. A pool
// with no upper bound has the same last problem: during a spike it opens as
// many connections as there are concurrent requests.
//
// Pool[C] bounds and reuses connections:
//
//   - MaxOpen caps connections that are open, in use or being dialed. When
//     the cap is reached, callers wait in a FIFO queue, bounded by their
//     context. A released connection is handed directly to the oldest
//     waiter, so waiters are served in arrival order and nobody can jump the
//     queue by arriving just as a connection comes back.
//   - MaxIdle caps how many unused connections are kept open.
//   - IdleTimeout closes connections that sat unused for too long.
//   - MaxLifetime retires connections after a fixed age, so a pool notices
//     DNS changes, load-balancer rebalancing and server-side rotation instead
//     of holding the same sockets forever.
//   - A validate function checks idle connections when they are borrowed, so
//     connections broken while idle (backend restart, proxy timeout) are
//     discarded instead of being handed to a request.
//
// The clock is injectable so idle timeout and lifetime can be tested without
// sleeping.
package connectionpool

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"
)

// ErrClosed is returned by Get after Close, and to callers that were waiting
// when the pool was closed.
var ErrClosed = errors.New("connectionpool: pool closed")

// DialFunc opens a new connection.
type DialFunc[C any] func(ctx context.Context) (C, error)

// CloseFunc closes a connection. The pool calls it outside its lock.
type CloseFunc[C any] func(C)

// ValidateFunc checks that an idle connection still works before it is lent
// out. A non-nil error discards the connection.
type ValidateFunc[C any] func(C) error

// Stats is a snapshot of the pool's counters and gauges.
type Stats struct {
	// Gauges.
	Open    int // connections open, in use or being dialed
	Idle    int // open connections waiting to be borrowed
	InUse   int // Open - Idle
	Waiting int // callers queued for a connection

	// Counters.
	Opens           uint64        // successful dials
	DialErrors      uint64        // failed dials
	Reuses          uint64        // borrows served by an existing connection
	Waits           uint64        // borrows that had to queue
	WaitTime        time.Duration // total time spent queued (by the pool's clock)
	Timeouts        uint64        // waiters whose context ended before they got a connection
	Discards        uint64        // connections failing validation or passed to Discard
	ExpiredIdle     uint64        // closed by IdleTimeout
	ExpiredLifetime uint64        // closed by MaxLifetime
	ClosedMaxIdle   uint64        // closed on release because MaxIdle idle connections were already kept
}

// ReuseRate is Reuses / (Reuses + Opens), the fraction of borrows that did not
// pay for a new connection.
func (s Stats) ReuseRate() float64 {
	if s.Reuses+s.Opens == 0 {
		return 0
	}
	return float64(s.Reuses) / float64(s.Reuses+s.Opens)
}

// Option configures a Pool.
type Option func(*options)

type options struct {
	maxOpen     int
	maxIdle     int
	maxIdleSet  bool
	idleTimeout time.Duration
	maxLifetime time.Duration
	validate    any // ValidateFunc[C]; typed in New
	now         func() time.Time
}

// WithMaxOpen caps open connections (in use, idle or being dialed). 0, the
// default, means unlimited, which is rarely what you want in production.
func WithMaxOpen(n int) Option { return func(o *options) { o.maxOpen = n } }

// WithMaxIdle caps idle connections kept for reuse. 0 keeps none (every
// release closes the connection). The default is MaxOpen (unlimited when
// MaxOpen is unlimited).
func WithMaxIdle(n int) Option {
	return func(o *options) { o.maxIdle, o.maxIdleSet = n, true }
}

// WithIdleTimeout closes connections that have been idle for at least d.
// 0 disables it.
func WithIdleTimeout(d time.Duration) Option { return func(o *options) { o.idleTimeout = d } }

// WithMaxLifetime closes connections at least d old when they are released or
// found idle. 0 disables it.
func WithMaxLifetime(d time.Duration) Option { return func(o *options) { o.maxLifetime = d } }

// WithValidate sets a health check run on idle connections when they are
// borrowed. Its type parameter must match the pool's.
func WithValidate[C any](f ValidateFunc[C]) Option { return func(o *options) { o.validate = f } }

// WithClock injects the clock used for idle timeout, lifetime and wait time.
func WithClock(now func() time.Time) Option {
	return func(o *options) {
		if now != nil {
			o.now = now
		}
	}
}

type pconn[C any] struct {
	c        C
	created  time.Time
	returned time.Time // when it last became idle
}

// grant is what a waiter receives: a connection, permission to dial (a free
// slot was transferred to it), or an error.
type grant[C any] struct {
	pc   *pconn[C]
	dial bool
	err  error
}

type waiter[C any] struct {
	ch   chan grant[C] // buffered 1: granting never blocks
	enq  time.Time
	elem *list.Element // nil once granted or removed
}

// Pool is a concurrency-safe pool of connections of type C.
type Pool[C any] struct {
	dial        DialFunc[C]
	close       CloseFunc[C]
	validate    ValidateFunc[C]
	maxOpen     int // 0: unlimited
	maxIdle     int // -1: unlimited
	idleTimeout time.Duration
	maxLifetime time.Duration
	now         func() time.Time

	mu      sync.Mutex
	numOpen int
	idle    []*pconn[C] // oldest return first; borrowed from the end (LIFO)
	waiters *list.List  // of *waiter[C], oldest first
	closed  bool
	stats   Stats
}

// New returns a pool that opens connections with dial and closes them with
// closeFn (which may be nil).
func New[C any](dial DialFunc[C], closeFn CloseFunc[C], opts ...Option) (*Pool[C], error) {
	if dial == nil {
		return nil, errors.New("connectionpool: dial func is required")
	}
	o := options{now: time.Now}
	for _, opt := range opts {
		opt(&o)
	}
	switch {
	case o.maxOpen < 0:
		return nil, errors.New("connectionpool: max open must be >= 0")
	case o.maxIdleSet && o.maxIdle < 0:
		return nil, errors.New("connectionpool: max idle must be >= 0")
	case o.maxIdleSet && o.maxOpen > 0 && o.maxIdle > o.maxOpen:
		return nil, errors.New("connectionpool: max idle must not exceed max open")
	case o.idleTimeout < 0:
		return nil, errors.New("connectionpool: idle timeout must be >= 0")
	case o.maxLifetime < 0:
		return nil, errors.New("connectionpool: max lifetime must be >= 0")
	}
	var validate ValidateFunc[C]
	if o.validate != nil {
		v, ok := o.validate.(ValidateFunc[C])
		if !ok {
			return nil, errors.New("connectionpool: validate func has the wrong connection type")
		}
		validate = v
	}
	maxIdle := o.maxIdle
	if !o.maxIdleSet {
		maxIdle = o.maxOpen
		if o.maxOpen == 0 {
			maxIdle = -1
		}
	}
	if closeFn == nil {
		closeFn = func(C) {}
	}
	return &Pool[C]{
		dial:        dial,
		close:       closeFn,
		validate:    validate,
		maxOpen:     o.maxOpen,
		maxIdle:     maxIdle,
		idleTimeout: o.idleTimeout,
		maxLifetime: o.maxLifetime,
		now:         o.now,
		waiters:     list.New(),
	}, nil
}

// Conn is one borrow of a pooled connection. Call exactly one of Release or
// Discard when done; later calls are no-ops.
type Conn[C any] struct {
	pool   *Pool[C]
	pc     *pconn[C]
	reused bool
	done   bool
	mu     sync.Mutex
}

// Value returns the underlying connection.
func (c *Conn[C]) Value() C { return c.pc.c }

// Reused reports whether this borrow got an existing connection rather than
// a freshly dialed one.
func (c *Conn[C]) Reused() bool { return c.reused }

func (c *Conn[C]) finish() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done {
		return false
	}
	c.done = true
	return true
}

// Release returns a healthy connection to the pool: to the oldest waiter if
// there is one, otherwise to the idle set (or closed, if too old or MaxIdle is
// reached).
func (c *Conn[C]) Release() {
	if c.finish() {
		c.pool.put(c.pc)
	}
}

// Discard closes a broken connection instead of returning it.
func (c *Conn[C]) Discard() {
	if !c.finish() {
		return
	}
	p := c.pool
	p.mu.Lock()
	p.stats.Discards++
	p.mu.Unlock()
	p.retire(c.pc)
}

// Get borrows a connection. It reuses a valid idle connection, dials a new
// one if fewer than MaxOpen are open, and otherwise waits in FIFO order until
// one is released or ctx ends.
func (p *Pool[C]) Get(ctx context.Context) (*Conn[C], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, ErrClosed
		}
		now := p.now()

		// 0. Retire idle connections past IdleTimeout or MaxLifetime, then
		// look again: closing them frees slots.
		if expired := p.takeExpiredIdleLocked(now); len(expired) > 0 {
			p.mu.Unlock()
			p.retire(expired...)
			continue
		}

		// 1. Reuse the most recently returned idle connection.
		if n := len(p.idle); n > 0 {
			pc := p.idle[n-1]
			p.idle = p.idle[:n-1]
			p.mu.Unlock()
			if p.validate != nil {
				if err := p.validate(pc.c); err != nil {
					p.mu.Lock()
					p.stats.Discards++
					p.mu.Unlock()
					p.retire(pc)
					continue // try the next idle connection, or dial
				}
			}
			p.mu.Lock()
			p.stats.Reuses++
			p.mu.Unlock()
			return &Conn[C]{pool: p, pc: pc, reused: true}, nil
		}

		// 2. Dial if under the cap and nobody is queued ahead of us.
		if (p.maxOpen == 0 || p.numOpen < p.maxOpen) && p.waiters.Len() == 0 {
			p.numOpen++ // reserve the slot before dialing outside the lock
			p.mu.Unlock()
			return p.dialConn(ctx)
		}

		// 3. Queue.
		w := &waiter[C]{ch: make(chan grant[C], 1), enq: now}
		w.elem = p.waiters.PushBack(w)
		p.stats.Waits++
		p.mu.Unlock()

		select {
		case g := <-w.ch:
			return p.useGrant(ctx, g)
		case <-ctx.Done():
			p.mu.Lock()
			p.stats.Timeouts++
			if w.elem != nil { // still queued: just leave
				p.waiters.Remove(w.elem)
				w.elem = nil
				p.stats.WaitTime += p.now().Sub(w.enq)
				p.mu.Unlock()
				return nil, ctx.Err()
			}
			p.mu.Unlock()
			// Granted at the same moment ctx ended: give the grant back.
			g := <-w.ch
			switch {
			case g.pc != nil:
				p.put(g.pc)
			case g.dial:
				p.mu.Lock()
				p.releaseSlotLocked()
				p.mu.Unlock()
			}
			return nil, ctx.Err()
		}
	}
}

func (p *Pool[C]) useGrant(ctx context.Context, g grant[C]) (*Conn[C], error) {
	switch {
	case g.err != nil:
		return nil, g.err
	case g.pc != nil:
		// Handed over straight from Release: it was in use a moment ago, so it
		// is not re-validated.
		p.mu.Lock()
		p.stats.Reuses++
		p.mu.Unlock()
		return &Conn[C]{pool: p, pc: g.pc, reused: true}, nil
	default: // g.dial: a slot was transferred to us
		return p.dialConn(ctx)
	}
}

// dialConn dials with a slot already reserved in numOpen.
func (p *Pool[C]) dialConn(ctx context.Context) (*Conn[C], error) {
	c, err := p.dial(ctx)
	p.mu.Lock()
	if err != nil {
		p.stats.DialErrors++
		p.releaseSlotLocked()
		p.mu.Unlock()
		return nil, err
	}
	p.stats.Opens++
	closed := p.closed
	created := p.now()
	p.mu.Unlock()
	pc := &pconn[C]{c: c, created: created}
	if closed { // closed while we were dialing
		p.retire(pc)
		return nil, ErrClosed
	}
	return &Conn[C]{pool: p, pc: pc}, nil
}

// put takes back a connection that was lent out.
func (p *Pool[C]) put(pc *pconn[C]) {
	p.mu.Lock()
	now := p.now()
	switch {
	case p.closed:
	case p.tooOld(pc, now):
		p.stats.ExpiredLifetime++ // retired below; its slot may go to a waiter
	default:
		if w := p.popWaiterLocked(now); w != nil {
			w.ch <- grant[C]{pc: pc}
			p.mu.Unlock()
			return
		}
		if p.maxIdle < 0 || len(p.idle) < p.maxIdle {
			pc.returned = now
			p.idle = append(p.idle, pc)
			p.mu.Unlock()
			return
		}
		p.stats.ClosedMaxIdle++
	}
	p.mu.Unlock()
	p.retire(pc)
}

// retire closes connections and only then frees their slots, so the number
// of connections the backend sees never exceeds MaxOpen, even briefly.
func (p *Pool[C]) retire(pcs ...*pconn[C]) {
	for _, pc := range pcs {
		p.close(pc.c)
		p.mu.Lock()
		p.releaseSlotLocked()
		p.mu.Unlock()
	}
}

// releaseSlotLocked gives up one open-connection slot. If someone is waiting,
// the slot goes to them (they dial); otherwise the open count drops.
func (p *Pool[C]) releaseSlotLocked() {
	if !p.closed {
		if w := p.popWaiterLocked(p.now()); w != nil {
			w.ch <- grant[C]{dial: true}
			return
		}
	}
	p.numOpen--
}

func (p *Pool[C]) popWaiterLocked(now time.Time) *waiter[C] {
	front := p.waiters.Front()
	if front == nil {
		return nil
	}
	w := p.waiters.Remove(front).(*waiter[C])
	w.elem = nil
	p.stats.WaitTime += now.Sub(w.enq)
	return w
}

func (p *Pool[C]) tooOld(pc *pconn[C], now time.Time) bool {
	return p.maxLifetime > 0 && now.Sub(pc.created) >= p.maxLifetime
}

// takeExpiredIdleLocked removes idle connections past IdleTimeout or
// MaxLifetime from the idle set. They still count as open until retired.
func (p *Pool[C]) takeExpiredIdleLocked(now time.Time) []*pconn[C] {
	if p.idleTimeout <= 0 && p.maxLifetime <= 0 {
		return nil
	}
	var expired []*pconn[C]
	kept := p.idle[:0]
	for _, pc := range p.idle {
		switch {
		case p.idleTimeout > 0 && now.Sub(pc.returned) >= p.idleTimeout:
			p.stats.ExpiredIdle++
			expired = append(expired, pc)
		case p.tooOld(pc, now):
			p.stats.ExpiredLifetime++
			expired = append(expired, pc)
		default:
			kept = append(kept, pc)
		}
	}
	clear(p.idle[len(kept):])
	p.idle = kept
	return expired
}

// Reap closes idle connections past IdleTimeout or MaxLifetime and returns
// how many it closed. Get does the same lazily; call Reap from a ticker if
// the pool can sit unused for long periods.
func (p *Pool[C]) Reap() int {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return 0
	}
	expired := p.takeExpiredIdleLocked(p.now())
	p.mu.Unlock()
	p.retire(expired...)
	return len(expired)
}

// Stats returns a snapshot of the pool's counters.
func (p *Pool[C]) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.stats
	s.Open = p.numOpen
	s.Idle = len(p.idle)
	s.InUse = p.numOpen - len(p.idle)
	s.Waiting = p.waiters.Len()
	return s
}

// Close closes the pool: idle connections are closed now, waiters get
// ErrClosed, and connections still in use are closed when released. Close is
// idempotent.
func (p *Pool[C]) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	idle := p.idle
	p.idle = nil
	for e := p.waiters.Front(); e != nil; e = p.waiters.Front() {
		w := p.waiters.Remove(e).(*waiter[C])
		w.elem = nil
		w.ch <- grant[C]{err: ErrClosed}
	}
	p.mu.Unlock()
	p.retire(idle...)
}
