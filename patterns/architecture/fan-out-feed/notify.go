package fanoutfeed

// This file adds a push-notification dispatcher for the fan-out-on-write
// path. When a celebrity posts, the fan-out can enqueue hundreds of
// thousands of "new post" pushes in one go. Two things then go wrong with a
// single FIFO queue:
//
//   - Transactional pushes (OTP codes, payment receipts, "your ride is here")
//     queue behind the storm, and a 30-second-old OTP is useless.
//   - Push providers (APNs, FCM) throttle senders, so the storm cannot simply
//     be sent faster.
//
// The Dispatcher keeps one queue per (provider, lane). Each provider has its
// own token bucket, and within a provider the lanes are drained by strict
// priority with a starvation limit (see WithStarvationLimit). Social pushes
// can be coalesced per user and collapse key, so a like storm becomes
// "alice and 41 others liked your post": one push per user per window.

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	tokenbucket "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/rate-limiter-token-bucket"
)

// Lane is a notification priority class. Lower values are drained first.
type Lane int

// Lanes, highest priority first.
const (
	Transactional Lane = iota // OTP, payment, ride arrival: must never wait behind a storm
	Social                    // likes, comments, "new post from someone you follow"
	Marketing                 // promotions, digests
	numLanes
)

func (l Lane) String() string {
	switch l {
	case Transactional:
		return "transactional"
	case Social:
		return "social"
	case Marketing:
		return "marketing"
	}
	return "unknown"
}

// Notification is one push to enqueue.
type Notification struct {
	User     string // recipient
	Provider string // e.g. "apns" or "fcm"; must be configured with WithProvider
	Lane     Lane
	Actor    string // who caused it, e.g. "alice"
	Text     string // verb phrase, e.g. "liked your post"
	// CollapseKey groups Social notifications for coalescing, e.g.
	// "like:post42". Empty means never coalesce.
	CollapseKey string
	EnqueuedAt  time.Time // set by Enqueue
}

// Delivery is one push handed to a provider. With coalescing, it can stand
// for several notifications.
type Delivery struct {
	Notification           // the first notification in the group
	Count        int       // how many notifications this push represents
	SentAt       time.Time // simulated send time
}

// Delay is how long the (first) notification waited before being sent.
func (d Delivery) Delay() time.Duration { return d.SentAt.Sub(d.EnqueuedAt) }

// Summary renders the push text, e.g. "alice and 41 others liked your post".
func (d Delivery) Summary() string {
	switch d.Count {
	case 0, 1:
		return fmt.Sprintf("%s %s", d.Actor, d.Text)
	case 2:
		return fmt.Sprintf("%s and 1 other %s", d.Actor, d.Text)
	}
	return fmt.Sprintf("%s and %d others %s", d.Actor, d.Count-1, d.Text)
}

// DispatchStats counts dispatcher work.
type DispatchStats struct {
	Enqueued  int // notifications accepted
	Coalesced int // notifications merged into an already-queued push
	Sent      int // pushes handed to providers
	Throttled int // times a provider had work ready but no token
}

// DispatcherOption configures NewDispatcher.
type DispatcherOption func(*dispatcherConfig)

type providerConfig struct {
	name  string
	rate  float64
	burst int
}

type dispatcherConfig struct {
	now        func() time.Time
	providers  []providerConfig
	starveAt   int
	window     time.Duration
	singleFIFO bool
}

// WithDispatchClock sets the clock used for enqueue times, send times,
// coalescing windows and the provider token buckets. Default time.Now.
func WithDispatchClock(now func() time.Time) DispatcherOption {
	return func(c *dispatcherConfig) { c.now = now }
}

// WithProvider registers a push provider limited to ratePerSec sends per
// second with the given burst. At least one provider is required.
func WithProvider(name string, ratePerSec float64, burst int) DispatcherOption {
	return func(c *dispatcherConfig) {
		c.providers = append(c.providers, providerConfig{name, ratePerSec, burst})
	}
}

// WithStarvationLimit sets the anti-starvation rule. Lanes are drained by
// strict priority, except that a lower lane which has been passed over n
// times in a row while it had a sendable push gets the next send for that
// provider. A backlogged lane therefore gets at least about 1 in n+1 sends,
// and a higher lane gives up at most one send in n+1 to each lower lane.
// 0 disables the rule (pure strict priority, lower lanes can starve).
// Default 20.
func WithStarvationLimit(n int) DispatcherOption {
	return func(c *dispatcherConfig) { c.starveAt = n }
}

// WithCoalesceWindow enables coalescing of Social notifications that carry
// a CollapseKey. The first notification for a (user, key) waits window
// before it becomes sendable, and later ones for the same pair merge into
// it until it is sent, so each pair gets at most one push per window.
// 0 (the default) disables coalescing.
func WithCoalesceWindow(window time.Duration) DispatcherOption {
	return func(c *dispatcherConfig) { c.window = window }
}

// WithSingleFIFO turns off lanes: each provider has one FIFO queue and the
// Lane field is ignored for ordering. It exists to show the failure mode
// the lanes fix. It cannot be combined with WithCoalesceWindow.
func WithSingleFIFO() DispatcherOption {
	return func(c *dispatcherConfig) { c.singleFIFO = true }
}

type entry struct {
	n       Notification
	count   int
	readyAt time.Time
	key     string // coalescing key, empty if not coalescable
}

// fifo is a slice-backed queue that reclaims its consumed prefix.
type fifo struct {
	items []*entry
	head  int
}

func (q *fifo) len() int { return len(q.items) - q.head }

func (q *fifo) peek() *entry { return q.items[q.head] }

func (q *fifo) push(e *entry) { q.items = append(q.items, e) }

func (q *fifo) pop() *entry {
	e := q.items[q.head]
	q.items[q.head] = nil
	q.head++
	if q.head > 1024 && q.head*2 > len(q.items) {
		q.items = append([]*entry(nil), q.items[q.head:]...)
		q.head = 0
	}
	return e
}

type provider struct {
	name    string
	limiter *tokenbucket.Limiter
	lanes   [numLanes]fifo
	skips   [numLanes]int // consecutive sends this lane was passed over while sendable
}

// Dispatcher queues notifications by provider and lane and sends them within
// each provider's rate limit. Safe for concurrent use.
type Dispatcher struct {
	now        func() time.Time
	starveAt   int
	window     time.Duration
	singleFIFO bool

	mu        sync.Mutex
	providers map[string]*provider
	order     []*provider       // sorted by name, for deterministic output
	pending   map[string]*entry // coalescing key -> queued, unsent entry
	stats     DispatchStats
}

// NewDispatcher validates the options and builds a dispatcher.
func NewDispatcher(opts ...DispatcherOption) (*Dispatcher, error) {
	c := dispatcherConfig{now: time.Now, starveAt: 20}
	for _, o := range opts {
		o(&c)
	}
	if c.now == nil {
		return nil, errors.New("fanoutfeed: nil clock")
	}
	if len(c.providers) == 0 {
		return nil, errors.New("fanoutfeed: at least one provider is required")
	}
	if c.starveAt < 0 {
		return nil, errors.New("fanoutfeed: starvation limit must be >= 0")
	}
	if c.window < 0 {
		return nil, errors.New("fanoutfeed: coalesce window must be >= 0")
	}
	if c.singleFIFO && c.window > 0 {
		return nil, errors.New("fanoutfeed: single FIFO mode does not support coalescing")
	}
	d := &Dispatcher{
		now: c.now, starveAt: c.starveAt, window: c.window, singleFIFO: c.singleFIFO,
		providers: map[string]*provider{}, pending: map[string]*entry{},
	}
	for _, pc := range c.providers {
		if pc.name == "" {
			return nil, errors.New("fanoutfeed: empty provider name")
		}
		if pc.rate <= 0 || pc.burst < 1 {
			return nil, fmt.Errorf("fanoutfeed: provider %q needs rate > 0 and burst >= 1", pc.name)
		}
		if _, dup := d.providers[pc.name]; dup {
			return nil, fmt.Errorf("fanoutfeed: duplicate provider %q", pc.name)
		}
		p := &provider{name: pc.name, limiter: tokenbucket.New(pc.rate, pc.burst, c.now)}
		d.providers[pc.name] = p
		d.order = append(d.order, p)
	}
	sort.Slice(d.order, func(i, j int) bool { return d.order[i].name < d.order[j].name })
	return d, nil
}

// Enqueue accepts a notification. It returns an error for an unknown
// provider, an invalid lane or an empty user.
func (d *Dispatcher) Enqueue(n Notification) error {
	if n.User == "" {
		return errors.New("fanoutfeed: notification needs a user")
	}
	if n.Lane < 0 || n.Lane >= numLanes {
		return fmt.Errorf("fanoutfeed: invalid lane %d", n.Lane)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	p, ok := d.providers[n.Provider]
	if !ok {
		return fmt.Errorf("fanoutfeed: unknown provider %q", n.Provider)
	}
	now := d.now()
	n.EnqueuedAt = now
	d.stats.Enqueued++

	e := &entry{n: n, count: 1, readyAt: now}
	if d.window > 0 && n.Lane == Social && n.CollapseKey != "" {
		e.key = n.User + "\x00" + n.CollapseKey
		if q := d.pending[e.key]; q != nil {
			q.count++
			d.stats.Coalesced++
			return nil
		}
		// Every coalescable entry waits the same window, so readyAt stays
		// non-decreasing along the Social queue and only the head needs a
		// readiness check.
		e.readyAt = now.Add(d.window)
		d.pending[e.key] = e
	}
	lane := n.Lane
	if d.singleFIFO {
		lane = 0
	}
	p.lanes[lane].push(e)
	return nil
}

// pick chooses the lane to send from next, or -1 if nothing is sendable.
// Caller holds d.mu.
func (d *Dispatcher) pick(p *provider, now time.Time) Lane {
	first := Lane(-1)
	for l := Lane(0); l < numLanes; l++ {
		q := &p.lanes[l]
		if q.len() == 0 || q.peek().readyAt.After(now) {
			continue
		}
		if first < 0 {
			first = l
		}
		if d.starveAt > 0 && p.skips[l] >= d.starveAt {
			return l // starved: the highest starved lane wins
		}
	}
	return first
}

// Dispatch sends every push that is sendable now and fits within its
// provider's token bucket, and returns them in send order. Call it on every
// tick of the (simulated) clock.
func (d *Dispatcher) Dispatch() []Delivery {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := d.now()
	var out []Delivery
	for _, p := range d.order {
		for {
			l := d.pick(p, now)
			if l < 0 {
				break
			}
			if !p.limiter.Allow() {
				d.stats.Throttled++
				break
			}
			e := p.lanes[l].pop()
			if e.key != "" {
				delete(d.pending, e.key)
			}
			// Lower lanes that were sendable but passed over get closer to
			// their guaranteed slot. Higher lanes passed over for a starved
			// lane are not starving, so they are not counted.
			p.skips[l] = 0
			for o := l + 1; o < numLanes; o++ {
				q := &p.lanes[o]
				if q.len() > 0 && !q.peek().readyAt.After(now) {
					p.skips[o]++
				}
			}
			d.stats.Sent++
			out = append(out, Delivery{Notification: e.n, Count: e.count, SentAt: now})
		}
	}
	return out
}

// Pending returns the number of queued (unsent) pushes.
func (d *Dispatcher) Pending() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, p := range d.order {
		for l := range p.lanes {
			n += p.lanes[l].len()
		}
	}
	return n
}

// Stats returns the dispatcher counters.
func (d *Dispatcher) Stats() DispatchStats {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.stats
}
