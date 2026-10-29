// Package longpolling implements HTTP long polling over a cursor-numbered
// event log.
//
// Some clients need near-real-time updates but cannot keep a streaming
// connection open: corporate proxies buffer or kill Server-Sent Events,
// WebSockets are blocked or unsupported, or the platform (serverless
// functions, some API gateways) only speaks request/response. Short polling,
// asking "anything new?" every N seconds, works everywhere but wastes most
// requests on empty answers and delays each update by up to N seconds.
//
// Long polling keeps plain request/response but lets the server hold the
// request open until there is something to say:
//
//   - every published event gets a monotonically increasing cursor;
//   - GET /poll?cursor=N answers at once if an event newer than N exists;
//   - otherwise the request waits until an event is published or a timeout
//     passes, and then returns 204 No Content so the client simply asks again.
//
// Because the client always sends the cursor of the last event it saw, an
// event published while the client is between two requests is not lost: the
// next poll finds it in the log and returns immediately.
//
// Waiting is done without busy loops. A Topic keeps one "notify" channel that
// every held request selects on; Publish closes it (waking them all) and
// installs a fresh one. Held requests are capped, so a burst of idle clients
// cannot exhaust the server; past the cap a request gets 503 with
// Retry-After. When a client disconnects, its request context is cancelled
// and the waiter is removed.
package longpolling

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Event is one entry in the log.
type Event struct {
	Cursor uint64 `json:"cursor"`
	Data   string `json:"data"`
}

// Batch is the answer to one poll.
type Batch struct {
	// Events are the events after the requested cursor, oldest first. Empty
	// means the poll timed out (or was a short poll with nothing new).
	Events []Event `json:"events"`
	// Cursor is what the client should send next: the last event returned,
	// or the requested cursor when nothing was returned.
	Cursor uint64 `json:"cursor"`
	// Reset is true when the requested cursor was older than the retained
	// history or newer than anything published (for example after a server
	// restart). Events then start at the oldest retained event, and the
	// client should refetch full state.
	Reset bool `json:"reset,omitempty"`
}

// Stats counts how held requests ended.
type Stats struct {
	Published uint64 // events published
	Immediate uint64 // polls answered at once because events were already waiting
	Woken     uint64 // held polls answered by a publish
	TimedOut  uint64 // held polls that hit their timeout and returned empty
	Cancelled uint64 // held polls whose client went away
	Rejected  uint64 // polls refused because the held-request cap was reached
}

var (
	// ErrTooManyWaiters means the held-request cap was reached.
	ErrTooManyWaiters = errors.New("longpolling: too many held requests")
	// ErrClosed means the topic was closed.
	ErrClosed = errors.New("longpolling: topic closed")
)

// Topic is a concurrency-safe event log that long-poll requests wait on.
type Topic struct {
	maxWait    time.Duration
	maxHeld    int
	history    int
	maxBatch   int
	retryAfter time.Duration

	mu     sync.Mutex
	head   uint64  // cursor of the newest event, 0 before the first publish
	events []Event // the last `history` events, oldest first
	notify chan struct{}
	held   int
	closed bool
	stats  Stats
}

// Option configures a Topic.
type Option func(*options)

type options struct {
	maxWait    time.Duration
	maxHeld    int
	history    int
	maxBatch   int
	retryAfter time.Duration
}

// WithMaxWait sets the longest a request is held before it returns empty.
// Clients may ask for less with ?wait=. The default is 25s, which stays
// under the common 30s-60s idle timeouts of proxies and load balancers.
func WithMaxWait(d time.Duration) Option { return func(o *options) { o.maxWait = d } }

// WithMaxHeld caps how many requests may be held at once. Past the cap a
// poll is rejected with ErrTooManyWaiters (HTTP 503). The default is 10000.
func WithMaxHeld(n int) Option { return func(o *options) { o.maxHeld = n } }

// WithHistory sets how many recent events are kept for clients that fall
// behind. The default is 1024.
func WithHistory(n int) Option { return func(o *options) { o.history = n } }

// WithMaxBatch caps how many events one poll returns. The client gets the
// rest on its next poll, which returns immediately. 0 (the default) means
// no cap beyond the history size.
func WithMaxBatch(n int) Option { return func(o *options) { o.maxBatch = n } }

// WithRetryAfter sets the Retry-After advertised with a 503 when the cap
// is reached. It is sent in whole seconds, at least 1. The default is 1s.
func WithRetryAfter(d time.Duration) Option { return func(o *options) { o.retryAfter = d } }

// New returns an empty Topic.
func New(opts ...Option) (*Topic, error) {
	o := options{maxWait: 25 * time.Second, maxHeld: 10000, history: 1024, retryAfter: time.Second}
	for _, opt := range opts {
		opt(&o)
	}
	switch {
	case o.maxWait <= 0:
		return nil, errors.New("longpolling: max wait must be positive")
	case o.maxHeld <= 0:
		return nil, errors.New("longpolling: max held must be positive")
	case o.history <= 0:
		return nil, errors.New("longpolling: history must be positive")
	case o.maxBatch < 0:
		return nil, errors.New("longpolling: max batch must not be negative")
	case o.retryAfter <= 0:
		return nil, errors.New("longpolling: retry-after must be positive")
	}
	return &Topic{
		maxWait: o.maxWait, maxHeld: o.maxHeld, history: o.history,
		maxBatch: o.maxBatch, retryAfter: o.retryAfter,
		notify: make(chan struct{}),
	}, nil
}

// Publish appends an event and wakes every held request.
func (t *Topic) Publish(data string) (Event, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return Event{}, ErrClosed
	}
	t.head++
	ev := Event{Cursor: t.head, Data: data}
	t.events = append(t.events, ev)
	if len(t.events) > t.history {
		t.events = t.events[len(t.events)-t.history:]
	}
	t.stats.Published++
	close(t.notify) // broadcast: every waiter selecting on it wakes
	t.notify = make(chan struct{})
	return ev, nil
}

// Wait returns the events after cursor. If there are none it holds for up
// to hold (capped at the topic's max wait) until one is published. A hold of
// 0 or less answers at once (short polling). On timeout it returns an empty
// Batch and a nil error. It returns ctx.Err() if ctx ends first,
// ErrTooManyWaiters past the held-request cap, and ErrClosed after Close.
func (t *Topic) Wait(ctx context.Context, cursor uint64, hold time.Duration) (Batch, error) {
	hold = min(hold, t.maxWait)
	var timeout <-chan time.Time
	registered := false
	defer func() {
		if registered {
			t.mu.Lock()
			t.held--
			t.mu.Unlock()
		}
	}()
	for {
		t.mu.Lock()
		if t.closed {
			t.mu.Unlock()
			return Batch{Cursor: cursor}, ErrClosed
		}
		// Checking the log and taking the notify channel under the same lock
		// is what prevents a lost wakeup: a Publish either happened before
		// (and collect sees its event) or happens after (and closes ch).
		if b, ok := t.collect(cursor); ok {
			if registered {
				t.stats.Woken++
			} else {
				t.stats.Immediate++
			}
			t.mu.Unlock()
			return b, nil
		}
		if hold <= 0 {
			t.mu.Unlock()
			return Batch{Cursor: cursor}, nil
		}
		if !registered {
			if t.held >= t.maxHeld {
				t.stats.Rejected++
				t.mu.Unlock()
				return Batch{Cursor: cursor}, ErrTooManyWaiters
			}
			t.held++
			registered = true
		}
		ch := t.notify
		t.mu.Unlock()

		if timeout == nil {
			timer := time.NewTimer(hold)
			defer timer.Stop()
			timeout = timer.C
		}
		select {
		case <-ch:
			// Something was published (or the topic closed): re-check.
		case <-timeout:
			t.count(func(s *Stats) { s.TimedOut++ })
			return Batch{Cursor: cursor}, nil
		case <-ctx.Done():
			t.count(func(s *Stats) { s.Cancelled++ })
			return Batch{Cursor: cursor}, ctx.Err()
		}
	}
}

// collect returns the events after cursor. ok is false when there are none
// and the caller should wait. The caller holds t.mu.
func (t *Topic) collect(cursor uint64) (Batch, bool) {
	if cursor == t.head {
		return Batch{}, false
	}
	var from int // index into t.events of the first event to return
	reset := false
	if cursor > t.head {
		reset = true // a cursor from the future: the server lost its log
	} else {
		oldest := t.events[0].Cursor
		if cursor+1 < oldest {
			reset = true // fell further behind than history reaches
		} else {
			from = int(cursor + 1 - oldest)
		}
	}
	evs := t.events[from:]
	if t.maxBatch > 0 && len(evs) > t.maxBatch {
		evs = evs[:t.maxBatch]
	}
	b := Batch{Events: append([]Event(nil), evs...), Reset: reset, Cursor: t.head}
	if len(b.Events) > 0 {
		b.Cursor = b.Events[len(b.Events)-1].Cursor
	}
	return b, true
}

func (t *Topic) count(f func(*Stats)) {
	t.mu.Lock()
	f(&t.stats)
	t.mu.Unlock()
}

// Held returns how many requests are being held right now.
func (t *Topic) Held() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.held
}

// Head returns the cursor of the newest event.
func (t *Topic) Head() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.head
}

// Stats returns a snapshot of the counters.
func (t *Topic) Stats() Stats {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stats
}

// Close wakes every held request with ErrClosed and refuses new polls.
func (t *Topic) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.closed {
		t.closed = true
		close(t.notify)
	}
}

// ServeHTTP handles GET ?cursor=N&wait=MS.
//
//   - cursor (default 0) is the last event the client has; 0 means "from the
//     start of retained history".
//   - wait (milliseconds, optional) is how long the client is willing to be
//     held, capped at the topic's max wait. wait=0 is a short poll.
//
// It answers 200 with a JSON Batch, 204 when the hold timed out, 400 on bad
// parameters, and 503 with Retry-After when the held-request cap is reached
// or the topic is closed.
func (t *Topic) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	var cursor uint64
	if raw := q.Get("cursor"); raw != "" {
		c, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			http.Error(w, "bad cursor", http.StatusBadRequest)
			return
		}
		cursor = c
	}
	hold := t.maxWait
	if raw := q.Get("wait"); raw != "" {
		ms, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || ms < 0 {
			http.Error(w, "bad wait", http.StatusBadRequest)
			return
		}
		hold = time.Duration(min(ms, math.MaxInt64/int64(time.Millisecond))) * time.Millisecond
	}

	b, err := t.Wait(r.Context(), cursor, hold)
	switch {
	case errors.Is(err, ErrTooManyWaiters), errors.Is(err, ErrClosed):
		secs := int64(math.Ceil(t.retryAfter.Seconds()))
		w.Header().Set("Retry-After", strconv.FormatInt(max(secs, 1), 10))
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	case err != nil:
		return // the client went away; nobody is listening
	}
	w.Header().Set("Cache-Control", "no-store")
	if len(b.Events) == 0 && !b.Reset {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(b)
}

// NoHold, as Client.Hold, asks the server to answer at once: short polling.
const NoHold time.Duration = -1

// BusyError is returned by Client.Poll when the server is at its
// held-request cap or shutting down (HTTP 503).
type BusyError struct {
	RetryAfter time.Duration // from the Retry-After header, 0 if absent
}

func (e *BusyError) Error() string {
	return fmt.Sprintf("longpolling: server busy, retry after %v", e.RetryAfter)
}

// Is makes errors.Is(err, ErrTooManyWaiters) true for a BusyError.
func (e *BusyError) Is(target error) bool { return target == ErrTooManyWaiters }

// ClientStats counts what a Client saw.
type ClientStats struct {
	Requests uint64 // polls sent
	Empty    uint64 // polls that returned no events
	Events   uint64 // events received
	Resets   uint64 // batches flagged Reset
	Errors   uint64 // failed polls, including 503s
}

// Client polls a long-poll endpoint and tracks its cursor.
type Client struct {
	// HTTP is the client used. nil means http.DefaultClient. Its Timeout,
	// if set, must exceed Hold, or every held poll fails.
	HTTP *http.Client
	// Hold is how long the server may hold each poll. 0 lets the server use
	// its default; NoHold makes every poll return at once (short polling).
	Hold time.Duration
	// Interval is a pause after each successful poll. Long polling leaves it
	// at 0 and re-polls immediately; short polling sets it.
	Interval time.Duration
	// Backoff is the pause after a failed poll, or after a 503 without
	// Retry-After. 0 means 1s.
	Backoff time.Duration
	// MaxBackoff, if positive, caps the pause a server's Retry-After can ask for.
	MaxBackoff time.Duration
	// OnReset, if set, is called when the server says the cursor was lost
	// and the client should refetch full state.
	OnReset func()

	url string

	mu     sync.Mutex
	cursor uint64

	requests, empty, events, resets, errs atomic.Uint64
}

// NewClient returns a client for endpoint, starting after cursor (0 means
// from the start of the server's retained history).
func NewClient(endpoint string, cursor uint64) (*Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, errors.New("longpolling: endpoint must be an absolute URL")
	}
	return &Client{url: endpoint, cursor: cursor}, nil
}

// Cursor returns the cursor the next poll will send.
func (c *Client) Cursor() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cursor
}

// Stats returns a snapshot of the client's counters.
func (c *Client) Stats() ClientStats {
	return ClientStats{
		Requests: c.requests.Load(), Empty: c.empty.Load(), Events: c.events.Load(),
		Resets: c.resets.Load(), Errors: c.errs.Load(),
	}
}

// Poll sends one request and advances the cursor past what it returns.
func (c *Client) Poll(ctx context.Context) (Batch, error) {
	httpc := c.HTTP
	if httpc == nil {
		httpc = http.DefaultClient
	}
	u, _ := url.Parse(c.url) // validated by NewClient
	q := u.Query()
	cursor := c.Cursor()
	q.Set("cursor", strconv.FormatUint(cursor, 10))
	switch {
	case c.Hold < 0:
		q.Set("wait", "0")
	case c.Hold > 0:
		q.Set("wait", strconv.FormatInt(max(c.Hold.Milliseconds(), 1), 10))
	}
	u.RawQuery = q.Encode()

	c.requests.Add(1)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		c.errs.Add(1)
		return Batch{Cursor: cursor}, err
	}
	resp, err := httpc.Do(req)
	if err != nil {
		c.errs.Add(1)
		return Batch{Cursor: cursor}, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		_, _ = io.Copy(io.Discard, resp.Body)
		c.empty.Add(1)
		return Batch{Cursor: cursor}, nil
	case http.StatusOK:
		var b Batch
		if err := json.NewDecoder(resp.Body).Decode(&b); err != nil {
			c.errs.Add(1)
			return Batch{Cursor: cursor}, fmt.Errorf("longpolling: decode: %w", err)
		}
		c.mu.Lock()
		c.cursor = b.Cursor
		c.mu.Unlock()
		c.events.Add(uint64(len(b.Events)))
		if len(b.Events) == 0 {
			c.empty.Add(1)
		}
		if b.Reset {
			c.resets.Add(1)
		}
		return b, nil
	case http.StatusServiceUnavailable:
		_, _ = io.Copy(io.Discard, resp.Body)
		c.errs.Add(1)
		be := &BusyError{}
		if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && s > 0 {
			be.RetryAfter = time.Duration(s) * time.Second
		}
		return Batch{Cursor: cursor}, be
	default:
		_, _ = io.Copy(io.Discard, resp.Body)
		c.errs.Add(1)
		return Batch{Cursor: cursor}, fmt.Errorf("longpolling: unexpected status %d", resp.StatusCode)
	}
}

// Run polls until ctx is done, calling handle for every event in order. It
// re-polls immediately after each answer (plus Interval), and backs off
// after errors, honouring Retry-After. It returns ctx.Err().
func (c *Client) Run(ctx context.Context, handle func(Event)) error {
	for {
		b, err := c.Poll(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		pause := c.Interval
		if err != nil {
			pause = c.Backoff
			if pause <= 0 {
				pause = time.Second
			}
			var be *BusyError
			if errors.As(err, &be) && be.RetryAfter > 0 {
				pause = be.RetryAfter
			}
			if c.MaxBackoff > 0 {
				pause = min(pause, c.MaxBackoff)
			}
		} else {
			if b.Reset && c.OnReset != nil {
				c.OnReset()
			}
			for _, ev := range b.Events {
				handle(ev)
			}
		}
		if pause > 0 {
			timer := time.NewTimer(pause)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
}
