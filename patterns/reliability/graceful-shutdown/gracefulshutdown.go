// Package gracefulshutdown coordinates the shutdown of an HTTP service so
// that deploys and autoscaling do not cost requests.
//
// Killing a process mid-request has several distinct failure modes:
//
//   - In-flight HTTP requests see a connection reset.
//   - The load balancer keeps routing new requests to the instance for a few
//     seconds after it starts exiting, because deregistration is
//     asynchronous; those requests hit a closed port.
//   - Long-lived connections (SSE, WebSocket) drop without a hint, so every
//     client reconnects at once, possibly to the same dying instance.
//   - Queue consumers abandon the message they were processing, which is then
//     either lost or redelivered.
//
// A Drainer runs the shutdown in a fixed order:
//
//  1. Fail readiness, so the load balancer stops choosing this instance.
//  2. Wait a deregistration delay while still serving normally, so the load
//     balancer has time to notice.
//  3. Stop accepting new connections (http.Server.Shutdown), tell long-lived
//     handlers to wrap up (Draining channel and OnDrain callbacks), and cancel
//     background workers' context so they finish the current item and stop
//     pulling more.
//  4. Wait for in-flight requests and workers up to a drain deadline, then
//     force-close whatever is left (http.Server.Close) and report it.
//  5. Run stop hooks in reverse registration order, each with its own
//     timeout, so dependencies close after the things that use them.
package gracefulshutdown

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Readiness is whatever serves the readiness probe. healthcheck.Checker from
// ../health-check satisfies it.
type Readiness interface {
	SetDraining(draining bool)
}

// ErrShuttingDown is returned when registering work after draining began.
var ErrShuttingDown = errors.New("gracefulshutdown: shutting down")

// Phase names recorded in Report.Timeline, in the order they happen.
const (
	PhaseRequested    = "shutdown requested"
	PhaseNotReady     = "readiness failing"
	PhaseDeregistered = "deregistration delay over"
	PhaseStopAccept   = "listener closed, draining"
	PhaseDrained      = "drained"
	PhaseForced       = "drain deadline hit, force closed"
	PhaseHooksDone    = "stop hooks done"
)

// Event is one step of the shutdown, timed from the shutdown request.
type Event struct {
	Phase string
	At    time.Duration
}

// HookResult is the outcome of one stop hook.
type HookResult struct {
	Name     string
	Err      error
	TimedOut bool
	Took     time.Duration
}

// WorkerResult is the outcome of one background worker.
type WorkerResult struct {
	Name string
	Err  error // what the worker returned
	// Cut is true when the worker had not returned by the drain deadline. Go
	// cannot kill a goroutine, so it may still be running; the process is
	// expected to exit soon after.
	Cut bool
}

// Report describes what a shutdown did and what it could not save.
type Report struct {
	Timeline []Event
	// InFlightAtDrain is the number of requests running when the listener
	// closed.
	InFlightAtDrain int
	// Forced is true when the drain deadline passed and the server was
	// closed with http.Server.Close.
	Forced bool
	// Cut lists requests ("METHOD /path") still running at the force close.
	// Their clients see a connection error.
	Cut []string
	// ForcedConns is the number of connections the force close dropped. It
	// can exceed len(Cut): Shutdown treats a connection that was opened but
	// has not sent a request yet as busy for up to 5 seconds, and HTTP
	// clients often open such spare connections.
	ForcedConns int
	Workers     []WorkerResult
	Hooks       []HookResult
	Elapsed     time.Duration
}

// At returns when phase happened and whether it happened at all.
func (r Report) At(phase string) (time.Duration, bool) {
	for _, e := range r.Timeline {
		if e.Phase == phase {
			return e.At, true
		}
	}
	return 0, false
}

type options struct {
	readiness Readiness
	delay     time.Duration
	drain     time.Duration
	now       func() time.Time
	sleep     func(ctx context.Context, d time.Duration)
}

// Option configures a Drainer.
type Option func(*options)

// WithReadiness sets what to fail when shutdown starts. Without it, the
// deregistration delay still runs, but nothing tells the load balancer.
func WithReadiness(r Readiness) Option { return func(o *options) { o.readiness = r } }

// WithDeregistrationDelay sets how long to keep serving after readiness
// fails (default 5s). It should exceed the load balancer's probe interval
// times its unhealthy threshold. Zero skips the wait.
func WithDeregistrationDelay(d time.Duration) Option { return func(o *options) { o.delay = d } }

// WithDrainTimeout sets how long in-flight requests and workers get after
// the listener closes before being cut (default 20s).
func WithDrainTimeout(d time.Duration) Option { return func(o *options) { o.drain = d } }

// WithClock sets the clock used for the report's timeline.
func WithClock(now func() time.Time) Option { return func(o *options) { o.now = now } }

// WithSleep replaces how the deregistration delay is waited out. Tests use
// it to act while the instance is not ready but still serving. The function
// should return early when ctx is done.
func WithSleep(sleep func(ctx context.Context, d time.Duration)) Option {
	return func(o *options) { o.sleep = sleep }
}

func timerSleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

type hook struct {
	name    string
	timeout time.Duration
	fn      func(context.Context) error
}

type worker struct {
	name string
	err  error
	done chan struct{}
}

// Drainer owns the shutdown of one http.Server. Safe for concurrent use.
type Drainer struct {
	srv  *http.Server
	opts options

	workCtx    context.Context
	cancelWork context.CancelFunc
	draining   chan struct{}

	mu       sync.Mutex
	closing  bool
	nextID   uint64
	inflight map[uint64]string
	conns    map[net.Conn]struct{}
	served   uint64
	hooks    []hook
	onDrain  []func()
	workers  []*worker

	once   sync.Once
	done   chan struct{}
	report Report
}

// New wraps srv. It replaces srv.Handler with a tracking wrapper (nil means
// http.DefaultServeMux, as for http.Server), so configure the handler first.
func New(srv *http.Server, opts ...Option) (*Drainer, error) {
	if srv == nil {
		return nil, errors.New("gracefulshutdown: server must not be nil")
	}
	o := options{delay: 5 * time.Second, drain: 20 * time.Second, now: time.Now, sleep: timerSleep}
	for _, opt := range opts {
		opt(&o)
	}
	if o.delay < 0 {
		return nil, errors.New("gracefulshutdown: deregistration delay must not be negative")
	}
	if o.drain <= 0 {
		return nil, errors.New("gracefulshutdown: drain timeout must be positive")
	}
	if o.now == nil || o.sleep == nil {
		return nil, errors.New("gracefulshutdown: clock and sleep must not be nil")
	}
	d := &Drainer{srv: srv, opts: o, draining: make(chan struct{}), inflight: map[uint64]string{}, conns: map[net.Conn]struct{}{}, done: make(chan struct{})}
	d.workCtx, d.cancelWork = context.WithCancel(context.Background())
	next := srv.Handler
	if next == nil {
		next = http.DefaultServeMux
	}
	srv.Handler = d.track(next)
	prev := srv.ConnState
	srv.ConnState = func(c net.Conn, st http.ConnState) {
		d.mu.Lock()
		switch st {
		case http.StateNew:
			d.conns[c] = struct{}{}
		case http.StateHijacked, http.StateClosed:
			delete(d.conns, c)
		}
		d.mu.Unlock()
		if prev != nil {
			prev(c, st)
		}
	}
	return d, nil
}

func (d *Drainer) track(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		d.mu.Lock()
		id := d.nextID
		d.nextID++
		d.inflight[id] = r.Method + " " + r.URL.Path
		d.mu.Unlock()
		defer func() {
			d.mu.Lock()
			delete(d.inflight, id)
			d.served++
			d.mu.Unlock()
		}()
		next.ServeHTTP(w, r)
	})
}

// InFlight returns the number of requests currently being handled.
func (d *Drainer) InFlight() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.inflight)
}

// Served returns the number of requests that have finished (successfully or not).
func (d *Drainer) Served() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.served
}

// Draining is closed when the listener closes. Long-lived handlers (SSE,
// streaming) select on it, send the client a reconnect hint, and return,
// otherwise they hold the drain open until the deadline.
func (d *Drainer) Draining() <-chan struct{} { return d.draining }

// OnDrain registers fn to run in its own goroutine when draining starts,
// like http.Server.RegisterOnShutdown. Use it to tell a WebSocket hub, whose
// hijacked connections Shutdown does not track, to ask clients to reconnect.
func (d *Drainer) OnDrain(fn func()) error {
	if fn == nil {
		return errors.New("gracefulshutdown: OnDrain func must not be nil")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closing {
		return ErrShuttingDown
	}
	d.onDrain = append(d.onDrain, fn)
	return nil
}

// OnStop registers a stop hook. Register hooks in startup order (database
// pool, then cache client, then metrics exporter...); they run in reverse
// after the HTTP server and workers have drained, each limited to timeout.
// A hook that ignores its context cannot stall the shutdown past timeout.
func (d *Drainer) OnStop(name string, timeout time.Duration, fn func(ctx context.Context) error) error {
	if fn == nil {
		return errors.New("gracefulshutdown: OnStop func must not be nil")
	}
	if timeout <= 0 {
		return errors.New("gracefulshutdown: hook timeout must be positive")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closing {
		return ErrShuttingDown
	}
	d.hooks = append(d.hooks, hook{name: name, timeout: timeout, fn: fn})
	return nil
}

// Go starts a background worker, such as a queue consumer. Its context is
// cancelled when draining starts: the worker should finish (and acknowledge)
// the item in hand, stop pulling new ones, and return. The drain waits for it
// up to the drain deadline.
func (d *Drainer) Go(name string, fn func(ctx context.Context) error) error {
	if fn == nil {
		return errors.New("gracefulshutdown: worker func must not be nil")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closing {
		return ErrShuttingDown
	}
	w := &worker{name: name, done: make(chan struct{})}
	d.workers = append(d.workers, w)
	go func() {
		defer close(w.done)
		w.err = fn(d.workCtx)
	}()
	return nil
}

// Run serves on ln until ctx is done, then shuts down. In main, pass the
// context from signal.NotifyContext(ctx, syscall.SIGTERM, os.Interrupt).
// The shutdown itself is not cancelled by ctx (that would abort it the moment
// it started). It returns an error only if Serve fails for another reason.
func (d *Drainer) Run(ctx context.Context, ln net.Listener) (Report, error) {
	errc := make(chan error, 1)
	go func() { errc <- d.srv.Serve(ln) }()
	select {
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			d.cancelWork()
			return Report{}, err
		}
		// Someone else called Shutdown; return its report.
		return d.Shutdown(context.WithoutCancel(ctx)), nil
	case <-ctx.Done():
	}
	rep := d.Shutdown(context.WithoutCancel(ctx))
	<-errc
	return rep, nil
}

// Shutdown runs the shutdown sequence once and returns its report. Later
// and concurrent calls wait for the same shutdown and get the same report.
// ctx is an outer bound: if it ends early, the remaining waits are cut short.
func (d *Drainer) Shutdown(ctx context.Context) Report {
	d.once.Do(func() {
		go func() {
			d.report = d.shutdown(ctx)
			close(d.done)
		}()
	})
	<-d.done
	return d.report
}

func (d *Drainer) shutdown(ctx context.Context) Report {
	var r Report
	start := d.opts.now()
	mark := func(phase string) { r.Timeline = append(r.Timeline, Event{phase, d.opts.now().Sub(start)}) }
	mark(PhaseRequested)

	// 1. Fail readiness first, while everything still works.
	if d.opts.readiness != nil {
		d.opts.readiness.SetDraining(true)
	}
	mark(PhaseNotReady)

	// 2. Keep serving while the load balancer notices.
	if d.opts.delay > 0 {
		d.opts.sleep(ctx, d.opts.delay)
	}
	mark(PhaseDeregistered)

	// 3. Stop accepting, notify long-lived handlers and workers.
	d.mu.Lock()
	d.closing = true
	r.InFlightAtDrain = len(d.inflight)
	hooks := append([]hook(nil), d.hooks...)
	onDrain := append([]func(){}, d.onDrain...)
	workers := append([]*worker(nil), d.workers...)
	d.mu.Unlock()

	drainCtx, cancel := context.WithTimeout(ctx, d.opts.drain)
	defer cancel()
	srvErr := make(chan error, 1)
	go func() { srvErr <- d.srv.Shutdown(drainCtx) }() // closes listeners at once
	mark(PhaseStopAccept)
	close(d.draining)
	for _, fn := range onDrain {
		go fn()
	}
	d.cancelWork()

	// 4. Wait for requests and workers, up to the drain deadline.
	workersDone := make(chan struct{})
	go func() {
		for _, w := range workers {
			<-w.done
		}
		close(workersDone)
	}()
	select {
	case <-workersDone:
	case <-drainCtx.Done():
	}
	if err := <-srvErr; err != nil {
		d.mu.Lock()
		ids := make([]uint64, 0, len(d.inflight))
		for id := range d.inflight {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		for _, id := range ids {
			r.Cut = append(r.Cut, d.inflight[id])
		}
		r.ForcedConns = len(d.conns)
		d.mu.Unlock()
		_ = d.srv.Close()
		r.Forced = true
		mark(PhaseForced)
	} else {
		mark(PhaseDrained)
	}
	for _, w := range workers {
		select {
		case <-w.done:
			r.Workers = append(r.Workers, WorkerResult{Name: w.name, Err: w.err})
		default:
			r.Workers = append(r.Workers, WorkerResult{Name: w.name, Cut: true})
		}
	}

	// 5. Stop hooks, last registered first, each with its own timeout.
	for i := len(hooks) - 1; i >= 0; i-- {
		r.Hooks = append(r.Hooks, d.runHook(ctx, hooks[i]))
	}
	mark(PhaseHooksDone)
	r.Elapsed = d.opts.now().Sub(start)
	return r
}

func (d *Drainer) runHook(ctx context.Context, h hook) HookResult {
	hctx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()
	t0 := d.opts.now()
	res := make(chan error, 1) // buffered: a hook that ignores ctx cannot leak us
	go func() { res <- h.fn(hctx) }()
	var err error
	select {
	case err = <-res:
	case <-hctx.Done():
		err = hctx.Err()
	}
	return HookResult{
		Name:     h.name,
		Err:      err,
		TimedOut: errors.Is(err, context.DeadlineExceeded),
		Took:     d.opts.now().Sub(t0),
	}
}
