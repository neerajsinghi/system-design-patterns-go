// Package asyncrequestreply turns long-running work into a short HTTP
// conversation.
//
// A video encode, a report or a bulk import can take minutes. Behind a plain
// synchronous endpoint that is a problem: the load balancer cuts the request
// after 30 to 60 seconds, the client sees a timeout and retries, and the
// server starts the same work again while the first copy is still running.
// Every attempt also holds a server goroutine (or thread) for the whole
// duration.
//
// The async request-reply pattern splits the call in two:
//
//   - POST /jobs stores the request, puts it on a queue and answers at once
//     with 202 Accepted, a Location: /jobs/{id} header and a Retry-After hint.
//   - GET /jobs/{id} reports the state (queued, running, succeeded, failed,
//     canceled) and progress. Once the job has succeeded it answers
//     303 See Other with Location: /jobs/{id}/result, where the result lives.
//   - DELETE /jobs/{id} cancels a queued or running job.
//
// A fixed pool of workers executes the jobs. Submits are idempotent through
// the Idempotency-Key header: the same key and body return the same job, and
// the same key with a different body is rejected with 422. Finished jobs are
// kept for a retention period (measured with an injectable clock) and then
// forgotten. If the submit carries a Callback-Url header, the server also
// POSTs the final status there once, signed and retried by the
// webhookdelivery package.
//
// Client is the matching caller: it submits, then polls with exponential
// backoff that never polls sooner than the server's Retry-After.
package asyncrequestreply

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	webhookdelivery "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/webhook-delivery"
	idempotencykey "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/idempotency-key"
)

// Request and response headers.
const (
	HeaderIdempotencyKey = idempotencykey.HeaderName // "Idempotency-Key"
	HeaderCallbackURL    = "Callback-Url"            // where to POST the final status
	HeaderReplayed       = "Idempotent-Replayed"     // "true" when a submit returned an existing job
)

// State is the lifecycle state of a job.
type State string

// Job states. Queued and Running are in progress; the others are terminal.
const (
	Queued    State = "queued"
	Running   State = "running"
	Succeeded State = "succeeded"
	Failed    State = "failed"
	Canceled  State = "canceled"
)

// Terminal reports whether the job will never change state again.
func (s State) Terminal() bool { return s == Succeeded || s == Failed || s == Canceled }

// Status is the JSON document served by GET /jobs/{id} and sent to callbacks.
type Status struct {
	ID              string    `json:"id"`
	State           State     `json:"state"`
	Progress        float64   `json:"progress"` // 0 to 1
	Error           string    `json:"error,omitempty"`
	CancelRequested bool      `json:"cancelRequested,omitempty"`
	ResultURL       string    `json:"resultUrl,omitempty"` // set once succeeded
	CreatedAt       time.Time `json:"createdAt"`
	StartedAt       time.Time `json:"startedAt"`
	FinishedAt      time.Time `json:"finishedAt"`
}

// Work executes one job. It should report progress (0 to 1) as it goes and
// return promptly with ctx.Err() when ctx is canceled, which happens on
// DELETE /jobs/{id} and on Server.Close.
type Work func(ctx context.Context, input []byte, progress func(float64)) ([]byte, error)

// Stats are counters for monitoring and tests.
type Stats struct {
	Submitted       int64 // new jobs accepted (replays not counted)
	Executions      int64 // times Work was called
	CallbacksSent   int64 // callbacks acknowledged by the receiver
	CallbacksFailed int64 // callbacks that gave up
}

type config struct {
	workers    int
	queueSize  int
	retention  time.Duration
	retryAfter time.Duration
	maxBody    int64
	now        func() time.Time
	sender     *webhookdelivery.Sender
	senderSet  bool
	onUpdate   func(Status)
}

// Option configures a Server.
type Option func(*config)

// WithWorkers sets how many jobs run at once (default 4).
func WithWorkers(n int) Option { return func(c *config) { c.workers = n } }

// WithQueueSize sets how many accepted jobs may wait for a worker (default
// 64). A submit that finds the queue full gets 503 with Retry-After.
func WithQueueSize(n int) Option { return func(c *config) { c.queueSize = n } }

// WithRetention sets how long a finished job (and its idempotency key) is
// kept before it is forgotten (default 24h).
func WithRetention(d time.Duration) Option { return func(c *config) { c.retention = d } }

// WithRetryAfter sets the Retry-After hint sent while a job is in progress
// (default 1s). It is rounded up to whole seconds; 0 omits the header.
func WithRetryAfter(d time.Duration) Option { return func(c *config) { c.retryAfter = d } }

// WithMaxBody caps the size of a submitted job body (default 1 MiB).
func WithMaxBody(n int64) Option { return func(c *config) { c.maxBody = n } }

// WithClock injects the clock used for timestamps and retention.
func WithClock(now func() time.Time) Option { return func(c *config) { c.now = now } }

// WithCallbacks enables the Callback-Url header. The final status of each
// such job is POSTed once through s, signed with its secret, with the job ID
// as the webhook event ID so receivers can deduplicate retries.
func WithCallbacks(s *webhookdelivery.Sender) Option {
	return func(c *config) { c.sender, c.senderSet = s, true }
}

// WithOnUpdate registers a hook called after every state or progress change.
// It is the place to push progress to Server-Sent Events or long-poll
// waiters instead of making clients poll. It must not block for long; it runs
// on the worker goroutine (or the DELETE handler).
func WithOnUpdate(fn func(Status)) Option { return func(c *config) { c.onUpdate = fn } }

type job struct {
	st          Status
	input       []byte
	fingerprint string
	key         string
	callback    string
	result      []byte
	cancel      context.CancelFunc
}

// Server is an http.Handler that accepts, runs and reports jobs. Create it
// with New and stop it with Close. Safe for concurrent use.
type Server struct {
	work Work
	cfg  config
	mux  *http.ServeMux

	ctx   context.Context
	stop  context.CancelFunc
	queue chan *job

	workers   sync.WaitGroup
	callbacks sync.WaitGroup
	closeOnce sync.Once

	submitted, executions, cbSent, cbFailed atomic.Int64

	mu     sync.Mutex
	jobs   map[string]*job
	keys   map[string]string // idempotency key -> job ID
	nextID int
	closed bool
}

// New validates the configuration and starts the worker pool.
func New(work Work, opts ...Option) (*Server, error) {
	cfg := config{
		workers:    4,
		queueSize:  64,
		retention:  24 * time.Hour,
		retryAfter: time.Second,
		maxBody:    1 << 20,
		now:        time.Now,
	}
	for _, o := range opts {
		o(&cfg)
	}
	switch {
	case work == nil:
		return nil, errors.New("asyncrequestreply: work must not be nil")
	case cfg.workers < 1:
		return nil, errors.New("asyncrequestreply: workers must be at least 1")
	case cfg.queueSize < 1:
		return nil, errors.New("asyncrequestreply: queue size must be at least 1")
	case cfg.retention <= 0:
		return nil, errors.New("asyncrequestreply: retention must be positive")
	case cfg.retryAfter < 0:
		return nil, errors.New("asyncrequestreply: retry-after must not be negative")
	case cfg.maxBody <= 0:
		return nil, errors.New("asyncrequestreply: max body must be positive")
	case cfg.now == nil:
		return nil, errors.New("asyncrequestreply: clock must not be nil")
	case cfg.senderSet && cfg.sender == nil:
		return nil, errors.New("asyncrequestreply: callback sender must not be nil")
	}
	ctx, stop := context.WithCancel(context.Background())
	s := &Server{
		work:  work,
		cfg:   cfg,
		mux:   http.NewServeMux(),
		ctx:   ctx,
		stop:  stop,
		queue: make(chan *job, cfg.queueSize),
		jobs:  map[string]*job{},
		keys:  map[string]string{},
	}
	s.mux.HandleFunc("POST /jobs", s.handleSubmit)
	s.mux.HandleFunc("GET /jobs/{id}", s.handleStatus)
	s.mux.HandleFunc("GET /jobs/{id}/result", s.handleResult)
	s.mux.HandleFunc("DELETE /jobs/{id}", s.handleCancel)
	for range cfg.workers {
		s.workers.Add(1)
		go s.worker()
	}
	return s, nil
}

// ServeHTTP routes POST /jobs, GET /jobs/{id}, GET /jobs/{id}/result and
// DELETE /jobs/{id}.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// Close stops accepting submits and cancels, cancels running jobs, fails the
// ones still queued, and waits for workers and pending callbacks. GET keeps
// working afterwards. Close is idempotent.
func (s *Server) Close() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		s.stop()       // running jobs see ctx.Done; queued ones are failed below
		close(s.queue) // safe: submitters check closed under mu before sending
		s.workers.Wait()
		s.callbacks.Wait()
	})
}

// Get returns a snapshot of a job's status.
func (s *Server) Get(id string) (Status, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.lookupLocked(id)
	if j == nil {
		return Status{}, false
	}
	return j.st, true
}

// Sweep forgets finished jobs older than the retention period and returns how
// many it removed. Expired jobs are also dropped lazily when looked up, so
// Sweep only bounds memory; call it from a ticker.
func (s *Server) Sweep() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now, n := s.cfg.now(), 0
	for _, j := range s.jobs {
		if s.expiredLocked(j, now) {
			s.deleteLocked(j)
			n++
		}
	}
	return n
}

// Stats returns the server's counters.
func (s *Server) Stats() Stats {
	return Stats{
		Submitted:       s.submitted.Load(),
		Executions:      s.executions.Load(),
		CallbacksSent:   s.cbSent.Load(),
		CallbacksFailed: s.cbFailed.Load(),
	}
}

// ---- HTTP handlers ----

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.cfg.maxBody))
	if err != nil {
		http.Error(w, "request body too large or unreadable", http.StatusRequestEntityTooLarge)
		return
	}
	cb := r.Header.Get(HeaderCallbackURL)
	if cb != "" {
		if s.cfg.sender == nil {
			http.Error(w, "callbacks are not enabled on this server", http.StatusBadRequest)
			return
		}
		if u, err := url.Parse(cb); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			http.Error(w, "Callback-Url must be an absolute http(s) URL", http.StatusBadRequest)
			return
		}
	}
	key := r.Header.Get(HeaderIdempotencyKey)
	// The callback URL is part of the request, so it is part of the fingerprint.
	fp := idempotencykey.Fingerprint([]byte(cb), body)

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		http.Error(w, "server shutting down", http.StatusServiceUnavailable)
		return
	}
	if key != "" {
		if j := s.lookupLocked(s.keys[key]); j != nil {
			st := j.st
			match := j.fingerprint == fp
			s.mu.Unlock()
			if !match {
				http.Error(w, "Idempotency-Key reused with a different request", http.StatusUnprocessableEntity)
				return
			}
			w.Header().Set(HeaderReplayed, "true")
			s.writeAccepted(w, st)
			return
		}
	}
	s.nextID++
	now := s.cfg.now()
	j := &job{
		st:          Status{ID: "job-" + strconv.Itoa(s.nextID), State: Queued, CreatedAt: now},
		input:       body,
		fingerprint: fp,
		key:         key,
		callback:    cb,
	}
	s.jobs[j.st.ID] = j
	if key != "" {
		s.keys[key] = j.st.ID
	}
	select {
	case s.queue <- j:
	default:
		s.deleteLocked(j)
		s.mu.Unlock()
		s.setRetryAfter(w)
		http.Error(w, "job queue is full", http.StatusServiceUnavailable)
		return
	}
	st := j.st
	s.mu.Unlock()
	s.submitted.Add(1)
	s.notify(st)
	s.writeAccepted(w, st)
}

func (s *Server) writeAccepted(w http.ResponseWriter, st Status) {
	w.Header().Set("Location", "/jobs/"+st.ID)
	if !st.State.Terminal() {
		s.setRetryAfter(w)
	}
	writeJSON(w, http.StatusAccepted, st)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	st, ok := s.Get(r.PathValue("id"))
	switch {
	case !ok:
		http.Error(w, "job not found", http.StatusNotFound)
	case st.State == Succeeded:
		// 303 tells the client to GET the result elsewhere. The status body
		// is still sent for clients that do not follow redirects.
		w.Header().Set("Location", st.ResultURL)
		writeJSON(w, http.StatusSeeOther, st)
	default:
		if !st.State.Terminal() {
			s.setRetryAfter(w)
		}
		writeJSON(w, http.StatusOK, st)
	}
}

func (s *Server) handleResult(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	j := s.lookupLocked(r.PathValue("id"))
	var st Status
	var result []byte
	if j != nil {
		st, result = j.st, j.result // result is never modified after it is set
	}
	s.mu.Unlock()
	switch {
	case j == nil:
		http.Error(w, "job not found", http.StatusNotFound)
	case st.State != Succeeded:
		writeJSON(w, http.StatusConflict, st)
	default:
		w.Header().Set("Content-Type", http.DetectContentType(result))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(result)
	}
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		http.Error(w, "server shutting down", http.StatusServiceUnavailable)
		return
	}
	j := s.lookupLocked(r.PathValue("id"))
	if j == nil {
		s.mu.Unlock()
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}
	switch j.st.State {
	case Queued:
		// The worker skips it when it reaches the front of the queue.
		st, cb := s.finishLocked(j, Canceled, nil, "canceled before it started")
		s.mu.Unlock()
		s.afterFinish(st, cb)
		writeJSON(w, http.StatusOK, st)
	case Running:
		j.st.CancelRequested = true
		j.cancel()
		st := j.st
		s.mu.Unlock()
		s.notify(st)
		writeJSON(w, http.StatusAccepted, st) // the worker finishes the transition
	case Canceled:
		st := j.st
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, st) // DELETE is idempotent
	default:
		st := j.st
		s.mu.Unlock()
		writeJSON(w, http.StatusConflict, st) // already succeeded or failed
	}
}

// ---- workers ----

func (s *Server) worker() {
	defer s.workers.Done()
	for j := range s.queue {
		s.run(j)
	}
}

func (s *Server) run(j *job) {
	s.mu.Lock()
	if j.st.State != Queued { // canceled while it waited
		s.mu.Unlock()
		return
	}
	if s.ctx.Err() != nil { // shutting down: do not start new work
		st, cb := s.finishLocked(j, Failed, nil, "server shut down before the job started")
		s.mu.Unlock()
		s.afterFinish(st, cb)
		return
	}
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	j.cancel = cancel
	j.st.State = Running
	j.st.StartedAt = s.cfg.now()
	st := j.st
	s.mu.Unlock()
	s.notify(st)

	progress := func(p float64) {
		p = min(max(p, 0), 1)
		s.mu.Lock()
		if j.st.State != Running {
			s.mu.Unlock()
			return
		}
		j.st.Progress = p
		st := j.st
		s.mu.Unlock()
		s.notify(st)
	}
	s.executions.Add(1)
	out, err := s.safeWork(ctx, j.input, progress)

	s.mu.Lock()
	var cb string
	switch {
	case err == nil:
		// Success wins even if a cancel arrived after the work was done.
		j.st.Progress = 1
		st, cb = s.finishLocked(j, Succeeded, out, "")
	case j.st.CancelRequested:
		st, cb = s.finishLocked(j, Canceled, nil, "canceled while running")
	default:
		st, cb = s.finishLocked(j, Failed, nil, err.Error())
	}
	s.mu.Unlock()
	s.afterFinish(st, cb)
}

// safeWork turns a panic in Work into a failed job instead of a dead worker.
func (s *Server) safeWork(ctx context.Context, in []byte, progress func(float64)) (out []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("work panicked: %v", r)
		}
	}()
	return s.work(ctx, in, progress)
}

// finishLocked moves j to a terminal state. It returns the callback URL to
// notify ("" for none) and, if there is one, registers the delivery with the
// callbacks WaitGroup while still holding mu (Close relies on that ordering).
func (s *Server) finishLocked(j *job, state State, result []byte, msg string) (Status, string) {
	j.st.State = state
	j.st.Error = msg
	j.st.FinishedAt = s.cfg.now()
	if state == Succeeded {
		j.result = result
		j.st.ResultURL = "/jobs/" + j.st.ID + "/result"
	}
	j.input = nil // no longer needed
	if j.callback == "" || s.cfg.sender == nil {
		return j.st, ""
	}
	s.callbacks.Add(1)
	return j.st, j.callback
}

func (s *Server) afterFinish(st Status, callback string) {
	s.notify(st)
	if callback != "" {
		go s.deliver(callback, st)
	}
}

// deliver POSTs the final status once. The job ID is the webhook event ID,
// so a receiver that sees a retried delivery can drop the duplicate.
func (s *Server) deliver(target string, st Status) {
	defer s.callbacks.Done()
	body, _ := json.Marshal(st)
	if _, err := s.cfg.sender.Deliver(context.Background(), target, st.ID, body); err != nil {
		s.cbFailed.Add(1)
		return
	}
	s.cbSent.Add(1)
}

// ---- helpers ----

func (s *Server) notify(st Status) {
	if s.cfg.onUpdate != nil {
		s.cfg.onUpdate(st)
	}
}

func (s *Server) lookupLocked(id string) *job {
	j := s.jobs[id]
	if j == nil {
		return nil
	}
	if s.expiredLocked(j, s.cfg.now()) {
		s.deleteLocked(j)
		return nil
	}
	return j
}

func (s *Server) expiredLocked(j *job, now time.Time) bool {
	return j.st.State.Terminal() && now.Sub(j.st.FinishedAt) >= s.cfg.retention
}

func (s *Server) deleteLocked(j *job) {
	delete(s.jobs, j.st.ID)
	if j.key != "" && s.keys[j.key] == j.st.ID {
		delete(s.keys, j.key)
	}
}

func (s *Server) setRetryAfter(w http.ResponseWriter) {
	if s.cfg.retryAfter <= 0 {
		return
	}
	secs := (s.cfg.retryAfter + time.Second - 1) / time.Second
	w.Header().Set("Retry-After", strconv.FormatInt(int64(secs), 10))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
