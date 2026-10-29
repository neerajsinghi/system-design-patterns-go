// Package healthcheck separates the two questions an orchestrator asks:
//
//   - Liveness: "is this process stuck beyond repair?" If no, restart it.
//     Liveness checks must only look at the process itself.
//   - Readiness: "should this instance receive traffic right now?" If no,
//     take it out of the load balancer but leave it running. Readiness may
//     look at dependencies, warm-up state, and draining.
//
// It also provides a heartbeat failure detector, the peer-to-peer equivalent:
// nodes are alive while heartbeats arrive, suspect after a short silence, and
// dead after a long one.
//
// HeartbeatMonitor applies the same idea to a server's long-lived client
// connections: each connection must beat (heartbeat or pong) every interval,
// and one that misses K consecutive intervals is reaped by Sweep.
package healthcheck

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// CheckFunc reports nil when healthy. It must honour ctx.
type CheckFunc func(ctx context.Context) error

// Report is the outcome of running a set of checks.
type Report struct {
	Healthy bool              `json:"healthy"`
	Checks  map[string]string `json:"checks"` // name -> "ok" or error text
}

// Checker holds liveness and readiness checks. Safe for concurrent use.
type Checker struct {
	timeout  time.Duration
	draining atomic.Bool

	mu        sync.RWMutex
	liveness  map[string]CheckFunc
	readiness map[string]CheckFunc
}

// NewChecker returns a Checker whose individual checks time out after
// timeout (<=0 means 1s). A slow check counts as a failure.
func NewChecker(timeout time.Duration) *Checker {
	if timeout <= 0 {
		timeout = time.Second
	}
	return &Checker{timeout: timeout, liveness: map[string]CheckFunc{}, readiness: map[string]CheckFunc{}}
}

// AddLiveness registers a liveness check (e.g. "event loop responsive").
// Do not put dependency checks here: a database outage would restart every pod.
func (c *Checker) AddLiveness(name string, fn CheckFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.liveness[name] = fn
}

// AddReadiness registers a readiness check (e.g. "db reachable", "cache warm").
func (c *Checker) AddReadiness(name string, fn CheckFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readiness[name] = fn
}

// SetDraining marks the instance as shutting down: readiness fails so the
// load balancer stops sending traffic, while liveness keeps passing so the
// orchestrator does not kill in-flight requests.
func (c *Checker) SetDraining(d bool) { c.draining.Store(d) }

// Live runs the liveness checks.
func (c *Checker) Live(ctx context.Context) Report {
	c.mu.RLock()
	checks := copyChecks(c.liveness)
	c.mu.RUnlock()
	return c.run(ctx, checks)
}

// Ready runs readiness checks (liveness is implied: a stuck process is not ready).
func (c *Checker) Ready(ctx context.Context) Report {
	c.mu.RLock()
	checks := copyChecks(c.liveness)
	for k, v := range c.readiness {
		checks[k] = v
	}
	c.mu.RUnlock()
	r := c.run(ctx, checks)
	if c.draining.Load() {
		r.Healthy = false
		r.Checks["draining"] = "instance is shutting down"
	}
	return r
}

func copyChecks(m map[string]CheckFunc) map[string]CheckFunc {
	out := make(map[string]CheckFunc, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// run executes checks concurrently, each with its own timeout.
func (c *Checker) run(ctx context.Context, checks map[string]CheckFunc) Report {
	r := Report{Healthy: true, Checks: make(map[string]string, len(checks))}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for name, fn := range checks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, c.timeout)
			defer cancel()
			done := make(chan error, 1) // buffered: a check that ignores ctx cannot leak us
			go func() { done <- fn(cctx) }()
			var err error
			select {
			case err = <-done:
			case <-cctx.Done():
				err = cctx.Err()
			}
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				r.Healthy = false
				r.Checks[name] = err.Error()
			} else {
				r.Checks[name] = "ok"
			}
		}()
	}
	wg.Wait()
	return r
}

// LiveHandler serves liveness as 200/503 with a JSON report.
func (c *Checker) LiveHandler() http.Handler {
	return reportHandler(c.Live)
}

// ReadyHandler serves readiness as 200/503 with a JSON report.
func (c *Checker) ReadyHandler() http.Handler {
	return reportHandler(c.Ready)
}

func reportHandler(fn func(context.Context) Report) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rep := fn(r.Context())
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if !rep.Healthy {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(rep)
	})
}

// NodeState is the detector's verdict for a peer.
type NodeState int

const (
	Unknown NodeState = iota // never heard from
	Alive
	Suspect // missed heartbeats; maybe slow, maybe dead
	Dead
)

func (s NodeState) String() string {
	names := [...]string{"unknown", "alive", "suspect", "dead"}
	if s < 0 || int(s) >= len(names) {
		return "unknown"
	}
	return names[s]
}

// Detector is a heartbeat-based failure detector. Safe for concurrent use.
type Detector struct {
	suspectAfter, deadAfter time.Duration
	now                     func() time.Time

	mu   sync.Mutex
	last map[string]time.Time
}

// NewDetector marks a node suspect after suspectAfter without a heartbeat
// and dead after deadAfter. deadAfter is raised to suspectAfter if smaller.
func NewDetector(suspectAfter, deadAfter time.Duration, now func() time.Time) *Detector {
	if suspectAfter <= 0 {
		suspectAfter = time.Second
	}
	if deadAfter < suspectAfter {
		deadAfter = suspectAfter
	}
	if now == nil {
		now = time.Now
	}
	return &Detector{suspectAfter: suspectAfter, deadAfter: deadAfter, now: now, last: map[string]time.Time{}}
}

// Heartbeat records that node is alive now.
func (d *Detector) Heartbeat(node string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.last[node] = d.now()
}

// State returns node's current state.
func (d *Detector) State(node string) NodeState {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.state(node, d.now())
}

func (d *Detector) state(node string, now time.Time) NodeState {
	t, ok := d.last[node]
	if !ok {
		return Unknown
	}
	switch silence := now.Sub(t); {
	case silence >= d.deadAfter:
		return Dead
	case silence >= d.suspectAfter:
		return Suspect
	default:
		return Alive
	}
}

// Snapshot returns every known node's state, sorted by name.
func (d *Detector) Snapshot() []NodeStatus {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := d.now()
	out := make([]NodeStatus, 0, len(d.last))
	for n := range d.last {
		out = append(out, NodeStatus{Node: n, State: d.state(n, now), Silence: now.Sub(d.last[n])})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Node < out[j].Node })
	return out
}

// Forget removes a node (e.g. after it has been declared dead and replaced).
func (d *Detector) Forget(node string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.last, node)
}

// NodeStatus is one entry in a Snapshot.
type NodeStatus struct {
	Node    string
	State   NodeState
	Silence time.Duration
}
