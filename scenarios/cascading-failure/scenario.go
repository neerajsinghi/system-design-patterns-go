// Package cascadingfailure simulates the "one slow microservice takes down
// the whole product page" incident. An e-commerce product page is built by a
// product service that calls catalog, pricing and inventory (all required)
// and a recommendations service (nice to have). One recommendations instance
// starts answering in seconds instead of milliseconds; later the whole
// recommendations service does. The product service has no timeouts and one
// shared pool of worker threads, so workers pile up waiting on
// recommendations, the pool runs out, and every product page fails, even
// though the page could have been rendered without recommendations. The API
// gateway in front of it times out and retries, which adds load to the
// already-saturated service, and the gateway's own open connections grow with
// every retry.
//
// Broken calls every dependency without a timeout from a shared pool, routes
// recommendations round robin, and has no breaker. Fixed keeps the same pool
// size and the same gateway (retries included) but:
//
//   - gives every hop a slice of the gateway's propagated deadline
//     (patterns/reliability/timeout HopPolicy.Budget), and drops queued work
//     whose deadline has already passed;
//   - caps how many workers may be inside a recommendations call at once
//     (patterns/reliability/bulkhead);
//   - wraps recommendations in a circuit breaker
//     (patterns/reliability/circuit-breaker);
//   - balances recommendations with passive outlier detection, which ejects
//     the one slow instance (patterns/reliability/outlier-detection);
//   - renders the page with "popular items" when recommendations cannot
//     answer in time (patterns/reliability/fallback).
//
// The simulation is discrete-event, on a simulated clock: page requests
// arrive at a fixed Poisson rate from a seeded generator, and every
// dependency latency is a pure function of (seed, page, attempt, dependency,
// instance, time), so Broken and Fixed see identical traffic and identical
// dependency behaviour. A worker is occupied from the moment it picks up a
// request until it writes the response.
package cascadingfailure

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"
	"time"

	bulkhead "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/bulkhead"
	circuitbreaker "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/circuit-breaker"
	fallback "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/fallback"
	outlierdetection "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/outlier-detection"
	timeout "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/timeout"
)

const (
	Seed        = 42
	ArrivalRate = 400 // product page requests per second at the gateway
	Duration    = 300 * time.Second

	// Product service capacity, identical in both designs.
	Workers = 200 // worker threads shared by every request
	Backlog = 100 // requests that may wait for a worker; beyond that, 503

	// API gateway, identical in both designs.
	GatewayTimeout = 2 * time.Second // per attempt; also the deadline it propagates
	GatewayRetries = 2               // extra attempts after a timeout or a 5xx

	RenderTime    = 5 * time.Millisecond
	RecsInstances = 4
	SlowInstance  = "recs-2" // the instance that goes bad in phase 1

	// Fixed design only.
	CoreTimeout      = 300 * time.Millisecond // cap per catalog/pricing/inventory hop
	RecsTimeout      = 150 * time.Millisecond // cap per recommendations hop
	RecsBulkhead     = 40                     // workers allowed inside recommendations at once
	BreakerThreshold = 20                     // consecutive recommendation failures that open the breaker
	BreakerOpenFor   = 5 * time.Second
	EjectFor         = 30 * time.Second // base ejection time for an outlier instance

	// AvailabilityTarget is the invariant: success + degraded pages, per phase.
	AvailabilityTarget = 99.9
)

// Phase is a window of the run, keyed by when pages arrive at the gateway.
type Phase struct {
	Name     string
	From, To time.Duration
}

// Phases are the windows the report is broken into.
var Phases = [4]Phase{
	{"baseline", 0, 30 * time.Second},
	{"phase 1: one recs instance slow", 30 * time.Second, 150 * time.Second},
	{"phase 2: all of recs slow", 150 * time.Second, 270 * time.Second},
	{"recovery", 270 * time.Second, Duration},
}

func phaseOf(t time.Duration) int {
	for i := len(Phases) - 1; i > 0; i-- {
		if t >= Phases[i].From {
			return i
		}
	}
	return 0
}

// PhaseStats is what one phase measured.
type PhaseStats struct {
	Pages    int // page requests that arrived at the gateway
	Success  int // full page with personalised recommendations
	Degraded int // page rendered with "popular items" instead
	Failed   int // the shopper got an error page
	// Attempts is how many requests the gateway sent to the product service
	// for this phase's pages (retries included).
	Attempts int
	// PeakWorkers is the most product-service workers busy at once.
	PeakWorkers int
	// PeakGateway is the most page requests open at the gateway at once.
	PeakGateway int
	// P99 is the 99th percentile page latency seen by shoppers, failures included.
	P99 time.Duration
}

// Availability is the percentage of pages that rendered, degraded or not.
func (p PhaseStats) Availability() float64 {
	if p.Pages == 0 {
		return 100
	}
	return 100 * float64(p.Success+p.Degraded) / float64(p.Pages)
}

func pct(n, of int) float64 {
	if of == 0 {
		return 0
	}
	return 100 * float64(n) / float64(of)
}

// Report is what a run measured.
type Report struct {
	Name   string
	Phases [len(Phases)]PhaseStats
	// Wasted counts requests the product service finished after the gateway
	// had already given up on them.
	Wasted int
	// Dropped counts queued requests the fixed product service discarded
	// because their deadline had passed before a worker was free.
	Dropped int
	// Degraded pages by reason (fixed only).
	DegradedTimeout, DegradedBreaker, DegradedBulkhead, DegradedBudget int
	Ejections, BreakerOpens                                            int
}

// PoolExhausted reports whether the worker pool was ever completely busy.
func (r Report) PoolExhausted() bool {
	for _, p := range r.Phases {
		if p.PeakWorkers >= Workers {
			return true
		}
	}
	return false
}

// MinAvailability is the worst phase's availability.
func (r Report) MinAvailability() float64 {
	m := 100.0
	for _, p := range r.Phases {
		m = min(m, p.Availability())
	}
	return m
}

// Print writes the per-phase table and a summary.
func (r Report) Print(w io.Writer) {
	fmt.Fprintf(w, "%s\n", r.Name)
	fmt.Fprintf(w, "%d pages/s for %s; product service %d workers + %d backlog; gateway timeout %s with %d retries\n",
		ArrivalRate, Duration, Workers, Backlog, GatewayTimeout, GatewayRetries)
	fmt.Fprintf(w, "%s turns slow at t=%s; all of recommendations is slow from t=%s to t=%s\n\n",
		SlowInstance, Phases[1].From, Phases[2].From, Phases[2].To)
	fmt.Fprintf(w, "  %-42s %6s %8s %9s %8s %8s %9s %8s %9s\n",
		"phase", "pages", "success", "degraded", "failed", "workers", "p99", "tries/pg", "gw conns")
	for i, p := range r.Phases {
		ph := Phases[i]
		name := fmt.Sprintf("%s (%d-%ds)", ph.Name, int(ph.From.Seconds()), int(ph.To.Seconds()))
		fmt.Fprintf(w, "  %-42s %6d %7.2f%% %8.2f%% %7.2f%% %7.0f%% %9s %8.2f %9d\n",
			name, p.Pages, pct(p.Success, p.Pages), pct(p.Degraded, p.Pages), pct(p.Failed, p.Pages),
			pct(p.PeakWorkers, Workers), p.P99.Round(time.Millisecond),
			float64(p.Attempts)/float64(max(p.Pages, 1)), p.PeakGateway)
	}
	fmt.Fprintf(w, "\n  worst-phase availability (success + degraded): %.2f%% (target %.1f%%)\n", r.MinAvailability(), AvailabilityTarget)
	fmt.Fprintf(w, "  worker pool exhausted: %v\n", r.PoolExhausted())
	if r.Wasted > 0 {
		fmt.Fprintf(w, "  requests finished after the gateway had given up (wasted work): %d\n", r.Wasted)
	}
	if r.Dropped > 0 {
		fmt.Fprintf(w, "  queued requests dropped because their deadline had passed: %d\n", r.Dropped)
	}
	if deg := r.DegradedTimeout + r.DegradedBreaker + r.DegradedBulkhead + r.DegradedBudget; deg > 0 {
		fmt.Fprintf(w, "  degraded pages by cause: recs timed out %d, breaker open %d, bulkhead full %d, no budget left %d\n",
			r.DegradedTimeout, r.DegradedBreaker, r.DegradedBulkhead, r.DegradedBudget)
		fmt.Fprintf(w, "  outlier ejections of %s: %d; breaker opened %d times\n", SlowInstance, r.Ejections, r.BreakerOpens)
	}
}

// ---- dependency behaviour (identical for both designs) ----

const (
	depCatalog = iota
	depPricing
	depInventory
	depRecs
)

// rng is a tiny allocation-free wrapper over a seeded PCG; one is seeded
// per (page, attempt, dependency) so each call's latency does not depend on
// what other calls the design happened to make.
type rng struct{ pcg rand.PCG }

func draw(page, attempt, dep int) *rng {
	r := &rng{}
	r.pcg.Seed(Seed, uint64(page)<<8|uint64(attempt)<<4|uint64(dep))
	return r
}

func (r *rng) IntN(n int) int { return int(r.pcg.Uint64() % uint64(n)) }

func ms(r *rng, lo, span int) time.Duration {
	return time.Duration(lo+r.IntN(span)) * time.Millisecond
}

// coreLatency is catalog, pricing or inventory: 10-30ms, with a 0.5% tail of 80-150ms.
func coreLatency(page, attempt, dep int) time.Duration {
	r := draw(page, attempt, dep)
	if r.IntN(200) == 0 {
		return ms(r, 80, 70)
	}
	return ms(r, 10, 20)
}

// recsLatency is one recommendations call to host, started at t.
func recsLatency(page, attempt int, host string, t time.Duration) time.Duration {
	r := draw(page, attempt, depRecs)
	switch {
	case phaseOf(t) == 2:
		return ms(r, 3000, 2000)
	case phaseOf(t) == 1 && host == SlowInstance:
		return ms(r, 2500, 1000)
	case r.IntN(100) == 0:
		return ms(r, 200, 200) // healthy tail
	default:
		return ms(r, 25, 35)
	}
}

func recsHosts() []string {
	hosts := make([]string, RecsInstances)
	for i := range hosts {
		hosts[i] = fmt.Sprintf("recs-%d", i)
	}
	return hosts
}

// ---- event loop ----

type event struct {
	at  time.Duration
	seq int
	fn  func()
}

type events []event

func (e events) Len() int { return len(e) }
func (e events) Less(i, j int) bool {
	if e[i].at != e[j].at {
		return e[i].at < e[j].at
	}
	return e[i].seq < e[j].seq
}
func (e events) Swap(i, j int) { e[i], e[j] = e[j], e[i] }
func (e *events) Push(x any)   { *e = append(*e, x.(event)) }
func (e *events) Pop() any {
	old := *e
	x := old[len(old)-1]
	*e = old[:len(old)-1]
	return x
}

type outcome int

const (
	ok outcome = iota
	degraded
	failed
)

type page struct {
	id       int
	arrived  time.Duration
	attempts int
}

type attempt struct {
	p         *page
	n         int
	deadline  time.Duration // propagated from the gateway
	answered  bool
	abandoned bool
}

var errRecsTimeout = errors.New("recommendations: deadline exceeded")

// recsCall is the fallback key: which page asked, and how the call went.
type recsCall struct {
	page int
	err  error
}

type sim struct {
	fixed  bool
	now    time.Duration
	seq    int
	ev     events
	busy   int
	queue  []*attempt
	gwOpen int
	r      *Report
	lat    [len(Phases)][]time.Duration

	hosts *outlierdetection.Pool
	// fixed design only
	bh       *bulkhead.Bulkhead
	cb       *circuitbreaker.Breaker
	fb       *fallback.Fallback[recsCall, string]
	corePol  timeout.HopPolicy
	recsPol  timeout.HopPolicy
	queuePol timeout.HopPolicy
}

var epoch = time.Unix(1_700_000_000, 0)

func (s *sim) clock() time.Time { return epoch.Add(s.now) }

func (s *sim) at(t time.Duration, fn func()) {
	s.seq++
	heap.Push(&s.ev, event{t, s.seq, fn})
}

func (s *sim) phase() *PhaseStats { return &s.r.Phases[phaseOf(s.now)] }

// budget asks the timeout package how long a hop may take, on simulated time.
func (s *sim) budget(p timeout.HopPolicy, a *attempt) (time.Duration, error) {
	return p.Budget(epoch.Add(a.deadline), true, s.clock())
}

// ---- gateway ----

func (s *sim) arrive(p *page) {
	s.gwOpen++
	ph := s.phase()
	ph.Pages++
	ph.PeakGateway = max(ph.PeakGateway, s.gwOpen)
	s.send(p)
}

func (s *sim) send(p *page) {
	a := &attempt{p: p, n: p.attempts, deadline: s.now + GatewayTimeout}
	p.attempts++
	s.r.Phases[phaseOf(p.arrived)].Attempts++
	s.at(a.deadline, func() {
		if !a.answered {
			a.abandoned = true
			s.retryOrFail(p)
		}
	})
	s.accept(a)
}

func (s *sim) retryOrFail(p *page) {
	if p.attempts <= GatewayRetries {
		s.send(p)
		return
	}
	s.finish(p, failed)
}

func (s *sim) answer(a *attempt, o outcome) {
	if a.abandoned {
		s.r.Wasted++
		return
	}
	a.answered = true
	if o == failed {
		s.retryOrFail(a.p)
		return
	}
	s.finish(a.p, o)
}

func (s *sim) finish(p *page, o outcome) {
	s.gwOpen--
	i := phaseOf(p.arrived)
	ph := &s.r.Phases[i]
	switch o {
	case ok:
		ph.Success++
	case degraded:
		ph.Degraded++
	default:
		ph.Failed++
	}
	s.lat[i] = append(s.lat[i], s.now-p.arrived)
}

// ---- product service ----

func (s *sim) accept(a *attempt) {
	switch {
	case s.busy < Workers:
		s.start(a)
	case len(s.queue) < Backlog:
		s.queue = append(s.queue, a)
	default:
		s.answer(a, failed) // 503: no worker and no room to wait
	}
}

func (s *sim) start(a *attempt) {
	s.busy++
	ph := s.phase()
	ph.PeakWorkers = max(ph.PeakWorkers, s.busy)
	s.callCore(a, depCatalog)
}

func (s *sim) respond(a *attempt, o outcome) {
	s.at(s.now+RenderTime, func() {
		s.busy--
		s.answer(a, o)
		s.dequeue()
	})
}

func (s *sim) dequeue() {
	for s.busy < Workers && len(s.queue) > 0 {
		a := s.queue[0]
		s.queue = s.queue[1:]
		if s.fixed {
			if _, err := s.budget(s.queuePol, a); err != nil {
				// The gateway has given up (or is about to): don't spend a
				// worker on it.
				s.r.Dropped++
				s.answer(a, failed)
				continue
			}
		}
		s.start(a)
	}
}

func (s *sim) callCore(a *attempt, dep int) {
	if dep == depRecs {
		s.callRecs(a)
		return
	}
	lat := coreLatency(a.p.id, a.n, dep)
	if s.fixed {
		d, err := s.budget(s.corePol, a)
		if err != nil {
			s.respond(a, failed)
			return
		}
		if lat > d {
			s.at(s.now+d, func() { s.respond(a, failed) })
			return
		}
	}
	s.at(s.now+lat, func() { s.callCore(a, dep+1) })
}

func (s *sim) callRecs(a *attempt) {
	if !s.fixed {
		host, _ := s.hosts.Pick() // plain round robin: detection disabled
		lat := recsLatency(a.p.id, a.n, host, s.now)
		s.at(s.now+lat, func() { s.respond(a, ok) })
		return
	}
	d, err := s.budget(s.recsPol, a)
	if err != nil {
		s.r.DegradedBudget++
		s.recsDone(a, err)
		return
	}
	if err := s.bh.Acquire(context.Background()); err != nil { // MaxWaiting 0: never blocks
		s.r.DegradedBulkhead++
		s.recsDone(a, err)
		return
	}
	done, err := s.cb.Allow()
	if err != nil {
		s.bh.Release()
		s.r.DegradedBreaker++
		s.recsDone(a, err)
		return
	}
	host, err := s.hosts.Pick()
	if err != nil {
		s.bh.Release()
		done(nil)
		s.recsDone(a, err)
		return
	}
	took, callErr := recsLatency(a.p.id, a.n, host, s.now), error(nil)
	if took > d {
		took, callErr = d, errRecsTimeout
	}
	s.at(s.now+took, func() {
		s.bh.Release()
		done(callErr)
		s.hosts.Report(host, took, callErr)
		if callErr != nil {
			s.r.DegradedTimeout++
		}
		s.recsDone(a, callErr)
	})
}

// recsDone renders the page through the fallback: personalised when the call
// worked, "popular items" otherwise.
func (s *sim) recsDone(a *attempt, callErr error) {
	res, err := s.fb.Get(context.Background(), recsCall{a.p.id, callErr})
	switch {
	case err != nil:
		s.respond(a, failed)
	case res.Source == fallback.Primary:
		s.respond(a, ok)
	default:
		s.respond(a, degraded)
	}
}

// ---- runs ----

func (s *sim) run() Report {
	arrivals := rand.New(rand.NewPCG(Seed, 0xA11))
	var next func(id int, t time.Duration)
	next = func(id int, t time.Duration) {
		if t >= Duration {
			return
		}
		s.at(t, func() {
			s.arrive(&page{id: id, arrived: s.now})
			gap := time.Duration(arrivals.ExpFloat64() / ArrivalRate * float64(time.Second))
			next(id+1, t+gap)
		})
	}
	next(0, 0)
	for s.ev.Len() > 0 {
		e := heap.Pop(&s.ev).(event)
		s.now = e.at
		e.fn()
	}
	for i, l := range s.lat {
		if len(l) == 0 {
			continue
		}
		slices.Sort(l)
		s.r.Phases[i].P99 = l[(len(l)*99-1)/100]
	}
	return *s.r
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// Broken: no timeouts anywhere, one shared worker pool, plain round robin
// over the recommendations instances, no breaker, no fallback.
func Broken() Report {
	s := &sim{r: &Report{Name: "BROKEN: no timeouts, shared worker pool, no breaker, gateway retries"}}
	s.hosts = must(outlierdetection.New(recsHosts(),
		outlierdetection.WithConsecutiveErrors(0), outlierdetection.WithLatencyFactor(0),
		outlierdetection.WithClock(s.clock)))
	return s.run()
}

// Fixed: same pool, same gateway, same traffic; every hop gets a slice of the
// propagated deadline, recommendations sit behind a bulkhead, a circuit
// breaker and outlier detection, and a failed recommendations call renders
// "popular items" instead of failing the page.
func Fixed() Report {
	s := &sim{fixed: true, r: &Report{Name: "FIXED: deadlines + recs bulkhead + circuit breaker + outlier detection + fallback"}}
	s.hosts = must(outlierdetection.New(recsHosts(),
		outlierdetection.WithConsecutiveErrors(5),
		outlierdetection.WithMinLatency(100*time.Millisecond),
		outlierdetection.WithEjectionTime(EjectFor, 300*time.Second),
		outlierdetection.WithClock(s.clock),
		outlierdetection.WithOnEvent(func(e outlierdetection.Event) {
			if e.Kind == outlierdetection.Ejected && e.Host == SlowInstance {
				s.r.Ejections++
			}
		})))
	s.bh = bulkhead.New(bulkhead.Config{MaxConcurrent: RecsBulkhead, MaxWaiting: 0})
	s.cb = circuitbreaker.New(circuitbreaker.Settings{
		FailureThreshold: BreakerThreshold,
		OpenTimeout:      BreakerOpenFor,
		Now:              s.clock,
		OnStateChange: func(_, to circuitbreaker.State) {
			if to == circuitbreaker.Open {
				s.r.BreakerOpens++
			}
		},
	})
	s.fb = fallback.New(func(_ context.Context, c recsCall) (string, error) {
		if c.err != nil {
			return "", c.err
		}
		return fmt.Sprintf("personalised recommendations for page %d", c.page), nil
	}, fallback.Options[recsCall, string]{
		Default: func(recsCall) (string, bool) { return "popular items", true },
	})
	s.corePol = timeout.HopPolicy{Max: CoreTimeout, Reserve: RenderTime, Min: 5 * time.Millisecond}
	s.recsPol = timeout.HopPolicy{Max: RecsTimeout, Reserve: RenderTime, Min: 20 * time.Millisecond}
	// A queued request is only worth starting if the whole page still fits.
	s.queuePol = timeout.HopPolicy{Reserve: RenderTime, Min: 100 * time.Millisecond}
	return s.run()
}
