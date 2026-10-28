// Command example simulates a service calling a database that costs 30ms to
// connect to and accepts at most 100 connections. It compares dialing per
// request, an unbounded "pool" and a bounded pool under a traffic spike, then
// shows a backend restart (health check on borrow) and a DNS change (max
// lifetime rotation).
//
// Time is simulated in 1ms ticks with an injected clock: dialing is counted
// as 30ms of simulated latency, not slept, so the whole run takes well under
// a second and prints the same numbers every time.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"runtime"
	"slices"
	"sync"
	"time"

	connectionpool "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/connection-pool"
)

const (
	dialCost     = 30  // ms: TCP + TLS + auth handshake
	serviceTime  = 20  // ms a request holds its connection
	backendLimit = 100 // max_connections on the database
	waitTimeout  = 100 // ms a request will wait for a pooled connection
)

var errRefused = errors.New("backend: too many connections")

// ---- simulated database ----

type conn struct {
	gen  int    // backend generation it was opened against
	addr string // which instance DNS pointed at when it was dialed
	be   *backend
}

func (c *conn) alive() bool {
	c.be.mu.Lock()
	defer c.be.mu.Unlock()
	return c.gen == c.be.gen
}

type backend struct {
	mu       sync.Mutex
	addr     string         // where DNS currently points
	open     map[string]int // server-side connections per instance
	peak     int
	gen      int
	dials    int // handshakes attempted
	refused  int
	timeWait int // client-side closes; each leaves a socket in TIME_WAIT
}

func newBackend() *backend { return &backend{addr: "db-a", open: map[string]int{}} }

func (b *backend) dial(context.Context) (*conn, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.dials++
	if b.open[b.addr] >= backendLimit {
		b.refused++
		return nil, errRefused
	}
	b.open[b.addr]++
	b.peak = max(b.peak, b.open[b.addr])
	return &conn{gen: b.gen, addr: b.addr, be: b}, nil
}

func (b *backend) close(c *conn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.timeWait++
	if c.gen == b.gen {
		b.open[c.addr]--
	}
}

// restart drops every server-side connection; clients still hold the sockets.
func (b *backend) restart() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.gen++
	b.open = map[string]int{}
}

func (b *backend) switchDNS(addr string) { b.mu.Lock(); b.addr = addr; b.mu.Unlock() }

func (b *backend) openOn(addr string) int { b.mu.Lock(); defer b.mu.Unlock(); return b.open[addr] }

// ---- simulation driver ----

type traffic func(ms int) float64 // requests per second at time ms

type active struct {
	pc    *connectionpool.Conn[*conn] // pooled designs
	raw   *conn                       // no-pool design
	start int
}

type waiting struct {
	arrived int
	cancel  context.CancelFunc
	done    chan getResult
}

type getResult struct {
	c   *connectionpool.Conn[*conn]
	err error
}

type result struct {
	requests, ok, refused, timeouts int
	failedInFlight, failedStale     int
	latencies                       []int
	be                              *backend
	stats                           connectionpool.Stats
	samples                         []sample
}

type sample struct {
	ms, onB, total int
	opens          uint64
}

type sim struct {
	mu      sync.Mutex
	nowMS   int
	be      *backend
	pool    *connectionpool.Pool[*conn] // nil: dial per request
	maxOpen int
	finish  map[int][]active
	pending []*waiting // same order as the pool's wait queue
	res     result
}

func (s *sim) clock() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return time.UnixMilli(int64(s.nowMS))
}

type scenario struct {
	durationMS int
	load       traffic
	poolOpts   []connectionpool.Option // nil: no pool
	maxOpen    int
	events     map[int]func(*backend)
	sampleMS   int
}

func run(sc scenario) result {
	s := &sim{be: newBackend(), maxOpen: sc.maxOpen, finish: map[int][]active{}}
	s.res.be = s.be
	if sc.poolOpts != nil {
		opts := append([]connectionpool.Option{connectionpool.WithClock(s.clock)}, sc.poolOpts...)
		p, err := connectionpool.New(s.be.dial, s.be.close, opts...)
		if err != nil {
			log.Fatal(err)
		}
		s.pool = p
	}
	rng := rand.New(rand.NewPCG(42, 7)) // identical traffic for every design
	for t := 0; t < sc.durationMS; t++ {
		s.mu.Lock()
		s.nowMS = t
		s.mu.Unlock()
		if ev := sc.events[t]; ev != nil {
			ev(s.be)
		}
		for _, a := range s.finish[t] {
			s.complete(a)
		}
		delete(s.finish, t)
		s.expireWaiters(t)
		for range poisson(rng, sc.load(t)/1000) {
			s.arrive(t)
		}
		if sc.sampleMS > 0 && t%sc.sampleMS == sc.sampleMS-1 {
			st := s.pool.Stats()
			s.res.samples = append(s.res.samples, sample{ms: t + 1, onB: s.be.openOn("db-b"),
				total: s.be.openOn("db-a") + s.be.openOn("db-b"), opens: st.Opens})
		}
	}
	if s.pool != nil {
		s.res.stats = s.pool.Stats()
	}
	return s.res
}

func (s *sim) arrive(t int) {
	s.res.requests++
	if s.pool == nil {
		c, err := s.be.dial(context.Background())
		if err != nil {
			s.res.refused++
			return
		}
		s.schedule(t, t, active{raw: c, start: t}, true)
		return
	}
	st := s.pool.Stats()
	blocks := st.Waiting > 0 || (st.Idle == 0 && s.maxOpen > 0 && st.Open >= s.maxOpen)
	if !blocks {
		c, err := s.pool.Get(context.Background())
		s.started(t, t, c, err)
		s.collectGrants(t)
		return
	}
	// Get will queue: run it in a goroutine and wait until it has joined the
	// pool's wait queue, so the simulation stays in lockstep.
	ctx, cancel := context.WithCancel(context.Background())
	w := &waiting{arrived: t, cancel: cancel, done: make(chan getResult, 1)}
	go func() { c, err := s.pool.Get(ctx); w.done <- getResult{c, err} }()
	for s.pool.Stats().Waiting == st.Waiting {
		runtime.Gosched()
	}
	s.pending = append(s.pending, w)
}

func (s *sim) started(arrived, t int, c *connectionpool.Conn[*conn], err error) {
	switch {
	case errors.Is(err, errRefused):
		s.res.refused++
		return
	case err != nil:
		s.res.timeouts++
		return
	}
	if !c.Value().alive() { // stale socket from before a restart: first write fails
		s.res.failedStale++
		c.Discard()
		return
	}
	s.schedule(arrived, t, active{pc: c, start: t}, !c.Reused())
}

func (s *sim) schedule(arrived, t int, a active, dialed bool) {
	end := t + serviceTime
	if dialed {
		end += dialCost
	}
	s.res.ok++
	s.res.latencies = append(s.res.latencies, end-arrived)
	s.finish[end] = append(s.finish[end], a)
}

func (s *sim) complete(a active) {
	if a.raw != nil {
		s.be.close(a.raw)
		return
	}
	if !a.pc.Value().alive() { // backend restarted mid-request
		s.res.failedInFlight++
		s.res.ok--
		a.pc.Discard()
	} else {
		a.pc.Release()
	}
	s.collectGrants(s.nowMS)
}

// collectGrants hands results to waiters the pool has just served (oldest
// first, matching the pool's FIFO queue).
func (s *sim) collectGrants(t int) {
	for len(s.pending) > s.pool.Stats().Waiting {
		w := s.pending[0]
		s.pending = s.pending[1:]
		r := <-w.done
		w.cancel()
		s.started(w.arrived, t, r.c, r.err)
	}
}

func (s *sim) expireWaiters(t int) {
	for len(s.pending) > 0 && t-s.pending[0].arrived >= waitTimeout {
		w := s.pending[0]
		s.pending = s.pending[1:]
		w.cancel()
		r := <-w.done
		s.started(w.arrived, t, r.c, r.err)
		s.collectGrants(t)
	}
}

// poisson draws arrivals for one tick (Knuth's method; lambda is small).
func poisson(rng *rand.Rand, lambda float64) int {
	l, k, p := math.Exp(-lambda), 0, 1.0
	for {
		p *= rng.Float64()
		if p <= l {
			return k
		}
		k++
	}
}

func pct(lat []int, q float64) int {
	if len(lat) == 0 {
		return 0
	}
	s := slices.Clone(lat)
	slices.Sort(s)
	return s[int(q*float64(len(s)-1))]
}

// ---- scenarios ----

func spike(ms int) float64 {
	if ms >= 1000 && ms < 2000 {
		return 4000
	}
	return 1500
}

func steady(ms int) float64 { return 1500 }

func main() {
	fmt.Printf("Database: %dms to connect (TCP+TLS+auth), max %d connections. Each request holds a connection for %dms.\n",
		dialCost, backendLimit, serviceTime)
	fmt.Println("Traffic: 1500 req/s, with a spike to 4000 req/s from t=1s to t=2s; 3s simulated per run.")
	fmt.Println()

	fmt.Println("1-3) Per-request dial vs unbounded pool vs bounded pool (MaxOpen 90), same traffic")
	fmt.Printf("   %-22s %8s %8s %8s %8s %10s %7s %10s %7s %9s %6s %9s\n",
		"design", "requests", "ok", "refused", "timeout", "handshakes", "reuse", "peak conns", "waits", "avg wait", "p50", "p99")
	designs := []struct {
		name    string
		opts    []connectionpool.Option
		maxOpen int
	}{
		{"no pool (dial each)", nil, 0},
		{"unbounded pool", []connectionpool.Option{}, 0},
		{"bounded pool (90)", []connectionpool.Option{connectionpool.WithMaxOpen(90)}, 90},
	}
	for _, d := range designs {
		r := run(scenario{durationMS: 3000, load: spike, poolOpts: d.opts, maxOpen: d.maxOpen})
		reuse, waits, avgWait := "-", "-", "-"
		if d.opts != nil {
			reuse = fmt.Sprintf("%.1f%%", 100*r.stats.ReuseRate())
			waits = fmt.Sprint(r.stats.Waits)
			if r.stats.Waits > 0 {
				avgWait = (r.stats.WaitTime / time.Duration(r.stats.Waits)).Round(100 * time.Microsecond).String()
			}
		}
		fmt.Printf("   %-22s %8d %8d %8d %8d %10d %7s %10d %7s %9s %4dms %7dms\n",
			d.name, r.requests, r.ok, r.refused, r.timeouts, r.be.dials, reuse, r.be.peak, waits, avgWait,
			pct(r.latencies, 0.5), pct(r.latencies, 0.99))
		if d.opts == nil {
			fmt.Printf("   %-22s (%d sockets closed by the client, all still in TIME_WAIT for the next 60s)\n", "", r.be.timeWait)
		}
	}
	fmt.Println()
	fmt.Println("   No pool: every request pays the 30ms handshake, and 1500 req/s x 50ms already keeps ~75 connections")
	fmt.Println("   open; the spike needs ~200, so the backend refuses requests. The unbounded pool reuses connections,")
	fmt.Println("   but in the spike it dials one per concurrent request, runs into the limit and some requests fail.")
	fmt.Println("   The bounded pool stays at 90: a few hundred requests wait ~2ms for a returned connection instead.")
	fmt.Println()

	fmt.Println("4) The database restarts at t=1.5s (steady 1500 req/s, MaxOpen 90): idle sockets are dead")
	fmt.Printf("   %-26s %12s %16s %20s\n", "pool", "failed: in", "failed: handed a", "discarded by check")
	fmt.Printf("   %-26s %12s %16s %20s\n", "", "flight", "dead idle conn", "on borrow")
	restart := map[int]func(*backend){1500: (*backend).restart}
	for _, v := range []struct {
		name string
		opts []connectionpool.Option
	}{
		{"no health check", []connectionpool.Option{connectionpool.WithMaxOpen(90)}},
		{"validate on borrow", []connectionpool.Option{connectionpool.WithMaxOpen(90),
			connectionpool.WithValidate(func(c *conn) error {
				if !c.alive() {
					return errors.New("connection reset by peer")
				}
				return nil
			})}},
	} {
		r := run(scenario{durationMS: 3000, load: steady, poolOpts: v.opts, maxOpen: 90, events: restart})
		fmt.Printf("   %-26s %12d %16d %20d\n", v.name, r.failedInFlight, r.failedStale, r.stats.Discards-uint64(r.failedInFlight+r.failedStale))
	}
	fmt.Println("   Requests in flight at the restart fail either way. Without a check, every idle socket is lent")
	fmt.Println("   out once and fails a request; with validate-on-borrow they are closed and replaced instead.")
	fmt.Println()

	fmt.Println("5) DNS for the database moves from db-a to db-b at t=1s (steady 1500 req/s, MaxOpen 90)")
	fmt.Println("   share of open connections on db-b (opens so far):")
	dns := map[int]func(*backend){1000: func(b *backend) { b.switchDNS("db-b") }}
	var rows [][]sample
	names := []string{"no max lifetime", "max lifetime 1s"}
	for _, opts := range [][]connectionpool.Option{
		{connectionpool.WithMaxOpen(90)},
		{connectionpool.WithMaxOpen(90), connectionpool.WithMaxLifetime(time.Second)},
	} {
		r := run(scenario{durationMS: 3500, load: steady, poolOpts: opts, maxOpen: 90, events: dns, sampleMS: 500})
		rows = append(rows, r.samples)
	}
	fmt.Printf("   %-18s", "t")
	for _, sm := range rows[0] {
		fmt.Printf(" %12s", fmt.Sprintf("%.1fs", float64(sm.ms)/1000))
	}
	fmt.Println()
	for i, name := range names {
		fmt.Printf("   %-18s", name)
		for _, sm := range rows[i] {
			share := 0.0
			if sm.total > 0 {
				share = 100 * float64(sm.onB) / float64(sm.total)
			}
			fmt.Printf(" %12s", fmt.Sprintf("%3.0f%% (%d)", share, sm.opens))
		}
		fmt.Println()
	}
	fmt.Println("   Without a lifetime the pool keeps reusing its db-a connections forever. With one, each connection")
	fmt.Println("   is retired a second after it was opened and redialed, which follows DNS. The scaled 1s here stands")
	fmt.Println("   in for the usual 5-30 minutes; the cost is the extra dials in parentheses. All connections here were")
	fmt.Println("   dialed together at startup, so they also expire together (a reconnect burst every second); real pools")
	fmt.Println("   add jitter to the lifetime to spread that out.")
}
