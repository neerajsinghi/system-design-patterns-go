// Package gatewaybottleneck simulates the "API gateway becomes the
// bottleneck" incident. A flash sale starts, traffic triples, and the
// gateway's CPU pins at 100% while p99 latency goes from tens of milliseconds
// to seconds, even though every backend is healthy. Three design choices
// combine to do it:
//
//   - the gateway asks a central auth service to validate the bearer token on
//     every request, so the auth service (sized for normal traffic) saturates
//     and every request pays a round trip plus its queue;
//   - the gateway opens a new TCP+TLS connection to the backend for every
//     request and closes it afterwards, so it burns CPU on handshakes and
//     piles up sockets in TIME_WAIT, eating its ephemeral port range;
//   - one partner integration sends ten times its normal traffic and nothing
//     stops it, so it takes the capacity that everybody else needed.
//
// An autoscaler adds gateways when CPU stays above 80%. That brings gateway
// CPU down, but it cannot help the auth service: every new gateway forwards
// every request to it, and opens a new backend connection per request too.
//
// Broken is the design above. Fixed keeps the same traffic, the same fleet,
// the same autoscaler, the same auth service and the same backend, but:
//
//   - verifies the signed token locally at the edge with a per-gateway
//     verified-token cache (patterns/architecture/api-gateway TokenVerifier),
//     so the auth service is not on the request path at all;
//   - reuses a bounded pool of upstream connections per gateway
//     (patterns/architecture/connection-pool), sized so that even a fully
//     scaled-out fleet stays under the backend's connection limit;
//   - gives every client a token-bucket quota
//     (patterns/reliability/rate-limiter-token-bucket Keyed), so the noisy
//     partner gets fast 429s instead of a share of the connection pool.
//
// The simulation is discrete-event, on a simulated clock. Arrivals are
// Poisson streams per client from seeded generators and every backend
// latency is a pure function of the request's index, so both designs see
// identical traffic and identical backend behaviour. Gateway CPU is modelled
// as cores that run jobs first come, first served, with a fixed CPU cost per
// kind of work (the cost model is the constants below).
package gatewaybottleneck

import (
	"container/heap"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	apigateway "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/api-gateway"
	connectionpool "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/connection-pool"
	tokenbucket "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/rate-limiter-token-bucket"
)

const (
	Seed     = 11
	SaleFrom = 20 * time.Second // the sale starts here; before it is baseline traffic
	Duration = 80 * time.Second

	// Traffic (requests per second), identical in both designs.
	NormalClients    = 10  // well-behaved API clients (apps, web, partners)
	NormalRate       = 25  // per normal client before the sale
	NormalSaleRate   = 75  // per normal client during the sale (3x)
	NoisyRate        = 50  // the noisy partner before the sale
	NoisySaleRate    = 500 // the noisy partner during the sale (10x)
	RevokedRate      = 5   // a client whose token is revoked mid-sale
	RevokeAt         = 50 * time.Second
	TokenLifetime    = 15 * time.Minute
	ClientNoisy      = "partner-noisy"
	ClientRevoked    = "leaked-key"
	normalClientName = "client-%02d"

	// Gateway fleet and its cost model (CPU time per piece of work).
	Gateways        = 2 // at the start
	MaxGateways     = 4 // the autoscaler's ceiling
	CoresPerGateway = 1
	ProxyCPU        = 300 * time.Microsecond  // parse, route, rewrite headers, copy bodies
	AuthCallCPU     = 300 * time.Microsecond  // build, send and parse a remote token check
	HandshakeCPU    = 1500 * time.Microsecond // client side of a new TCP + TLS connection
	VerifyCPU       = 30 * time.Microsecond   // local HS256 verification (JSON + HMAC)
	CacheHitCPU     = 5 * time.Microsecond    // local verification answered from the cache
	RejectCPU       = 20 * time.Microsecond   // writing a 429 / 401
	GatewayTimeout  = 2 * time.Second         // the gateway answers 504 after this

	// Autoscaler, identical in both designs: one more gateway when fleet CPU
	// averaged over ScaleWindow is above ScaleAbove, at most every ScaleCooldown.
	ScaleAbove    = 0.80
	ScaleWindow   = 5 * time.Second
	ScaleCooldown = 15 * time.Second

	// Auth service: AuthWorkers checks in parallel, AuthServiceTime each.
	AuthWorkers     = 4
	AuthServiceTime = 3600 * time.Microsecond // so capacity is 1,111 checks/s
	NetworkRTT      = 500 * time.Microsecond  // inside the data centre
	HandshakeRTTs   = 2                       // TCP SYN/ACK + TLS 1.3

	// Backend: healthy throughout. 20-40 ms per request, 1% at 60-90 ms.
	BackendConnLimit = 70 // concurrent connections the backend accepts
	EphemeralPorts   = 28232
	TimeWait         = 60 * time.Second // Linux keeps a closed socket this long

	// Fixed design only.
	PoolPerGateway = 16  // MaxGateways × PoolPerGateway = 64 ≤ BackendConnLimit
	ClientQuota    = 100 // requests per second per client
	ClientBurst    = 100
	TokenCacheSize = 1024
	TokenCacheTTL  = 60 * time.Second

	// Invariant targets.
	CPUTarget = 0.80
	P99Target = 100 * time.Millisecond
)

// Client classes.
const (
	ClassNormal = iota
	ClassNoisy
	ClassRevoked
	numClasses
)

var classNames = [numClasses]string{"well-behaved", "noisy partner", "revoked token"}

// Outcomes.
const (
	OK          = iota
	TooMany     // 429 from the per-client limit
	Unauthed    // 401
	BadGateway  // 502: could not connect upstream (ports or backend limit)
	GatewayTO   // 504: gave up after GatewayTimeout
	numOutcomes //
)

var outcomeNames = [numOutcomes]string{"200", "429", "401", "502", "504"}

// ClassStats is what one client class saw during the sale.
type ClassStats struct {
	Requests int
	Outcomes [numOutcomes]int
	P50, P99 time.Duration
}

// Second is one second of the run.
type Second struct {
	CPU        float64 // fleet CPU utilisation, 0..1
	Gateways   int
	AuthCalls  int
	Handshakes int
	TimeWait   int // TIME_WAIT sockets across the fleet at the end of the second
	NormalP99  time.Duration
	Refused    int // upstream connects refused (ports or backend limit)
}

// Report is what a run measured. Rates are averaged over the sale.
type Report struct {
	Name    string
	Seconds [Duration / time.Second]Second
	// Classes is per client class, for requests that arrived during the sale.
	Classes [numClasses]ClassStats
	// TooManyByClient counts 429s per client ID over the whole run.
	TooManyByClient map[string]int
	// NoisyAdmitted is how many of the noisy partner's sale requests got past
	// the gateway's admission checks (no 429).
	NoisyAdmitted int

	PeakCPU              float64
	AuthCallsPerRequest  float64
	AuthCallsPerSec      float64
	HandshakesPerSec     float64
	PeakTimeWait         int // most TIME_WAIT sockets on one gateway
	PeakBackendConns     int
	PortExhausted        int // connects refused because the gateway had no free port
	BackendRefused       int // connects refused by the backend's connection limit
	PoolWaits            int // requests that queued for a pooled connection
	PoolWaitP99          time.Duration
	MaxGatewaysUsed      int
	RevokedAcceptedAfter int // requests on the revoked token answered 200 after RevokeAt
	TokenCacheHitRate    float64
}

// RevocationWindow is how long a revoked token keeps working when the
// gateway verifies tokens locally: until the token's own expiry.
func RevocationWindow() time.Duration { return TokenLifetime - RevokeAt }

// SaleSeconds is the length of the sale window.
func SaleSeconds() float64 { return (Duration - SaleFrom).Seconds() }

// NoisyQuota is the most the noisy partner may be admitted during the sale:
// its sustained quota plus one full burst.
func NoisyQuota() int { return int(ClientQuota*SaleSeconds()) + ClientBurst }

// Print writes a timeline and a summary.
func (r Report) Print(w io.Writer) {
	fmt.Fprintf(w, "%s\n", r.Name)
	fmt.Fprintf(w, "sale at t=%ds: %d clients %d→%d req/s, noisy partner %d→%d req/s; %d gateways (%d core each) to start, max %d; auth service %d checks/s\n\n",
		int(SaleFrom.Seconds()), NormalClients, NormalRate, NormalSaleRate, NoisyRate, NoisySaleRate,
		Gateways, CoresPerGateway, MaxGateways, AuthWorkers*int(time.Second)/int(AuthServiceTime))
	fmt.Fprintln(w, "  window     gateways  gateway CPU            auth calls/s  handshakes/s  TIME_WAIT  refused  well-behaved p99")
	for start := 0; start < len(r.Seconds); start += 10 {
		var cpu float64
		var auth, hs, refused int
		for s := start; s < start+10; s++ {
			cpu += r.Seconds[s].CPU
			auth += r.Seconds[s].AuthCalls
			hs += r.Seconds[s].Handshakes
			refused += r.Seconds[s].Refused
		}
		cpu /= 10
		last := r.Seconds[start+9]
		p99 := time.Duration(0)
		for s := start; s < start+10; s++ {
			p99 = max(p99, r.Seconds[s].NormalP99)
		}
		fmt.Fprintf(w, "  t=%2d-%2ds   %d         %5.1f%% %-12s  %6d        %6d       %6d     %5d    %s\n",
			start, start+9, last.Gateways, 100*cpu, strings.Repeat("█", int(math.Round(cpu*10))),
			auth/10, hs/10, last.TimeWait, refused, p99.Round(time.Millisecond))
	}
	fmt.Fprintf(w, "\n  during the sale (t=%d-%ds):\n", int(SaleFrom.Seconds()), int(Duration.Seconds()))
	fmt.Fprintf(w, "    %-14s %8s  %7s %7s %7s %7s %7s  %9s %9s\n", "client class", "requests",
		outcomeNames[0], outcomeNames[1], outcomeNames[2], outcomeNames[3], outcomeNames[4], "p50", "p99")
	for c, cs := range r.Classes {
		fmt.Fprintf(w, "    %-14s %8d  %7d %7d %7d %7d %7d  %9s %9s\n", classNames[c], cs.Requests,
			cs.Outcomes[OK], cs.Outcomes[TooMany], cs.Outcomes[Unauthed], cs.Outcomes[BadGateway], cs.Outcomes[GatewayTO],
			cs.P50.Round(10*time.Microsecond), cs.P99.Round(100*time.Microsecond))
	}
	fmt.Fprintf(w, "\n  peak gateway CPU (1s): %.0f%% (target ≤ %.0f%%); gateways used: %d of %d\n", 100*r.PeakCPU, 100*CPUTarget, r.MaxGatewaysUsed, MaxGateways)
	fmt.Fprintf(w, "  auth service checks requested: %.0f/s (capacity %d/s), %.2f per request\n",
		r.AuthCallsPerSec, AuthWorkers*int(time.Second)/int(AuthServiceTime), r.AuthCallsPerRequest)
	fmt.Fprintf(w, "  upstream handshakes: %.1f/s; peak TIME_WAIT on one gateway: %d of %d ephemeral ports\n", r.HandshakesPerSec, r.PeakTimeWait, EphemeralPorts)
	fmt.Fprintf(w, "  peak backend connections: %d (limit %d); refused: %d by port exhaustion, %d by the backend\n",
		r.PeakBackendConns, BackendConnLimit, r.PortExhausted, r.BackendRefused)
	if r.TokenCacheHitRate > 0 {
		fmt.Fprintf(w, "  local token verification: %.2f%% answered from the per-gateway cache\n", 100*r.TokenCacheHitRate)
	}
	if r.PoolWaits > 0 {
		fmt.Fprintf(w, "  requests that waited for a pooled connection: %d (p99 wait %s)\n", r.PoolWaits, r.PoolWaitP99.Round(10*time.Microsecond))
	}
	keys := make([]string, 0, len(r.TooManyByClient))
	for k := range r.TooManyByClient {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	fmt.Fprint(w, "  429s by client:")
	if len(keys) == 0 {
		fmt.Fprint(w, " none")
	}
	for _, k := range keys {
		fmt.Fprintf(w, " %s=%d", k, r.TooManyByClient[k])
	}
	fmt.Fprintf(w, "\n  noisy partner admitted during the sale: %d (quota %d)\n", r.NoisyAdmitted, NoisyQuota())
	fmt.Fprintf(w, "  requests on the revoked token served after it was revoked at t=%ds: %d", int(RevokeAt.Seconds()), r.RevokedAcceptedAfter)
	if r.RevokedAcceptedAfter > 0 {
		fmt.Fprintf(w, " (it stays valid until its exp, %s later)", RevocationWindow())
	}
	fmt.Fprintln(w)
}

// ---- traffic ----

type request struct {
	at     time.Duration
	client int // index into clients
	class  int
	idx    uint64

	gw       int
	conn     *connectionpool.Conn[int]
	queuedAt time.Duration
}

type clientSpec struct {
	id                string
	class             int
	baseRate, saleRte float64
}

func clients() []clientSpec {
	cs := make([]clientSpec, 0, NormalClients+2)
	for i := range NormalClients {
		cs = append(cs, clientSpec{fmt.Sprintf(normalClientName, i), ClassNormal, NormalRate, NormalSaleRate})
	}
	cs = append(cs, clientSpec{ClientNoisy, ClassNoisy, NoisyRate, NoisySaleRate})
	cs = append(cs, clientSpec{ClientRevoked, ClassRevoked, RevokedRate, RevokedRate})
	return cs
}

// traffic returns every request of the run, sorted by arrival time. It is a
// pure function of Seed.
func traffic() []request {
	var reqs []request
	for ci, c := range clients() {
		rng := rand.New(rand.NewPCG(Seed, uint64(ci)))
		t := time.Duration(0)
		for {
			rate := c.baseRate
			if t >= SaleFrom {
				rate = c.saleRte
			}
			t += time.Duration(rng.ExpFloat64() / rate * float64(time.Second))
			if t >= Duration {
				break
			}
			reqs = append(reqs, request{at: t, client: ci, class: c.class})
		}
	}
	slices.SortFunc(reqs, func(a, b request) int {
		switch {
		case a.at < b.at:
			return -1
		case a.at > b.at:
			return 1
		}
		return a.client - b.client
	})
	for i := range reqs {
		reqs[i].idx = uint64(i)
	}
	return reqs
}

// backendLatency is a pure function of the request index.
func backendLatency(idx uint64) time.Duration {
	h := splitmix(Seed<<32 ^ idx)
	u := float64(h>>11) / (1 << 53)
	v := float64(splitmix(h)>>11) / (1 << 53)
	if u < 0.01 {
		return 60*time.Millisecond + time.Duration(v*float64(30*time.Millisecond))
	}
	return 20*time.Millisecond + time.Duration(v*float64(20*time.Millisecond))
}

func splitmix(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// ---- event loop ----

const (
	evAuthReply = iota
	evUpstream
	evBackendDone
	evTick
)

type event struct {
	at   time.Duration
	seq  uint64
	kind int
	req  int
}

type eventQueue []event

func (q eventQueue) Len() int { return len(q) }
func (q eventQueue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}
func (q eventQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *eventQueue) Push(x any)   { *q = append(*q, x.(event)) }
func (q *eventQueue) Pop() any {
	old := *q
	e := old[len(old)-1]
	*q = old[:len(old)-1]
	return e
}

type design struct {
	name       string
	localAuth  bool // verify tokens at the edge instead of calling the auth service
	pool       bool // pooled upstream connections instead of one per request
	clientRate bool // per-client token-bucket quotas
}

type gateway struct {
	cores    []time.Duration // when each core is next free
	verifier *apigateway.TokenVerifier
	pool     *connectionpool.Pool[int]
	waiters  []int // requests waiting for a pooled connection, FIFO
	active   int   // open upstream sockets
	timeWait []time.Duration
	twHead   int
	lastDial bool // set by the pool's dial func
	peakTW   int
}

func (g *gateway) tw(now time.Duration) int {
	for g.twHead < len(g.timeWait) && g.timeWait[g.twHead] <= now {
		g.twHead++
	}
	return len(g.timeWait) - g.twHead
}

type sim struct {
	d      design
	now    time.Duration
	seq    uint64
	q      eventQueue
	reqs   []request
	specs  []clientSpec
	tokens []string

	gws       []*gateway
	rr        int
	lastScale time.Duration

	authFree    [AuthWorkers]time.Duration
	authChecker *apigateway.TokenVerifier
	limiter     *tokenbucket.Keyed

	backendConns int
	cpuBusy      []time.Duration // per second
	cpuCap       []time.Duration // per second
	latencies    [numClasses][]time.Duration
	perSecNormal [][]time.Duration
	poolWaits    []time.Duration
	r            *Report
	saleReqs     int
	saleAuth     int
	saleHS       int
}

var epoch = time.Unix(1_767_225_600, 0) // 2026-01-01, an arbitrary fixed start

func (s *sim) clock() time.Time { return epoch.Add(s.now) }

var secret = []byte("gateway-bottleneck-demo-signing-key")

func (s *sim) schedule(at time.Duration, kind, req int) {
	s.seq++
	heap.Push(&s.q, event{at: at, seq: s.seq, kind: kind, req: req})
}

// cpu runs a job of cost on gateway g's earliest-free core, starting no
// earlier than now, and returns when it finishes.
func (s *sim) cpu(g int, cost time.Duration) time.Duration {
	gw := s.gws[g]
	best := 0
	for i, f := range gw.cores {
		if f < gw.cores[best] {
			best = i
		}
	}
	start := max(s.now, gw.cores[best])
	gw.cores[best] = start + cost
	if sec := int(start / time.Second); sec < len(s.cpuBusy) {
		s.cpuBusy[sec] += cost
	}
	return start + cost
}

func (s *sim) addGateway() {
	gw := &gateway{cores: make([]time.Duration, CoresPerGateway)}
	if s.d.localAuth {
		v, err := apigateway.NewTokenVerifier(secret,
			apigateway.WithVerifierClock(s.clock), apigateway.WithTokenCache(TokenCacheSize, TokenCacheTTL))
		if err != nil {
			panic(err)
		}
		gw.verifier = v
	}
	if s.d.pool {
		dial := func(context.Context) (int, error) {
			gw.lastDial = true
			gw.active++
			s.backendConns++
			return 0, nil
		}
		p, err := connectionpool.New(dial, func(int) { gw.active--; s.backendConns-- },
			connectionpool.WithMaxOpen(PoolPerGateway), connectionpool.WithClock(s.clock))
		if err != nil {
			panic(err)
		}
		gw.pool = p
	}
	s.gws = append(s.gws, gw)
}

func (s *sim) finish(ri, outcome int, at time.Duration) {
	rq := &s.reqs[ri]
	lat := at - rq.at
	if lat > GatewayTimeout {
		lat, outcome = GatewayTimeout, GatewayTO
	}
	spec := s.specs[rq.client]
	if outcome == TooMany {
		s.r.TooManyByClient[spec.id]++
	}
	if rq.class == ClassRevoked && rq.at >= RevokeAt && outcome == OK {
		s.r.RevokedAcceptedAfter++
	}
	if rq.class == ClassNormal {
		sec := int(rq.at / time.Second)
		s.perSecNormal[sec] = append(s.perSecNormal[sec], lat)
	}
	if rq.at < SaleFrom {
		return
	}
	cs := &s.r.Classes[rq.class]
	cs.Requests++
	cs.Outcomes[outcome]++
	s.latencies[rq.class] = append(s.latencies[rq.class], lat)
	if rq.class == ClassNoisy && outcome != TooMany {
		s.r.NoisyAdmitted++
	}
}

func (s *sim) timedOut(ri int) bool { return s.now-s.reqs[ri].at > GatewayTimeout }

func (s *sim) arrive(ri int) {
	rq := &s.reqs[ri]
	rq.gw = s.rr % len(s.gws)
	s.rr++
	gw := s.gws[rq.gw]
	if rq.at >= SaleFrom {
		s.saleReqs++
	}
	if !s.d.localAuth {
		done := s.cpu(rq.gw, ProxyCPU+AuthCallCPU)
		// The check reaches the auth service half an RTT later and waits for
		// a free worker; the decision is made when a worker picks it up.
		arrive := done + NetworkRTT/2
		w := 0
		for i, f := range s.authFree {
			if f < s.authFree[w] {
				w = i
			}
		}
		start := max(arrive, s.authFree[w])
		s.authFree[w] = start + AuthServiceTime
		if sec := int(start / time.Second); sec < len(s.r.Seconds) {
			s.r.Seconds[sec].AuthCalls++
		}
		if rq.at >= SaleFrom {
			s.saleAuth++
		}
		rq.queuedAt = start // reused: when the auth service looked at the token
		s.schedule(start+AuthServiceTime+NetworkRTT/2, evAuthReply, ri)
		return
	}
	before := gw.verifier.Stats().CacheHits
	_, err := gw.verifier.Verify(s.tokens[rq.client])
	cost := VerifyCPU
	if gw.verifier.Stats().CacheHits > before {
		cost = CacheHitCPU
	}
	if err != nil {
		s.finish(ri, Unauthed, s.cpu(rq.gw, cost+RejectCPU))
		return
	}
	if s.d.clientRate && !s.limiter.Allow(s.specs[rq.client].id) {
		s.finish(ri, TooMany, s.cpu(rq.gw, cost+RejectCPU))
		return
	}
	s.schedule(s.cpu(rq.gw, cost+ProxyCPU), evUpstream, ri)
}

// authReply: the auth service's answer is back at the gateway.
func (s *sim) authReply(ri int) {
	rq := &s.reqs[ri]
	if s.timedOut(ri) {
		s.finish(ri, GatewayTO, s.now)
		return
	}
	_, err := s.authChecker.Verify(s.tokens[rq.client])
	if err != nil || (rq.class == ClassRevoked && rq.queuedAt >= RevokeAt) {
		s.finish(ri, Unauthed, s.cpu(rq.gw, RejectCPU))
		return
	}
	if s.d.clientRate && !s.limiter.Allow(s.specs[rq.client].id) {
		s.finish(ri, TooMany, s.cpu(rq.gw, RejectCPU))
		return
	}
	s.upstream(ri)
}

func (s *sim) upstream(ri int) {
	rq := &s.reqs[ri]
	gw := s.gws[rq.gw]
	if s.timedOut(ri) {
		s.finish(ri, GatewayTO, s.now)
		return
	}
	if !s.d.pool {
		if gw.active+gw.tw(s.now) >= EphemeralPorts {
			s.r.PortExhausted++
			s.refused()
			s.finish(ri, BadGateway, s.now)
			return
		}
		if s.backendConns >= BackendConnLimit {
			s.r.BackendRefused++
			s.refused()
			s.finish(ri, BadGateway, s.now+NetworkRTT)
			return
		}
		gw.active++
		s.backendConns++
		s.handshake()
		ready := s.cpu(rq.gw, HandshakeCPU) + HandshakeRTTs*NetworkRTT
		s.trackConns()
		s.schedule(ready+backendLatency(rq.idx), evBackendDone, ri)
		return
	}
	st := gw.pool.Stats()
	if len(gw.waiters) > 0 || (st.Idle == 0 && st.Open >= PoolPerGateway) {
		rq.queuedAt = s.now
		gw.waiters = append(gw.waiters, ri)
		return
	}
	s.borrow(ri, 0)
}

// borrow takes a pooled connection that is known to be available now.
func (s *sim) borrow(ri int, waited time.Duration) {
	rq := &s.reqs[ri]
	gw := s.gws[rq.gw]
	gw.lastDial = false
	c, err := gw.pool.Get(context.Background())
	if err != nil {
		panic(err)
	}
	rq.conn = c
	ready := s.now
	if gw.lastDial {
		s.handshake()
		ready = s.cpu(rq.gw, HandshakeCPU) + HandshakeRTTs*NetworkRTT
		s.trackConns()
	}
	if waited > 0 {
		s.poolWaits = append(s.poolWaits, waited)
	}
	s.schedule(ready+backendLatency(rq.idx), evBackendDone, ri)
}

func (s *sim) handshake() {
	if sec := int(s.now / time.Second); sec < len(s.r.Seconds) {
		s.r.Seconds[sec].Handshakes++
	}
	if s.now >= SaleFrom {
		s.saleHS++
	}
}

func (s *sim) refused() {
	if sec := int(s.now / time.Second); sec < len(s.r.Seconds) {
		s.r.Seconds[sec].Refused++
	}
}

func (s *sim) trackConns() { s.r.PeakBackendConns = max(s.r.PeakBackendConns, s.backendConns) }

func (s *sim) backendDone(ri int) {
	rq := &s.reqs[ri]
	gw := s.gws[rq.gw]
	s.finish(ri, OK, s.now)
	if !s.d.pool {
		// The gateway closes the connection, so its socket sits in TIME_WAIT.
		gw.active--
		s.backendConns--
		gw.timeWait = append(gw.timeWait, s.now+TimeWait)
		gw.peakTW = max(gw.peakTW, gw.tw(s.now))
		return
	}
	rq.conn.Release()
	rq.conn = nil
	for len(gw.waiters) > 0 {
		next := gw.waiters[0]
		gw.waiters = gw.waiters[1:]
		if s.timedOut(next) {
			s.finish(next, GatewayTO, s.now)
			continue
		}
		s.borrow(next, s.now-s.reqs[next].queuedAt)
		break
	}
}

func (s *sim) tick(sec int) {
	cores := time.Duration(len(s.gws) * CoresPerGateway)
	s.cpuCap[sec] = cores * time.Second
	s.r.Seconds[sec].Gateways = len(s.gws)
	tw := 0
	for _, g := range s.gws {
		tw += g.tw(s.now)
	}
	if sec > 0 {
		s.r.Seconds[sec-1].TimeWait = tw
	}
	// Autoscaler: look at the last ScaleWindow full seconds.
	n := int(ScaleWindow / time.Second)
	if sec >= n && len(s.gws) < MaxGateways && s.now-s.lastScale >= ScaleCooldown {
		var busy, capa time.Duration
		for i := sec - n; i < sec; i++ {
			busy += s.cpuBusy[i]
			capa += s.cpuCap[i]
		}
		if float64(busy)/float64(capa) > ScaleAbove {
			s.addGateway()
			s.lastScale = s.now
			s.cpuCap[sec] = time.Duration(len(s.gws)*CoresPerGateway) * time.Second
			s.r.Seconds[sec].Gateways = len(s.gws)
		}
	}
}

func run(d design) Report {
	r := Report{Name: d.name, TooManyByClient: map[string]int{}}
	s := &sim{d: d, r: &r, specs: clients(), reqs: traffic()}
	secs := len(r.Seconds)
	s.cpuBusy = make([]time.Duration, secs+1)
	s.cpuCap = make([]time.Duration, secs+1)
	s.perSecNormal = make([][]time.Duration, secs)
	for i := range s.specs {
		tok, err := apigateway.SignHS256(secret, apigateway.Claims{Subject: s.specs[i].id, ExpiresAt: epoch.Add(TokenLifetime)})
		if err != nil {
			panic(err)
		}
		s.tokens = append(s.tokens, tok)
	}
	var err error
	if s.authChecker, err = apigateway.NewTokenVerifier(secret, apigateway.WithVerifierClock(s.clock)); err != nil {
		panic(err)
	}
	s.limiter = tokenbucket.NewKeyed(ClientQuota, ClientBurst, s.clock)
	for range Gateways {
		s.addGateway()
	}
	for sec := range secs {
		s.schedule(time.Duration(sec)*time.Second, evTick, sec)
	}
	// Arrivals are already sorted, so they are merged with the event queue
	// instead of being pushed into it. An event at the same instant as an
	// arrival runs first.
	next := 0
	for s.q.Len() > 0 || next < len(s.reqs) {
		if next < len(s.reqs) && (s.q.Len() == 0 || s.reqs[next].at < s.q[0].at) {
			s.now = s.reqs[next].at
			s.arrive(next)
			next++
			continue
		}
		e := heap.Pop(&s.q).(event)
		s.now = e.at
		switch e.kind {
		case evAuthReply:
			s.authReply(e.req)
		case evUpstream:
			s.upstream(e.req)
		case evBackendDone:
			s.backendDone(e.req)
		case evTick:
			s.tick(e.req)
		}
	}
	s.summarise()
	return r
}

func (s *sim) summarise() {
	r := s.r
	for sec := range r.Seconds {
		r.Seconds[sec].CPU = min(1, float64(s.cpuBusy[sec])/float64(s.cpuCap[sec]))
		r.PeakCPU = max(r.PeakCPU, r.Seconds[sec].CPU)
		r.MaxGatewaysUsed = max(r.MaxGatewaysUsed, r.Seconds[sec].Gateways)
		r.Seconds[sec].NormalP99 = percentile(s.perSecNormal[sec], 0.99)
	}
	r.Seconds[len(r.Seconds)-1].TimeWait = 0
	var hits, checks uint64
	for _, g := range s.gws {
		r.PeakTimeWait = max(r.PeakTimeWait, g.peakTW)
		r.Seconds[len(r.Seconds)-1].TimeWait += g.tw(Duration)
		if g.verifier != nil {
			st := g.verifier.Stats()
			hits += st.CacheHits
			checks += st.CacheHits + st.Verified + st.Rejected
		}
	}
	if checks > 0 {
		r.TokenCacheHitRate = float64(hits) / float64(checks)
	}
	r.AuthCallsPerRequest = float64(s.saleAuth) / float64(s.saleReqs)
	r.AuthCallsPerSec = float64(s.saleAuth) / SaleSeconds()
	r.HandshakesPerSec = float64(s.saleHS) / SaleSeconds()
	for c := range r.Classes {
		r.Classes[c].P50 = percentile(s.latencies[c], 0.50)
		r.Classes[c].P99 = percentile(s.latencies[c], 0.99)
	}
	r.PoolWaits = len(s.poolWaits)
	r.PoolWaitP99 = percentile(s.poolWaits, 0.99)
}

func percentile(xs []time.Duration, p float64) time.Duration {
	if len(xs) == 0 {
		return 0
	}
	ys := slices.Clone(xs)
	slices.Sort(ys)
	return ys[min(len(ys)-1, int(math.Ceil(p*float64(len(ys))))-1)]
}

// Broken: every request is checked by the auth service, gets a fresh
// upstream connection, and no client is limited.
func Broken() Report {
	return run(design{name: "BROKEN: remote auth check per request, new upstream connection per request, no per-client limits"})
}

// Fixed: tokens are verified at the edge, upstream connections are pooled,
// and every client has a token-bucket quota.
func Fixed() Report {
	return run(design{name: "FIXED: local token verification + pooled upstream connections + per-client quotas",
		localAuth: true, pool: true, clientRate: true})
}

// withoutQuotas is Fixed minus the per-client limits, used by the tests to
// show that the first two fixes alone do not protect well-behaved clients.
func withoutQuotas() Report {
	return run(design{name: "FIXED minus per-client quotas", localAuth: true, pool: true})
}
