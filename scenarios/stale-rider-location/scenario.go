// Package staleriderlocation simulates the "delivery map shows the rider in
// the wrong place" incident. A customer watches their rider on a map, and the
// dot is hundreds of metres behind, jumps backwards, or keeps sitting
// confidently at the last position of a rider whose phone lost signal a
// minute ago.
//
// Broken has the customer app poll a location API every 30s. The API reads
// through a cache with a 60s TTL, and the store behind it applies rider GPS
// fixes in the order they come off the queue, so a delayed or redelivered old
// fix overwrites a newer one. Nothing tells the customer how old the dot is.
//
// Fixed has riders' fixes carry their device timestamps into a presence
// tracker that keeps only newer fixes and holds a heartbeat-renewed lease per
// rider. Customers hold a WebSocket to a gateway node; each accepted fix is
// pushed to whichever node the connection registry says holds the customer's
// socket, through a one-slot drop-oldest send queue. When a rider's lease
// expires, the customer gets a "stale" push and the UI greys the dot and
// shows "last updated N s ago".
package staleriderlocation

import (
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/presence"
	websockethub "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/websocket-hub"
	geospatialindex "github.com/neerajsinghi/system-design-patterns-go/patterns/data/geospatial-index"
	"github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/backpressure"
)

const (
	Riders   = 40  // one active order, one rider and one watching customer each
	Duration = 600 // simulated seconds
	Warmup   = 35  // seconds before sampling starts, so both versions have shown a first position
	FixEvery = 2   // seconds between GPS fixes from a rider's phone

	// Network and queue behaviour of rider uploads (identical in both runs).
	ReorderRate   = 0.12 // fraction of fixes delayed 4-12s, so newer fixes overtake them
	RedeliverRate = 0.03 // fraction of fixes stuck in a consumer retry and redelivered 35-90s late

	// Some riders' phones lose signal; their fixes in this window are never sent.
	SignalLossFrom = 180
	SignalLossTo   = 300

	// One customer-facing gateway node crashes; its customers reconnect elsewhere.
	Gateways        = 4
	CrashedGateway  = "gw-1"
	CrashAt         = 420
	NodeDeadAfter   = 2 // seconds until the cluster notices the dead node and drops its registry entries
	SlowLinkEvery   = 5 // a customer on a slow link can take one message every this many seconds
	PollEvery       = 30
	CacheTTL        = 60 * time.Second // "positions barely change", someone decided
	RiderLeaseTTL   = 8 * time.Second  // three missed fixes and the rider counts as gone
	SendQueueSize   = 1                // only the latest position matters
	MaxShownAge     = 25 * time.Second
	MaxP95ErrMetres = 100.0

	baseLat, baseLng = 12.9716, 77.5946
	seed             = 2026
)

// Report is what a run measured, sampled once per customer per simulated
// second from Warmup to Duration.
type Report struct {
	Name string

	CustomerSeconds int
	// Display error: distance between the dot the customer sees and where
	// the rider really is.
	ErrP50, ErrP95, ErrMax float64
	// ErrMaxUnflagged is the largest error while the dot was not flagged stale.
	ErrMaxUnflagged float64
	// Age of the shown position (now minus the fix's device timestamp).
	AgeP50, AgeP95, AgeMax time.Duration
	// MaxUnflaggedAge is the oldest position ever shown without a stale flag.
	MaxUnflaggedAge time.Duration
	// BackwardJumps counts display updates to a fix older than the one shown before.
	BackwardJumps int
	// StaleUnflaggedSeconds counts customer-seconds showing a position older
	// than MaxShownAge with no stale indicator.
	StaleUnflaggedSeconds int
	// FlaggedSeconds counts customer-seconds the UI showed "last updated ... ago".
	FlaggedSeconds int

	// Traffic, identical in both runs.
	FixesSent, ReorderedFixes, RedeliveredFixes, LostToSignal int
	// What the location store did with arriving fixes.
	OverwrittenByOlder int // broken: an older fix replaced a newer one
	RejectedOlder      int // fixed: UpdatePosition refused an older fix

	// Customer-facing messages.
	PollRequests int // broken: HTTP polls
	CacheHits    int // broken: polls answered from the 60s cache
	Pushes       int // fixed: messages enqueued to customer sockets (positions, stale markers, resyncs)
	Delivered    int // fixed: messages written to customer sockets
	StaleMarkers int // fixed: "rider went quiet" pushes driven by lease expiry
	Dropped      int // fixed: intermediate positions replaced by a newer one in the one-slot queue
	Undelivered  int // fixed: still queued when the gateway crashed or the run ended

	// Connections and routing.
	PersistentConns     int // fixed: customer WebSockets held open (one per customer)
	FanOut              float64
	ReconnectedCustomer int // customers whose gateway crashed
	LostToDeadNode      int // pushes routed to the crashed node before the cluster dropped it
	RoutedToNewNode     int // pushes after reconnect that the registry sent to the new node
}

// Print writes the summary.
func (r Report) Print(w io.Writer) {
	fmt.Fprintf(w, "%s\n", r.Name)
	fmt.Fprintf(w, "%d riders for %ds; fix every %ds; %.0f%% of fixes delayed 4-12s, %.0f%% redelivered 35-90s late;\n",
		Riders, Duration, FixEvery, 100*ReorderRate, 100*RedeliverRate)
	fmt.Fprintf(w, "riders 0,10,20,30 lose signal t=%d-%ds; gateway %s crashes at t=%ds\n\n",
		SignalLossFrom, SignalLossTo, CrashedGateway, CrashAt)
	fmt.Fprintf(w, "  display error:       p50 %5.0f m   p95 %5.0f m   max %5.0f m   (target p95 <= %.0f m)\n",
		r.ErrP50, r.ErrP95, r.ErrMax, MaxP95ErrMetres)
	fmt.Fprintf(w, "  largest error while the dot was not flagged stale: %.0f m\n", r.ErrMaxUnflagged)
	fmt.Fprintf(w, "  shown position age:  p50 %5.1f s   p95 %5.1f s   max %5.1f s\n",
		r.AgeP50.Seconds(), r.AgeP95.Seconds(), r.AgeMax.Seconds())
	fmt.Fprintf(w, "  oldest position shown without a stale flag: %.1f s (limit %s)\n",
		r.MaxUnflaggedAge.Seconds(), MaxShownAge)
	fmt.Fprintf(w, "  stale-but-unflagged customer-seconds: %d of %d; flagged \"last updated ... ago\": %d\n",
		r.StaleUnflaggedSeconds, r.CustomerSeconds, r.FlaggedSeconds)
	fmt.Fprintf(w, "  backward jumps on the map: %d\n", r.BackwardJumps)
	fmt.Fprintf(w, "  rider fixes: %d sent (%d reordered, %d redelivered), %d lost to signal loss\n",
		r.FixesSent, r.ReorderedFixes, r.RedeliveredFixes, r.LostToSignal)
	if r.PollRequests > 0 {
		fmt.Fprintf(w, "  store: %d newer fixes overwritten by an older one\n", r.OverwrittenByOlder)
		fmt.Fprintf(w, "  customer messages: %d polls (%d answered from cache), 0 persistent connections\n",
			r.PollRequests, r.CacheHits)
	}
	if r.Pushes > 0 {
		fmt.Fprintf(w, "  store: %d older fixes rejected\n", r.RejectedOlder)
		fmt.Fprintf(w, "  customer messages: %d pushes enqueued, %d written, %d stale markers, %d intermediate positions dropped\n",
			r.Pushes, r.Delivered, r.StaleMarkers, r.Dropped)
		fmt.Fprintf(w, "  cost: %d persistent connections, fan-out %.3f pushes per accepted fix\n", r.PersistentConns, r.FanOut)
		fmt.Fprintf(w, "  gateway crash: %d customers reconnected, %d pushes lost to the dead node, %d routed to the new node\n",
			r.ReconnectedCustomer, r.LostToDeadNode, r.RoutedToNewNode)
	}
}

// ---- shared traffic ----

type fix struct {
	rider, seq int
	at         time.Duration // device timestamp, since the start
	arrive     time.Duration // when the location service sees it
	lat, lng   float64
}

type traffic struct {
	truth                              [Riders][Duration + 1][2]float64 // true lat/lng per rider per second
	fixes                              []fix                            // sorted by arrival
	sent, reordered, redelivered, lost int
}

func signalLoss(rider int) bool { return rider%10 == 0 }

func toLatLng(x, y float64) (float64, float64) {
	lat := baseLat + y/111_320
	lng := baseLng + x/(111_320*math.Cos(baseLat*math.Pi/180))
	return lat, lng
}

func ms(rng *rand.Rand, lo, hi int) time.Duration {
	return time.Duration(lo+rng.IntN(hi-lo+1)) * time.Millisecond
}

func newTraffic() *traffic {
	rng := rand.New(rand.NewPCG(seed, 7))
	tr := &traffic{}
	for r := range Riders {
		x, y := rng.Float64()*4000-2000, rng.Float64()*4000-2000
		heading := rng.Float64() * 2 * math.Pi
		speed := 0.0
		for s := 0; s <= Duration; s++ {
			if s%20 == 0 { // a new stretch of road every 20s
				heading += rng.NormFloat64() * 0.6
				if rng.Float64() < 0.15 {
					speed = 0 // waiting at a signal or the restaurant
				} else {
					speed = 4 + rng.Float64()*5 // 14-32 km/h
				}
			}
			if s > 0 {
				x += speed * math.Cos(heading)
				y += speed * math.Sin(heading)
			}
			lat, lng := toLatLng(x, y)
			tr.truth[r][s] = [2]float64{lat, lng}
		}
	}
	for r := range Riders {
		seq := 0
		for s := r % FixEvery; s < Duration; s += FixEvery {
			if signalLoss(r) && s >= SignalLossFrom && s < SignalLossTo {
				tr.lost++
				continue
			}
			// GPS noise of a few metres around the true position.
			lat := tr.truth[r][s][0] + rng.NormFloat64()*4/111_320
			lng := tr.truth[r][s][1] + rng.NormFloat64()*4/111_320
			at := time.Duration(s) * time.Second
			var d time.Duration
			switch u := rng.Float64(); {
			case u < RedeliverRate:
				d = ms(rng, 35_000, 90_000)
				tr.redelivered++
			case u < RedeliverRate+ReorderRate:
				d = ms(rng, 4_000, 12_000)
				tr.reordered++
			default:
				d = ms(rng, 200, 1_500)
			}
			tr.fixes = append(tr.fixes, fix{rider: r, seq: seq, at: at, arrive: at + d, lat: lat, lng: lng})
			tr.sent++
			seq++
		}
	}
	sort.Slice(tr.fixes, func(i, j int) bool {
		a, b := tr.fixes[i], tr.fixes[j]
		if a.arrive != b.arrive {
			return a.arrive < b.arrive
		}
		if a.rider != b.rider {
			return a.rider < b.rider
		}
		return a.seq < b.seq
	})
	return tr
}

// ---- what the customer sees, and how it is scored ----

type display struct {
	ok      bool
	pos     presence.Position
	stale   bool
	shownAt time.Duration
}

type scorer struct {
	r      *Report
	errs   []float64
	ages   []time.Duration
	shown  [Riders]display
	newest [Riders]time.Time // newest fix ever displayed to this customer
}

func (sc *scorer) show(c int, p presence.Position, stale bool) {
	d := &sc.shown[c]
	if d.ok && p.At.Before(sc.newest[c]) {
		sc.r.BackwardJumps++
	}
	if !d.ok || p.At.After(sc.newest[c]) {
		sc.newest[c] = p.At
	}
	d.ok, d.pos, d.stale = true, p, stale
}

func (sc *scorer) sample(tr *traffic, sec int) {
	if sec < Warmup {
		return
	}
	now := start.Add(time.Duration(sec) * time.Second)
	for c := range Riders {
		d := sc.shown[c]
		if !d.ok {
			continue
		}
		sc.r.CustomerSeconds++
		t := tr.truth[c][sec]
		e := geospatialindex.Distance(d.pos.Lat, d.pos.Lng, t[0], t[1])
		sc.errs = append(sc.errs, e)
		age := now.Sub(d.pos.At)
		sc.ages = append(sc.ages, age)
		if d.stale {
			sc.r.FlaggedSeconds++
			continue
		}
		sc.r.MaxUnflaggedAge = max(sc.r.MaxUnflaggedAge, age)
		sc.r.ErrMaxUnflagged = max(sc.r.ErrMaxUnflagged, e)
		if age > MaxShownAge {
			sc.r.StaleUnflaggedSeconds++
		}
	}
}

func pct[T float64 | time.Duration](xs []T, p float64) T {
	if len(xs) == 0 {
		return 0
	}
	s := append([]T(nil), xs...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[min(len(s)-1, int(math.Ceil(p*float64(len(s))))-1)]
}

func (sc *scorer) finish(tr *traffic) {
	r := sc.r
	r.ErrP50, r.ErrP95, r.ErrMax = pct(sc.errs, 0.50), pct(sc.errs, 0.95), pct(sc.errs, 1)
	r.AgeP50, r.AgeP95, r.AgeMax = pct(sc.ages, 0.50), pct(sc.ages, 0.95), pct(sc.ages, 1)
	r.FixesSent, r.ReorderedFixes, r.RedeliveredFixes, r.LostToSignal = tr.sent, tr.reordered, tr.redelivered, tr.lost
}

var start = time.Unix(1_800_000_000, 0)

func riderID(r int) string    { return fmt.Sprintf("rider-%d", r) }
func customerID(c int) string { return fmt.Sprintf("cust-%d", c) }

// ---- Broken ----

// Broken: the store applies fixes in arrival order (last write by arrival
// wins), the location API caches each rider for 60s, and customers poll it
// every 30s. No timestamps reach the UI, so nothing is ever flagged stale.
func Broken() Report {
	r := Report{Name: "BROKEN: 30s polling, 60s cache, store applies fixes in arrival order, no staleness"}
	tr := newTraffic()
	sc := &scorer{r: &r}
	var store [Riders]*presence.Position
	type cached struct {
		p  presence.Position
		at time.Duration
		ok bool
	}
	var cache [Riders]cached
	next := 0
	for sec := 1; sec <= Duration; sec++ {
		now := time.Duration(sec) * time.Second
		for ; next < len(tr.fixes) && tr.fixes[next].arrive <= now; next++ {
			f := tr.fixes[next]
			if old := store[f.rider]; old != nil && old.At.After(start.Add(f.at)) {
				r.OverwrittenByOlder++
			}
			store[f.rider] = &presence.Position{Lat: f.lat, Lng: f.lng, At: start.Add(f.at)}
		}
		for c := range Riders {
			if (sec+c)%PollEvery != 0 {
				continue
			}
			r.PollRequests++
			if e := cache[c]; e.ok && now-e.at < CacheTTL {
				r.CacheHits++
				sc.show(c, e.p, false)
				continue
			}
			if p := store[c]; p != nil {
				cache[c] = cached{p: *p, at: now, ok: true}
				sc.show(c, *p, false)
			}
		}
		if sec < Duration {
			sc.sample(tr, sec)
		}
	}
	sc.finish(tr)
	return r
}

// ---- Fixed ----

type message struct {
	pos   presence.Position
	stale bool
}

type socket struct {
	node string
	q    *backpressure.SendQueue[message]
}

type fixedSim struct {
	r       *Report
	sc      *scorer
	now     time.Time
	tracker *presence.Tracker
	reg     *websockethub.Registry
	// sockets[node][customer] is a live customer WebSocket on that node.
	sockets   map[string]map[int]*socket
	down      map[string]bool
	restarted map[int]bool
	queues    []*backpressure.SendQueue[message] // every queue ever opened, for drop stats
	accepted  int                                // fixes the tracker accepted
	fixPushes int                                // pushes caused by an accepted fix
}

func (s *fixedSim) connect(c int, node string) {
	q, err := backpressure.NewSendQueue[message](SendQueueSize, backpressure.DropOldest)
	if err != nil {
		panic(err)
	}
	s.sockets[node][c] = &socket{node: node, q: q}
	s.queues = append(s.queues, q)
	s.reg.Add(customerID(c), node)
	// On (re)connect the client gets the current position as a snapshot.
	p, err := s.tracker.LatestPosition(riderID(c), MaxShownAge)
	if err == nil || errors.Is(err, presence.ErrStalePosition) {
		s.push(c, message{pos: p, stale: err != nil})
	}
}

// push routes a message to every node the registry lists for the customer.
func (s *fixedSim) push(c int, m message) {
	for _, node := range s.reg.Nodes(customerID(c)) {
		if s.down[node] {
			s.r.LostToDeadNode++
			continue
		}
		sock := s.sockets[node][c]
		if sock == nil {
			continue
		}
		if err := sock.q.Push(m); err == nil {
			s.r.Pushes++
			if s.restarted[c] {
				s.r.RoutedToNewNode++
			}
		}
	}
}

// pushLatest sends the rider's latest position, flagged if it is too old.
func (s *fixedSim) pushLatest(rider int, marker bool) {
	p, err := s.tracker.LatestPosition(riderID(rider), MaxShownAge)
	if err != nil && !errors.Is(err, presence.ErrStalePosition) {
		return
	}
	if marker {
		s.r.StaleMarkers++
	}
	s.push(rider, message{pos: p, stale: marker || err != nil})
}

func (s *fixedSim) ingest(f fix) {
	rid := riderID(f.rider)
	p := presence.Position{Lat: f.lat, Lng: f.lng, At: start.Add(f.at)}
	err := s.tracker.UpdatePosition(rid, "phone", p)
	if errors.Is(err, presence.ErrUnknownConnection) && s.now.Sub(p.At) <= RiderLeaseTTL {
		// The rider's lease expired (signal loss); a fresh fix means the
		// phone is back, so it reconnects. Old, redelivered fixes don't.
		if err = s.tracker.Connect(presence.Connection{User: rid, ID: "phone", Node: "ingest"}); err == nil {
			err = s.tracker.UpdatePosition(rid, "phone", p)
		}
	}
	switch {
	case err == nil:
		s.accepted++
		before := s.r.Pushes
		s.pushLatest(f.rider, false)
		s.fixPushes += s.r.Pushes - before
	case errors.Is(err, presence.ErrStalePosition):
		s.r.RejectedOlder++
	}
}

// Fixed: device-timestamped fixes, presence keeps only newer ones and leases
// riders; customers get pushes over WebSockets routed through the registry,
// with a one-slot drop-oldest queue per connection.
func Fixed() Report {
	r := Report{Name: "FIXED: push over WebSocket, newest-fix-wins presence store, leases flag silent riders, drop-oldest queue"}
	tr := newTraffic()
	s := &fixedSim{
		r: &r, sc: &scorer{r: &r}, now: start,
		reg: websockethub.NewRegistry(), sockets: map[string]map[int]*socket{},
		down: map[string]bool{}, restarted: map[int]bool{},
	}
	tracker, err := presence.New(
		presence.WithClock(func() time.Time { return s.now }),
		presence.WithTTL(RiderLeaseTTL), presence.WithOfflineGrace(0), presence.WithAwayAfter(0))
	if err != nil {
		panic(err)
	}
	s.tracker = tracker
	riders := make([]string, Riders)
	for i := range Riders {
		riders[i] = riderID(i)
		if err := tracker.Connect(presence.Connection{User: riders[i], ID: "phone", Node: "ingest"}); err != nil {
			panic(err)
		}
	}
	watcher, err := tracker.Watch(riders...)
	if err != nil {
		panic(err)
	}
	watcher.Drain() // the initial "online" events
	for g := range Gateways {
		s.sockets[fmt.Sprintf("gw-%d", g)] = map[int]*socket{}
	}
	for c := range Riders {
		s.connect(c, fmt.Sprintf("gw-%d", c%Gateways))
	}
	var crashed []int
	for c := range Riders {
		if fmt.Sprintf("gw-%d", c%Gateways) == CrashedGateway {
			crashed = append(crashed, c)
		}
	}

	next := 0
	for sec := 1; sec <= Duration; sec++ {
		now := time.Duration(sec) * time.Second
		for ; next < len(tr.fixes) && tr.fixes[next].arrive <= now; next++ {
			f := tr.fixes[next]
			s.now = start.Add(f.arrive)
			s.ingest(f)
		}
		s.now = start.Add(now)

		// The gateway crash: sockets vanish at once; the registry keeps
		// pointing at the dead node until the cluster notices.
		switch sec {
		case CrashAt:
			s.down[CrashedGateway] = true
			for _, sock := range s.sockets[CrashedGateway] {
				r.Undelivered += sock.q.Len()
			}
			s.sockets[CrashedGateway] = map[int]*socket{}
		case CrashAt + NodeDeadAfter:
			for _, c := range crashed {
				s.reg.Remove(customerID(c), CrashedGateway)
			}
		}
		for i, c := range crashed {
			if sec == CrashAt+3+i%5 { // clients reconnect 3-7s later, to another node
				r.ReconnectedCustomer++
				s.restarted[c] = true
				s.connect(c, fmt.Sprintf("gw-%d", (c+1)%Gateways))
			}
		}

		// Leases: a rider who has sent nothing for the TTL goes offline,
		// and their customer is told the dot is stale.
		tracker.Sweep()
		for _, ev := range watcher.Drain() {
			if ev.State == presence.Offline {
				var rider int
				fmt.Sscanf(ev.User, "rider-%d", &rider)
				s.pushLatest(rider, true)
			}
		}

		// Each socket's writer sends what is queued; slow links only every few seconds.
		for node, socks := range s.sockets {
			if s.down[node] {
				continue
			}
			for c, sock := range socks {
				if c%SlowLinkEvery == 2 && sec%SlowLinkEvery != 0 {
					continue
				}
				for {
					m, ok := sock.q.TryPop()
					if !ok {
						break
					}
					r.Delivered++
					s.sc.show(c, m.pos, m.stale)
				}
			}
		}
		if sec < Duration {
			s.sc.sample(tr, sec)
		}
	}
	for _, q := range s.queues {
		r.Dropped += int(q.Stats().Dropped)
	}
	for _, socks := range s.sockets {
		for _, sock := range socks {
			r.Undelivered += sock.q.Len()
		}
	}
	r.PersistentConns = Riders
	if s.accepted > 0 {
		r.FanOut = float64(s.fixPushes) / float64(s.accepted)
	}
	s.sc.finish(tr)
	return r
}
