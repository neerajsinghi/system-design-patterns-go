// Package notificationstorm simulates the "celebrity post melts the push
// pipeline" incident. A celebrity with a huge following posts, and the
// fan-out job enqueues one "new post" push per follower onto the same queue
// that carries transactional pushes (login codes, "your order is out for
// delivery"). The push providers (APNs, FCM) only accept so many sends per
// second; the senders blast past that, get 429s, and retry straight away, so
// the provider throttles them harder. Login codes wait behind the storm until
// they have expired. Devices that uninstalled the app return a permanent
// "unregistered" error that the senders treat like any other failure and
// retry forever. And everyone whose comment is liked gets one push per like.
//
// Broken is a single FIFO with an unthrottled worker pool that retries every
// error immediately. Fixed routes pushes through a fanoutfeed.Dispatcher
// (priority lanes with a starvation limit, a token bucket per provider kept
// under the provider's limit, coalescing of likes per user), and hands each
// push to workers through a deadletterqueue.Queue so a permanent provider
// rejection parks the push in a dead-letter queue and marks the device token
// dead, instead of retrying it.
package notificationstorm

import (
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"
	"strconv"
	"time"

	fanoutfeed "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/fan-out-feed"
	deadletterqueue "github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/dead-letter-queue"
)

const (
	Followers          = 20000 // the celebrity's followers, each with one device
	InvalidTokenShare  = 0.05  // share of devices whose app was uninstalled
	Commenters         = 300   // followers whose comment under the post gets liked
	LikesPerCommenter  = 42    // likes each of those comments receives
	LikesWithin        = 4 * time.Second
	TransactionalRate  = 10 // OTP and delivery pushes per second
	TransactionalUntil = 60 * time.Second

	ProviderLimit = 400 // sends per second each provider accepts per sender
	// WorkerCapacity is how many provider calls the worker pool can make per
	// second, across both providers. It is well above the providers' limits,
	// which is what lets the broken senders overrun them.
	WorkerCapacity = 2000
	// ImmediateRetries is how many times a broken worker retries a failed
	// send in a tight loop before putting it back at the tail of the queue.
	ImmediateRetries = 2

	// The fix's settings.
	SendRate        = 0.9 * ProviderLimit // token bucket refill per provider
	SendBurst       = 4                   // so any one second carries at most SendRate+SendBurst sends
	CoalesceWindow  = 5 * time.Second
	StarvationLimit = 20

	Tick     = 10 * time.Millisecond
	Duration = 240 * time.Second // simulation cap
	seed     = 2026
)

// TransactionalBudget is the invariant's limit on transactional p99 delay.
const TransactionalBudget = time.Second

var (
	err429          = errors.New("429 too many requests")
	errUnregistered = errors.New("410 device token unregistered")
	providers       = []string{"apns", "fcm"}
)

// Report is what a run measured.
type Report struct {
	Name string

	Notifications int // notifications the app generated
	Transactional int // of which transactional

	TransactionalP50, TransactionalP99, TransactionalMax time.Duration

	ProviderCalls   int // send requests that reached a provider
	Received429     int // of which the provider throttled
	Received410     int // of which the provider rejected as unregistered (a permanent error)
	Sent            int // pushes the providers accepted (each lands on a phone)
	PeakCallsPerSec int // busiest provider-second, against ProviderLimit
	// WastedOnInvalid counts provider calls to device tokens that had
	// already been rejected as permanently invalid.
	WastedOnInvalid int
	DLQDepth        int // pushes parked after a permanent rejection
	// Suppressed counts pushes dropped without a provider call because their
	// token was already known dead (a coalesced push counts once).
	Suppressed int

	MaxPushesPerUser int    // most pushes any one phone received
	Coalesced        int    // notifications merged into another push
	BusiestSummary   string // the text of the push standing for the most notifications

	// StormDrain is when the last social push for a valid device was
	// accepted; StormUndelivered counts social notifications for valid
	// devices that never were by the end of the run.
	StormDrain       time.Duration
	StormUndelivered int
	// MinSocialDelay is the shortest wait of any delivered social push.
	MinSocialDelay time.Duration
	// LikePushes is how many like pushes each valid commenter received.
	LikePushes map[string]int
}

// MaxLikePushes is the most like pushes any one commenter received.
func (r Report) MaxLikePushes() int {
	m := 0
	for _, n := range r.LikePushes {
		m = max(m, n)
	}
	return m
}

// Print writes a summary.
func (r Report) Print(w io.Writer) {
	fmt.Fprintf(w, "%s\n", r.Name)
	fmt.Fprintf(w, "%d followers (%.0f%% stale tokens), %d commenters × %d likes, %d transactional/s; providers accept %d sends/s each\n\n",
		Followers, InvalidTokenShare*100, Commenters, LikesPerCommenter, TransactionalRate, ProviderLimit)
	fmt.Fprintf(w, "  transactional delay (%d pushes): p50 %v, p99 %v, max %v  (budget p99 ≤ %v)\n",
		r.Transactional, r.TransactionalP50, r.TransactionalP99, r.TransactionalMax, TransactionalBudget)
	fmt.Fprintf(w, "  provider calls: %d; 429s received: %d; busiest provider-second: %d calls (limit %d)\n",
		r.ProviderCalls, r.Received429, r.PeakCallsPerSec, ProviderLimit)
	fmt.Fprintf(w, "  pushes sent: %d for %d notifications (%d coalesced away)\n", r.Sent, r.Notifications, r.Coalesced)
	fmt.Fprintf(w, "  most pushes to one phone: %d; most like pushes for one comment: %d\n", r.MaxPushesPerUser, r.MaxLikePushes())
	if r.BusiestSummary != "" {
		fmt.Fprintf(w, "  busiest push reads %q\n", r.BusiestSummary)
	}
	fmt.Fprintf(w, "  410 unregistered responses: %d; calls to tokens already known invalid: %d\n", r.Received410, r.WastedOnInvalid)
	fmt.Fprintf(w, "  DLQ depth: %d; pushes dropped before sending (token known dead): %d\n", r.DLQDepth, r.Suppressed)
	if r.StormUndelivered > 0 {
		fmt.Fprintf(w, "  storm drain: not drained after %v (%d social pushes still undelivered)\n", Duration, r.StormUndelivered)
	} else {
		fmt.Fprintf(w, "  storm drain: last social push accepted at t=%v; shortest social delay %v\n", r.StormDrain, r.MinSocialDelay)
	}
}

// arrival is one notification the app produces at a point in simulated time.
type arrival struct {
	at time.Duration
	n  fanoutfeed.Notification
}

type user struct {
	provider string
	invalid  bool
}

// traffic is the seeded workload, identical for both runs.
type traffic struct {
	users    map[string]user
	arrivals []arrival
	// socialValid counts social notifications addressed to valid devices.
	socialValid int
	commenters  []string // valid ones
}

func newTraffic() *traffic {
	rng := rand.New(rand.NewPCG(seed, seed+1))
	t := &traffic{users: make(map[string]user, Followers)}
	names := make([]string, Followers)
	var valid []string
	for i := range Followers {
		name := fmt.Sprintf("u%05d", i)
		names[i] = name
		u := user{provider: providers[rng.IntN(2)], invalid: rng.Float64() < InvalidTokenShare}
		t.users[name] = u
		if !u.invalid {
			valid = append(valid, name)
		}
	}
	social := func(at time.Duration, to, actor, text, key string) {
		t.arrivals = append(t.arrivals, arrival{at, fanoutfeed.Notification{
			User: to, Provider: t.users[to].provider, Lane: fanoutfeed.Social,
			Actor: actor, Text: text, CollapseKey: key,
		}})
		if !t.users[to].invalid {
			t.socialValid++
		}
	}
	// The fan-out job enqueues every follower's "new post" push at once.
	for _, name := range names {
		social(0, name, "celebrity", "just posted a new photo", "post:celebrity-1")
	}
	// Followers' comments under the post collect likes over a few seconds.
	for _, idx := range rng.Perm(Followers)[:Commenters] {
		to := names[idx]
		if !t.users[to].invalid {
			t.commenters = append(t.commenters, to)
		}
		for range LikesPerCommenter {
			at := time.Duration(rng.Int64N(int64(LikesWithin/time.Millisecond))) * time.Millisecond
			social(at, to, names[rng.IntN(Followers)], "liked your comment", "like:"+to)
		}
	}
	// Transactional pushes go to people using the app right now, so their
	// tokens are valid.
	for i := range int(TransactionalUntil/time.Second) * TransactionalRate {
		at := time.Duration(i)*time.Second/TransactionalRate + time.Duration(rng.IntN(100))*time.Millisecond
		text := "your login code is 4821"
		if i%2 == 1 {
			text = "your order is out for delivery"
		}
		to := valid[rng.IntN(len(valid))]
		t.arrivals = append(t.arrivals, arrival{at, fanoutfeed.Notification{
			User: to, Provider: t.users[to].provider, Lane: fanoutfeed.Transactional, Actor: "shop", Text: text,
		}})
	}
	slices.SortStableFunc(t.arrivals, func(a, b arrival) int { return int(a.at - b.at) })
	return t
}

// providerSim models APNs or FCM for one sender: a per-second allowance,
// halved for the next second whenever the sender was throttled in the
// previous one. Every call counts toward the allowance, throttled ones
// included.
type providerSim struct {
	sec       int
	calls     int
	throttled bool // a 429 was returned in sec
	penalised bool // sec's allowance is halved
}

// sim holds the clock, the providers and the measurements shared by both
// designs.
type sim struct {
	now       time.Duration
	t         *traffic
	providers map[string]*providerSim
	knownDead map[string]bool // tokens a provider has already rejected as unregistered
	r         *Report

	transDelays []time.Duration
	perUser     map[string]int
	busiest     int
}

func newSim(name string, t *traffic) *sim {
	s := &sim{
		t: t, providers: map[string]*providerSim{}, knownDead: map[string]bool{},
		r:       &Report{Name: name, Notifications: len(t.arrivals), MinSocialDelay: -1, LikePushes: map[string]int{}},
		perUser: map[string]int{},
	}
	for _, p := range providers {
		s.providers[p] = &providerSim{sec: -1}
	}
	for _, a := range t.arrivals {
		if a.n.Lane == fanoutfeed.Transactional {
			s.r.Transactional++
		}
	}
	return s
}

func (s *sim) clock() time.Time { return time.Unix(0, 0).Add(s.now) }

// call sends one push to its provider at the current time.
func (s *sim) call(n fanoutfeed.Notification) error {
	p := s.providers[n.Provider]
	if sec := int(s.now / time.Second); sec != p.sec {
		p.penalised = p.throttled && sec == p.sec+1
		p.sec, p.calls, p.throttled = sec, 0, false
	}
	p.calls++
	s.r.ProviderCalls++
	s.r.PeakCallsPerSec = max(s.r.PeakCallsPerSec, p.calls)
	if s.knownDead[n.User] {
		s.r.WastedOnInvalid++
	}
	allowance := ProviderLimit
	if p.penalised {
		allowance /= 2
	}
	switch {
	case p.calls > allowance:
		p.throttled = true
		s.r.Received429++
		return err429
	case s.t.users[n.User].invalid:
		s.knownDead[n.User] = true
		s.r.Received410++
		return errUnregistered
	}
	return nil
}

// delivered records a push the provider accepted. count is how many
// notifications it stands for and enqueued when the first of them arrived.
func (s *sim) delivered(n fanoutfeed.Notification, count int, enqueued time.Duration, summary string) {
	delay := s.now - enqueued
	s.r.Sent++
	s.perUser[n.User]++
	s.r.MaxPushesPerUser = max(s.r.MaxPushesPerUser, s.perUser[n.User])
	if count > 1 && count > s.busiest {
		s.busiest, s.r.BusiestSummary = count, summary
	}
	switch n.Lane {
	case fanoutfeed.Transactional:
		s.transDelays = append(s.transDelays, delay)
	case fanoutfeed.Social:
		s.r.StormDrain = s.now
		s.t.socialValid -= count // reused as "still undelivered"
		if s.r.MinSocialDelay < 0 || delay < s.r.MinSocialDelay {
			s.r.MinSocialDelay = delay
		}
		if n.Text == "liked your comment" {
			s.r.LikePushes[n.User]++
		}
	}
}

func (s *sim) finish() Report {
	// A transactional push still queued at the cap counts as having waited
	// the whole run (an underestimate, which only flatters the design).
	for len(s.transDelays) < s.r.Transactional {
		s.transDelays = append(s.transDelays, Duration)
	}
	slices.Sort(s.transDelays)
	pct := func(p float64) time.Duration {
		if len(s.transDelays) == 0 {
			return 0
		}
		return s.transDelays[int(p*float64(len(s.transDelays)-1))]
	}
	s.r.TransactionalP50, s.r.TransactionalP99, s.r.TransactionalMax = pct(0.50), pct(0.99), pct(1)
	s.r.StormUndelivered = s.t.socialValid
	return *s.r
}

// run drives the clock in ticks: arrivals up to the end of a tick are handed
// to enqueue at their exact time, then step does the tick's sending. It
// stops early once nothing is left to do.
func (s *sim) run(enqueue func(arrival), step func(), idle func() bool) {
	next := 0
	for s.now = 0; s.now < Duration; {
		end := s.now + Tick
		for next < len(s.t.arrivals) && s.t.arrivals[next].at < end {
			s.now = max(s.now, s.t.arrivals[next].at)
			enqueue(s.t.arrivals[next])
			next++
		}
		s.now = end
		step()
		if next == len(s.t.arrivals) && idle() {
			return
		}
	}
}

// pending is a push waiting in the broken queue.
type pending struct {
	n        fanoutfeed.Notification
	enqueued time.Duration
}

// Broken: one FIFO for everything; the worker pool sends as fast as it can,
// retries any error twice on the spot, then puts the push back at the tail.
// An unregistered token is just another error, so it cycles forever.
func Broken() Report {
	t := newTraffic()
	s := newSim("BROKEN: one shared FIFO, no rate limiting, immediate retries, dead tokens retried forever", t)
	var queue []pending
	head := 0
	perTick := WorkerCapacity * int(Tick) / int(time.Second)
	step := func() {
		budget := perTick
		for budget > 0 && head < len(queue) {
			p := queue[head]
			head++
			var err error
			for try := 0; try <= ImmediateRetries && budget > 0; try++ {
				budget--
				if err = s.call(p.n); err == nil {
					break
				}
			}
			if err != nil {
				queue = append(queue, p)
				continue
			}
			s.delivered(p.n, 1, p.enqueued, p.n.Actor+" "+p.n.Text)
		}
		if head > 4096 && head*2 > len(queue) {
			queue = append([]pending(nil), queue[head:]...)
			head = 0
		}
	}
	// Never idle: once only dead tokens are left, the workers keep cycling
	// them until the cap, as the real system did.
	idle := func() bool { return false }
	s.run(func(a arrival) { queue = append(queue, pending{a.n, a.at}) }, step, idle)
	return s.finish()
}

// Fixed: a fanoutfeed.Dispatcher holds a transactional, a social and a
// marketing lane per provider and sends within a token bucket set below the
// provider's limit; likes are coalesced per user for CoalesceWindow. What the
// dispatcher releases goes to the workers through a deadletterqueue.Queue: a
// permanent rejection parks the push in the DLQ and marks the token dead, and
// later pushes to a dead token are dropped before they cost a provider call.
func Fixed() Report {
	t := newTraffic()
	s := newSim("FIXED: priority lanes + per-provider token buckets + coalescing + DLQ for dead tokens", t)
	opts := []fanoutfeed.DispatcherOption{
		fanoutfeed.WithDispatchClock(s.clock),
		fanoutfeed.WithStarvationLimit(StarvationLimit),
		fanoutfeed.WithCoalesceWindow(CoalesceWindow),
	}
	for _, p := range providers {
		opts = append(opts, fanoutfeed.WithProvider(p, SendRate, SendBurst))
	}
	d, err := fanoutfeed.NewDispatcher(opts...)
	if err != nil {
		panic(err)
	}
	q, err := deadletterqueue.New(deadletterqueue.WithClock(s.clock), deadletterqueue.WithMaxAttempts(5))
	if err != nil {
		panic(err)
	}
	dead := map[string]bool{} // the token registry's view, fed by the DLQ path
	inflight := map[string]fanoutfeed.Delivery{}
	seq := 0
	perTick := WorkerCapacity * int(Tick) / int(time.Second)

	handle := func(m deadletterqueue.Delivery) error {
		dl := inflight[m.ID]
		if dead[dl.User] {
			s.r.Suppressed++
			delete(inflight, m.ID)
			return nil // nothing to send: the device is gone
		}
		switch err := s.call(dl.Notification); {
		case errors.Is(err, errUnregistered):
			dead[dl.User] = true
			delete(inflight, m.ID)
			return deadletterqueue.Permanent(err)
		case err != nil:
			return err // transient: the DLQ queue retries it after a backoff
		}
		delete(inflight, m.ID)
		s.delivered(dl.Notification, dl.Count, dl.EnqueuedAt.Sub(time.Unix(0, 0)), dl.Summary())
		return nil
	}
	step := func() {
		for _, dl := range d.Dispatch() {
			if dead[dl.User] {
				s.r.Suppressed++
				continue
			}
			seq++
			id := strconv.Itoa(seq)
			inflight[id] = dl
			if err := q.Send(id, dl.User, ""); err != nil {
				panic(err)
			}
		}
		for range perTick {
			if ok, _ := q.ProcessOne(handle); !ok {
				break
			}
		}
	}
	enqueue := func(a arrival) {
		if dead[a.n.User] {
			s.r.Suppressed++
			return
		}
		if err := d.Enqueue(a.n); err != nil {
			panic(err)
		}
	}
	s.run(enqueue, step, func() bool { return d.Pending() == 0 && q.Stats().Depth == 0 })
	s.r.Coalesced = d.Stats().Coalesced
	s.r.DLQDepth = q.DLQDepth()
	return s.finish()
}
