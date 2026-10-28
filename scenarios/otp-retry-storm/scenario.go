// Package otpretrystorm simulates the "OTP service fails during peak traffic"
// incident. At a traffic peak (a sale, exam results) the primary SMS provider
// degrades: its submit API gets slow and fails 60–90% of calls. Every layer
// that talks to it retries on its own: each API server tries the provider 3
// times per request, the mobile app retries a failed request 3 times, and
// users tap "resend" when no code arrives. The retries multiply, the provider
// is pushed from degraded to saturated, the SMS that do get through arrive
// late and in bunches carrying codes that a newer resend has already
// replaced, and users who type those stale codes get locked out. A few
// numbers spamming "resend" add load nobody asked for.
//
// Broken has per-request retries at both layers, no per-number limit, and a
// single provider. Fixed sends each tap once through a failover chain
// (primary SMS, secondary SMS, voice) whose per-provider circuit breaker stops
// calling the primary once it keeps failing, spends a shared retry budget of
// 10% on retrying the cheap primary for transient errors, and caps sends per
// phone number with a sliding window, answering refused taps with "wait N
// seconds". Both runs get identical seeded users and the same provider outage
// on simulated time.
package otpretrystorm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/fallback"
	retrybudget "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/retry-budget"
	swrl "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/sliding-window-rate-limiter"
)

const (
	LoginsPerSecond = 10  // legitimate users starting a login, every second until ArrivalsUntil
	ArrivalsUntil   = 720 // seconds of peak traffic
	OutageFrom      = 120
	OutageTo        = 420 // the primary provider is degraded in [OutageFrom, OutageTo)
	Horizon         = 1200

	Abusers    = 20 // numbers hammering "resend" all through the peak
	AbuseEvery = 4  // seconds between one abuser's taps

	MaxTaps     = 5   // a user taps "send code" at most this many times
	GiveUpAfter = 240 // seconds after starting, a user still without a code leaves
	WrongCodes  = 3   // stale codes typed before the account is locked
	SLO         = 30  // seconds: a user should have a working code within this

	ServerTries = 3 // broken: provider attempts per API request
	AppTries    = 3 // broken: API requests per tap

	ResendLimit    = 3 // fixed: sends per number per ResendWindow
	ResendWindow   = 10 * time.Minute
	RetryRatio     = 0.1 // fixed: retry budget
	PrimaryTimeout = 4   // fixed: seconds before a primary submit counts as failed
	LoadCap        = 1.2 // invariant: provider calls per legitimate login, per phase

	// Prices per message in paise (₹1 = 100 paise).
	PrimaryPrice   = 12
	SecondaryPrice = 20
	VoicePrice     = 50
)

// Provider indexes.
const (
	Primary = iota
	Secondary
	Voice
	nProviders
)

var (
	providerNames = [nProviders]string{"sms-primary", "sms-secondary", "voice"}
	prices        = [nProviders]int{PrimaryPrice, SecondaryPrice, VoicePrice}

	errProvider = errors.New("provider: 503 / submit failed")
	errTimeout  = errors.New("provider: submit timed out")
)

// PhaseNames label the three phases of the peak.
var PhaseNames = [3]string{"before outage", "outage", "after outage"}

func phase(sec int) int {
	switch {
	case sec < OutageFrom:
		return 0
	case sec < OutageTo:
		return 1
	}
	return 2
}

// Phase is the load seen in one phase.
type Phase struct {
	Logins int             // legitimate logins started in the phase
	Calls  [nProviders]int // provider calls made in the phase (calls after ArrivalsUntil count as "after")
}

// PrimaryMultiplier is primary calls per legitimate login.
func (p Phase) PrimaryMultiplier() float64 { return float64(p.Calls[Primary]) / float64(p.Logins) }

// Multiplier is calls to all providers per legitimate login.
func (p Phase) Multiplier() float64 {
	n := 0
	for _, c := range p.Calls {
		n += c
	}
	return float64(n) / float64(p.Logins)
}

// Report is what a run measured.
type Report struct {
	Name   string
	Phases [3]Phase

	Users     int   // legitimate users
	OnTime    int   // logged in within SLO seconds
	Late      int   // logged in, but later than SLO
	LockedOut int   // typed WrongCodes stale codes
	GaveUp    int   // no working code within GiveUpAfter
	Delays    []int // seconds from first tap to login, for users who logged in

	Sent       [nProviders]int // messages accepted (and billed) per provider
	Cost       int             // paise
	StaleCodes int             // codes delivered after a newer one had replaced them
	Wasted     int             // messages delivered to users who had already logged in, left or been locked
	AbuseSent  int             // messages sent to abusing numbers

	ResendsBlocked int    // taps refused by the per-number limit
	LegitBlocked   int    // of those, taps by legitimate users
	WaitExample    string // what a refused tap was told
	MaxPerNumber   int    // most messages sent to one number in any ResendWindow

	PrimarySkipped int    // fixed: requests that skipped the primary while its breaker was open
	BudgetRetries  uint64 // fixed: primary retries the retry budget allowed
	BudgetDenied   uint64 // fixed: primary retries the retry budget refused
	BudgetRequests uint64 // fixed: taps recorded with the budget
	ChainFailures  int    // fixed: taps where every provider failed
}

// OnTimeRate is the share of legitimate users who logged in within the SLO.
func (r Report) OnTimeRate() float64 { return float64(r.OnTime) / float64(r.Users) }

// P95Delay is the 95th-percentile seconds to log in, among users who did.
func (r Report) P95Delay() int {
	if len(r.Delays) == 0 {
		return 0
	}
	d := slices.Clone(r.Delays)
	slices.Sort(d)
	return d[int(math.Ceil(0.95*float64(len(d))))-1]
}

// TotalSent sums Sent.
func (r Report) TotalSent() int { return r.Sent[Primary] + r.Sent[Secondary] + r.Sent[Voice] }

// CostPerLogin is paise spent per legitimate user who logged in.
func (r Report) CostPerLogin() float64 { return float64(r.Cost) / float64(r.OnTime+r.Late) }

// Print writes per-phase load and a summary.
func (r Report) Print(w io.Writer) {
	fmt.Fprintf(w, "%s\n", r.Name)
	fmt.Fprintf(w, "%d logins/s for %ds, %d abusing numbers tapping every %ds; primary SMS provider degraded t=%d-%ds\n\n",
		LoginsPerSecond, ArrivalsUntil, Abusers, AbuseEvery, OutageFrom, OutageTo-1)
	fmt.Fprintln(w, "  phase            logins   primary calls (× logins)   all provider calls (× logins)")
	for i, p := range r.Phases {
		all := p.Calls[Primary] + p.Calls[Secondary] + p.Calls[Voice]
		fmt.Fprintf(w, "  %-15s  %6d   %13d  (%5.2f×)   %18d  (%5.2f×)\n",
			PhaseNames[i], p.Logins, p.Calls[Primary], p.PrimaryMultiplier(), all, p.Multiplier())
	}
	fmt.Fprintf(w, "\n  legitimate users: %d\n", r.Users)
	fmt.Fprintf(w, "    logged in within %ds:  %6d (%.2f%%)\n", SLO, r.OnTime, 100*r.OnTimeRate())
	fmt.Fprintf(w, "    logged in late:         %6d\n", r.Late)
	fmt.Fprintf(w, "    locked out (stale codes): %4d\n", r.LockedOut)
	fmt.Fprintf(w, "    gave up:                %6d\n", r.GaveUp)
	fmt.Fprintf(w, "    p95 time to a working code: %ds\n", r.P95Delay())
	fmt.Fprintf(w, "\n  messages sent: %d (primary %d, secondary %d, voice %d), cost ₹%.2f\n",
		r.TotalSent(), r.Sent[Primary], r.Sent[Secondary], r.Sent[Voice], float64(r.Cost)/100)
	fmt.Fprintf(w, "    cost per user who logged in: ₹%.3f\n", r.CostPerLogin()/100)
	fmt.Fprintf(w, "    stale codes delivered: %d; wasted on finished users: %d; sent to abusers: %d\n",
		r.StaleCodes, r.Wasted, r.AbuseSent)
	fmt.Fprintf(w, "    most messages to one number in any %s: %d (limit %d)\n", ResendWindow, r.MaxPerNumber, ResendLimit)
	if r.ResendsBlocked > 0 {
		fmt.Fprintf(w, "  resend taps refused: %d (%d from legitimate users), e.g. %s\n",
			r.ResendsBlocked, r.LegitBlocked, r.WaitExample)
	}
	if r.BudgetRequests > 0 {
		fmt.Fprintf(w, "  requests that skipped the primary (breaker open): %d; taps where every provider failed: %d\n",
			r.PrimarySkipped, r.ChainFailures)
		fmt.Fprintf(w, "  retry budget: %d primary retries allowed for %d taps (%.1f%%), %d refused\n",
			r.BudgetRetries, r.BudgetRequests, 100*float64(r.BudgetRetries)/float64(r.BudgetRequests), r.BudgetDenied)
	}
}

// User states.
const (
	active = iota
	done
	locked
	gaveUp
)

type session struct {
	number   int
	abuser   bool
	start    int
	patience [MaxTaps]int // seconds the user waits after each tap before tapping again
	taps     int
	code     int // the currently valid code's version; each new code replaces the last
	wrong    int
	state    int
}

// traffic builds the same seeded users and abusers for every run.
func traffic() []*session {
	rng := rand.New(rand.NewPCG(7, 11))
	var out []*session
	for i := range Abusers {
		out = append(out, &session{number: 1_000_000 + i, abuser: true, start: i % AbuseEvery})
	}
	n := 0
	for sec := range ArrivalsUntil {
		for range LoginsPerSecond {
			u := &session{number: n, start: sec}
			for k := range u.patience {
				u.patience[k] = 20 + rng.IntN(21) // 20–40s
			}
			out = append(out, u)
			n++
		}
	}
	return out
}

// sim is a discrete-event simulation with one-second ticks. It runs on one
// goroutine; the pattern types it drives are concurrency-safe anyway.
type sim struct {
	now    int
	queue  [Horizon + 1][]func()
	rng    *rand.Rand // provider behaviour
	r      *Report
	sent   map[int][]int // number -> seconds at which a message to it was accepted
	perSec [nProviders]int
	secOf  int
}

func newSim(r *Report) *sim {
	return &sim{rng: rand.New(rand.NewPCG(3, 5)), r: r, sent: map[int][]int{}}
}

func (s *sim) clock() time.Time { return time.Unix(int64(s.now), 0) }

func (s *sim) at(t int, fn func()) {
	if t <= Horizon {
		s.queue[t] = append(s.queue[t], fn)
	}
}

// outcome of one provider call: the submit took submit seconds; if ok, the
// message reaches the handset deliver seconds after that.
type outcome struct {
	ok              bool
	submit, deliver int
}

// call models one submit to provider p at the current second. Past its
// per-second capacity a provider rejects at once. The degraded primary fails
// 60% of calls when lightly loaded, rising to 90% at capacity; its submits
// take 3–8s and its accepted messages sit in a backlog for 15–45s.
func (s *sim) call(p int) outcome {
	if s.secOf != s.now {
		s.secOf, s.perSec = s.now, [nProviders]int{}
	}
	s.perSec[p]++
	n := s.perSec[p]
	s.r.Phases[phase(s.now)].Calls[p]++
	switch p {
	case Primary:
		if s.now >= OutageFrom && s.now < OutageTo {
			const capacity = 20
			if n > capacity {
				return outcome{submit: 1}
			}
			pErr := 0.6 + 0.3*float64(n)/capacity
			submit := 3 + s.rng.IntN(6)
			if s.rng.Float64() < pErr {
				return outcome{submit: submit}
			}
			return outcome{ok: true, submit: submit, deliver: 15 + s.rng.IntN(31)}
		}
		if n > 50 || s.rng.Float64() < 0.01 {
			return outcome{submit: 1}
		}
		return outcome{ok: true, submit: 1, deliver: 2 + s.rng.IntN(4)}
	case Secondary:
		if n > 40 || s.rng.Float64() < 0.03 {
			return outcome{submit: 1}
		}
		return outcome{ok: true, submit: 1, deliver: 2 + s.rng.IntN(5)}
	default: // voice
		if n > 20 || s.rng.Float64() < 0.01 {
			return outcome{submit: 2}
		}
		return outcome{ok: true, submit: 2, deliver: 5 + s.rng.IntN(6)}
	}
}

// accepted bills a message to u carrying code version code, and schedules its
// arrival after delay seconds.
func (s *sim) accepted(u *session, p, code, delay int) {
	s.r.Sent[p]++
	s.r.Cost += prices[p]
	s.sent[u.number] = append(s.sent[u.number], s.now)
	if u.abuser {
		s.r.AbuseSent++
		return
	}
	s.at(s.now+delay, func() { s.arrive(u, code) })
}

// arrive is a code reaching the user, who types it in.
func (s *sim) arrive(u *session, code int) {
	if u.state != active {
		s.r.Wasted++
		return
	}
	if code != u.code {
		s.r.StaleCodes++
		if u.wrong++; u.wrong >= WrongCodes {
			u.state = locked
			s.r.LockedOut++
		}
		return
	}
	u.state = done
	d := s.now - u.start
	s.r.Delays = append(s.r.Delays, d)
	if d <= SLO {
		s.r.OnTime++
	} else {
		s.r.Late++
	}
}

// run drives every user and abuser through tap, which sends a code and
// returns 0, or refuses and returns how many seconds the user must wait.
func (s *sim) run(tap func(u *session) int) {
	for _, u := range traffic() {
		if u.abuser {
			var spam func()
			spam = func() {
				tap(u)
				if s.now+AbuseEvery < ArrivalsUntil {
					s.at(s.now+AbuseEvery, spam)
				}
			}
			s.at(u.start, spam)
			continue
		}
		s.r.Users++
		s.r.Phases[phase(u.start)].Logins++
		var press func()
		press = func() {
			if u.state != active || u.taps >= MaxTaps {
				return
			}
			if wait := tap(u); wait > 0 {
				s.at(s.now+wait, press) // the app shows a countdown, then the user taps
				return
			}
			s.at(s.now+u.patience[u.taps], press)
			u.taps++
		}
		s.at(u.start, press)
		s.at(u.start+GiveUpAfter, func() {
			if u.state == active {
				u.state = gaveUp
				s.r.GaveUp++
			}
		})
	}
	for t := 0; t <= Horizon; t++ {
		s.now = t
		for i := 0; i < len(s.queue[t]); i++ { // events may add more for this second
			s.queue[t][i]()
		}
		s.queue[t] = nil
	}
	for _, times := range s.sent {
		lo := 0
		for hi, t := range times {
			for t-times[lo] >= int(ResendWindow/time.Second) {
				lo++
			}
			s.r.MaxPerNumber = max(s.r.MaxPerNumber, hi-lo+1)
		}
	}
}

// Broken: every tap is an API request; the app retries a failed request up
// to AppTries times (each retry is a new request, so a new code), and the
// API server tries the single provider up to ServerTries times per request,
// with no backoff. Nothing limits how often one number can ask.
func Broken() Report {
	r := Report{Name: "BROKEN: 3 server retries × 3 app retries per tap, single SMS provider, no per-number limit"}
	s := newSim(&r)

	var request func(u *session, appTry int)
	var attempt func(u *session, code, appTry, srvTry int)
	attempt = func(u *session, code, appTry, srvTry int) {
		o := s.call(Primary)
		if o.ok {
			s.accepted(u, Primary, code, o.submit+o.deliver)
			return
		}
		next := s.now + o.submit
		switch {
		case srvTry < ServerTries:
			s.at(next, func() { attempt(u, code, appTry, srvTry+1) })
		case appTry < AppTries:
			s.at(next+1, func() { request(u, appTry+1) })
		}
	}
	request = func(u *session, appTry int) {
		if !u.abuser && u.state != active {
			return // the user has left the screen; the app stops retrying
		}
		u.code++
		attempt(u, u.code, appTry, 1)
	}
	s.run(func(u *session) int { request(u, 1); return 0 })
	return r
}

// otpSend is one tap's trip through the failover chain.
type otpSend struct {
	u       *session
	code    int
	elapsed int // simulated seconds spent so far on submits
}

// Fixed: one send per tap, no app retries. A per-number sliding-window limit
// refuses taps beyond ResendLimit per ResendWindow with "wait N seconds".
// Accepted taps go through a failover chain (primary SMS, secondary SMS,
// voice) whose per-provider circuit breaker skips the primary after 3
// consecutive failures for 15s. The primary is retried once for transient
// errors only while a shared 10% retry budget has tokens.
func Fixed() Report {
	r := Report{Name: "FIXED: retry budget + circuit breaker + failover chain + per-number resend limit"}
	s := newSim(&r)
	ctx := context.Background()

	budget, err := retrybudget.New(RetryRatio, retrybudget.WithClock(s.clock))
	if err != nil {
		panic(err)
	}
	via := func(p int) func(context.Context, *otpSend) (struct{}, error) {
		return func(_ context.Context, req *otpSend) (struct{}, error) {
			for try := 0; ; try++ {
				o := s.call(p)
				var err error
				if p == Primary && o.submit > PrimaryTimeout {
					// The submit is abandoned at the timeout (simplification:
					// an abandoned submit is never sent).
					req.elapsed += PrimaryTimeout
					o.ok = false
					err = errTimeout
				} else {
					req.elapsed += o.submit
					err = errProvider
				}
				if o.ok {
					s.accepted(req.u, p, req.code, req.elapsed+o.deliver)
					return struct{}{}, nil
				}
				if p != Primary || try > 0 || !budget.Allow() {
					return struct{}{}, err
				}
			}
		}
	}
	providers := make([]fallback.Provider[*otpSend, struct{}], nProviders)
	for p := range providers {
		// Timeouts are enforced on simulated time inside the call, so the
		// chain's real-time per-attempt timeout is left at 0.
		providers[p] = fallback.Provider[*otpSend, struct{}]{Name: providerNames[p], Call: via(p)}
	}
	chain, err := fallback.NewChain(providers, fallback.ChainOptions{
		FailureThreshold: 3, Cooldown: 15 * time.Second, Now: s.clock,
	})
	if err != nil {
		panic(err)
	}

	limits := map[int]*swrl.Log{}
	tap := func(u *session) int {
		lim := limits[u.number]
		if lim == nil {
			lim = swrl.NewLog(ResendLimit, ResendWindow, s.clock)
			limits[u.number] = lim
		}
		if !lim.Allow() {
			r.ResendsBlocked++
			if !u.abuser {
				r.LegitBlocked++
			}
			wait := max(1, int(math.Ceil(lim.RetryAfter().Seconds())))
			if r.WaitExample == "" {
				r.WaitExample = fmt.Sprintf("%q", fmt.Sprintf("Too many codes requested. Try again in %d seconds.", wait))
			}
			return wait
		}
		u.code++
		budget.OnRequest()
		res, err := chain.Do(ctx, &otpSend{u: u, code: u.code})
		if err != nil {
			r.ChainFailures++
		}
		for _, a := range res.Attempts {
			if a.Skipped && a.Provider == providerNames[Primary] {
				r.PrimarySkipped++
			}
		}
		return 0
	}
	s.run(tap)
	st := budget.Stats()
	r.BudgetRetries, r.BudgetDenied, r.BudgetRequests = st.Retries, st.Denied, st.Requests
	return r
}
