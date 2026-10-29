// Package consumerdouble simulates the "Kafka consumer processes the same
// event twice" incident. A consumer group reads order events and, for each
// one, credits the customer's loyalty points and sends an email. Offsets are
// auto-committed every few seconds, after processing. During a rolling
// deploy each instance is killed with work processed but not yet committed,
// so the partitions it owned resume from the last commit and those events
// are credited again. Then one malformed event makes the handler fail on
// every attempt. The consumer crashes, restarts, replays its uncommitted
// events, reaches the same event, and crashes again: the partition stalls,
// lag grows, and every lap of the crash loop credits the same customers once
// more.
//
// Broken commits after processing with no deduplication and lets handler
// errors crash the consumer (so the poison event is retried forever).
// Fixed applies each event's side effect and records its ID in an inbox in
// the same transaction (the idempotent-consumer pattern), and hands a failing
// event to a retry queue with a bounded number of attempts and a dead-letter
// queue behind it, so the partition keeps moving. After the parser fix is
// deployed, the dead letter is redriven and applied exactly once.
//
// Both runs replay the same seeded events, the same deploys and the same
// poison event on one simulated clock; only the consumer's design differs.
package consumerdouble

import (
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"time"

	deadletterqueue "github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/dead-letter-queue"
	idempotentconsumer "github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/idempotent-consumer"
)

const (
	Partitions     = 6
	Instances      = 3 // consumer instances; instance i owns partitions i and i+Instances
	Customers      = 500
	MaxPerSecond   = 30 // events produced per second, drawn uniformly from [0, MaxPerSecond]
	Capacity       = 20 // events one partition can process per second
	CommitEvery    = 5  // seconds between auto-commits
	RebalancePause = 3  // seconds the whole group stops while partitions are reassigned
	TrafficEnd     = 240
	Duration       = 300

	PoisonAt    = 120 // the malformed event is produced at this second
	TransientAt = 150 // an event whose email call fails twice, then succeeds
	FixAt       = 200 // the parser fix is deployed (all instances restart)

	CrashBackoff    = 10 // seconds before a crashed instance restarts; doubles per crash
	MaxCrashBackoff = 40

	RetryAttempts = 3 // fixed: retry-queue attempts before an event is dead-lettered

	PoisonID    = "evt-poison"
	TransientID = "evt-transient"
	group       = "loyalty-credit"
)

// DeployKills are the seconds at which the rolling deploy kills instance 0,
// 1 and 2 in turn. Each kill lands after some events were processed but
// before their offsets were committed.
var DeployKills = [Instances]int{64, 79, 94}

// KilledAt is where in a message's handling a deploy kill lands: the side
// effect has committed, the offset has not.
const KilledAt = idempotentconsumer.AfterEffect

var (
	errMalformed = errors.New("loyalty: event has no currency field; points cannot be computed")
	errEmail503  = errors.New("email: 503 service unavailable")
)

// Report is what a run measured.
type Report struct {
	Name     string
	Strategy idempotentconsumer.Strategy

	Events             int   // distinct events produced
	DoubleApplied      int   // events whose side effect was applied more than once
	ExtraApplications  int   // applications beyond the first, summed over events
	PointsOverCredited int64 // loyalty points credited by the extra applications
	MaxApplications    int   // most times one event was applied
	Lost               int   // events never applied by the end

	Redeliveries int // deliveries of an event whose effect had already been applied
	// ReplaysAfterDeploy and ReplaysAfterCrash split Redeliveries by what
	// killed the instance that had processed them without committing.
	ReplaysAfterDeploy int
	ReplaysAfterCrash  int
	InboxSkipped       int // redeliveries the inbox turned into no-ops
	Crashes            int // consumer crashes caused by handler errors
	PoisonCrashes      int // of which on the poison event
	Rebalances         int
	MaxLag             int64 // peak of (log end - committed offset) summed over partitions
	FinalLag           int64
	MaxStuck           int64 // peak count of events waiting behind the poison event in its partition
	PoisonApplied      int   // second the poison event's effect finally landed (-1 if never)

	RetryQueued int // events handed to the retry queue
	DLQPeak     int // most dead letters at once
	DLQFinal    int // dead letters left at the end
	Redriven    int
	InboxRows   int // rows in the dedup table at the end (0 without an inbox)

	Lag [Duration]int64 // total committed lag at the end of each second
}

// Print writes a lag timeline and a summary.
func (r Report) Print(w io.Writer) {
	fmt.Fprintf(w, "%s\n", r.Name)
	fmt.Fprintf(w, "%d events over %ds on %d partitions; deploy kills at t=%v; poison event at t=%d; parser fix at t=%d\n\n",
		r.Events, TrafficEnd, Partitions, DeployKills, PoisonAt, FixAt)
	fmt.Fprintln(w, "  window      peak lag")
	for start := 0; start < Duration; start += 20 {
		var peak int64
		for s := start; s < start+20; s++ {
			peak = max(peak, r.Lag[s])
		}
		bar := int(peak / 25)
		if peak > 0 && bar == 0 {
			bar = 1
		}
		fmt.Fprintf(w, "  t=%3d-%3ds  %5d %s\n", start, start+19, peak, bars(min(bar, 50)))
	}
	fmt.Fprintf(w, "\n  consumer strategy:                %s\n", r.Strategy)
	fmt.Fprintf(w, "  events applied more than once:    %d (%d extra credits, %d points over-credited; worst event applied %dx)\n",
		r.DoubleApplied, r.ExtraApplications, r.PointsOverCredited, r.MaxApplications)
	fmt.Fprintf(w, "  events never applied:             %d\n", r.Lost)
	fmt.Fprintf(w, "  redeliveries of applied events:   %d (%d after deploy kills, %d after crashes; skipped by the inbox: %d)\n",
		r.Redeliveries, r.ReplaysAfterDeploy, r.ReplaysAfterCrash, r.InboxSkipped)
	fmt.Fprintf(w, "  consumer crashes / rebalances:    %d (%d on the poison event) / %d\n", r.Crashes, r.PoisonCrashes, r.Rebalances)
	fmt.Fprintf(w, "  max lag / final lag:              %d / %d\n", r.MaxLag, r.FinalLag)
	fmt.Fprintf(w, "  peak events stuck behind poison:  %d\n", r.MaxStuck)
	if r.PoisonApplied >= 0 {
		fmt.Fprintf(w, "  poison event applied at:          t=%ds (%ds after it was produced)\n", r.PoisonApplied, r.PoisonApplied-PoisonAt)
	} else {
		fmt.Fprintln(w, "  poison event applied at:          never")
	}
	fmt.Fprintf(w, "  sent to retry queue / DLQ peak / DLQ at end / redriven: %d / %d / %d / %d\n",
		r.RetryQueued, r.DLQPeak, r.DLQFinal, r.Redriven)
	fmt.Fprintf(w, "  inbox rows at end:                %d\n", r.InboxRows)
}

func bars(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = '█'
	}
	return string(b)
}

// event is one produced order event, before it is appended to a log.
type event struct {
	id     string
	key    string
	points int64
}

// traffic returns the seeded events produced in each second. Both runs get
// exactly the same slice.
func traffic() [Duration][]event {
	rng := rand.New(rand.NewPCG(42, 2026))
	var out [Duration][]event
	n := 0
	for s := range TrafficEnd {
		if s == PoisonAt {
			out[s] = append(out[s], event{PoisonID, "cust-0042", 300})
		}
		if s == TransientAt {
			out[s] = append(out[s], event{TransientID, "cust-0077", 150})
		}
		for range rng.IntN(MaxPerSecond + 1) {
			n++
			out[s] = append(out[s], event{
				id:     fmt.Sprintf("evt-%05d", n),
				key:    fmt.Sprintf("cust-%04d", rng.IntN(Customers)),
				points: int64(10 * (1 + rng.IntN(10))),
			})
		}
	}
	return out
}

type instance struct {
	parts     []int
	pos       map[int]int64 // in-memory positions; lost when the instance dies
	up        bool
	downUntil int
	backoff   int
	killedBy  string // "deploy" or "crash": why it last lost its positions
}

type sim struct {
	fixed bool
	r     *Report
	sec   int

	log   *idempotentconsumer.Log
	store *idempotentconsumer.Store
	retry *deadletterqueue.Queue // fixed only

	insts       [Instances]*instance
	pausedUntil int

	byID           map[string]idempotentconsumer.Message
	points         map[string]int64
	ids            []string
	parserFixed    bool
	transientCalls int
	forwarded      map[string]bool // fixed: events already handed to the retry queue
	poisonPart     int
	poisonOffset   int64
	poisonBlocked  bool
	cur            *instance // instance whose partition is being processed (nil for the retry worker)
}

func (s *sim) now() time.Time { return time.Unix(int64(s.sec), 0) }

// credit is the business handler: credit loyalty points (inside the store's
// transaction) and send the order email.
func (s *sim) credit(tx *idempotentconsumer.Tx, m idempotentconsumer.Message) error {
	if m.ID == PoisonID && !s.parserFixed {
		return errMalformed
	}
	if m.ID == TransientID {
		s.transientCalls++
		if s.transientCalls <= 2 {
			return errEmail503
		}
	}
	tx.Add(m.Key, m.Value)
	return nil
}

// apply runs one delivery of m through the store, with or without the inbox.
func (s *sim) apply(m idempotentconsumer.Message) error {
	before := s.store.Applications(m.ID)
	if before > 0 {
		s.r.Redeliveries++
		if s.cur != nil && s.cur.killedBy == "crash" {
			s.r.ReplaysAfterCrash++
		} else {
			s.r.ReplaysAfterDeploy++
		}
	}
	applied, err := s.store.Apply(m, s.fixed, s.credit)
	if err != nil {
		return err
	}
	if !applied {
		s.r.InboxSkipped++
	}
	if m.ID == PoisonID && applied {
		s.poisonBlocked = false
		if s.r.PoisonApplied < 0 {
			s.r.PoisonApplied = s.sec
		}
	}
	return nil
}

// commit writes an instance's in-memory positions as its committed offsets.
func (s *sim) commit(in *instance) {
	for p, off := range in.pos {
		if err := s.log.Commit(group, p, off); err != nil {
			panic(err)
		}
	}
}

// rebalance stops the group for RebalancePause seconds. Surviving instances
// commit their positions first (as Kafka's auto-commit does when partitions
// are revoked); the killed ones lose theirs, so their partitions resume from
// the last commit.
func (s *sim) rebalance(cause string, killed ...*instance) {
	s.r.Rebalances++
	dead := map[*instance]bool{}
	for _, k := range killed {
		dead[k] = true
	}
	for _, in := range s.insts {
		if dead[in] {
			in.pos = make(map[int]int64)
			in.killedBy = cause
			continue
		}
		if in.up {
			s.commit(in)
		}
	}
	s.pausedUntil = s.sec + RebalancePause
}

// processPartition handles up to Capacity events of p. It returns a handler
// error only in the broken design, where that error crashes the instance.
func (s *sim) processPartition(in *instance, p int) error {
	s.cur = in
	defer func() { s.cur = nil }()
	from, ok := in.pos[p]
	if !ok {
		from = s.log.Committed(group, p)
	}
	msgs, err := s.log.Read(p, from, Capacity)
	if err != nil {
		panic(err)
	}
	for _, m := range msgs {
		if err := s.apply(m); err != nil {
			if !s.fixed {
				if m.ID == PoisonID {
					s.poisonBlocked = true
					s.r.PoisonCrashes++
				}
				return err
			}
			// Fixed: park the event on the retry queue and move on.
			if !s.forwarded[m.ID] {
				s.forwarded[m.ID] = true
				s.r.RetryQueued++
				if err := s.retry.Send(m.ID, m.Key, m.ID); err != nil {
					panic(err)
				}
			}
		}
		in.pos[p] = m.Offset + 1
	}
	return nil
}

// crash takes an instance down after a handler error. Kubernetes restarts it
// after an exponentially growing backoff (CrashLoopBackOff).
func (s *sim) crash(in *instance) {
	s.r.Crashes++
	in.up = false
	if in.backoff == 0 {
		in.backoff = CrashBackoff
	}
	in.downUntil = s.sec + in.backoff
	in.backoff = min(2*in.backoff, MaxCrashBackoff)
	s.rebalance("crash", in)
}

func (s *sim) drainRetry() {
	for {
		got, _ := s.retry.ProcessOne(func(d deadletterqueue.Delivery) error {
			return s.apply(s.byID[d.Payload])
		})
		if !got {
			break
		}
	}
	s.r.DLQPeak = max(s.r.DLQPeak, s.retry.DLQDepth())
}

func (s *sim) run() {
	tr := traffic()
	for sec := range Duration {
		s.sec = sec
		for _, e := range tr[sec] {
			m, err := s.log.Append(idempotentconsumer.Message{ID: e.id, Key: e.key, Value: e.points})
			if err != nil {
				panic(err)
			}
			s.byID[m.ID] = m
			s.points[m.ID] = m.Value
			s.ids = append(s.ids, m.ID)
			if m.ID == PoisonID {
				s.poisonPart, s.poisonOffset = m.Partition, m.Offset
			}
		}

		// Deploys and restarts.
		for i, at := range DeployKills {
			if sec == at {
				s.insts[i].up = true
				s.rebalance("deploy", s.insts[i])
			}
		}
		if sec == FixAt {
			s.parserFixed = true
			for _, in := range s.insts {
				in.up, in.backoff = true, 0
			}
			s.rebalance("deploy", s.insts[:]...)
			if s.fixed {
				s.r.Redriven += s.retry.Redrive(nil)
			}
		}
		for _, in := range s.insts {
			if !in.up && sec >= in.downUntil {
				in.up = true
				s.rebalance("") // the restarted instance rejoins the group
			}
		}

		// Processing.
		if sec >= s.pausedUntil {
		instances:
			for _, in := range s.insts {
				if !in.up {
					continue
				}
				for _, p := range in.parts {
					if err := s.processPartition(in, p); err != nil {
						s.crash(in)
						break instances // the rebalance stops the whole group
					}
				}
			}
		}
		if s.fixed {
			s.drainRetry()
		}
		if sec%CommitEvery == 0 && sec >= s.pausedUntil {
			for _, in := range s.insts {
				if in.up {
					s.commit(in)
				}
			}
		}

		// Metrics.
		var lag int64
		for p := range Partitions {
			lag += s.log.End(p) - s.log.Committed(group, p)
		}
		s.r.Lag[sec] = lag
		s.r.MaxLag = max(s.r.MaxLag, lag)
		if s.poisonBlocked {
			s.r.MaxStuck = max(s.r.MaxStuck, s.log.End(s.poisonPart)-s.poisonOffset-1)
		}
	}
	s.r.FinalLag = s.r.Lag[Duration-1]

	a := s.store.Audit(s.ids)
	s.r.Events, s.r.DoubleApplied, s.r.Lost = a.Expected, a.Duplicated, a.Lost
	s.r.ExtraApplications = a.Applied - (a.Expected - a.Lost)
	for _, id := range s.ids {
		n := s.store.Applications(id)
		s.r.MaxApplications = max(s.r.MaxApplications, n)
		if n > 1 {
			s.r.PointsOverCredited += int64(n-1) * s.points[id]
		}
	}
	s.r.InboxRows = s.store.InboxSize()
	if s.fixed {
		s.r.DLQFinal = s.retry.DLQDepth()
	}
}

func newSim(fixed bool, r *Report) *sim {
	s := &sim{
		fixed: fixed, r: r,
		byID: map[string]idempotentconsumer.Message{}, points: map[string]int64{},
		forwarded: map[string]bool{},
	}
	var err error
	if s.log, err = idempotentconsumer.NewLog(Partitions); err != nil {
		panic(err)
	}
	// Inbox rows are kept for a week: far longer than any replay here.
	if s.store, err = idempotentconsumer.NewStore(idempotentconsumer.WithClock(s.now),
		idempotentconsumer.WithRetention(7*24*time.Hour)); err != nil {
		panic(err)
	}
	if fixed {
		if s.retry, err = deadletterqueue.New(deadletterqueue.WithClock(s.now),
			deadletterqueue.WithMaxAttempts(RetryAttempts),
			deadletterqueue.WithBackoff(2*time.Second, 8*time.Second)); err != nil {
			panic(err)
		}
	}
	for i := range s.insts {
		s.insts[i] = &instance{parts: []int{i, i + Instances}, pos: map[int]int64{}, up: true}
	}
	r.PoisonApplied = -1
	return s
}

// Broken: offsets are auto-committed after processing, nothing deduplicates,
// and a handler error crashes the consumer, which retries the poison event
// forever (one crash-loop lap at a time).
func Broken() Report {
	r := Report{Name: "BROKEN: commit after processing, no dedup, a failing event crashes the consumer",
		Strategy: idempotentconsumer.CommitAfter}
	newSim(false, &r).run()
	return r
}

// Fixed: the side effect and the inbox row commit in one transaction, so a
// replayed event is a no-op; a failing event goes to a retry queue with
// RetryAttempts attempts and then to a dead-letter queue, and the partition
// keeps moving. The dead letter is redriven once the parser fix is out.
func Fixed() Report {
	r := Report{Name: "FIXED: inbox dedup in the same transaction + retry queue with a DLQ, redrive after the fix",
		Strategy: idempotentconsumer.Inbox}
	newSim(true, &r).run()
	return r
}
