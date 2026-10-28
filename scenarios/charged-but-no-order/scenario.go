// Package chargednoorder simulates the "payment deducted but order failed"
// incident. Checkout is four steps: create the order, reserve inventory,
// charge the card, confirm the order. The order service calls the payment
// service synchronously. When the payment goes through but its response is
// lost (a timeout), or the order service crashes before it records "paid",
// the customer is charged and has no order. The client sees an error and
// retries checkout, which charges the card a second time. And the "order
// confirmed" event is published from inside the database transaction, so a
// transaction that later aborts leaves an event for an order that does not
// exist, while a publish that fails is logged and forgotten, leaving an
// order that downstream services never hear about.
//
// Broken runs the steps as plain sequential calls with no compensation,
// calls a payment API that charges on every request, and dual-writes the
// order row and the event.
// Fixed runs checkout as an orchestrated saga (patterns/distributed/
// saga-orchestration) whose progress is persisted after every step, so a
// crashed saga is resumed rather than forgotten; charges the card through an
// idempotency key (patterns/reliability/idempotency-key), so retries and
// resumes get the original charge back; refunds and releases inventory when
// checkout cannot finish; and writes the event to a transactional outbox
// (patterns/distributed/transactional-outbox) in the same transaction as the
// order.
//
// Both designs replay the same seeded checkouts with the same injected
// faults. Everything runs in one goroutine on a simulated clock, so every
// run prints the same numbers.
package chargednoorder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"time"

	sagaorchestration "github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/saga-orchestration"
	transactionaloutbox "github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/transactional-outbox"
	idempotencykey "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/idempotency-key"
)

const (
	Checkouts = 500
	// Fault mix: each checkout gets at most one injected fault.
	TimeoutRate    = 0.06 // payment charges, then 1-2 responses are lost
	OutageRate     = 0.02 // payment charges, then 4 responses in a row are lost
	CrashRate      = 0.05 // order service crashes after reserving or after charging
	BrokerDownRate = 0.05 // message broker is down while this checkout confirms
	CommitFailRate = 0.03 // the confirm transaction aborts once
	DeclineRate    = 0.04 // card declined (a clean, definitive failure)
	// ClientRetryRate is the share of customers who press "Pay" again after
	// an ambiguous error (timeout, connection reset, 500). Nobody retries a
	// definitive answer such as "card declined".
	ClientRetryRate = 0.6

	// Simulated costs of each call.
	DBWrite        = 4 * time.Millisecond
	SagaLogWrite   = 3 * time.Millisecond // fixed only: persist saga progress
	InventoryCall  = 20 * time.Millisecond
	ChargeCall     = 250 * time.Millisecond
	PaymentTimeout = 3 * time.Second
	ChargeBackoff  = 200 * time.Millisecond
	PublishCall    = 5 * time.Millisecond

	ChargeAttempts   = 3 // fixed: tries per charge, all with the same key
	ConfirmAttempts  = 3 // fixed: tries for the confirm transaction
	ArrivalGap       = 2 * time.Second
	ClientRetryAfter = 5 * time.Second
	RecoveryAfter    = 30 * time.Second // fixed: restarted service sweeps unfinished sagas
	RelayInterval    = time.Second      // fixed: outbox relay poll interval

	seed1 = 2026
	seed2 = 1028

	topicConfirmed = "order.confirmed"
)

var (
	errTimeout    = errors.New("payment: timed out waiting for response")
	errDeclined   = errors.New("payment: card declined")
	errCommit     = errors.New("db: transaction aborted")
	errBrokerDown = errors.New("broker: unavailable")
	errCrash      = errors.New("order service: process crashed")
)

// Faults counts the injected faults. They are identical in both runs.
type Faults struct {
	Timeouts, Outages, Crashes, BrokerDown, CommitFails, Declines, Clean int
}

// Report is what a run measured.
type Report struct {
	Name   string
	Faults Faults

	// ClientRetries counts checkouts where the customer pressed "Pay" again.
	ClientRetries int
	// Confirmed counts checkouts that ended with a confirmed order.
	Confirmed int
	// ChargedNoOrder counts checkouts with money taken and no confirmed order.
	ChargedNoOrder int
	// DoubleCharged counts checkouts with two or more charges not refunded.
	DoubleCharged int
	// OrphanEvents are "order confirmed" events for orders not in the
	// database; MissingEvents are confirmed orders downstream never heard of.
	OrphanEvents, MissingEvents int
	// EventsBeforeCommit counts events published before their order's
	// transaction committed (a consumer reading the order back may not find it).
	EventsBeforeCommit int
	// Violations counts checkouts breaking the invariant.
	Violations int
	// StuckPending counts orders left "pending" forever; InventoryLeaked
	// counts units still reserved for orders that were never confirmed.
	StuckPending, InventoryLeaked int

	// Refunds counts charges refunded by a compensation.
	Refunds int
	// SagasResumed counts sagas picked up again after a crash.
	SagasResumed int
	// PaymentReplays counts charge requests answered from the idempotency
	// store instead of charging again.
	PaymentReplays int
	// ConfirmRetries counts confirm transactions retried after aborting.
	ConfirmRetries int
	// Compensated counts sagas that undid their steps.
	Compensated int
	// RelayFailures counts outbox relay passes that hit a down broker.
	RelayFailures int

	// CleanLatency is the response time of a checkout with no fault;
	// MeanLatency and MaxLatency cover every first response.
	CleanLatency, MeanLatency, MaxLatency time.Duration
	// MeanEventLag and MaxEventLag measure commit-to-delivery time of events.
	MeanEventLag, MaxEventLag time.Duration
}

// Print writes the summary.
func (r Report) Print(w io.Writer) {
	f := r.Faults
	fmt.Fprintf(w, "%s\n", r.Name)
	fmt.Fprintf(w, "%d checkouts: create order -> reserve inventory -> charge card -> confirm order\n", Checkouts)
	fmt.Fprintf(w, "injected: %d payment timeouts after charging, %d payment outages, %d crashes, %d broker outages, %d aborted commits, %d declines (%d clean)\n",
		f.Timeouts, f.Outages, f.Crashes, f.BrokerDown, f.CommitFails, f.Declines, f.Clean)
	fmt.Fprintf(w, "%.0f%% of customers press Pay again after an ambiguous error\n\n", ClientRetryRate*100)

	fmt.Fprintf(w, "  checkouts breaking the invariant:        %d\n", r.Violations)
	fmt.Fprintf(w, "    charged, but no order:                 %d\n", r.ChargedNoOrder)
	fmt.Fprintf(w, "    charged more than once:                %d\n", r.DoubleCharged)
	fmt.Fprintf(w, "    orphan events (order not in the DB):   %d\n", r.OrphanEvents)
	fmt.Fprintf(w, "    missing events (never delivered):      %d\n", r.MissingEvents)
	fmt.Fprintf(w, "  orders confirmed:                        %d\n", r.Confirmed)
	fmt.Fprintf(w, "  customers who pressed Pay again:         %d\n", r.ClientRetries)
	fmt.Fprintf(w, "  events published before their commit:    %d\n", r.EventsBeforeCommit)
	fmt.Fprintf(w, "  orders stuck pending / inventory leaked: %d / %d units\n", r.StuckPending, r.InventoryLeaked)
	fmt.Fprintf(w, "  sagas compensated / refunds issued:      %d / %d\n", r.Compensated, r.Refunds)
	fmt.Fprintf(w, "  sagas resumed after a crash:             %d\n", r.SagasResumed)
	fmt.Fprintf(w, "  charges replayed by idempotency key:     %d\n", r.PaymentReplays)
	fmt.Fprintf(w, "  confirm transactions retried:            %d\n", r.ConfirmRetries)
	fmt.Fprintf(w, "  checkout response time: clean %v, mean %v, max %v\n",
		r.CleanLatency, r.MeanLatency.Round(time.Millisecond), r.MaxLatency)
	if r.MaxEventLag > 0 {
		fmt.Fprintf(w, "  event lag after commit: mean %v, max %v (%d relay passes hit the down broker)\n",
			r.MeanEventLag.Round(time.Millisecond), r.MaxEventLag.Round(time.Millisecond), r.RelayFailures)
	}
}

// checkout is one customer's script; both designs replay the same scripts.
// The fault counters are consumed as the run goes.
type checkout struct {
	id          string
	amount      int // cents
	lost        int // payment responses to lose (after the charge is made)
	crashAfter  string
	brokerDown  bool
	commitFails int
	declined    bool
	retries     bool // presses Pay again after an ambiguous error
	clean       bool
}

func traffic() ([]*checkout, Faults) {
	rng := rand.New(rand.NewPCG(seed1, seed2))
	var f Faults
	out := make([]*checkout, Checkouts)
	for i := range out {
		c := &checkout{id: fmt.Sprintf("co%03d", i+1), amount: 1000 + 100*rng.IntN(190)}
		p := rng.Float64()
		switch {
		case p < TimeoutRate:
			c.lost = 1 + rng.IntN(2)
			f.Timeouts++
		case p < TimeoutRate+OutageRate:
			c.lost = 4
			f.Outages++
		case p < TimeoutRate+OutageRate+CrashRate:
			c.crashAfter = [2]string{stepReserve, stepCharge}[rng.IntN(2)]
			f.Crashes++
		case p < TimeoutRate+OutageRate+CrashRate+BrokerDownRate:
			c.brokerDown = true
			f.BrokerDown++
		case p < TimeoutRate+OutageRate+CrashRate+BrokerDownRate+CommitFailRate:
			c.commitFails = 1
			f.CommitFails++
		case p < TimeoutRate+OutageRate+CrashRate+BrokerDownRate+CommitFailRate+DeclineRate:
			c.declined = true
			f.Declines++
		default:
			c.clean = true
			f.Clean++
		}
		c.retries = rng.Float64() < ClientRetryRate
		out[i] = c
	}
	return out, f
}

const (
	stepCreate  = "create-order"
	stepReserve = "reserve-inventory"
	stepKey     = "assign-payment-key"
	stepCharge  = "charge-card"
	stepConfirm = "confirm-order"
)

// outcome is what the customer is told.
type outcome int

const (
	ok         outcome = iota
	definitive         // "declined" or "we couldn't take payment, nothing charged"
	ambiguous          // timeout, 500 or connection reset: "did it work?"
)

type charge struct {
	checkout string
	amount   int
	refunded bool
}

// sim holds what both designs share: the clock, the order database, the
// payment ledger, inventory, the broker and the downstream consumer.
type sim struct {
	now      time.Duration
	db       *transactionaloutbox.DB
	charges  []*charge
	reserved map[string]int // order ID -> units
	orders   map[string][]string
	broker   *broker
	commitAt map[string]time.Duration
	r        *Report
	lagSum   time.Duration
	lagN     int
}

func newSim(name string) *sim {
	s := &sim{
		db:       transactionaloutbox.NewDB(),
		reserved: map[string]int{},
		orders:   map[string][]string{},
		commitAt: map[string]time.Duration{},
		r:        &Report{Name: name},
	}
	s.broker = &broker{s: s, consumer: &transactionaloutbox.DedupConsumer{}}
	return s
}

func (s *sim) clock() time.Time { return time.Unix(0, 0).Add(s.now) }

func (s *sim) newCharge(c *checkout) *charge {
	ch := &charge{checkout: c.id, amount: c.amount}
	s.charges = append(s.charges, ch)
	return ch
}

// transport delivers (or loses) the payment service's answer.
func (s *sim) transport(c *checkout) error {
	if c.lost > 0 {
		c.lost--
		s.now += PaymentTimeout
		return errTimeout
	}
	s.now += ChargeCall
	return nil
}

// broker stands in for the message broker; behind it is one downstream
// consumer (say, fulfilment) that deduplicates by message ID.
type broker struct {
	s        *sim
	down     bool
	consumer *transactionaloutbox.DedupConsumer
}

func (b *broker) Publish(ctx context.Context, m transactionaloutbox.Message) error {
	if b.down {
		return errBrokerDown
	}
	b.s.now += PublishCall
	if t, ok := b.s.commitAt[m.Key]; ok {
		lag := b.s.now - t
		b.s.lagSum += lag
		b.s.lagN++
		b.s.r.MaxEventLag = max(b.s.r.MaxEventLag, lag)
	} else {
		b.s.r.EventsBeforeCommit++
	}
	return b.consumer.Publish(ctx, m)
}

type design interface {
	checkout(c *checkout, attempt int) outcome
	// afterAttempt runs once the customer has their answer (fixed: a relay pass).
	afterAttempt()
	// afterCheckout runs when this customer's story is over (fixed: crash recovery).
	afterCheckout(c *checkout)
	finish()
	stuck() int
}

func (s *sim) run(d design) Report {
	cs, faults := traffic()
	s.r.Faults = faults
	var latSum time.Duration
	for i, c := range cs {
		s.now = max(s.now, time.Duration(i)*ArrivalGap)
		s.broker.down = c.brokerDown
		start := s.now
		out := d.checkout(c, 1)
		lat := s.now - start
		latSum += lat
		s.r.MaxLatency = max(s.r.MaxLatency, lat)
		if c.clean && s.r.CleanLatency == 0 {
			s.r.CleanLatency = lat
		}
		d.afterAttempt()
		s.broker.down = false
		if out == ambiguous && c.retries {
			s.r.ClientRetries++
			s.now += ClientRetryAfter
			d.checkout(c, 2)
			d.afterAttempt()
		}
		d.afterCheckout(c)
	}
	d.finish()
	s.r.MeanLatency = latSum / time.Duration(len(cs))
	if s.lagN > 0 {
		s.r.MeanEventLag = s.lagSum / time.Duration(s.lagN)
	}
	s.evaluate(cs)
	s.r.StuckPending = d.stuck()
	return *s.r
}

// evaluate checks the invariant for every checkout: either the order is
// confirmed and the customer was charged exactly once, or there is no order
// and the net charge is zero; and downstream's view matches the database.
func (s *sim) evaluate(cs []*checkout) {
	msgs, _ := s.broker.consumer.Snapshot()
	downstream := map[string]bool{}
	for _, m := range msgs {
		if m.Topic == topicConfirmed {
			downstream[m.Key] = true
		}
	}
	live := map[string][]*charge{}
	for _, ch := range s.charges {
		if !ch.refunded {
			live[ch.checkout] = append(live[ch.checkout], ch)
		}
	}
	for _, c := range cs {
		confirmed, viewOK := 0, true
		for _, id := range s.orders[c.id] {
			_, inDB := s.db.Order(id)
			if inDB {
				confirmed++
				if s.reserved[id] == 0 {
					panic("confirmed order without inventory: " + id)
				}
			} else {
				s.r.InventoryLeaked += s.reserved[id]
			}
			switch {
			case inDB && !downstream[id]:
				s.r.MissingEvents++
				viewOK = false
			case !inDB && downstream[id]:
				s.r.OrphanEvents++
				viewOK = false
			}
		}
		net := 0
		for _, ch := range live[c.id] {
			net += ch.amount
		}
		if confirmed > 0 {
			s.r.Confirmed++
		}
		if confirmed == 0 && net > 0 {
			s.r.ChargedNoOrder++
		}
		if len(live[c.id]) > 1 {
			s.r.DoubleCharged++
		}
		good := (confirmed == 1 && len(live[c.id]) == 1 && net == c.amount) || (confirmed == 0 && net == 0)
		if !good || !viewOK {
			s.r.Violations++
		}
	}
}

// broken: one function, four calls in a row, nothing undone on failure.
type broken struct {
	*sim
	pending map[string]bool
	msgID   int64
}

func (b *broken) checkout(c *checkout, attempt int) outcome {
	// Each press of "Pay" is a new request with nothing tying it to the
	// last one, so it creates a new order.
	id := fmt.Sprintf("%s-%d", c.id, attempt)
	b.orders[c.id] = append(b.orders[c.id], id)
	b.now += DBWrite
	b.pending[id] = true

	b.now += InventoryCall
	b.reserved[id] = 1
	if c.crashAfter == stepReserve {
		c.crashAfter = ""
		return ambiguous // connection reset; nothing remembers this order
	}

	// The payment API has no idempotency key: every call is a new charge.
	if c.declined {
		b.now += ChargeCall
		return definitive // the pending order and the reservation stay behind
	}
	b.newCharge(c)
	if err := b.transport(c); err != nil {
		return ambiguous // the charge went through; we just never heard
	}
	if c.crashAfter == stepCharge {
		c.crashAfter = ""
		return ambiguous // charged, and the process died before writing "paid"
	}

	// Dual write: publish from inside the transaction, then commit.
	b.now += DBWrite
	err := b.db.WithTx(func(tx *transactionaloutbox.Tx) error {
		tx.PutOrder(transactionaloutbox.Order{ID: id, Amount: c.amount})
		b.msgID++
		if err := b.broker.Publish(context.Background(), transactionaloutbox.Message{
			ID: b.msgID, Topic: topicConfirmed, Key: id,
		}); err != nil {
			// "log and carry on": the order is fine, the event is gone.
			_ = err
		}
		if c.commitFails > 0 {
			c.commitFails--
			return errCommit // the event is already out
		}
		return nil
	})
	if err != nil {
		return ambiguous
	}
	b.commitAt[id] = b.now
	delete(b.pending, id)
	return ok
}

func (*broken) afterAttempt()           {}
func (*broken) afterCheckout(*checkout) {}
func (*broken) finish()                 {}
func (b *broken) stuck() int            { return len(b.pending) }

// Broken runs the broken design.
func Broken() Report {
	s := newSim("BROKEN: sequential calls, no compensation, payment without idempotency key, publish inside the DB transaction")
	return s.run(&broken{sim: s, pending: map[string]bool{}})
}

// sagaState is the persisted saga row: which steps are done, and whether
// the saga has finished.
type sagaState struct {
	c     *checkout
	done  map[string]bool
	final bool
	out   outcome // the answer, once final
}

type fixed struct {
	*sim
	orch  *sagaorchestration.Orchestrator
	idem  *idempotencykey.Store
	relay *transactionaloutbox.Relay
	sagas map[string]*sagaState
	byKey map[string]*charge // payment service: charge recorded under each key
}

func paymentKey(c *checkout) string { return "charge-" + c.id }

// charge is the payment service's endpoint. The key and fingerprint make
// it execute once; every retry gets the stored answer.
func (f *fixed) charge(c *checkout) error {
	key := paymentKey(c)
	fp := idempotencykey.Fingerprint([]byte(c.id), []byte(fmt.Sprint(c.amount)))
	resp, out, err := f.idem.Do(context.Background(), key, fp, func(context.Context) (idempotencykey.Response, error) {
		if c.declined {
			return idempotencykey.Response{Status: 402, Body: []byte("declined")}, nil
		}
		f.byKey[key] = f.newCharge(c)
		return idempotencykey.Response{Status: 201, Body: []byte(key)}, nil
	})
	if err != nil {
		panic(err)
	}
	if out == idempotencykey.Replayed {
		f.r.PaymentReplays++
	}
	if err := f.transport(c); err != nil {
		return err
	}
	if resp.Status == 402 {
		return errDeclined
	}
	return nil
}

// refund undoes whatever was charged under the checkout's key, if anything.
// Safe to call any number of times.
func (f *fixed) refund(c *checkout) error {
	f.now += ChargeCall
	if ch, ok := f.byKey[paymentKey(c)]; ok && !ch.refunded {
		ch.refunded = true
		f.r.Refunds++
	}
	return nil
}

func (f *fixed) steps(st *sagaState) []sagaorchestration.Step {
	c := st.c
	id := c.id
	// step wraps an action so it is skipped once persisted as done, and so
	// the injected crash lands after the action took effect but before its
	// completion was persisted: the worst moment.
	step := func(name string, act, comp func() error) sagaorchestration.Step {
		s := sagaorchestration.Step{Name: name, Action: func(context.Context) error {
			if st.done[name] {
				return nil
			}
			if err := act(); err != nil {
				return err
			}
			if c.crashAfter == name {
				c.crashAfter = ""
				panic(errCrash)
			}
			f.now += SagaLogWrite
			st.done[name] = true
			return nil
		}}
		if comp != nil {
			s.Compensate = func(context.Context) error { return comp() }
		}
		return s
	}
	return []sagaorchestration.Step{
		step(stepCreate, func() error {
			f.now += DBWrite // the pending order row
			return nil
		}, func() error {
			f.now += DBWrite // mark it cancelled
			return nil
		}),
		step(stepReserve, func() error {
			f.now += InventoryCall
			f.reserved[id] = 1 // keyed by order: reserving twice is a no-op
			return nil
		}, func() error {
			f.now += InventoryCall
			delete(f.reserved, id)
			return nil
		}),
		// A timed-out charge may have gone through, and the orchestrator
		// only compensates steps that succeeded. So the refund hangs off
		// this step, which always succeeds before the charge is attempted:
		// if anything after it fails, whatever was charged under the key is
		// refunded (and nothing happens if nothing was charged).
		step(stepKey, func() error { return nil }, func() error { return f.refund(c) }),
		step(stepCharge, func() error {
			var err error
			for a := range ChargeAttempts {
				if a > 0 {
					f.now += ChargeBackoff
				}
				if err = f.charge(c); !errors.Is(err, errTimeout) {
					return err
				}
			}
			return err
		}, nil),
		// After the charge, go forward: retry the confirm rather than undo a
		// successful payment over a transient abort.
		step(stepConfirm, func() error {
			var err error
			for a := range ConfirmAttempts {
				if a > 0 {
					f.r.ConfirmRetries++
				}
				f.now += DBWrite
				err = f.db.WithTx(func(tx *transactionaloutbox.Tx) error {
					if _, exists := tx.GetOrder(id); exists {
						return nil
					}
					tx.PutOrder(transactionaloutbox.Order{ID: id, Amount: c.amount})
					tx.AddOutbox(topicConfirmed, id, fmt.Sprintf(`{"order":%q,"amount":%d}`, id, c.amount))
					if c.commitFails > 0 {
						c.commitFails--
						return errCommit
					}
					return nil
				})
				if err == nil {
					f.commitAt[id] = f.now
					return nil
				}
			}
			return err
		}, nil),
	}
}

// drive runs (or resumes) a saga. A crash unwinds out of the orchestrator
// without compensating, exactly like a dead process.
func (f *fixed) drive(st *sagaState) (res sagaorchestration.Result, crashed bool) {
	defer func() {
		if p := recover(); p != nil {
			if p != errCrash {
				panic(p)
			}
			crashed = true
		}
	}()
	return f.orch.Run(context.Background(), f.steps(st)), false
}

func (f *fixed) resume(st *sagaState) outcome {
	res, crashed := f.drive(st)
	if crashed {
		return ambiguous
	}
	switch res.Status {
	case sagaorchestration.Completed:
		st.out = ok
	case sagaorchestration.Compensated:
		f.r.Compensated++
		st.out = definitive
	default:
		panic(fmt.Sprintf("saga %s: %v: %v", st.c.id, res.Status, res.Err))
	}
	st.final = true
	return st.out
}

func (f *fixed) checkout(c *checkout, attempt int) outcome {
	// The checkout ID is the client's idempotency key for the whole
	// checkout: pressing Pay again finds the same saga.
	st, exists := f.sagas[c.id]
	switch {
	case exists && st.final:
		return st.out // a finished saga answers from its row (not reached in this traffic)
	case exists:
		f.r.SagasResumed++
	default:
		f.now += SagaLogWrite
		st = &sagaState{c: c, done: map[string]bool{}}
		f.sagas[c.id] = st
		f.orders[c.id] = []string{c.id}
	}
	return f.resume(st)
}

func (f *fixed) relayPass() {
	f.now += RelayInterval
	if _, err := f.relay.RelayOnce(context.Background()); err != nil {
		f.r.RelayFailures++
	}
}

func (f *fixed) afterAttempt() { f.relayPass() }

// afterCheckout: if the customer didn't come back, the restarted service's
// recovery sweep finds the unfinished saga and resumes it.
func (f *fixed) afterCheckout(c *checkout) {
	st := f.sagas[c.id]
	if st == nil || st.final {
		return
	}
	f.now += RecoveryAfter
	f.r.SagasResumed++
	f.resume(st)
	f.relayPass()
}

func (f *fixed) finish() {
	f.broker.down = false
	for len(f.db.Pending(1)) > 0 {
		f.relayPass()
	}
}

func (f *fixed) stuck() int {
	n := 0
	for _, st := range f.sagas {
		if !st.final {
			n++
		}
	}
	return n
}

// Fixed runs the fixed design.
func Fixed() Report {
	s := newSim("FIXED: persisted orchestrated saga with compensations + idempotency-keyed payment + transactional outbox")
	f := &fixed{
		sim:   s,
		orch:  &sagaorchestration.Orchestrator{CompensationAttempts: 3},
		idem:  idempotencykey.NewStore(24*time.Hour, s.clock),
		sagas: map[string]*sagaState{},
		byKey: map[string]*charge{},
	}
	f.relay = &transactionaloutbox.Relay{DB: s.db, Publisher: s.broker}
	return s.run(f)
}
