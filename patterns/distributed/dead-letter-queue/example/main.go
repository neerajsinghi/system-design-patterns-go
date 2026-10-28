// Command example pushes a stream of 400 events, 4 of them poison, through a
// queue four ways: ordered without a DLQ, unordered without a DLQ, with a DLQ,
// and ordered with a DLQ that parks the whole key. It then "deploys a fix" and
// redrives the DLQ. Everything runs on a simulated clock.
package main

import (
	"errors"
	"fmt"
	"log"
	"slices"
	"strings"
	"time"

	dlq "github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/dead-letter-queue"
)

const (
	events      = 400
	keys        = 8
	interArrive = 50 * time.Millisecond // 20 events/s
	processTime = 45 * time.Millisecond
	horizon     = 5 * time.Minute
	maxAttempts = 5
)

var start = time.Unix(0, 0)

type event struct {
	id, key, payload string
	at               time.Duration
}

// stream is the same traffic for every run: mostly orders, plus two refunds
// that hit a bug in the v1 handler and two events in a legacy format it can't
// parse.
func stream() []event {
	poison := map[int]string{37: "legacy-format", 120: "refund", 251: "refund", 300: "legacy-format"}
	out := make([]event, events)
	for i := range out {
		p := "order"
		if kind, ok := poison[i]; ok {
			p = kind
		}
		out[i] = event{id: fmt.Sprintf("evt-%03d", i), key: fmt.Sprintf("user-%d", i%keys), payload: p,
			at: time.Duration(i) * interArrive}
	}
	return out
}

var (
	errRefundBug = errors.New("refund handler: nil currency")
	errFormat    = errors.New("unrecognised payload format")
)

// handler is the consumer; fixedRefunds and fixedParser model the two deploys.
func handler(fixedRefunds, fixedParser bool) func(dlq.Delivery) error {
	return func(d dlq.Delivery) error {
		switch {
		case d.Payload == "refund" && !fixedRefunds:
			return errRefundBug // looks transient, so it is retried
		case d.Payload == "legacy-format" && !fixedParser:
			return dlq.Permanent(errFormat) // retrying a parse error is pointless
		}
		return nil
	}
}

type result struct {
	goodDone, poisonDone int
	goodLatency          []time.Duration
	poisonAttempts       int
	applied              []dlq.Delivery
	end                  time.Duration
}

type sim struct {
	now time.Time
	q   *dlq.Queue
}

func newSim(opts ...dlq.Option) *sim {
	s := &sim{now: start}
	q, err := dlq.New(append([]dlq.Option{dlq.WithClock(func() time.Time { return s.now }),
		dlq.WithBackoff(time.Second, 30*time.Second)}, opts...)...)
	if err != nil {
		log.Fatal(err)
	}
	s.q = q
	return s
}

// run feeds evs into the queue and consumes with one worker until everything
// is settled or the simulated horizon is reached.
func (s *sim) run(evs []event, h func(dlq.Delivery) error) result {
	var r result
	stopAt := s.now.Add(horizon)
	base := s.now
	next := 0
	for s.now.Before(stopAt) {
		for next < len(evs) && !base.Add(evs[next].at).After(s.now) {
			e := evs[next]
			if err := s.q.Send(e.id, e.key, e.payload); err != nil {
				log.Fatal(err)
			}
			next++
		}
		got, _ := s.q.ProcessOne(func(d dlq.Delivery) error {
			s.now = s.now.Add(processTime)
			if d.Payload != "order" {
				r.poisonAttempts++
			}
			err := h(d)
			if err == nil {
				r.applied = append(r.applied, d)
				if d.Payload == "order" {
					r.goodDone++
					r.goodLatency = append(r.goodLatency, s.now.Sub(d.SentAt))
				} else {
					r.poisonDone++
				}
			}
			return err
		})
		if got {
			continue
		}
		wake, more := s.q.NextVisible()
		if next < len(evs) {
			if arr := base.Add(evs[next].at); !more || arr.Before(wake) {
				wake, more = arr, true
			}
		}
		if !more {
			break
		}
		if wake.After(stopAt) {
			s.now = stopAt
			break
		}
		s.now = wake
	}
	r.end = s.now.Sub(base)
	return r
}

func pct(ds []time.Duration, p float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	ds = slices.Clone(ds)
	slices.Sort(ds)
	return ds[int(p*float64(len(ds)-1))]
}

func (r result) print(s *sim) {
	st := s.q.Stats()
	good := events - 4
	fmt.Printf("   good events done %d/%d   latency p50 %v  p99 %v  max %v\n",
		r.goodDone, good, pct(r.goodLatency, .5), pct(r.goodLatency, .99), pct(r.goodLatency, 1))
	fmt.Printf("   attempts spent on poison: %d   still in main queue: %d   DLQ depth: %d   sim time: %v\n",
		r.poisonAttempts, st.Depth, st.DLQDepth, r.end.Round(time.Millisecond))
}

func outOfOrder(applied []dlq.Delivery) int {
	last := map[string]string{}
	n := 0
	for _, d := range applied {
		if prev, ok := last[d.Key]; ok && d.ID < prev {
			n++
		}
		if d.ID > last[d.Key] {
			last[d.Key] = d.ID
		}
	}
	return n
}

func main() {
	evs := stream()
	fmt.Printf("%d events over %v on %d keys, 4 poison (2 refunds hit a bug, 2 in a legacy format).\n",
		events, time.Duration(events)*interArrive, keys)
	fmt.Printf("One consumer, %v per event, retry backoff 1s doubling to 30s, %v horizon.\n\n", processTime, horizon)

	fmt.Println("1) Ordered per key, no DLQ: each poison event blocks its key forever")
	s1 := newSim(dlq.WithMaxAttempts(dlq.RetryForever), dlq.WithPerKeyOrder())
	r1 := s1.run(evs, handler(false, false))
	r1.print(s1)
	fmt.Println()

	fmt.Println("2) Unordered, no DLQ: good events flow, poison is retried until someone notices")
	s2 := newSim(dlq.WithMaxAttempts(dlq.RetryForever))
	r2 := s2.run(evs, handler(false, false))
	r2.print(s2)
	fmt.Println()

	fmt.Printf("3) Unordered, DLQ after %d attempts, Permanent errors parked at once\n", maxAttempts)
	s3 := newSim(dlq.WithMaxAttempts(maxAttempts))
	r3 := s3.run(evs, handler(false, false))
	r3.print(s3)
	for _, d := range s3.q.DeadLetters() {
		fmt.Printf("     DLQ %s %-13s attempts=%d permanent=%-5v failing %v..%v  %q\n",
			d.ID, d.Payload, d.Attempts, d.Permanent,
			d.FirstFailure.Sub(start).Round(time.Millisecond), d.LastFailure.Sub(start).Round(time.Millisecond), d.Reason)
	}
	if depth := s3.q.DLQDepth(); depth > 0 {
		fmt.Printf("     ALERT: dlq_depth=%d > 0\n", depth)
	}
	overtook := 0
	for _, d := range s3.q.DeadLetters() {
		for _, a := range r3.applied {
			if a.Key == d.Key && a.ID > d.ID {
				overtook++
			}
		}
	}
	fmt.Printf("     but %d later events on the poisoned keys were applied before them: per-key order is gone\n\n",
		overtook)

	fmt.Println("4) Ordered per key, DLQ that parks the whole key behind a poison event")
	s4 := newSim(dlq.WithMaxAttempts(maxAttempts), dlq.WithPerKeyOrder(), dlq.WithParkKey())
	r4 := s4.run(evs, handler(false, false))
	r4.print(s4)
	parkedBehind := 0
	for _, d := range s4.q.DeadLetters() {
		if strings.HasPrefix(d.Reason, "key parked") {
			parkedBehind++
		}
	}
	fmt.Printf("     %d of the %d dead letters are good events parked to keep their key in order\n\n",
		parkedBehind, s4.q.DLQDepth())

	fmt.Println("5) Deploy the refund fix, redrive only refunds (whole keys come back, in order)")
	n := s4.q.Redrive(func(d dlq.DeadLetter) bool { return d.Payload == "refund" })
	r5 := s4.run(nil, handler(true, false))
	fmt.Printf("   redriven %d, applied %d, DLQ depth now %d\n", n, len(r5.applied), s4.q.DLQDepth())

	fmt.Println("6) Deploy the legacy-format parser, redrive the rest")
	n = s4.q.Redrive(nil)
	r6 := s4.run(nil, handler(true, true))
	fmt.Printf("   redriven %d, applied %d, DLQ depth now %d\n", n, len(r6.applied), s4.q.DLQDepth())

	all := slices.Concat(r4.applied, r5.applied, r6.applied)
	ids := map[string]int{}
	for _, d := range all {
		ids[d.ID]++
	}
	dups := 0
	for _, c := range ids {
		dups += c - 1
	}
	fmt.Printf("   total applied %d/%d unique IDs, %d duplicates, %d applied out of per-key order\n",
		len(ids), events, dups, outOfOrder(all))
	fmt.Println("   Redriven events kept their IDs, so an idempotent consumer could dedupe any replays.")
}
