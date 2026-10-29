package deadletterqueue

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *fakeClock                   { return &fakeClock{t: time.Unix(1_000, 0)} }
func (f *fakeClock) Now() time.Time          { f.mu.Lock(); defer f.mu.Unlock(); return f.t }
func (f *fakeClock) Advance(d time.Duration) { f.mu.Lock(); f.t = f.t.Add(d); f.mu.Unlock() }
func (f *fakeClock) Set(t time.Time) {
	f.mu.Lock()
	if t.After(f.t) {
		f.t = t
	}
	f.mu.Unlock()
}

var errBug = errors.New("handler bug")

func mustNew(t *testing.T, opts ...Option) *Queue {
	t.Helper()
	q, err := New(opts...)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

// drain processes until the queue is empty, jumping the clock to the next
// visible message whenever nothing is receivable. It returns acked IDs in order.
func drain(t *testing.T, q *Queue, c *fakeClock, h func(Delivery) error) []string {
	t.Helper()
	var acked []string
	for range 10_000 {
		ok, err := q.ProcessOne(func(d Delivery) error {
			if err := h(d); err != nil {
				return err
			}
			acked = append(acked, d.ID)
			return nil
		})
		if ok {
			_ = err
			continue
		}
		next, more := q.NextVisible()
		if !more {
			return acked
		}
		c.Set(next)
	}
	t.Fatal("drain did not terminate")
	return nil
}

func failIDs(ids ...string) func(Delivery) error {
	return func(d Delivery) error {
		for _, id := range ids {
			if d.ID == id {
				return errBug
			}
		}
		return nil
	}
}

func TestNewValidation(t *testing.T) {
	tests := []struct {
		name string
		opts []Option
	}{
		{"negative max attempts", []Option{WithMaxAttempts(-1)}},
		{"zero visibility", []Option{WithVisibilityTimeout(0)}},
		{"zero base backoff", []Option{WithBackoff(0, time.Second)}},
		{"max below base", []Option{WithBackoff(time.Second, time.Millisecond)}},
		{"park key without ordering", []Option{WithParkKey()}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.opts...); err == nil {
				t.Fatal("expected error")
			}
		})
	}
	if _, err := New(WithMaxAttempts(RetryForever), WithPerKeyOrder(), WithParkKey(), WithClock(nil)); err != nil {
		t.Fatalf("valid options rejected: %v", err)
	}
}

func TestSendValidationAndUnknownReceipt(t *testing.T) {
	q := mustNew(t)
	if err := q.Send("", "k", "p"); err == nil {
		t.Fatal("empty ID accepted")
	}
	if err := q.Ack(42); !errors.Is(err, ErrUnknownReceipt) {
		t.Fatalf("Ack unknown = %v", err)
	}
	if err := q.Nack(42, errBug); !errors.Is(err, ErrUnknownReceipt) {
		t.Fatalf("Nack unknown = %v", err)
	}
}

func TestPoisonIsolatedAfterMaxAttempts(t *testing.T) {
	c := newClock()
	start := c.Now()
	q := mustNew(t, WithClock(c.Now), WithMaxAttempts(3), WithBackoff(time.Second, time.Minute))
	for _, id := range []string{"a", "poison", "b"} {
		_ = q.Send(id, "", id)
	}
	acked := drain(t, q, c, failIDs("poison"))
	if strings.Join(acked, ",") != "a,b" {
		t.Fatalf("acked %v, want a,b", acked)
	}
	dl := q.DeadLetters()
	if len(dl) != 1 || q.DLQDepth() != 1 {
		t.Fatalf("DLQ = %+v", dl)
	}
	d := dl[0]
	if d.ID != "poison" || d.Attempts != 3 || d.Permanent || d.Reason != errBug.Error() {
		t.Fatalf("dead letter = %+v", d)
	}
	// Failures at t=0, then after 1s and 2s of backoff.
	if d.FirstFailure != start || d.LastFailure != start.Add(3*time.Second) || d.DeadAt != d.LastFailure {
		t.Fatalf("failure times first=%v last=%v", d.FirstFailure.Sub(start), d.LastFailure.Sub(start))
	}
	s := q.Stats()
	if s.Acked != 2 || s.Retried != 2 || s.DeadLettered != 1 || s.Depth != 0 || s.DLQDepth != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestPermanentErrorsSkipRetries(t *testing.T) {
	c := newClock()
	q := mustNew(t, WithClock(c.Now), WithMaxAttempts(5))
	_ = q.Send("bad", "", "{not json")
	_ = q.Send("good", "", "{}")
	acked := drain(t, q, c, func(d Delivery) error {
		if d.ID == "bad" {
			return fmt.Errorf("decode %s: %w", d.ID, Permanent(errors.New("malformed payload")))
		}
		return nil
	})
	if len(acked) != 1 {
		t.Fatalf("acked %v", acked)
	}
	dl := q.DeadLetters()
	if len(dl) != 1 || dl[0].Attempts != 1 || !dl[0].Permanent || q.Stats().Retried != 0 {
		t.Fatalf("dead letters %+v stats %+v", dl, q.Stats())
	}
	if Permanent(nil) != nil || IsPermanent(errBug) || !IsPermanent(fmt.Errorf("x: %w", Permanent(errBug))) {
		t.Fatal("Permanent/IsPermanent helpers wrong")
	}
	if !errors.Is(Permanent(errBug), errBug) {
		t.Fatal("Permanent must unwrap")
	}
}

func TestOtherMessagesKeepFlowing(t *testing.T) {
	c := newClock()
	q := mustNew(t, WithClock(c.Now), WithMaxAttempts(4), WithBackoff(time.Second, time.Second))
	_ = q.Send("poison", "", "")
	for i := range 10 {
		_ = q.Send(fmt.Sprint("m", i), "", "")
	}
	// Without moving the clock, the poison message fails once and then waits
	// out its backoff while all ten good messages are processed.
	for {
		ok, _ := q.ProcessOne(failIDs("poison"))
		if !ok {
			break
		}
	}
	if s := q.Stats(); s.Acked != 10 || s.Depth != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestBackoffTimings(t *testing.T) {
	c := newClock()
	q := mustNew(t, WithClock(c.Now), WithMaxAttempts(5), WithBackoff(time.Second, 4*time.Second))
	_ = q.Send("p", "", "")
	wantDelays := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 4 * time.Second}
	for i, want := range wantDelays {
		ok, _ := q.ProcessOne(failIDs("p"))
		if !ok {
			t.Fatalf("attempt %d: not delivered", i+1)
		}
		c.Advance(want - time.Millisecond)
		if _, ok := q.Receive(); ok {
			t.Fatalf("attempt %d: redelivered before %v", i+1, want)
		}
		c.Advance(time.Millisecond)
	}
	ok, _ := q.ProcessOne(failIDs("p"))
	if !ok || q.DLQDepth() != 1 || q.DeadLetters()[0].Attempts != 5 {
		t.Fatalf("fifth attempt should dead-letter: %+v", q.DeadLetters())
	}
}

func TestVisibilityTimeoutCountsAsAttempt(t *testing.T) {
	c := newClock()
	q := mustNew(t, WithClock(c.Now), WithMaxAttempts(2), WithVisibilityTimeout(10*time.Second),
		WithBackoff(time.Second, time.Second))
	_ = q.Send("m", "", "")
	d1, _ := q.Receive()
	c.Advance(10 * time.Second) // the consumer "crashed"
	if _, ok := q.Receive(); ok {
		t.Fatal("expired message should first wait out its backoff")
	}
	if err := q.Ack(d1.Receipt); !errors.Is(err, ErrUnknownReceipt) {
		t.Fatalf("late ack = %v, want ErrUnknownReceipt", err)
	}
	c.Advance(time.Second)
	d2, ok := q.Receive()
	if !ok || d2.ID != "m" || d2.Attempts != 2 || d2.Receipt == d1.Receipt {
		t.Fatalf("redelivery = %+v", d2)
	}
	c.Advance(10 * time.Second)
	q.Receive() // triggers the expiry sweep
	dl := q.DeadLetters()
	if len(dl) != 1 || dl[0].Reason != "visibility timeout expired" || q.Stats().Expired != 2 {
		t.Fatalf("dead letters %+v stats %+v", dl, q.Stats())
	}
}

func TestRetryForeverNeverDeadLetters(t *testing.T) {
	c := newClock()
	q := mustNew(t, WithClock(c.Now), WithMaxAttempts(RetryForever), WithBackoff(time.Second, time.Second))
	_ = q.Send("p", "", "")
	for range 50 {
		q.ProcessOne(func(Delivery) error { return Permanent(errBug) })
		c.Advance(time.Second)
	}
	if s := q.Stats(); s.DLQDepth != 0 || s.Delivered != 50 || s.Depth != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestRedrivePreservesIDsAndResetsAttempts(t *testing.T) {
	c := newClock()
	q := mustNew(t, WithClock(c.Now), WithMaxAttempts(2))
	_ = q.Send("x", "", "payload-x")
	_ = q.Send("y", "", "payload-y")
	sentAt := c.Now()
	drain(t, q, c, failIDs("x", "y"))
	if q.DLQDepth() != 2 {
		t.Fatalf("DLQ depth %d", q.DLQDepth())
	}
	c.Advance(time.Hour) // the fix is deployed
	if n := q.Redrive(nil); n != 2 {
		t.Fatalf("redrove %d", n)
	}
	if q.DLQDepth() != 0 {
		t.Fatal("DLQ not empty after full redrive")
	}
	var got []Delivery
	drain(t, q, c, func(d Delivery) error { got = append(got, d); return nil })
	if len(got) != 2 {
		t.Fatalf("got %d deliveries", len(got))
	}
	for i, id := range []string{"x", "y"} {
		d := got[i]
		if d.ID != id || d.Payload != "payload-"+id || d.Attempts != 1 || d.Redrives != 1 || d.SentAt != sentAt {
			t.Fatalf("redriven delivery %+v", d)
		}
	}
	if s := q.Stats(); s.Redriven != 2 || s.Acked != 2 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestFilteredRedrive(t *testing.T) {
	c := newClock()
	q := mustNew(t, WithClock(c.Now), WithMaxAttempts(1))
	_ = q.Send("r1", "", "refund")
	_ = q.Send("o1", "", "order")
	_ = q.Send("r2", "", "refund")
	drain(t, q, c, func(Delivery) error { return errBug })
	n := q.Redrive(func(d DeadLetter) bool { return d.Payload == "refund" })
	if n != 2 {
		t.Fatalf("redrove %d, want 2", n)
	}
	left := q.DeadLetters()
	if len(left) != 1 || left[0].ID != "o1" {
		t.Fatalf("left in DLQ: %+v", left)
	}
	acked := drain(t, q, c, func(Delivery) error { return nil })
	if strings.Join(acked, ",") != "r1,r2" {
		t.Fatalf("acked %v", acked)
	}
}

func TestPerKeyOrderWithoutDLQBlocksKey(t *testing.T) {
	c := newClock()
	q := mustNew(t, WithClock(c.Now), WithMaxAttempts(RetryForever), WithPerKeyOrder())
	_ = q.Send("a1", "A", "")
	_ = q.Send("a2", "A", "")
	_ = q.Send("b1", "B", "")
	var acked []string
	for range 20 {
		q.ProcessOne(func(d Delivery) error {
			if d.ID == "a1" {
				return errBug
			}
			acked = append(acked, d.ID)
			return nil
		})
		c.Advance(100 * time.Millisecond)
	}
	if strings.Join(acked, ",") != "b1" {
		t.Fatalf("acked %v: a2 must stay stuck behind a1", acked)
	}
}

func TestPerKeyOrderWithoutParkBreaksOrder(t *testing.T) {
	c := newClock()
	q := mustNew(t, WithClock(c.Now), WithMaxAttempts(2), WithPerKeyOrder())
	_ = q.Send("a1", "A", "")
	_ = q.Send("a2", "A", "")
	acked := drain(t, q, c, failIDs("a1"))
	if strings.Join(acked, ",") != "a2" {
		t.Fatalf("acked %v", acked)
	}
	q.Redrive(nil)
	acked = drain(t, q, c, func(Delivery) error { return nil })
	if strings.Join(acked, ",") != "a1" {
		t.Fatalf("acked %v", acked)
	}
	// a2 was applied before a1: the key's order is broken, by design.
}

func TestParkKeyPreservesOrder(t *testing.T) {
	c := newClock()
	q := mustNew(t, WithClock(c.Now), WithMaxAttempts(2), WithPerKeyOrder(), WithParkKey())
	for _, id := range []string{"a1", "b1", "a2", "a3", "b2"} {
		_ = q.Send(id, id[:1], "")
	}
	acked := drain(t, q, c, failIDs("a1"))
	if strings.Join(acked, ",") != "b1,b2" {
		t.Fatalf("acked %v", acked)
	}
	_ = q.Send("a4", "a", "") // arrives while key a is parked
	dl := q.DeadLetters()
	var ids []string
	for _, d := range dl {
		ids = append(ids, d.ID)
	}
	if strings.Join(ids, ",") != "a1,a2,a3,a4" {
		t.Fatalf("DLQ %v", ids)
	}
	if !strings.Contains(dl[1].Reason, "parked behind a1") || dl[1].Attempts != 0 {
		t.Fatalf("parked entry %+v", dl[1])
	}
	// Selecting just a1 redrives the whole key, in order.
	if n := q.Redrive(func(d DeadLetter) bool { return d.ID == "a1" }); n != 4 {
		t.Fatalf("redrove %d, want 4", n)
	}
	acked = drain(t, q, c, func(Delivery) error { return nil })
	if strings.Join(acked, ",") != "a1,a2,a3,a4" {
		t.Fatalf("acked %v", acked)
	}
	_ = q.Send("a5", "a", "") // key unparked
	if q.DLQDepth() != 0 {
		t.Fatal("key should be unparked after redrive")
	}
}

func TestConcurrentConsumers(t *testing.T) {
	c := newClock()
	q := mustNew(t, WithClock(c.Now), WithMaxAttempts(3), WithBackoff(time.Millisecond, 4*time.Millisecond),
		WithVisibilityTimeout(time.Hour))
	const n = 600
	var processed sync.Map
	var dups atomic.Int64
	handler := func(d Delivery) error {
		var i int
		fmt.Sscan(d.Payload, &i)
		switch {
		case i%20 == 0:
			return Permanent(errors.New("malformed"))
		case i%20 == 1:
			return errBug
		}
		if _, loaded := processed.LoadOrStore(d.ID, true); loaded {
			dups.Add(1)
		}
		return nil
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // producer
		defer wg.Done()
		for i := range n {
			_ = q.Send(fmt.Sprint("m", i), "", fmt.Sprint(i))
		}
	}()
	done := make(chan struct{})
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				if ok, _ := q.ProcessOne(handler); !ok {
					c.Advance(time.Millisecond)
					_ = q.DLQDepth()
				}
			}
		}()
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		s := q.Stats()
		if s.Sent == n && s.Acked+s.DeadLettered == n {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("did not settle: %+v", s)
		}
		time.Sleep(time.Millisecond)
	}
	close(done)
	wg.Wait()
	s := q.Stats()
	if s.DeadLettered != 60 || s.Acked != 540 || dups.Load() != 0 {
		t.Fatalf("stats %+v dups %d", s, dups.Load())
	}
	for _, d := range q.DeadLetters() {
		if d.Permanent && d.Attempts != 1 || !d.Permanent && d.Attempts != 3 {
			t.Fatalf("dead letter %+v", d)
		}
	}
}
