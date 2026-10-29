package idempotentconsumer

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func credit(tx *Tx, m Message) error {
	tx.Add(m.Key, m.Value)
	return nil
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// fill appends n messages with IDs m1..mn to a one-partition log.
func fill(t *testing.T, n int) (*Log, []string) {
	t.Helper()
	l, err := NewLog(1)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, n)
	for i := range n {
		ids[i] = fmt.Sprintf("m%d", i+1)
		if _, err := l.Append(Message{ID: ids[i], Key: "acct", Value: 1}); err != nil {
			t.Fatal(err)
		}
	}
	return l, ids
}

func TestCrashPointsPerStrategy(t *testing.T) {
	tests := []struct {
		strategy  Strategy
		point     CrashPoint
		wantLost  int
		wantDup   int
		wantExact int
	}{
		{CommitBefore, BeforeCommit, 0, 0, 10},
		{CommitBefore, AfterCommit, 1, 0, 9}, // = before the effect: lost
		{CommitBefore, BeforeEffect, 1, 0, 9},
		{CommitBefore, AfterEffect, 0, 0, 10},
		{CommitAfter, BeforeEffect, 0, 0, 10},
		{CommitAfter, AfterEffect, 0, 1, 9}, // effect done, no commit: duplicated
		{CommitAfter, BeforeCommit, 0, 1, 9},
		{CommitAfter, AfterCommit, 0, 0, 10},
		{Inbox, BeforeEffect, 0, 0, 10},
		{Inbox, AfterEffect, 0, 0, 10}, // redelivered, skipped by the inbox
		{Inbox, BeforeCommit, 0, 0, 10},
		{Inbox, AfterCommit, 0, 0, 10},
	}
	for _, tt := range tests {
		t.Run(tt.strategy.String()+"/"+tt.point.String(), func(t *testing.T) {
			l, ids := fill(t, 10)
			s, _ := NewStore()
			c, err := NewConsumer(l, s, "g", tt.strategy, credit,
				WithCrashes(Crash{Partition: 0, Offset: 4, Point: tt.point}))
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Drain(); err != nil {
				t.Fatal(err)
			}
			a := s.Audit(ids)
			if a.Lost != tt.wantLost || a.Duplicated != tt.wantDup || a.Exact != tt.wantExact {
				t.Fatalf("audit %+v, want lost=%d dup=%d exact=%d", a, tt.wantLost, tt.wantDup, tt.wantExact)
			}
			if got := c.Stats().Crashes; got != 1 {
				t.Fatalf("crashes = %d, want 1", got)
			}
			if got := l.Committed("g", 0); got != 10 {
				t.Fatalf("committed = %d, want 10", got)
			}
			if want := int64(a.Applied); s.Balance("acct") != want {
				t.Fatalf("balance %d, want %d", s.Balance("acct"), want)
			}
		})
	}
}

func TestInboxSkipsRedeliveryAndCountsIt(t *testing.T) {
	l, ids := fill(t, 5)
	s, _ := NewStore()
	c, _ := NewConsumer(l, s, "g", Inbox, credit,
		WithCrashes(Crash{0, 2, AfterEffect}, Crash{0, 3, BeforeCommit}))
	if err := c.Drain(); err != nil {
		t.Fatal(err)
	}
	st := c.Stats()
	if st.Applied != 5 || st.Skipped != 2 || st.Crashes != 2 {
		t.Fatalf("stats %+v, want applied=5 skipped=2 crashes=2", st)
	}
	if a := s.Audit(ids); a.Exact != 5 || s.InboxSize() != 5 {
		t.Fatalf("audit %+v inbox %d", a, s.InboxSize())
	}
}

func TestHandlerErrorRollsBack(t *testing.T) {
	l, ids := fill(t, 3)
	s, _ := NewStore()
	fail := true
	h := func(tx *Tx, m Message) error {
		tx.Add(m.Key, m.Value)
		if m.ID == "m2" && fail {
			fail = false
			return errors.New("downstream unavailable")
		}
		return nil
	}
	c, _ := NewConsumer(l, s, "g", Inbox, h)
	if err := c.Drain(); err == nil {
		t.Fatal("want handler error")
	}
	if s.Balance("acct") != 1 || s.InboxSize() != 1 || l.Committed("g", 0) != 1 {
		t.Fatalf("partial writes leaked: balance=%d inbox=%d committed=%d",
			s.Balance("acct"), s.InboxSize(), l.Committed("g", 0))
	}
	if err := c.Drain(); err != nil {
		t.Fatal(err)
	}
	if a := s.Audit(ids); a.Exact != 3 {
		t.Fatalf("after retry %+v", a)
	}
}

func TestProducerRetransmitDedupedByInbox(t *testing.T) {
	l, _ := NewLog(2)
	// The first attempt of every third message loses its ack.
	faults := func(m Message, attempt int) Fault {
		if attempt == 1 && m.Value%3 == 0 {
			return LoseAck
		}
		return Delivered
	}
	p, err := NewProducer(l, "p1", WithFaults(faults))
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for i := range 30 {
		m, err := p.Send(fmt.Sprintf("user-%d", i%5), int64(i))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, m.ID)
	}
	if st := p.Stats(); st.Sent != 30 || st.Retransmits != 10 || st.Attempts != 40 {
		t.Fatalf("producer stats %+v", st)
	}
	if got := l.End(0) + l.End(1); got != 40 {
		t.Fatalf("log holds %d records, want 40 (10 duplicates)", got)
	}

	for _, tc := range []struct {
		s       Strategy
		wantDup int
	}{{CommitAfter, 10}, {Inbox, 0}} {
		s, _ := NewStore()
		c, _ := NewConsumer(l, s, "g-"+tc.s.String(), tc.s, credit)
		if err := c.Drain(); err != nil {
			t.Fatal(err)
		}
		if a := s.Audit(ids); a.Duplicated != tc.wantDup || a.Lost != 0 {
			t.Fatalf("%v: %+v, want %d duplicated", tc.s, a, tc.wantDup)
		}
	}
}

func TestProducerGivesUp(t *testing.T) {
	l, _ := NewLog(1)
	p, _ := NewProducer(l, "p", WithMaxAttempts(3),
		WithFaults(func(Message, int) Fault { return LoseRequest }))
	if _, err := p.Send("k", 1); !errors.Is(err, ErrNotAcked) {
		t.Fatalf("err = %v, want ErrNotAcked", err)
	}
	if l.End(0) != 0 || p.Stats().Failed != 1 || p.Stats().Attempts != 3 {
		t.Fatalf("log end %d stats %+v", l.End(0), p.Stats())
	}
	// Lost acks on every attempt: the producer reports failure, but the log
	// has the message three times.
	p2, _ := NewProducer(l, "p2", WithMaxAttempts(3),
		WithFaults(func(Message, int) Fault { return LoseAck }))
	if _, err := p2.Send("k", 1); !errors.Is(err, ErrNotAcked) || l.End(0) != 3 {
		t.Fatalf("err=%v end=%d", err, l.End(0))
	}
}

func TestProducerCustomIDs(t *testing.T) {
	l, _ := NewLog(1)
	n := 100
	p, _ := NewProducer(l, "p", WithIDs(func() string { n++; return fmt.Sprint(n) }))
	m, err := p.Send("k", 1)
	if err != nil || m.ID != "101" {
		t.Fatalf("id=%q err=%v", m.ID, err)
	}
	bad, _ := NewProducer(l, "p", WithIDs(func() string { return "" }))
	if _, err := bad.Send("k", 1); err == nil {
		t.Fatal("empty ID accepted")
	}
}

func TestDedupWindowExpiry(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	l, ids := fill(t, 10)
	s, err := NewStore(WithClock(clk.Now), WithRetention(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	c, _ := NewConsumer(l, s, "g", Inbox, credit)
	// One message a minute.
	for range 10 {
		if _, err := c.Poll(1); err != nil {
			t.Fatal(err)
		}
		clk.Advance(time.Minute)
	}
	// Now t=10m. Entries processed at 0..4m are older than 5m. Apply prunes
	// as it goes (the 0..3m entries went while handling later messages), and
	// Prune removes the 4m one.
	if n := s.Prune(); n != 1 || s.Pruned() != 5 || s.InboxSize() != 5 {
		t.Fatalf("pruned now %d, total %d, inbox %d; want 1, 5, 5", n, s.Pruned(), s.InboxSize())
	}
	// Replay the whole partition (offset reset): the five IDs still inside
	// the window are skipped, the five pruned ones are applied again.
	if err := l.Commit("g", 0, 0); err != nil {
		t.Fatal(err)
	}
	replay, _ := NewConsumer(l, s, "g", Inbox, credit)
	if err := replay.Drain(); err != nil {
		t.Fatal(err)
	}
	a := s.Audit(ids)
	if a.Duplicated != 5 || a.Exact != 5 {
		t.Fatalf("audit %+v, want 5 duplicated (pruned) and 5 exact", a)
	}
	for i, id := range ids {
		want := 1
		if i < 5 {
			want = 2
		}
		if got := s.Applications(id); got != want {
			t.Fatalf("%s applied %d times, want %d", id, got, want)
		}
	}
	if s.Pruned() != 5 {
		t.Fatalf("pruned total %d", s.Pruned())
	}
}

func TestNoRetentionKeepsEverything(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	l, ids := fill(t, 3)
	s, _ := NewStore(WithClock(clk.Now))
	c, _ := NewConsumer(l, s, "g", Inbox, credit)
	_ = c.Drain()
	clk.Advance(365 * 24 * time.Hour)
	_ = l.Commit("g", 0, 0)
	c2, _ := NewConsumer(l, s, "g", Inbox, credit)
	_ = c2.Drain()
	if a := s.Audit(ids); a.Exact != 3 || s.Prune() != 0 {
		t.Fatalf("audit %+v", a)
	}
}

func TestLogBasics(t *testing.T) {
	l, _ := NewLog(3)
	m, _ := l.Append(Message{ID: "a", Key: "k"})
	m2, _ := l.Append(Message{ID: "a", Key: "k"})
	if m.Partition != m2.Partition || m.Offset != 0 || m2.Offset != 1 {
		t.Fatalf("same key must share a partition with increasing offsets: %+v %+v", m, m2)
	}
	got, err := l.Read(m.Partition, 1, 10)
	if err != nil || len(got) != 1 || got[0].Offset != 1 {
		t.Fatalf("read %+v %v", got, err)
	}
	if got, _ := l.Read(m.Partition, 5, 10); got != nil {
		t.Fatalf("read past end = %+v", got)
	}
	if l.Committed("nobody", 0) != 0 {
		t.Fatal("default committed offset must be 0")
	}
}

func TestValidation(t *testing.T) {
	l, _ := NewLog(2)
	s, _ := NewStore()
	if _, err := NewLog(0); err == nil {
		t.Error("NewLog(0) accepted")
	}
	if _, err := l.Append(Message{Key: "k"}); err == nil {
		t.Error("empty ID accepted")
	}
	if _, err := l.Read(2, 0, 1); err == nil {
		t.Error("bad partition accepted")
	}
	if _, err := l.Read(0, -1, 1); err == nil {
		t.Error("negative offset accepted")
	}
	if _, err := l.Read(0, 0, 0); err == nil {
		t.Error("max 0 accepted")
	}
	if err := l.Commit("", 0, 0); err == nil {
		t.Error("empty group accepted")
	}
	if err := l.Commit("g", 5, 0); err == nil {
		t.Error("bad partition commit accepted")
	}
	if err := l.Commit("g", 0, -1); err == nil {
		t.Error("negative commit accepted")
	}
	if _, err := NewStore(WithRetention(-time.Second)); err == nil {
		t.Error("negative retention accepted")
	}
	if _, err := s.Apply(Message{ID: "x"}, true, nil); err == nil {
		t.Error("nil handler accepted")
	}

	consumerCases := []struct {
		name string
		make func() (*Consumer, error)
	}{
		{"nil log", func() (*Consumer, error) { return NewConsumer(nil, s, "g", Inbox, credit) }},
		{"nil store", func() (*Consumer, error) { return NewConsumer(l, nil, "g", Inbox, credit) }},
		{"nil handler", func() (*Consumer, error) { return NewConsumer(l, s, "g", Inbox, nil) }},
		{"empty group", func() (*Consumer, error) { return NewConsumer(l, s, "", Inbox, credit) }},
		{"bad strategy", func() (*Consumer, error) { return NewConsumer(l, s, "g", Strategy(9), credit) }},
		{"bad partition", func() (*Consumer, error) { return NewConsumer(l, s, "g", Inbox, credit, WithPartitions(7)) }},
		{"no partitions", func() (*Consumer, error) { return NewConsumer(l, s, "g", Inbox, credit, WithPartitions()) }},
		{"dup partition", func() (*Consumer, error) { return NewConsumer(l, s, "g", Inbox, credit, WithPartitions(1, 1)) }},
		{"bad crash point", func() (*Consumer, error) {
			return NewConsumer(l, s, "g", Inbox, credit, WithCrashes(Crash{0, 0, CrashPoint(0)}))
		}},
	}
	for _, tc := range consumerCases {
		if _, err := tc.make(); err == nil {
			t.Errorf("NewConsumer %s accepted", tc.name)
		}
	}
	c, _ := NewConsumer(l, s, "g", Inbox, credit)
	if _, err := c.Poll(0); err == nil {
		t.Error("Poll(0) accepted")
	}
	if _, err := NewProducer(nil, "p"); err == nil {
		t.Error("nil log producer accepted")
	}
	if _, err := NewProducer(l, ""); err == nil {
		t.Error("empty producer ID accepted")
	}
	if _, err := NewProducer(l, "p", WithMaxAttempts(0)); err == nil {
		t.Error("0 attempts accepted")
	}
	if Strategy(9).String() == "" || CrashPoint(9).String() == "" {
		t.Error("String on unknown values must not be empty")
	}
}

// TestConcurrentProducersAndConsumers runs several producers with lost acks
// and one consumer per partition with crashes, plus a second "rebalanced"
// consumer group member that overlaps on partition 0 (both read it at the
// same time, as during a rebalance). The inbox must still apply every
// message exactly once.
func TestConcurrentProducersAndConsumers(t *testing.T) {
	const partitions, producers, perProducer = 4, 4, 200
	l, _ := NewLog(partitions)
	s, _ := NewStore()

	var wg sync.WaitGroup
	idsCh := make(chan []string, producers)
	for pi := range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, _ := NewProducer(l, fmt.Sprintf("p%d", pi), WithFaults(func(m Message, attempt int) Fault {
				if attempt == 1 && m.Value%7 == 0 {
					return LoseAck
				}
				return Delivered
			}))
			var ids []string
			for i := range perProducer {
				m, err := p.Send(fmt.Sprintf("acct-%d", i%13), int64(i))
				if err != nil {
					t.Error(err)
					return
				}
				ids = append(ids, m.ID)
			}
			idsCh <- ids
		}()
	}
	wg.Wait()
	close(idsCh)
	var all []string
	for ids := range idsCh {
		all = append(all, ids...)
	}

	var consumers []*Consumer
	for p := range partitions {
		var crashes []Crash
		for off := int64(3); off < l.End(p); off += 17 {
			crashes = append(crashes, Crash{p, off, CrashPoint(1 + off%4)})
		}
		c, err := NewConsumer(l, s, "g", Inbox, credit, WithPartitions(p), WithCrashes(crashes...))
		if err != nil {
			t.Fatal(err)
		}
		consumers = append(consumers, c)
	}
	overlap, _ := NewConsumer(l, s, "g", Inbox, credit, WithPartitions(0))
	consumers = append(consumers, overlap)

	for _, c := range consumers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.Drain(); err != nil {
				t.Error(err)
			}
			_ = c.Stats()
		}()
	}
	wg.Wait()

	a := s.Audit(all)
	if a.Expected != producers*perProducer || a.Exact != a.Expected {
		t.Fatalf("audit %+v", a)
	}
	var want int64
	for range producers {
		for i := range perProducer {
			want += int64(i)
		}
	}
	var got int64
	for k := range 13 {
		got += s.Balance(fmt.Sprintf("acct-%d", k))
	}
	if got != want {
		t.Fatalf("total balance %d, want %d", got, want)
	}
}
