package pubsub

import (
	"errors"
	"sync"
	"testing"
)

func drain(s *Subscription) []Message {
	var out []Message
	for {
		select {
		case m, ok := <-s.C():
			if !ok {
				return out
			}
			out = append(out, m)
		default:
			return out
		}
	}
}

func TestFanOutToAllSubscribers(t *testing.T) {
	b := NewBroker()
	defer b.Close()
	a, _ := b.Subscribe("orders", 10, DropNewest)
	c, _ := b.Subscribe("orders", 10, DropNewest)
	other, _ := b.Subscribe("payments", 10, DropNewest)

	for i := 0; i < 3; i++ {
		st, err := b.Publish("orders", i)
		if err != nil || st.Delivered != 2 {
			t.Fatalf("publish %d: stats=%+v err=%v", i, st, err)
		}
	}
	for _, s := range []*Subscription{a, c} {
		got := drain(s)
		if len(got) != 3 {
			t.Fatalf("want 3 messages, got %d", len(got))
		}
		for i, m := range got {
			if m.Seq != uint64(i+1) || m.Payload != i {
				t.Fatalf("message %d out of order: %+v", i, m)
			}
		}
	}
	if n := len(drain(other)); n != 0 {
		t.Fatalf("other topic received %d messages", n)
	}
}

func TestSlowSubscriberPolicies(t *testing.T) {
	tests := []struct {
		name        string
		policy      Overflow
		wantSeqs    []uint64
		wantDropped uint64
		wantEvicted bool
	}{
		{"drop newest keeps first", DropNewest, []uint64{1, 2}, 3, false},
		{"drop oldest keeps latest", DropOldest, []uint64{4, 5}, 3, false},
		{"disconnect evicts", Disconnect, []uint64{1, 2}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewBroker()
			defer b.Close()
			slow, _ := b.Subscribe("t", 2, tt.policy)
			fast, _ := b.Subscribe("t", 10, DropNewest)
			for i := 1; i <= 5; i++ {
				if _, err := b.Publish("t", i); err != nil {
					t.Fatal(err)
				}
			}
			var seqs []uint64
			for m := range slowMessages(slow, tt.wantEvicted) {
				seqs = append(seqs, m.Seq)
			}
			if !equal(seqs, tt.wantSeqs) {
				t.Fatalf("slow got %v want %v", seqs, tt.wantSeqs)
			}
			if slow.Dropped() != tt.wantDropped || slow.Evicted() != tt.wantEvicted {
				t.Fatalf("dropped=%d evicted=%v", slow.Dropped(), slow.Evicted())
			}
			if n := len(drain(fast)); n != 5 {
				t.Fatalf("fast subscriber should be unaffected, got %d", n)
			}
		})
	}
}

// slowMessages returns buffered messages; when evicted the channel is closed
// so ranging terminates on its own.
func slowMessages(s *Subscription, closed bool) <-chan Message {
	if closed {
		return s.C()
	}
	out := make(chan Message, 16)
	for _, m := range drain(s) {
		out <- m
	}
	close(out)
	return out
}

func equal(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestValidationAndClose(t *testing.T) {
	b := NewBroker()
	cases := []struct {
		topic  string
		buf    int
		policy Overflow
		want   error
	}{
		{"", 1, DropNewest, ErrEmptyTopic},
		{"t", 0, DropNewest, ErrBadBuffer},
		{"t", 1, Overflow(42), ErrUnknownPolicy},
	}
	for _, c := range cases {
		if _, err := b.Subscribe(c.topic, c.buf, c.policy); !errors.Is(err, c.want) {
			t.Errorf("Subscribe(%q,%d,%v) err=%v want %v", c.topic, c.buf, c.policy, err, c.want)
		}
	}
	s, _ := b.Subscribe("t", 1, DropNewest)
	s.Cancel()
	s.Cancel() // idempotent
	if b.Subscribers("t") != 0 {
		t.Fatal("cancel did not unsubscribe")
	}
	s2, _ := b.Subscribe("t", 1, DropNewest)
	b.Close()
	if _, ok := <-s2.C(); ok {
		t.Fatal("channel should be closed after broker Close")
	}
	if _, err := b.Publish("t", 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("publish after close: %v", err)
	}
	s2.Cancel() // safe after close
}

func TestConcurrentPublishAndConsume(t *testing.T) {
	b := NewBroker()
	const pubs, per = 4, 250
	sub, _ := b.Subscribe("t", pubs*per, DropNewest)

	var wg sync.WaitGroup
	for p := 0; p < pubs; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				_, _ = b.Publish("t", i)
			}
		}()
	}
	got := 0
	done := make(chan struct{})
	go func() {
		for range sub.C() {
			got++
		}
		close(done)
	}()
	wg.Wait()
	b.Close()
	<-done
	if got != pubs*per {
		t.Fatalf("got %d want %d", got, pubs*per)
	}
}
