package fanoutfeed

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func clock() func() time.Time {
	var mu sync.Mutex
	t := time.Unix(1_700_000_000, 0)
	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		t = t.Add(time.Second)
		return t
	}
}

func texts(posts []Post) string {
	var s []string
	for _, p := range posts {
		s = append(s, p.Text)
	}
	return strings.Join(s, ",")
}

// TestStrategiesAgree checks that all strategies produce identical
// timelines. Only the cost differs.
func TestStrategiesAgree(t *testing.T) {
	type cost struct{ inboxWrites, outboxReads int }
	tests := []struct {
		strategy Strategy
		want     cost
	}{
		{Push, cost{inboxWrites: 8, outboxReads: 0}},   // d1:2 + s1:3 + e1:1 + d2:2
		{Pull, cost{inboxWrites: 0, outboxReads: 5}},   // alice reads 3 outboxes, bob reads 2
		{Hybrid, cost{inboxWrites: 5, outboxReads: 2}}, // star is pulled
	}
	for _, tc := range tests {
		t.Run(tc.strategy.String(), func(t *testing.T) {
			f := New(tc.strategy, 3, 0, clock()) // "star" has 3 followers, so Hybrid pulls them
			for _, fan := range []string{"alice", "bob", "carol"} {
				f.Follow(fan, "star")
			}
			f.Follow("alice", "dave")
			f.Follow("alice", "erin")
			f.Follow("bob", "dave")

			f.Publish("dave", "d1")
			f.Publish("star", "s1")
			f.Publish("erin", "e1")
			f.Publish("dave", "d2")

			if got := texts(f.Timeline("alice", 10)); got != "d2,e1,s1,d1" {
				t.Errorf("alice = %s", got)
			}
			if got := texts(f.Timeline("bob", 2)); got != "d2,s1" {
				t.Errorf("bob = %s", got)
			}
			st := f.Stats()
			if st.InboxWrites != tc.want.inboxWrites || st.OutboxReads != tc.want.outboxReads || st.PostsWritten != 4 {
				t.Errorf("stats = %+v, want %+v", st, tc.want)
			}
		})
	}
}

func TestCelebrityPostIsNotFannedOut(t *testing.T) {
	f := New(Hybrid, 100, 0, clock())
	for i := 0; i < 1000; i++ {
		f.Follow(fmt.Sprintf("fan%d", i), "celeb")
	}
	f.Publish("celeb", "hello world")
	if w := f.Stats().InboxWrites; w != 0 {
		t.Errorf("celebrity post caused %d inbox writes", w)
	}
	if got := texts(f.Timeline("fan999", 5)); got != "hello world" {
		t.Errorf("timeline = %q", got)
	}

	push := New(Push, 0, 0, clock())
	for i := 0; i < 1000; i++ {
		push.Follow(fmt.Sprintf("fan%d", i), "celeb")
	}
	push.Publish("celeb", "hello world")
	if w := push.Stats().InboxWrites; w != 1000 {
		t.Errorf("push should write 1000 inboxes, wrote %d", w)
	}
}

func TestBecomingCelebrityDoesNotDuplicate(t *testing.T) {
	f := New(Hybrid, 2, 0, clock())
	f.Follow("a", "rising")
	f.Publish("rising", "early") // 1 follower: pushed into a's inbox
	f.Follow("b", "rising")      // now 2 followers: pulled from the outbox
	f.Publish("rising", "late")
	// "early" is in a's inbox and in the outbox. It must appear once.
	if got := texts(f.Timeline("a", 10)); got != "late,early" {
		t.Errorf("a = %s", got)
	}
	// b followed after "early" but sees it, because pulled authors expose
	// their whole outbox. That is one visible difference between push and pull.
	if got := texts(f.Timeline("b", 10)); got != "late,early" {
		t.Errorf("b = %s", got)
	}
}

func TestUnfollowAndEdgeCases(t *testing.T) {
	f := New(Push, 0, 0, clock())
	f.Follow("a", "x")
	f.Follow("a", "a") // self-follow ignored
	f.Publish("x", "x1")
	f.Publish("a", "mine")
	if got := texts(f.Timeline("a", 10)); got != "x1" {
		t.Errorf("before unfollow = %s", got)
	}
	f.Unfollow("a", "x")
	if got := f.Timeline("a", 10); len(got) != 0 {
		t.Errorf("unfollowed posts still visible: %v", got)
	}
	if got := f.Timeline("a", 0); got != nil {
		t.Error("limit 0 should return nil")
	}
	if got := f.Timeline("nobody", 5); len(got) != 0 {
		t.Error("unknown user should have empty timeline")
	}
}

func TestInboxCap(t *testing.T) {
	f := New(Push, 0, 5, clock())
	f.Follow("a", "x")
	for i := 0; i < 50; i++ {
		f.Publish("x", fmt.Sprint(i))
	}
	if n := len(f.inbox["a"]); n > 10 {
		t.Errorf("inbox grew to %d, cap 5 allows at most 10", n)
	}
	if got := texts(f.Timeline("a", 3)); got != "49,48,47" {
		t.Errorf("timeline = %s", got)
	}
}

func TestConcurrent(t *testing.T) {
	f := New(Hybrid, 5, 0, nil)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			u := fmt.Sprint("u", i)
			for j := 0; j < 8; j++ {
				f.Follow(u, fmt.Sprint("u", j))
			}
			for j := 0; j < 50; j++ {
				f.Publish(u, "p")
				f.Timeline(u, 10)
			}
		}(i)
	}
	wg.Wait()
	if f.Stats().PostsWritten != 400 {
		t.Error("lost posts")
	}
}
