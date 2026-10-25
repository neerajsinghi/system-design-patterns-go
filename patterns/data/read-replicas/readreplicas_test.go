package readreplicas

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func TestNewValidation(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Error("no followers: expected error")
	}
	if _, err := New([]time.Duration{-time.Second}); err == nil {
		t.Error("negative lag: expected error")
	}
}

func TestReplicationLag(t *testing.T) {
	clk := &clock{t: time.Unix(0, 0)}
	c, _ := New([]time.Duration{100 * time.Millisecond}, WithClock(clk.Now))
	c.Write("k", "v1")

	tests := []struct {
		advance time.Duration
		want    string
		wantOK  bool
	}{
		{0, "", false},                     // not replicated yet
		{99 * time.Millisecond, "", false}, // still inside the lag window
		{1 * time.Millisecond, "v1", true}, // exactly lag old: applied
		{time.Hour, "v1", true},            // stays applied
	}
	for i, tt := range tests {
		clk.Advance(tt.advance)
		got, ok, from := c.Read("k")
		if got != tt.want || ok != tt.wantOK || from != "replica-1" {
			t.Fatalf("step %d: got (%q,%v,%s) want (%q,%v)", i, got, ok, from, tt.want, tt.wantOK)
		}
	}
}

func TestFollowersApplyInOrder(t *testing.T) {
	clk := &clock{t: time.Unix(0, 0)}
	c, _ := New([]time.Duration{50 * time.Millisecond}, WithClock(clk.Now))
	c.Write("k", "a")
	clk.Advance(30 * time.Millisecond)
	c.Write("k", "b")
	clk.Advance(20 * time.Millisecond) // first write is 50ms old, second 20ms
	if v, _, _ := c.Read("k"); v != "a" {
		t.Fatalf("got %q, want a (prefix of the log)", v)
	}
	clk.Advance(30 * time.Millisecond)
	if v, _, _ := c.Read("k"); v != "b" {
		t.Fatalf("got %q, want b", v)
	}
}

func TestReadYourWrites(t *testing.T) {
	clk := &clock{t: time.Unix(0, 0)}
	c, _ := New([]time.Duration{10 * time.Millisecond, time.Second}, WithClock(clk.Now))
	s := c.NewSession()
	s.Write("profile", "new-name")

	// Immediately: no follower has it, so the session falls back to the leader.
	if v, _, from := s.Read("profile"); v != "new-name" || from != "leader" {
		t.Fatalf("got %q from %s", v, from)
	}
	// After 10ms the fast follower has caught up and can serve the session.
	clk.Advance(10 * time.Millisecond)
	for range 4 { // round-robin must skip the slow replica every time
		if v, _, from := s.Read("profile"); v != "new-name" || from != "replica-1" {
			t.Fatalf("got %q from %s, want replica-1", v, from)
		}
	}
	// A different client with no writes may still see stale data.
	other := c.NewSession()
	stale := false
	for range 2 {
		if _, ok, _ := other.Read("profile"); !ok {
			stale = true
		}
	}
	if !stale {
		t.Fatal("expected the lagging replica to serve a stale read to a session with no writes")
	}
	if st := c.Stats(); st.LeaderReads != 1 {
		t.Fatalf("leader reads=%d, want 1", st.LeaderReads)
	}
}

func TestReplicaVersions(t *testing.T) {
	clk := &clock{t: time.Unix(0, 0)}
	c, _ := New([]time.Duration{0, time.Minute}, WithClock(clk.Now))
	for i := range 3 {
		c.Write(fmt.Sprint(i), "x")
	}
	v := c.ReplicaVersions()
	if v["replica-1"] != 3 || v["replica-2"] != 0 {
		t.Fatalf("versions=%v", v)
	}
}

func TestConcurrentSessions(t *testing.T) {
	c, _ := New([]time.Duration{0, time.Millisecond, time.Hour})
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := c.NewSession()
			for i := range 100 {
				key := fmt.Sprintf("w%d", w)
				val := fmt.Sprint(i)
				s.Write(key, val)
				if got, _, _ := s.Read(key); got != val {
					t.Errorf("read-your-writes violated: wrote %s read %s", val, got)
					return
				}
			}
		}()
	}
	wg.Wait()
}
