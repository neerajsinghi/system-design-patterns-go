package readreplicas

import (
	"encoding/base64"
	"errors"
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

var testKey = []byte("0123456789abcdef-test-key")

func TestTokenKeyValidation(t *testing.T) {
	if _, err := New([]time.Duration{0}, WithTokenKey([]byte("short"))); err == nil {
		t.Error("short token key: expected error")
	}
	if _, err := New([]time.Duration{0}, WithTokenKey(testKey)); err != nil {
		t.Errorf("valid key: %v", err)
	}
}

func TestTokenRoundTrip(t *testing.T) {
	for _, signed := range []bool{false, true} {
		t.Run(fmt.Sprintf("signed=%v", signed), func(t *testing.T) {
			clk := &clock{t: time.Unix(0, 0)}
			opts := []Option{WithClock(clk.Now)}
			if signed {
				opts = append(opts, WithTokenKey(testKey))
			}
			c, _ := New([]time.Duration{0}, opts...)
			s := c.NewSession()
			s.Write("a", "1")
			s.Write("b", "2")
			s.Read("a")
			r, err := c.ResumeSession(s.Token())
			if err != nil {
				t.Fatal(err)
			}
			w1, r1 := s.State()
			w2, r2 := r.State()
			if w1 != w2 || r1 != r2 || !r.lastWriteAt.Equal(s.lastWriteAt) {
				t.Fatalf("state (%d,%d,%v) resumed as (%d,%d,%v)", w1, r1, s.lastWriteAt, w2, r2, r.lastWriteAt)
			}
			if w2 != 2 || r2 != 2 {
				t.Fatalf("got lastWrite=%d lastRead=%d, want 2,2", w2, r2)
			}
			// A brand-new session's token also round-trips.
			if _, err := c.ResumeSession(c.NewSession().Token()); err != nil {
				t.Fatalf("empty session token: %v", err)
			}
		})
	}
}

// Two stateless app servers share the cluster but no session memory: only
// the token (a cookie, say) travels with the user.
func TestCrossServerResumeKeepsReadYourWrites(t *testing.T) {
	clk := &clock{t: time.Unix(0, 0)}
	c, _ := New([]time.Duration{50 * time.Millisecond, 500 * time.Millisecond},
		WithClock(clk.Now), WithTokenKey(testKey))
	c.Write("name", "Ada")
	clk.Advance(time.Second)

	serverA := c.NewSession()
	serverA.Write("name", "Grace")
	cookie := serverA.Token()

	// Without the token, server B reads like a stranger and sees the old name.
	fresh := c.NewSession()
	if v, _, _ := fresh.Read("name"); v != "Ada" {
		t.Fatalf("expected a stale read without the token, got %q", v)
	}
	for _, step := range []time.Duration{0, 60 * time.Millisecond, 600 * time.Millisecond} {
		clk.Advance(step)
		serverB, err := c.ResumeSession(cookie)
		if err != nil {
			t.Fatal(err)
		}
		for range 3 {
			if v, _, from := serverB.Read("name"); v != "Grace" {
				t.Fatalf("after %v: server B read %q from %s, want Grace", step, v, from)
			}
		}
	}
}

func TestMalformedTokensRejected(t *testing.T) {
	c, _ := New([]time.Duration{0}, WithTokenKey(testKey))
	s := c.NewSession()
	s.Write("k", "v")
	good, _ := base64.RawURLEncoding.DecodeString(s.Token())
	enc := base64.RawURLEncoding.EncodeToString

	flip := func(i int) string {
		b := append([]byte(nil), good...)
		b[i] ^= 1
		return enc(b)
	}
	other, _ := New([]time.Duration{0}, WithTokenKey([]byte("another-secret-key!!")))
	otherSess := other.NewSession()
	otherSess.Write("k", "v")
	unsigned, _ := New([]time.Duration{0})

	tests := []struct {
		name, token string
	}{
		{"empty", ""},
		{"not base64", "!!!not-a-token***"},
		{"truncated", enc(good[:len(good)-1])},
		{"too long", enc(append(append([]byte(nil), good...), 0))},
		{"unknown format", flip(0)},
		{"forged lastWrite", flip(8)},
		{"forged signature", flip(len(good) - 1)},
		{"signed by another key", otherSess.Token()},
		{"unsigned token to signed cluster", unsigned.NewSession().Token()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := c.ResumeSession(tt.token); !errors.Is(err, ErrInvalidToken) {
				t.Fatalf("got err %v, want ErrInvalidToken", err)
			}
		})
	}
}

func TestUnsignedTokenFromTheFutureRejected(t *testing.T) {
	c, _ := New([]time.Duration{0})
	s := c.NewSession()
	s.Write("k", "v")
	b, _ := base64.RawURLEncoding.DecodeString(s.Token())
	b[8] = 99 // lastWrite = 99, but the leader only has version 1
	if _, err := c.ResumeSession(base64.RawURLEncoding.EncodeToString(b)); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("got %v, want ErrInvalidToken", err)
	}
}

// Replica-1 has the new value, replica-2 does not. A plain session that
// alternates between them goes back in time. A monotonic one does not.
func TestMonotonicReads(t *testing.T) {
	setup := func() (*Cluster, *clock) {
		clk := &clock{t: time.Unix(0, 0)}
		c, _ := New([]time.Duration{10 * time.Millisecond, time.Second}, WithClock(clk.Now))
		c.Write("score", "1-0")
		clk.Advance(2 * time.Second)
		c.Write("score", "2-0")
		clk.Advance(20 * time.Millisecond) // replica-1 has v2, replica-2 only v1
		return c, clk
	}
	readAll := func(s *Session) []string {
		var got []string
		for range 6 {
			v, _, from := s.Read("score")
			got = append(got, v+"@"+from)
		}
		return got
	}

	c, _ := setup()
	plain := readAll(c.NewSession())
	sawNew, wentBack := false, false
	for _, r := range plain {
		if r[:3] == "2-0" {
			sawNew = true
		} else if sawNew {
			wentBack = true
		}
	}
	if !wentBack {
		t.Fatalf("expected a plain session to go back in time, got %v", plain)
	}

	c, _ = setup()
	s := c.NewSession(WithMonotonicReads())
	got := readAll(s)
	sawNew = false
	for _, r := range got {
		if r[:3] == "2-0" {
			sawNew = true
		} else if sawNew {
			t.Fatalf("monotonic reads violated: %v", got)
		}
	}
	if _, lr := s.State(); lr != 2 {
		t.Fatalf("lastRead=%d, want 2", lr)
	}
	if st := c.Stats(); st.LeaderReads != 0 {
		t.Fatalf("leader reads=%d, want 0 (replica-1 can serve every read)", st.LeaderReads)
	}

	// Monotonic state survives a token hop to another app server.
	r, err := c.ResumeSession(s.Token(), WithMonotonicReads())
	if err != nil {
		t.Fatal(err)
	}
	for range 4 {
		if v, _, from := r.Read("score"); v != "2-0" {
			t.Fatalf("resumed session went back in time: %q from %s", v, from)
		}
	}
}

func TestLeaderAfterWrite(t *testing.T) {
	clk := &clock{t: time.Unix(0, 0)}
	c, _ := New([]time.Duration{10 * time.Millisecond, 500 * time.Millisecond}, WithClock(clk.Now))
	s := c.NewSession(WithLeaderAfterWrite(200 * time.Millisecond))

	// No write yet: reads go to replicas.
	if _, _, from := s.Read("k"); from == "leader" {
		t.Fatal("read before any write should use a replica")
	}
	s.Write("k", "v")
	clk.Advance(50 * time.Millisecond) // replica-1 has caught up, but the window ignores that
	for range 4 {
		if v, _, from := s.Read("k"); v != "v" || from != "leader" {
			t.Fatalf("inside window: got %q from %s, want leader", v, from)
		}
	}
	// After the window reads go anywhere, and replica-2 (500ms lag) is still
	// behind: the time-based strategy breaks read-your-writes here.
	clk.Advance(200 * time.Millisecond)
	stale := false
	for range 2 {
		if _, ok, _ := s.Read("k"); !ok {
			stale = true
		}
	}
	if !stale {
		t.Fatal("expected a stale read once the window expired with replica lag > window")
	}
	// Window survives a token hop.
	s.Write("k", "v2")
	r, err := c.ResumeSession(s.Token(), WithLeaderAfterWrite(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, from := r.Read("k"); from != "leader" {
		t.Fatalf("resumed session inside window read from %s", from)
	}
}

func TestConcurrentTokenHops(t *testing.T) {
	c, _ := New([]time.Duration{0, time.Millisecond, time.Hour}, WithTokenKey(testKey))
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			token := c.NewSession().Token()
			key := fmt.Sprintf("w%d", w)
			for i := range 50 {
				s, err := c.ResumeSession(token, WithMonotonicReads()) // each request on a "new server"
				if err != nil {
					t.Error(err)
					return
				}
				val := fmt.Sprint(i)
				s.Write(key, val)
				if got, _, _ := s.Read(key); got != val {
					t.Errorf("read-your-writes violated across hop: wrote %s read %s", val, got)
					return
				}
				token = s.Token()
			}
		}()
	}
	wg.Wait()
}
