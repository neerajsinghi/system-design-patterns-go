package servicediscovery

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

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

func newClock() *fakeClock { return &fakeClock{t: time.Unix(1_700_000_000, 0)} }

func ids(list []Instance) string {
	s := ""
	for _, in := range list {
		s += in.ID + ","
	}
	return s
}

func TestRegistryLeases(t *testing.T) {
	clk := newClock()
	r := NewRegistry(clk.Now)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(r.Register(Instance{Service: "api", ID: "b", Addr: "10.0.0.2:80"}, 10*time.Second))
	must(r.Register(Instance{Service: "api", ID: "a", Addr: "10.0.0.1:80"}, 10*time.Second))

	steps := []struct {
		name    string
		advance time.Duration
		action  func() error
		wantErr error
		want    string
	}{
		{"both live, sorted", 0, nil, nil, "a,b,"},
		{"a heartbeats at 8s", 8 * time.Second, func() error { return r.Heartbeat("api", "a") }, nil, "a,b,"},
		{"b expires at exactly 10s", 2 * time.Second, nil, nil, "a,"},
		{"heartbeat after expiry is rejected", 0, func() error { return r.Heartbeat("api", "b") }, ErrUnknownInstance, "a,"},
		{"re-register revives b", 0, func() error {
			return r.Register(Instance{Service: "api", ID: "b", Addr: "10.0.0.9:80"}, 10*time.Second)
		}, nil, "a,b,"},
		{"a expires at 18s", 8 * time.Second, nil, nil, "b,"},
		{"deregister b", 0, func() error { r.Deregister("api", "b"); return nil }, nil, ""},
		{"unknown service heartbeat", 0, func() error { return r.Heartbeat("nope", "x") }, ErrUnknownInstance, ""},
	}
	for _, s := range steps {
		clk.Advance(s.advance)
		if s.action != nil {
			if err := s.action(); !errors.Is(err, s.wantErr) {
				t.Fatalf("%s: err = %v, want %v", s.name, err, s.wantErr)
			}
		}
		if got := ids(r.Lookup("api")); got != s.want {
			t.Fatalf("%s: lookup = %q, want %q", s.name, got, s.want)
		}
	}
}

func TestRegisterValidation(t *testing.T) {
	r := NewRegistry(nil)
	bad := []struct {
		inst Instance
		ttl  time.Duration
	}{
		{Instance{ID: "a", Addr: "x"}, time.Second},
		{Instance{Service: "s", Addr: "x"}, time.Second},
		{Instance{Service: "s", ID: "a"}, time.Second},
		{Instance{Service: "s", ID: "a", Addr: "x"}, 0},
	}
	for i, b := range bad {
		if err := r.Register(b.inst, b.ttl); !errors.Is(err, ErrInvalid) {
			t.Errorf("case %d: err = %v", i, err)
		}
	}
}

func TestSweep(t *testing.T) {
	clk := newClock()
	r := NewRegistry(clk.Now)
	for i := 0; i < 3; i++ {
		_ = r.Register(Instance{Service: "s", ID: fmt.Sprint(i), Addr: "x"}, time.Duration(i+1)*time.Second)
	}
	clk.Advance(2 * time.Second)
	if n := r.Sweep(); n != 2 {
		t.Errorf("swept %d, want 2", n)
	}
	clk.Advance(time.Second)
	if n := r.Sweep(); n != 1 || len(r.services) != 0 {
		t.Errorf("swept %d, services left %d", n, len(r.services))
	}
}

func TestResolverRoundRobinAndCache(t *testing.T) {
	clk := newClock()
	r := NewRegistry(clk.Now)
	for _, id := range []string{"a", "b", "c"} {
		_ = r.Register(Instance{Service: "api", ID: id, Addr: id + ":80"}, time.Minute)
	}
	res := NewResolver(r, 5*time.Second, clk.Now)

	var got string
	for i := 0; i < 6; i++ {
		in, err := res.Pick("api")
		if err != nil {
			t.Fatal(err)
		}
		got += in.ID
	}
	if got != "abcabc" {
		t.Errorf("round robin = %s", got)
	}

	// The registry changes, but the client keeps using its cache until the TTL.
	r.Deregister("api", "a")
	r.Deregister("api", "b")
	r.Deregister("api", "c")
	if _, err := res.Pick("api"); err != nil {
		t.Errorf("cached pick failed: %v", err)
	}
	clk.Advance(5 * time.Second)
	if _, err := res.Pick("api"); !errors.Is(err, ErrNoInstances) {
		t.Errorf("after TTL err = %v, want ErrNoInstances", err)
	}

	// Empty results are not cached, so a new instance is visible at once.
	_ = r.Register(Instance{Service: "api", ID: "d", Addr: "d:80"}, time.Minute)
	if in, err := res.Pick("api"); err != nil || in.ID != "d" {
		t.Errorf("pick = %v %v", in, err)
	}

	_ = r.Register(Instance{Service: "api", ID: "e", Addr: "e:80"}, time.Minute)
	res.Invalidate("api")
	seen := map[string]bool{}
	for i := 0; i < 4; i++ {
		in, _ := res.Pick("api")
		seen[in.ID] = true
	}
	if !seen["d"] || !seen["e"] {
		t.Errorf("after invalidate saw %v", seen)
	}
}

func TestConcurrentAccess(t *testing.T) {
	r := NewRegistry(nil)
	res := NewResolver(r, time.Millisecond, nil)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprint(i)
			for j := 0; j < 200; j++ {
				_ = r.Register(Instance{Service: "s", ID: id, Addr: "x"}, time.Second)
				_ = r.Heartbeat("s", id)
				_, _ = res.Pick("s")
				r.Sweep()
			}
		}(i)
	}
	wg.Wait()
	if n := len(r.Lookup("s")); n != 8 {
		t.Errorf("got %d instances, want 8", n)
	}
}
