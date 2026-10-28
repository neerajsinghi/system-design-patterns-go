package presence

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

func (c *fakeClock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func newClock() *fakeClock { return &fakeClock{t: time.Unix(1_700_000_000, 0)} }

func newTracker(t *testing.T, clk *fakeClock, opts ...Option) *Tracker {
	t.Helper()
	tr, err := New(append([]Option{WithClock(clk.Now)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestNewValidation(t *testing.T) {
	tests := []struct {
		name string
		opt  Option
	}{
		{"nil clock", WithClock(nil)},
		{"zero ttl", WithTTL(0)},
		{"negative away", WithAwayAfter(-time.Second)},
		{"negative grace", WithOfflineGrace(-time.Second)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.opt); err == nil {
				t.Fatal("expected error")
			}
		})
	}
	if _, err := New(); err != nil {
		t.Fatalf("defaults: %v", err)
	}
}

func TestInputValidation(t *testing.T) {
	tr := newTracker(t, newClock())
	for i, c := range []Connection{{ID: "c", Node: "n"}, {User: "u", Node: "n"}, {User: "u", ID: "c"}} {
		if err := tr.Connect(c); !errors.Is(err, ErrInvalid) {
			t.Errorf("case %d: err = %v", i, err)
		}
	}
	if err := tr.Heartbeat("nobody", "c"); !errors.Is(err, ErrUnknownConnection) {
		t.Errorf("heartbeat unknown user: %v", err)
	}
	if err := tr.UpdatePosition("u", "c", Position{}); err == nil {
		t.Error("zero position time accepted")
	}
	if _, err := tr.Watch(""); err == nil {
		t.Error("empty watched user accepted")
	}
	w, _ := tr.Watch("a")
	w.Close()
	if err := w.Add("b"); err == nil {
		t.Error("add on closed watcher accepted")
	}
}

func TestLeaseExpiry(t *testing.T) {
	clk := newClock()
	tr := newTracker(t, clk, WithTTL(30*time.Second), WithOfflineGrace(0))
	must(t, tr.Connect(Connection{User: "u", ID: "c1", Node: "gw-1"}))

	steps := []struct {
		name     string
		advance  time.Duration
		action   func() error
		wantErr  error
		want     State
		wantConn int
	}{
		{"connected", 0, nil, nil, Online, 1},
		{"heartbeat at 20s", 20 * time.Second, func() error { return tr.Heartbeat("u", "c1") }, nil, Online, 1},
		{"still live at 49s", 29 * time.Second, nil, nil, Online, 1},
		{"expired at exactly 50s, no sweep needed", time.Second, nil, nil, Offline, 0},
		{"late heartbeat rejected", 0, func() error { return tr.Heartbeat("u", "c1") }, ErrUnknownConnection, Offline, 0},
		{"reconnect", 0, func() error { return tr.Connect(Connection{User: "u", ID: "c2", Node: "gw-2"}) }, nil, Online, 1},
	}
	for _, s := range steps {
		clk.Advance(s.advance)
		if s.action != nil {
			if err := s.action(); !errors.Is(err, s.wantErr) {
				t.Fatalf("%s: err = %v, want %v", s.name, err, s.wantErr)
			}
		}
		p := tr.Status("u")
		if p.State != s.want || p.Connections != s.wantConn || len(tr.Lookup("u")) != s.wantConn {
			t.Fatalf("%s: status %+v lookup %v", s.name, p, tr.Lookup("u"))
		}
	}
	if got := tr.Lookup("u"); got[0].Node != "gw-2" {
		t.Errorf("routing to %v, want gw-2", got)
	}
}

func TestLastSeen(t *testing.T) {
	clk := newClock()
	tr := newTracker(t, clk, WithTTL(30*time.Second), WithOfflineGrace(10*time.Second))
	start := clk.Now()
	must(t, tr.Connect(Connection{User: "u", ID: "c", Node: "n"}))
	clk.Advance(10 * time.Second)
	must(t, tr.Heartbeat("u", "c"))
	clk.Advance(5 * time.Minute) // lease expired long ago, then grace
	tr.Sweep()
	p := tr.Status("u")
	if p.State != Offline || !p.LastSeen.Equal(start.Add(10*time.Second)) {
		t.Errorf("status = %+v, want offline last seen at the last heartbeat", p)
	}
	must(t, tr.Connect(Connection{User: "u", ID: "c2", Node: "n"}))
	tr.Disconnect("u", "c2")
	if want := clk.Now(); !tr.Status("u").LastSeen.Equal(want) {
		t.Errorf("last seen after disconnect = %v, want %v", tr.Status("u").LastSeen, want)
	}
}

func TestFlapSuppression(t *testing.T) {
	for _, tt := range []struct {
		name        string
		grace       time.Duration
		gap         time.Duration
		wantChanges uint64
	}{
		{"no grace: every drop is announced", 0, 3 * time.Second, 2},
		{"reconnect inside grace: silent", 10 * time.Second, 3 * time.Second, 0},
		{"reconnect after grace: announced", 10 * time.Second, 12 * time.Second, 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			clk := newClock()
			tr := newTracker(t, clk, WithOfflineGrace(tt.grace))
			must(t, tr.Connect(Connection{User: "u", ID: "c1", Node: "gw-1"}))
			base := tr.Stats().StateChanges // the initial offline -> online
			tr.Disconnect("u", "c1")
			for step := time.Duration(0); step < tt.gap; step += time.Second {
				clk.Advance(time.Second)
				tr.Sweep()
			}
			must(t, tr.Connect(Connection{User: "u", ID: "c2", Node: "gw-3"}))
			if got := tr.Stats().StateChanges - base; got != tt.wantChanges {
				t.Errorf("state changes = %d, want %d", got, tt.wantChanges)
			}
		})
	}
}

func TestGraceStatusAndRouting(t *testing.T) {
	clk := newClock()
	tr := newTracker(t, clk, WithOfflineGrace(10*time.Second))
	must(t, tr.Connect(Connection{User: "u", ID: "c", Node: "n"}))
	tr.Disconnect("u", "c")
	clk.Advance(5 * time.Second)
	if st := tr.Status("u").State; st != Online {
		t.Errorf("during grace state = %v, want online", st)
	}
	if got := tr.Lookup("u"); len(got) != 0 {
		t.Errorf("during grace lookup = %v, want none", got)
	}
	clk.Advance(5 * time.Second)
	if st := tr.Status("u").State; st != Offline {
		t.Errorf("after grace state = %v, want offline", st)
	}
}

func TestMultiDeviceAndAway(t *testing.T) {
	clk := newClock()
	tr := newTracker(t, clk, WithTTL(30*time.Second), WithAwayAfter(time.Minute), WithOfflineGrace(0))
	must(t, tr.Connect(Connection{User: "u", ID: "phone", Device: "phone", Node: "gw-1"}))
	must(t, tr.Connect(Connection{User: "u", ID: "laptop", Device: "web", Node: "gw-2"}))
	if got := tr.Lookup("u"); len(got) != 2 || got[0].ID != "laptop" || got[1].Node != "gw-1" {
		t.Fatalf("lookup = %v", got)
	}
	tr.Disconnect("u", "phone")
	if p := tr.Status("u"); p.State != Online || p.Connections != 1 {
		t.Fatalf("one device left: %+v", p)
	}
	// The laptop keeps heartbeating but nobody touches it.
	for range 6 {
		clk.Advance(10 * time.Second)
		must(t, tr.Heartbeat("u", "laptop"))
	}
	if st := tr.Status("u").State; st != Away {
		t.Fatalf("idle laptop state = %v, want away", st)
	}
	// Phone reconnects and the user is active again: any active device wins.
	must(t, tr.Connect(Connection{User: "u", ID: "phone", Node: "gw-3"}))
	if st := tr.Status("u").State; st != Online {
		t.Fatalf("after phone reconnect state = %v", st)
	}
	tr.Disconnect("u", "phone")
	must(t, tr.Activity("u", "laptop"))
	if st := tr.Status("u").State; st != Online {
		t.Fatalf("after activity state = %v", st)
	}
	tr.Disconnect("u", "laptop")
	if st := tr.Status("u").State; st != Offline {
		t.Fatalf("no devices state = %v", st)
	}
}

func TestNodeCrashExpiry(t *testing.T) {
	clk := newClock()
	tr := newTracker(t, clk, WithTTL(30*time.Second), WithOfflineGrace(5*time.Second))
	for i := range 10 {
		node := fmt.Sprintf("gw-%d", i%2)
		must(t, tr.Connect(Connection{User: fmt.Sprint(i), ID: "c", Node: node}))
	}
	w, err := tr.Watch("0", "1", "2", "3")
	must(t, err)
	if evs := w.Drain(); len(evs) != 4 {
		t.Fatalf("initial events = %v", evs)
	}
	// gw-1 (odd users) crashes; gw-0 keeps heartbeating every 10s.
	for sec := 1; sec <= 40; sec++ {
		clk.Advance(time.Second)
		if sec%10 == 0 {
			for i := 0; i < 10; i += 2 {
				must(t, tr.Heartbeat(fmt.Sprint(i), "c"))
			}
		}
		tr.Sweep()
		if sec == 34 && tr.Count(Online) != 10 {
			t.Fatalf("at 34s (inside ttl+grace) online = %d, want 10", tr.Count(Online))
		}
	}
	if on, off := tr.Count(Online), tr.Count(Offline); on != 5 || off != 5 {
		t.Fatalf("after ttl+grace online=%d offline=%d", on, off)
	}
	evs := w.Drain()
	if len(evs) != 2 || evs[0].User != "1" || evs[1].User != "3" || evs[0].State != Offline || evs[0].Prev != Online {
		t.Fatalf("watcher events = %+v", evs)
	}
	if want := clk.Now().Add(-5 * time.Second); !evs[0].At.Equal(want) {
		t.Errorf("offline announced at %v, want ttl+grace = %v", evs[0].At, want)
	}
}

func TestWatcherCoalescing(t *testing.T) {
	clk := newClock()
	tr := newTracker(t, clk, WithOfflineGrace(0))
	w, err := tr.Watch("a", "b")
	must(t, err)
	if evs := w.Drain(); evs != nil {
		t.Fatalf("offline users produced %v", evs)
	}
	must(t, tr.Connect(Connection{User: "a", ID: "1", Node: "n"}))
	select {
	case <-w.Notify():
	default:
		t.Fatal("no notification")
	}
	// b flaps three times before the watcher looks.
	for i := range 3 {
		must(t, tr.Connect(Connection{User: "b", ID: fmt.Sprint(i), Node: "n"}))
		tr.Disconnect("b", fmt.Sprint(i))
	}
	must(t, tr.Connect(Connection{User: "c", ID: "1", Node: "n"})) // not watched
	evs := w.Drain()
	if len(evs) != 1 || evs[0].User != "a" || evs[0].Prev != Offline || evs[0].State != Online {
		t.Fatalf("events = %+v", evs)
	}
	// a goes offline and back: the watcher already shows online, so nothing.
	tr.Disconnect("a", "1")
	must(t, tr.Connect(Connection{User: "a", ID: "2", Node: "n"}))
	if evs := w.Drain(); evs != nil {
		t.Fatalf("round trip produced %v", evs)
	}
	if s := tr.Stats(); s.StateChanges != 10 || s.Delivered != 1 {
		t.Errorf("stats = %+v, want 10 changes, 1 delivered", s)
	}
	// Adding an online user delivers its current state; removing stops events.
	must(t, w.Add("c"))
	w.Remove("a")
	tr.Disconnect("a", "2")
	if evs := w.Drain(); len(evs) != 1 || evs[0].User != "c" || evs[0].State != Online {
		t.Fatalf("after add/remove events = %+v", evs)
	}
}

func TestPositions(t *testing.T) {
	clk := newClock()
	tr := newTracker(t, clk, WithTTL(30*time.Second))
	if _, err := tr.LatestPosition("rider", 0); !errors.Is(err, ErrNoPosition) {
		t.Fatalf("no position err = %v", err)
	}
	if err := tr.UpdatePosition("rider", "c", Position{At: clk.Now()}); !errors.Is(err, ErrUnknownConnection) {
		t.Fatalf("update without connection err = %v", err)
	}
	must(t, tr.Connect(Connection{User: "rider", ID: "c", Node: "n"}))
	t0 := clk.Now()
	must(t, tr.UpdatePosition("rider", "c", Position{Lat: 12.97, Lng: 77.59, At: t0}))
	clk.Advance(2 * time.Second)
	must(t, tr.UpdatePosition("rider", "c", Position{Lat: 12.98, Lng: 77.60, At: t0.Add(2 * time.Second)}))
	// A fix from t0+1s arrives late: it must not overwrite the newer one.
	if err := tr.UpdatePosition("rider", "c", Position{Lat: 1, Lng: 1, At: t0.Add(time.Second)}); !errors.Is(err, ErrStalePosition) {
		t.Fatalf("out-of-order fix err = %v", err)
	}
	p, err := tr.LatestPosition("rider", 10*time.Second)
	if err != nil || p.Lat != 12.98 {
		t.Fatalf("latest = %+v %v", p, err)
	}
	clk.Advance(20 * time.Second)
	if p, err := tr.LatestPosition("rider", 10*time.Second); !errors.Is(err, ErrStalePosition) || p.Lat != 12.98 {
		t.Fatalf("old fix = %+v %v, want ErrStalePosition with the value", p, err)
	}
	if _, err := tr.LatestPosition("rider", 0); err != nil {
		t.Fatalf("maxAge 0 disables the check: %v", err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	clk := newClock()
	tr := newTracker(t, clk, WithOfflineGrace(time.Second))
	w, err := tr.Watch("0", "1", "2", "3")
	must(t, err)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u := fmt.Sprint(g % 4)
			id := fmt.Sprint(g)
			for j := range 300 {
				_ = tr.Connect(Connection{User: u, ID: id, Node: "n"})
				_ = tr.Heartbeat(u, id)
				_ = tr.Activity(u, id)
				_ = tr.UpdatePosition(u, id, Position{At: clk.Now().Add(time.Duration(j) * time.Millisecond)})
				_ = tr.Lookup(u)
				_ = tr.Status(u)
				_, _ = tr.LatestPosition(u, time.Second)
				if j%3 == 0 {
					tr.Disconnect(u, id)
				}
				tr.Sweep()
				clk.Advance(time.Millisecond)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 300 {
			w.Drain()
		}
	}()
	wg.Wait()
	w.Drain()
	for g := range 8 {
		_ = tr.Connect(Connection{User: fmt.Sprint(g % 4), ID: fmt.Sprint(g), Node: "n"})
	}
	if n := tr.Count(Online); n != 4 {
		t.Errorf("online = %d, want 4", n)
	}
}
