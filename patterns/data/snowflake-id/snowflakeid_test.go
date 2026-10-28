package snowflakeid

import (
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
)

// pollClock advances by step every time it is read, so a generator waiting
// for the next millisecond always gets there.
type pollClock struct {
	mu    sync.Mutex
	t     time.Time
	step  time.Duration
	reads int
}

func newPollClock(step time.Duration) *pollClock {
	return &pollClock{t: DefaultEpoch.Add(time.Hour), step: step}
}

func (c *pollClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.t
	c.t = c.t.Add(c.step)
	c.reads++
	return t
}

func (c *pollClock) Set(t time.Time) { c.mu.Lock(); c.t = t; c.mu.Unlock() }

func (c *pollClock) Shift(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func TestNewValidation(t *testing.T) {
	now := func() time.Time { return DefaultEpoch.Add(time.Hour) }
	tests := []struct {
		name string
		node int64
		opts []Option
		ok   bool
	}{
		{"node 0", 0, nil, true},
		{"node 1023", 1023, nil, true},
		{"node -1", -1, nil, false},
		{"node 1024", 1024, nil, false},
		{"negative tolerance", 1, []Option{WithClockTolerance(-time.Millisecond)}, false},
		{"zero tolerance", 1, []Option{WithClockTolerance(0)}, true},
		{"epoch in the future", 1, []Option{WithEpoch(now().Add(time.Second))}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := append([]Option{WithClock(now)}, tt.opts...)
			_, err := New(tt.node, opts...)
			if (err == nil) != tt.ok {
				t.Fatalf("New(%d) err = %v, want ok=%v", tt.node, err, tt.ok)
			}
		})
	}
}

func TestComposeDecodeRoundTrip(t *testing.T) {
	tests := []struct{ ms, node, seq int64 }{
		{0, 0, 0},
		{1, 1, 1},
		{123456789, 42, 4095},
		{MaxTimestamp, MaxNode, MaxSequence},
		{1 << 40, 512, 2048},
	}
	for _, tt := range tests {
		id, err := Compose(tt.ms, tt.node, tt.seq)
		if err != nil {
			t.Fatal(err)
		}
		if id < 0 {
			t.Fatalf("sign bit set for %+v", tt)
		}
		if id.Millis() != tt.ms || id.Node() != tt.node || id.Seq() != tt.seq {
			t.Fatalf("decode %v = (%d,%d,%d), want %+v", id, id.Millis(), id.Node(), id.Seq(), tt)
		}
		back, err := Parse(id.String())
		if err != nil || back != id {
			t.Fatalf("Parse(String()) = %v, %v; want %v", back, err, id)
		}
	}
	for _, bad := range [][3]int64{{-1, 0, 0}, {MaxTimestamp + 1, 0, 0}, {0, 1024, 0}, {0, 0, 4096}} {
		if _, err := Compose(bad[0], bad[1], bad[2]); err == nil {
			t.Fatalf("Compose%v: expected error", bad)
		}
	}
	for _, s := range []string{"", "abc", "-5", "99999999999999999999"} {
		if _, err := Parse(s); err == nil {
			t.Fatalf("Parse(%q): expected error", s)
		}
	}
}

func TestGeneratedFieldsAndTime(t *testing.T) {
	epoch := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	at := epoch.Add(90*time.Minute + 1234*time.Millisecond)
	g, err := New(77, WithEpoch(epoch), WithClock(func() time.Time { return at }))
	if err != nil {
		t.Fatal(err)
	}
	a, _ := g.Next()
	b, _ := g.Next()
	if a.Node() != 77 || a.Seq() != 0 || b.Seq() != 1 {
		t.Fatalf("fields: a=(%d,%d) b seq %d", a.Node(), a.Seq(), b.Seq())
	}
	if !a.TimeFrom(epoch).Equal(at) {
		t.Fatalf("TimeFrom = %v, want %v", a.TimeFrom(epoch), at)
	}
	if want := epoch.Add(Lifetime); !g.EndOfLife().Equal(want) {
		t.Fatalf("EndOfLife = %v, want %v", g.EndOfLife(), want)
	}
	if y := Lifetime.Hours() / 24 / 365.25; y < 69 || y > 70 {
		t.Fatalf("lifetime %.1f years, want ~69.7", y)
	}
}

func TestDefaultEpochTime(t *testing.T) {
	at := DefaultEpoch.Add(48 * time.Hour)
	g, _ := New(1, WithClock(func() time.Time { return at }))
	id, _ := g.Next()
	if !id.Time().Equal(at) {
		t.Fatalf("Time() = %v, want %v", id.Time(), at)
	}
}

func TestMonotonicPerNode(t *testing.T) {
	clk := newPollClock(50 * time.Microsecond) // ~20 IDs per ms
	g, _ := New(3, WithClock(clk.Now))
	var prev ID = -1
	for i := range 20000 {
		id, err := g.Next()
		if err != nil {
			t.Fatal(err)
		}
		if id <= prev {
			t.Fatalf("id %d: %v not greater than %v", i, id, prev)
		}
		prev = id
	}
}

func TestUniqueAcrossGoroutines(t *testing.T) {
	const nodes, goroutinesPerNode, perGoroutine = 4, 4, 5000
	var (
		mu  sync.Mutex
		all = make([]ID, 0, nodes*goroutinesPerNode*perGoroutine)
		wg  sync.WaitGroup
	)
	for n := range nodes {
		clk := newPollClock(10 * time.Microsecond)
		g, err := New(int64(n), WithClock(clk.Now))
		if err != nil {
			t.Fatal(err)
		}
		for range goroutinesPerNode {
			wg.Add(1)
			go func() {
				defer wg.Done()
				local := make([]ID, 0, perGoroutine)
				for range perGoroutine {
					id, err := g.Next()
					if err != nil {
						t.Error(err)
						return
					}
					local = append(local, id)
				}
				mu.Lock()
				all = append(all, local...)
				mu.Unlock()
			}()
		}
	}
	wg.Wait()
	slices.Sort(all)
	if len(slices.Compact(all)) != nodes*goroutinesPerNode*perGoroutine {
		t.Fatal("duplicate IDs generated")
	}
}

func TestUniqueWithRealClock(t *testing.T) {
	g, _ := New(9)
	var wg sync.WaitGroup
	results := make([][]ID, 8)
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 2000 {
				id, err := g.Next()
				if err != nil {
					t.Error(err)
					return
				}
				results[i] = append(results[i], id)
			}
		}()
	}
	wg.Wait()
	all := slices.Concat(results...)
	slices.Sort(all)
	if len(slices.Compact(all)) != 8*2000 {
		t.Fatal("duplicate IDs with the real clock")
	}
}

func TestSequenceOverflowWaitsForNextMs(t *testing.T) {
	clk := newPollClock(100 * time.Nanosecond) // 10,000 reads per ms
	g, _ := New(5, WithClock(clk.Now))
	start := clk.Now().UnixMilli() - g.Epoch().UnixMilli()

	ids := make([]ID, 0, 10000)
	for range 10000 {
		id, err := g.Next()
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	perMs := map[int64]int{}
	for _, id := range ids {
		perMs[id.Millis()]++
	}
	for ms, n := range perMs {
		if n > MaxSequence+1 {
			t.Fatalf("ms %d has %d IDs, max is %d", ms, n, MaxSequence+1)
		}
	}
	if got := ids[4096]; got.Millis() != start+1 || got.Seq() != 0 {
		t.Fatalf("ID 4097 = (ms %d, seq %d), want (ms %d, seq 0)", got.Millis(), got.Seq(), start+1)
	}
	if s := g.Stats(); s.OverflowWaits != 2 || s.Generated != 10000 {
		t.Fatalf("stats = %+v, want 2 overflow waits", s)
	}
}

func TestClockBackwards(t *testing.T) {
	tests := []struct {
		name      string
		back      time.Duration
		tolerance time.Duration
		wantErr   bool
	}{
		{"small step waits", 3 * time.Millisecond, 10 * time.Millisecond, false},
		{"exactly the tolerance waits", 10 * time.Millisecond, 10 * time.Millisecond, false},
		{"large step refuses", time.Hour, 10 * time.Millisecond, true},
		{"zero tolerance refuses any step", time.Millisecond, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clk := newPollClock(250 * time.Microsecond)
			g, _ := New(1, WithClock(clk.Now), WithClockTolerance(tt.tolerance))
			var last ID
			for range 10 {
				last, _ = g.Next()
			}
			// The next read lands tt.back before the last issued timestamp.
			clk.Set(last.TimeFrom(g.Epoch()).Add(-tt.back))
			id, err := g.Next()
			s := g.Stats()
			if tt.wantErr {
				if !errors.Is(err, ErrClockMovedBackwards) {
					t.Fatalf("err = %v, want ErrClockMovedBackwards", err)
				}
				if s.BackwardsErrors != 1 {
					t.Fatalf("stats = %+v", s)
				}
				// Once the clock catches up, generation resumes and stays ordered.
				clk.Set(last.TimeFrom(g.Epoch()).Add(time.Millisecond))
				id, err = g.Next()
				if err != nil || id <= last {
					t.Fatalf("after recovery: %v, %v (last %v)", id, err, last)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if id <= last {
				t.Fatalf("id %v not after %v", id, last)
			}
			if s.BackwardsWaits != 1 {
				t.Fatalf("stats = %+v, want 1 backwards wait", s)
			}
		})
	}
}

func TestBeforeEpoch(t *testing.T) {
	clk := newPollClock(0)
	g, _ := New(1, WithClock(clk.Now), WithClockTolerance(0))
	clk.Set(DefaultEpoch.Add(-time.Second))
	// The first call has no previous timestamp, so this is an epoch error,
	// not a regression.
	if _, err := g.Next(); !errors.Is(err, ErrBeforeEpoch) {
		t.Fatalf("err = %v, want ErrBeforeEpoch", err)
	}
}

func TestTimestampOverflow(t *testing.T) {
	clk := newPollClock(0)
	g, _ := New(1, WithClock(clk.Now))
	clk.Set(g.EndOfLife())
	if _, err := g.Next(); !errors.Is(err, ErrTimestampOverflow) {
		t.Fatalf("err = %v, want ErrTimestampOverflow", err)
	}
}

func TestSortsByTimeAcrossNodes(t *testing.T) {
	// Two nodes with the same clock: the later ID always sorts later when
	// the milliseconds differ, whichever node made it.
	var ms int64
	now := func() time.Time { return DefaultEpoch.Add(time.Duration(ms) * time.Millisecond) }
	hi, _ := New(1000, WithClock(now))
	lo, _ := New(1, WithClock(now))
	ms = 5
	a, _ := hi.Next()
	ms = 6
	b, _ := lo.Next()
	if !(a < b) {
		t.Fatalf("earlier ID %v (node 1000) should sort before later %v (node 1)", a, b)
	}
}
