package twotier

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (f *fakeClock) Now() time.Time          { f.mu.Lock(); defer f.mu.Unlock(); return f.t }
func (f *fakeClock) Advance(d time.Duration) { f.mu.Lock(); f.t = f.t.Add(d); f.mu.Unlock() }

type db struct {
	mu    sync.Mutex
	rows  map[string]string
	loads atomic.Int64
}

func (d *db) load(_ context.Context, k string) (string, error) {
	d.loads.Add(1)
	d.mu.Lock()
	defer d.mu.Unlock()
	v, ok := d.rows[k]
	if !ok {
		return "", errors.New("not found")
	}
	return v, nil
}

func (d *db) set(k, v string) { d.mu.Lock(); d.rows[k] = v; d.mu.Unlock() }

func TestValidation(t *testing.T) {
	if _, err := NewShared[string](0, time.Minute, nil); err == nil {
		t.Fatal("expected error for zero nodes")
	}
	if _, err := NewShared[string](1, 0, nil); err == nil {
		t.Fatal("expected error for zero ttl")
	}
	s, _ := NewShared[string](1, time.Minute, nil)
	load := func(context.Context, string) (string, error) { return "", nil }
	tests := []struct {
		name  string
		s     *Shared[string]
		load  Loader[string]
		l1TTL time.Duration
		opts  []Option
	}{
		{"nil shared", nil, load, time.Second, nil},
		{"nil loader", s, nil, time.Second, nil},
		{"negative l1 ttl", s, load, -time.Second, nil},
		{"zero replicas", s, load, time.Second, []Option{WithHotKeys(0, "k")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewClient(tt.s, tt.load, tt.l1TTL, tt.opts...); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestReadPath(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	d := &db{rows: map[string]string{"k": "v1"}}
	shared, _ := NewShared[string](3, time.Hour, clk.Now)
	c, _ := NewClient(shared, d.load, time.Second, WithClock(clk.Now))
	ctx := context.Background()

	steps := []struct {
		name    string
		advance time.Duration
		want    Stats
	}{
		{"cold read loads and fills both tiers", 0, Stats{Loads: 1}},
		{"next read is an L1 hit", 0, Stats{Loads: 1, L1Hits: 1}},
		{"after L1 expiry, L2 serves it", time.Second, Stats{Loads: 1, L1Hits: 1, L2Hits: 1}},
		{"L1 was refilled from L2", 500 * time.Millisecond, Stats{Loads: 1, L1Hits: 2, L2Hits: 1}},
	}
	for _, s := range steps {
		clk.Advance(s.advance)
		v, err := c.Get(ctx, "k")
		if err != nil || v != "v1" || c.Stats() != s.want {
			t.Fatalf("%s: got (%q, %v) stats %+v, want %+v", s.name, v, err, c.Stats(), s.want)
		}
	}
}

func TestLoaderErrorIsNotCached(t *testing.T) {
	d := &db{rows: map[string]string{}}
	shared, _ := NewShared[string](1, time.Hour, nil)
	c, _ := NewClient(shared, d.load, time.Second)
	ctx := context.Background()
	if _, err := c.Get(ctx, "missing"); err == nil {
		t.Fatal("expected loader error")
	}
	d.set("missing", "now here")
	if v, err := c.Get(ctx, "missing"); err != nil || v != "now here" {
		t.Fatalf("got (%q, %v), want the newly written row", v, err)
	}
}

func TestReplicaKeysLandOnDistinctNodes(t *testing.T) {
	shared, _ := NewShared[string](4, time.Hour, nil)
	load := func(context.Context, string) (string, error) { return "", nil }
	tests := []struct {
		replicas, wantNodes int
	}{
		{1, 1}, {3, 3}, {4, 4}, {10, 4}, // can't use more nodes than exist
	}
	for _, tt := range tests {
		c, _ := NewClient(shared, load, 0, WithHotKeys(tt.replicas, "viral"))
		nodes := map[int]bool{}
		for _, k := range c.hot["viral"] {
			nodes[shared.NodeFor(k)] = true
		}
		if len(nodes) != tt.wantNodes || len(c.hot["viral"]) != tt.wantNodes {
			t.Fatalf("replicas=%d: keys %v cover %d nodes, want %d", tt.replicas, c.hot["viral"], len(nodes), tt.wantNodes)
		}
	}
}

func TestHotKeyReadsSpreadAcrossNodes(t *testing.T) {
	d := &db{rows: map[string]string{"viral": "x"}}
	shared, _ := NewShared[string](3, time.Hour, nil)
	turn := 0
	roundRobin := func(n int) int { turn++; return turn % n }
	c, _ := NewClient(shared, d.load, 0, WithHotKeys(3, "viral"), WithPicker(roundRobin))
	ctx := context.Background()
	c.Get(ctx, "viral") // load: writes all three replicas
	before := shared.Ops()
	for range 300 {
		c.Get(ctx, "viral")
	}
	after := shared.Ops()
	for n := range after {
		if got := after[n] - before[n]; got != 100 {
			t.Fatalf("node %d served %d reads, want 100 (ops %v)", n, got, after)
		}
	}
	if d.loads.Load() != 1 {
		t.Fatalf("db loads = %d, want 1", d.loads.Load())
	}
}

func TestInvalidateLeavesOtherServersStaleUntilL1TTL(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	d := &db{rows: map[string]string{"k": "old"}}
	shared, _ := NewShared[string](2, time.Hour, clk.Now)
	writer, _ := NewClient(shared, d.load, time.Second, WithClock(clk.Now), WithHotKeys(2, "k"))
	reader, _ := NewClient(shared, d.load, time.Second, WithClock(clk.Now), WithHotKeys(2, "k"))
	ctx := context.Background()
	writer.Get(ctx, "k")
	reader.Get(ctx, "k")

	d.set("k", "new")
	writer.Invalidate("k")

	if v, _ := writer.Get(ctx, "k"); v != "new" {
		t.Fatalf("writer read %q after its own invalidate, want new", v)
	}
	if v, _ := reader.Get(ctx, "k"); v != "old" {
		t.Fatalf("reader read %q inside its L1 TTL, want the stale old value", v)
	}
	clk.Advance(time.Second)
	if v, _ := reader.Get(ctx, "k"); v != "new" {
		t.Fatalf("reader read %q after its L1 TTL, want new", v)
	}
}

func TestConcurrentClients(t *testing.T) {
	d := &db{rows: map[string]string{"a": "1", "b": "2"}}
	shared, _ := NewShared[string](3, time.Hour, nil)
	var wg sync.WaitGroup
	for range 4 {
		c, _ := NewClient(shared, d.load, time.Millisecond, WithHotKeys(3, "a"))
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range 500 {
					k := "a"
					if i%5 == 0 {
						k = "b"
					}
					if v, err := c.Get(context.Background(), k); err != nil || v == "" {
						t.Errorf("Get(%s) = (%q, %v)", k, v, err)
						return
					}
					if i%100 == 0 {
						c.Invalidate(k)
					}
				}
			}()
		}
	}
	wg.Wait()
}
