package shardedcounters

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCounterConcurrentExact(t *testing.T) {
	tests := []struct {
		name    string
		shards  int
		workers int
		perG    int
		delta   int64
	}{
		{"single shard", 1, 8, 1000, 1},
		{"many shards", 16, 8, 1000, 1},
		{"default shards", 0, 4, 500, 3},
		{"negative deltas", 4, 4, 500, -2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := New(tc.shards)
			if tc.shards > 0 && c.Shards() != tc.shards {
				t.Fatalf("shards = %d", c.Shards())
			}
			var wg sync.WaitGroup
			for g := 0; g < tc.workers; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < tc.perG; i++ {
						c.Add(tc.delta)
					}
				}()
			}
			wg.Wait()
			want := int64(tc.workers*tc.perG) * tc.delta
			if got := c.Value(); got != want {
				t.Errorf("value = %d, want %d", got, want)
			}
		})
	}
}

func TestCounterSpreadsWrites(t *testing.T) {
	c := New(8)
	for i := 0; i < 8000; i++ {
		c.Inc()
	}
	for i := range c.shards {
		if n := c.shards[i].n.Load(); n < 700 || n > 1300 {
			t.Errorf("shard %d got %d of 8000 writes, expected about 1000", i, n)
		}
	}
}

func TestDistributed(t *testing.T) {
	store := NewMemStore(0)
	var next atomic.Int64
	roundRobin := func(n int) int { return int(next.Add(1)-1) % n }
	d, err := NewDistributed(store, "likes:post-1", 4, roundRobin)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err := d.Add(1); err != nil {
			t.Fatal(err)
		}
	}
	if v, _ := d.Value(); v != 10 {
		t.Errorf("value = %d", v)
	}
	// Writes landed on separate rows: 3,3,2,2.
	for i, want := range []int64{3, 3, 2, 2} {
		if got, _ := store.Get(d.row(i)); got != want {
			t.Errorf("row %d = %d, want %d", i, got, want)
		}
	}
	other, _ := NewDistributed(store, "likes:post-2", 4, nil)
	if v, _ := other.Value(); v != 0 {
		t.Errorf("counters must not share rows, got %d", v)
	}
}

func TestDistributedValidation(t *testing.T) {
	s := NewMemStore(0)
	for _, tc := range []struct {
		store  RowStore
		name   string
		shards int
	}{{s, "", 2}, {s, "x", 0}, {nil, "x", 2}} {
		if _, err := NewDistributed(tc.store, tc.name, tc.shards, nil); !errors.Is(err, ErrInvalid) {
			t.Errorf("%+v: err = %v", tc, err)
		}
	}
}

type failingStore struct{}

func (failingStore) Increment(string, int64) error { return nil }
func (failingStore) Get(string) (int64, error)     { return 0, errors.New("unavailable") }

func TestDistributedReadError(t *testing.T) {
	d, _ := NewDistributed(failingStore{}, "x", 2, nil)
	if _, err := d.Value(); err == nil {
		t.Error("expected error")
	}
}

// TestShardingRaisesWriteThroughput shows the point of the pattern. With a
// 2ms row-write cost, one row serialises 16 writers. Sixteen rows let them
// proceed in parallel.
func TestShardingRaisesWriteThroughput(t *testing.T) {
	run := func(shards int) time.Duration {
		d, _ := NewDistributed(NewMemStore(2*time.Millisecond), "c", shards, nil)
		start := time.Now()
		var wg sync.WaitGroup
		for g := 0; g < 16; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 5; i++ {
					_ = d.Add(1)
				}
			}()
		}
		wg.Wait()
		if v, _ := d.Value(); v != 80 {
			t.Errorf("value = %d", v)
		}
		return time.Since(start)
	}
	one, many := run(1), run(16)
	t.Logf("80 writes: 1 shard %v, 16 shards %v", one, many)
	if many*2 > one {
		t.Errorf("sharding should be well over 2x faster: 1 shard %v, 16 shards %v", one, many)
	}
}

func BenchmarkSingleAtomic(b *testing.B) {
	var n atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n.Add(1)
		}
	})
}

func BenchmarkShardedCounter(b *testing.B) {
	c := New(0)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Inc()
		}
	})
}
