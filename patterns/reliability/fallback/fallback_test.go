package fallback

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

var (
	errDown     = errors.New("primary down")
	errNotFound = errors.New("not found")
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Add(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func TestTiers(t *testing.T) {
	def := func(k string) (string, bool) {
		if k == "nodefault" {
			return "", false
		}
		return "default-" + k, true
	}
	tests := []struct {
		name       string
		warm       bool          // primary succeeded once before
		age        time.Duration // time since warm-up
		primaryErr error
		key        string
		opt        Options[string, string]
		wantSource Source
		wantValue  string
		wantErr    error
	}{
		{"primary ok", false, 0, nil, "a", Options[string, string]{MaxStale: time.Minute, Default: def}, Primary, "fresh-a", nil},
		{"stale within bound", true, 30 * time.Second, errDown, "a", Options[string, string]{MaxStale: time.Minute, Default: def}, Stale, "fresh-a", nil},
		{"stale at exact bound", true, time.Minute, errDown, "a", Options[string, string]{MaxStale: time.Minute, Default: def}, Stale, "fresh-a", nil},
		{"too stale uses default", true, 2 * time.Minute, errDown, "a", Options[string, string]{MaxStale: time.Minute, Default: def}, Default, "default-a", nil},
		{"never cached uses default", false, 0, errDown, "a", Options[string, string]{MaxStale: time.Minute, Default: def}, Default, "default-a", nil},
		{"stale disabled", true, time.Second, errDown, "a", Options[string, string]{Default: def}, Default, "default-a", nil},
		{"no tier available", false, 0, errDown, "nodefault", Options[string, string]{MaxStale: time.Minute, Default: def}, 0, "", errDown},
		{"no options at all", true, 0, errDown, "a", Options[string, string]{}, 0, "", errDown},
		{"ineligible error propagates", true, time.Second, errNotFound, "a", Options[string, string]{
			MaxStale: time.Minute, Default: def,
			ShouldFallback: func(err error) bool { return !errors.Is(err, errNotFound) },
		}, 0, "", errNotFound},
		{"caller cancelled propagates", true, time.Second, context.Canceled, "a", Options[string, string]{MaxStale: time.Minute, Default: def}, 0, "", context.Canceled},
		{"timeout falls back", true, time.Second, context.DeadlineExceeded, "a", Options[string, string]{MaxStale: time.Minute}, Stale, "fresh-a", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clk := &fakeClock{t: time.Unix(0, 0)}
			var fail error
			tt.opt.Now = clk.Now
			f := New(func(_ context.Context, k string) (string, error) {
				if fail != nil {
					return "", fail
				}
				return "fresh-" + k, nil
			}, tt.opt)
			if tt.warm {
				if _, err := f.Get(context.Background(), tt.key); err != nil {
					t.Fatal(err)
				}
			}
			clk.Add(tt.age)
			fail = tt.primaryErr
			res, err := f.Get(context.Background(), tt.key)
			if !errors.Is(err, tt.wantErr) || (tt.wantErr == nil && err != nil) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if res.Source != tt.wantSource || res.Value != tt.wantValue {
				t.Fatalf("got %v %q, want %v %q", res.Source, res.Value, tt.wantSource, tt.wantValue)
			}
			if res.Source != Primary && !errors.Is(res.Cause, tt.primaryErr) {
				t.Fatalf("cause = %v, want %v", res.Cause, tt.primaryErr)
			}
			if res.Source == Stale && res.Age != tt.age {
				t.Fatalf("age = %v, want %v", res.Age, tt.age)
			}
		})
	}
}

func TestEvictsOldest(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	var down bool
	f := New(func(_ context.Context, k int) (int, error) {
		if down {
			return 0, errDown
		}
		return k * 10, nil
	}, Options[int, int]{MaxStale: time.Hour, MaxEntries: 2, Now: clk.Now})
	for k := 1; k <= 3; k++ {
		_, _ = f.Get(context.Background(), k)
		clk.Add(time.Second)
	}
	down = true
	if _, err := f.Get(context.Background(), 1); err == nil {
		t.Fatal("key 1 should have been evicted")
	}
	for _, k := range []int{2, 3} {
		r, err := f.Get(context.Background(), k)
		if err != nil || r.Value != k*10 || r.Source != Stale {
			t.Fatalf("key %d: %+v %v", k, r, err)
		}
	}
}

func TestConcurrent(t *testing.T) {
	f := New(func(_ context.Context, k int) (string, error) {
		if k%2 == 0 {
			return "", errDown
		}
		return fmt.Sprint(k), nil
	}, Options[int, string]{MaxStale: time.Minute, MaxEntries: 8, Default: func(int) (string, bool) { return "d", true }})
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if _, err := f.Get(context.Background(), g*100+i); err != nil {
					t.Error(err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestSourceString(t *testing.T) {
	for s, want := range map[Source]string{Primary: "primary", Stale: "stale", Default: "default", 9: "unknown"} {
		if s.String() != want {
			t.Fatalf("%d.String() = %q", s, s.String())
		}
	}
}
