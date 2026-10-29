package optimisticlocking

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

var errTaken = errors.New("seat already taken")

// book returns an UpdateFunc that claims a free seat for buyer.
func book(buyer string) UpdateFunc[string] {
	return func(cur Versioned[string]) (string, error) {
		if cur.Value != "" {
			return "", errTaken
		}
		return buyer, nil
	}
}

func TestUpdateValidation(t *testing.T) {
	s := NewStore[string, string]()
	ctx := context.Background()
	tests := []struct {
		name string
		fn   UpdateFunc[string]
		max  int
	}{
		{"nil func", nil, 3},
		{"zero attempts", book("a"), 0},
		{"negative attempts", book("a"), -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := s.Update(ctx, "A1", tt.fn, tt.max); err == nil {
				t.Fatal("expected error")
			}
		})
	}
	if st := s.Stats(); st.Reads != 0 || st.Writes != 0 {
		t.Fatalf("invalid calls touched the store: %+v", st)
	}
}

func TestVersionIncrements(t *testing.T) {
	s := NewStore[string, int]()
	if got := s.Get("k"); got.Version != 0 || got.Value != 0 {
		t.Fatalf("missing key = %+v, want zero value at version 0", got)
	}
	steps := []struct {
		name     string
		expected uint64
		value    int
		wantVer  uint64
		conflict bool
	}{
		{"create at version 0", 0, 10, 1, false},
		{"create again conflicts", 0, 99, 1, true},
		{"update at current version", 1, 11, 2, false},
		{"stale version conflicts", 1, 99, 2, true},
		{"update again", 2, 12, 3, false},
	}
	for _, st := range steps {
		v, err := s.CompareAndSet("k", st.expected, st.value)
		if st.conflict != errors.Is(err, ErrConflict) {
			t.Fatalf("%s: err = %v, conflict want %v", st.name, err, st.conflict)
		}
		if v != st.wantVer {
			t.Fatalf("%s: version = %d, want %d", st.name, v, st.wantVer)
		}
	}
	if got := s.Get("k"); got.Value != 12 || got.Version != 3 {
		t.Fatalf("final = %+v, want 12 at version 3", got)
	}
	if got := s.Put("k", 13); got != 4 {
		t.Fatalf("Put version = %d, want 4", got)
	}
	st := s.Stats()
	if st.Writes != 4 || st.Conflicts != 2 {
		t.Fatalf("stats = %+v, want 4 writes, 2 conflicts", st)
	}
}

// TestABA: the value returns to what a writer saw, but the version does not,
// so a writer acting on an old read is still rejected.
func TestABA(t *testing.T) {
	s := NewStore[string, string]()
	stale := s.Get("A1") // free, version 0
	v1, _ := s.CompareAndSet("A1", 0, "carol")
	if _, err := s.CompareAndSet("A1", v1, ""); err != nil { // Carol cancels: free again
		t.Fatal(err)
	}
	if got := s.Get("A1"); got.Value != stale.Value {
		t.Fatalf("value = %q, want it back to %q", got.Value, stale.Value)
	}
	if _, err := s.CompareAndSet("A1", stale.Version, "bob"); !errors.Is(err, ErrConflict) {
		t.Fatalf("write based on the pre-ABA read: err = %v, want ErrConflict", err)
	}
}

func TestUpdateRetriesAfterConflict(t *testing.T) {
	s := NewStore[string, int]()
	calls := 0
	fn := func(cur Versioned[int]) (int, error) {
		calls++
		if calls == 1 {
			s.Put("n", 100) // another writer sneaks in after our read
		}
		return cur.Value + 1, nil
	}
	res, err := s.Update(context.Background(), "n", fn, 3)
	if err != nil {
		t.Fatal(err)
	}
	if res.Attempts != 2 || res.Value != 101 || res.Version != 2 {
		t.Fatalf("result = %+v, want 101 at version 2 after 2 attempts", res)
	}
}

func TestUpdateBoundRespected(t *testing.T) {
	for _, max := range []int{1, 3, 10} {
		t.Run(fmt.Sprint(max), func(t *testing.T) {
			s := NewStore[string, int]()
			calls := 0
			fn := func(cur Versioned[int]) (int, error) {
				calls++
				s.Put("n", -1) // someone always writes between our read and our CAS
				return cur.Value + 1, nil
			}
			_, err := s.Update(context.Background(), "n", fn, max)
			if !errors.Is(err, ErrTooManyConflicts) || !errors.Is(err, ErrConflict) {
				t.Fatalf("err = %v, want ErrTooManyConflicts wrapping ErrConflict", err)
			}
			if calls != max {
				t.Fatalf("fn called %d times, want %d", calls, max)
			}
			if c := s.Stats().Conflicts; c != uint64(max) {
				t.Fatalf("conflicts = %d, want %d", c, max)
			}
		})
	}
}

func TestUpdateAbortPropagates(t *testing.T) {
	s := NewStore[string, string]()
	ctx := context.Background()
	if _, err := s.Update(ctx, "A1", book("alice"), 3); err != nil {
		t.Fatal(err)
	}
	_, err := s.Update(ctx, "A1", book("bob"), 3)
	if !errors.Is(err, errTaken) {
		t.Fatalf("err = %v, want errTaken", err)
	}
	if got := s.Get("A1"); got.Value != "alice" || got.Version != 1 {
		t.Fatalf("seat = %+v, want alice at version 1", got)
	}
}

func TestUpdateContextCancelled(t *testing.T) {
	s := NewStore[string, string]()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Update(ctx, "A1", book("alice"), 3); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if got := s.Get("A1"); got.Version != 0 {
		t.Fatalf("cancelled update wrote: %+v", got)
	}
}

func TestNaivePutLosesUpdate(t *testing.T) {
	// Both buyers read before either writes: blind writes both "succeed".
	s := NewStore[string, string]()
	a, b := s.Get("A1"), s.Get("A1")
	if a.Value != "" || b.Value != "" {
		t.Fatal("seat should start free")
	}
	s.Put("A1", "alice")
	s.Put("A1", "bob")
	if got := s.Get("A1").Value; got != "bob" {
		t.Fatalf("owner = %q; alice's booking should have been overwritten", got)
	}
	// The same interleaving with CompareAndSet rejects the second writer.
	s2 := NewStore[string, string]()
	a, b = s2.Get("A1"), s2.Get("A1")
	if _, err := s2.CompareAndSet("A1", a.Version, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.CompareAndSet("A1", b.Version, "bob"); !errors.Is(err, ErrConflict) {
		t.Fatalf("bob's write: err = %v, want ErrConflict", err)
	}
}

func TestExactlyOneWinner(t *testing.T) {
	const buyers = 200
	s := NewStore[string, string]()
	var wins, taken atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range buyers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := s.Update(context.Background(), "A1", book(fmt.Sprint("buyer-", i)), buyers)
			switch {
			case err == nil:
				wins.Add(1)
			case errors.Is(err, errTaken):
				taken.Add(1)
			default:
				t.Errorf("buyer %d: unexpected error %v", i, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if wins.Load() != 1 || taken.Load() != buyers-1 {
		t.Fatalf("wins = %d, taken = %d; want exactly 1 winner", wins.Load(), taken.Load())
	}
	if got := s.Get("A1"); got.Version != 1 || got.Value == "" {
		t.Fatalf("seat = %+v, want one owner at version 1", got)
	}
	if st := s.Stats(); st.Writes != 1 {
		t.Fatalf("writes = %d, want 1", st.Writes)
	}
}

func TestConcurrentCounterNoLostUpdates(t *testing.T) {
	// With an unbounded-enough retry budget, every increment lands exactly once.
	const workers, each = 16, 50
	s := NewStore[string, int]()
	incr := func(cur Versioned[int]) (int, error) { return cur.Value + 1, nil }
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range each {
				if _, err := s.Update(context.Background(), "n", incr, 1_000_000); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	got := s.Get("n")
	if got.Value != workers*each || got.Version != workers*each {
		t.Fatalf("counter = %+v, want %d at version %d", got, workers*each, workers*each)
	}
}
