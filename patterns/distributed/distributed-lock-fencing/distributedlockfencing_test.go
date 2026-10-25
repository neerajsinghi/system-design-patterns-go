package distributedlockfencing

import (
	"errors"
	"sync"
	"testing"
	"time"
)

const ttl = 10 * time.Second

func TestLockLifecycle(t *testing.T) {
	clk := NewManualClock(time.Unix(0, 0))
	ls := NewLockService(clk)

	a, err := ls.Acquire("job", "A", ttl)
	if err != nil || a.Token != 1 {
		t.Fatalf("acquire: %+v %v", a, err)
	}
	if _, err := ls.Acquire("job", "B", ttl); !errors.Is(err, ErrLockHeld) {
		t.Fatalf("want held, got %v", err)
	}
	clk.Advance(ttl / 2)
	if a, err = ls.Renew(a, ttl); err != nil {
		t.Fatal(err)
	}
	clk.Advance(ttl - time.Second)
	if _, err := ls.Acquire("job", "B", ttl); !errors.Is(err, ErrLockHeld) {
		t.Fatal("renewal should have extended the lease")
	}
	if err := ls.Release(a); err != nil {
		t.Fatal(err)
	}
	b, err := ls.Acquire("job", "B", ttl)
	if err != nil || b.Token != 2 {
		t.Fatalf("after release: %+v %v", b, err)
	}
	if err := ls.Release(a); !errors.Is(err, ErrNotHolder) {
		t.Fatal("old grant must not release new holder's lock")
	}
}

func TestExpiredLeaseCannotRenew(t *testing.T) {
	clk := NewManualClock(time.Unix(0, 0))
	ls := NewLockService(clk)
	a, _ := ls.Acquire("job", "A", ttl)
	clk.Advance(ttl)
	if _, err := ls.Renew(a, ttl); !errors.Is(err, ErrNotHolder) {
		t.Fatalf("renew after expiry: %v", err)
	}
	// Re-acquiring, even by the same owner, issues a new token.
	a2, _ := ls.Acquire("job", "A", ttl)
	if a2.Token <= a.Token {
		t.Fatalf("token must increase: %d -> %d", a.Token, a2.Token)
	}
}

func TestFencing(t *testing.T) {
	tests := []struct {
		name    string
		writes  []uint64
		wantErr []bool
		want    string
	}{
		{"increasing tokens accepted", []uint64{1, 2, 3}, []bool{false, false, false}, "w2"},
		{"same holder writes repeatedly", []uint64{5, 5}, []bool{false, false}, "w1"},
		{"stale token rejected", []uint64{33, 34, 33}, []bool{false, false, true}, "w1"},
		{"zero token rejected", []uint64{0}, []bool{true}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewStorage()
			for i, tok := range tt.writes {
				err := s.Write("file", "w"+string(rune('0'+i)), tok)
				if (err != nil) != tt.wantErr[i] {
					t.Fatalf("write %d token %d: err=%v", i, tok, err)
				}
			}
			if got, _ := s.Read("file"); got != tt.want {
				t.Fatalf("value=%q want %q", got, tt.want)
			}
		})
	}
}

func TestPausedClientIsFenced(t *testing.T) {
	clk := NewManualClock(time.Unix(0, 0))
	ls := NewLockService(clk)
	st := NewStorage()

	a, _ := ls.Acquire("file", "A", ttl)
	clk.Advance(ttl + time.Second) // A pauses past its lease
	b, err := ls.Acquire("file", "B", ttl)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Write("file", "from B", b.Token); err != nil {
		t.Fatal(err)
	}
	if err := st.Write("file", "from A", a.Token); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("A must be fenced off, got %v", err)
	}
	if v, _ := st.Read("file"); v != "from B" {
		t.Fatalf("value=%q", v)
	}
}

func TestConcurrentAcquireSingleWinner(t *testing.T) {
	clk := NewManualClock(time.Unix(0, 0))
	ls := NewLockService(clk)
	var wg sync.WaitGroup
	var mu sync.Mutex
	winners := 0
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := ls.Acquire("job", string(rune('a'+i%26)), ttl); err == nil {
				mu.Lock()
				winners++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if winners != 1 {
		t.Fatalf("winners=%d", winners)
	}
}

func TestInvalidInput(t *testing.T) {
	ls := NewLockService(NewManualClock(time.Unix(0, 0)))
	for _, c := range []struct {
		name, owner string
		ttl         time.Duration
	}{{"", "a", ttl}, {"x", "", ttl}, {"x", "a", 0}} {
		if _, err := ls.Acquire(c.name, c.owner, c.ttl); !errors.Is(err, ErrInvalidInput) {
			t.Errorf("Acquire(%q,%q,%v) err=%v", c.name, c.owner, c.ttl, err)
		}
	}
}
