package quorum

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func newStore(t *testing.T, n, r, w int) (*Store, []*Replica) {
	t.Helper()
	reps := make([]*Replica, n)
	for i := range reps {
		reps[i] = NewReplica(fmt.Sprintf("r%d", i))
	}
	s, err := NewStore(Config{N: n, R: r, W: w}, reps)
	if err != nil {
		t.Fatal(err)
	}
	return s, reps
}

func TestConfig(t *testing.T) {
	tests := []struct {
		cfg    Config
		valid  bool
		strong bool
	}{
		{Config{3, 2, 2}, true, true},
		{Config{3, 1, 3}, true, true},
		{Config{3, 1, 1}, true, false},
		{Config{3, 0, 2}, false, false},
		{Config{3, 4, 1}, false, false},
		{Config{0, 1, 1}, false, false},
	}
	for _, tt := range tests {
		err := tt.cfg.Validate()
		if (err == nil) != tt.valid {
			t.Errorf("%+v valid=%v err=%v", tt.cfg, tt.valid, err)
		}
		if tt.valid && tt.cfg.Strong() != tt.strong {
			t.Errorf("%+v strong=%v", tt.cfg, tt.cfg.Strong())
		}
	}
	if _, err := NewStore(Config{3, 2, 2}, []*Replica{NewReplica("a")}); !errors.Is(err, ErrBadConfig) {
		t.Fatal("replica count mismatch must fail")
	}
}

func TestStaleReplicaScenarios(t *testing.T) {
	tests := []struct {
		name      string
		r, w      int
		downOnV2  []int // replicas down during the second write
		readStart int
		want      string
	}{
		{"strong N3 R2 W2 reads latest despite stale replica", 2, 2, []int{2}, 2, "v2"},
		{"strong N3 R1 W3 never stale", 1, 3, nil, 1, "v2"},
		{"weak N3 R1 W1 can read stale", 1, 1, []int{1, 2}, 1, "v1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, reps := newStore(t, 3, tt.r, tt.w)
			if _, err := s.Put("c1", "k", "v1"); err != nil {
				t.Fatal(err)
			}
			for _, i := range tt.downOnV2 {
				reps[i].SetDown(true)
			}
			if _, err := s.Put("c1", "k", "v2"); err != nil {
				t.Fatal(err)
			}
			for _, i := range tt.downOnV2 {
				reps[i].SetDown(false)
			}
			got, err := s.GetFrom("k", tt.readStart)
			if err != nil || got.Data != tt.want {
				t.Fatalf("got %+v err=%v want %s", got, err, tt.want)
			}
		})
	}
}

func TestReadRepair(t *testing.T) {
	s, reps := newStore(t, 3, 2, 2)
	reps[2].SetDown(true)
	if _, err := s.Put("c1", "k", "v1"); err != nil {
		t.Fatal(err)
	}
	reps[2].SetDown(false)
	if v, _, _ := reps[2].Get("k"); v.Data != "" {
		t.Fatal("r2 should be missing the write")
	}
	if v, err := s.GetFrom("k", 2); err != nil || v.Data != "v1" {
		t.Fatalf("read: %+v %v", v, err)
	}
	if v, _, _ := reps[2].Get("k"); v.Data != "v1" || s.Repairs() != 1 {
		t.Fatalf("read repair failed: r2=%+v repairs=%d", v, s.Repairs())
	}
}

func TestQuorumFailures(t *testing.T) {
	s, reps := newStore(t, 3, 2, 2)
	reps[1].SetDown(true)
	reps[2].SetDown(true)
	if _, err := s.Put("c1", "k", "v1"); !errors.Is(err, ErrNoQuorum) {
		t.Fatalf("write with 1/3 up: %v", err)
	}
	if _, err := s.Get("k"); !errors.Is(err, ErrNoQuorum) {
		t.Fatalf("read with 1/3 up: %v", err)
	}

	// W=3 with one replica down: the write fails, but it still landed on
	// the two healthy replicas. A quorum failure is not a rollback.
	s, reps = newStore(t, 3, 1, 3)
	reps[2].SetDown(true)
	if _, err := s.Put("c1", "k", "v1"); !errors.Is(err, ErrNoQuorum) {
		t.Fatalf("write with 2/3 acks and W=3: %v", err)
	}
	if v, err := s.GetFrom("k", 0); err != nil || v.Data != "v1" {
		t.Fatalf("partial write should be visible: %+v %v", v, err)
	}
	if _, err := s.GetFrom("missing", 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing key: %v", err)
	}
}

func TestReplicaNeverRegresses(t *testing.T) {
	r := NewReplica("r")
	_ = r.Put("k", Value{"new", Version{5, "a"}})
	_ = r.Put("k", Value{"old", Version{4, "z"}})
	_ = r.Put("k", Value{"tie-lower", Version{5, "0"}})
	if v, _, _ := r.Get("k"); v.Data != "new" {
		t.Fatalf("regressed to %+v", v)
	}
}

func TestConcurrentWritersConverge(t *testing.T) {
	s, reps := newStore(t, 5, 3, 3)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := s.Put(fmt.Sprintf("w%02d", i), "k", fmt.Sprint(i)); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	// Read from every starting point so read repair touches all replicas.
	var first Value
	for start := 0; start < 5; start++ {
		v, err := s.GetFrom("k", start)
		if err != nil {
			t.Fatal(err)
		}
		if start == 0 {
			first = v
		} else if v != first {
			t.Fatalf("reads disagree: %+v vs %+v", v, first)
		}
	}
	for _, r := range reps {
		if v, _, _ := r.Get("k"); v != first {
			t.Fatalf("%s=%+v not converged to %+v", r.ID, v, first)
		}
	}
}
