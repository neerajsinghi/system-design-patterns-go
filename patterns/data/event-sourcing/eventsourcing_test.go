package eventsourcing

import (
	"errors"
	"testing"
)

func newRepo(t *testing.T, every int) (*Store, *Repository) {
	t.Helper()
	s := NewStore()
	r, err := NewRepository(s, every)
	if err != nil {
		t.Fatal(err)
	}
	return s, r
}

func TestNewRepositoryRejectsNilStore(t *testing.T) {
	if _, err := NewRepository(nil, 10); err == nil {
		t.Fatal("expected error")
	}
}

func TestCommandsValidate(t *testing.T) {
	a := NewAccount("acc")
	tests := []struct {
		name string
		run  func() error
		want error
	}{
		{"deposit before open", func() error { return a.Deposit(10) }, ErrInvalid},
		{"open with empty owner", func() error { return a.Open("") }, ErrInvalid},
		{"open", func() error { return a.Open("ada") }, nil},
		{"open twice", func() error { return a.Open("ada") }, ErrInvalid},
		{"deposit zero", func() error { return a.Deposit(0) }, ErrInvalid},
		{"deposit", func() error { return a.Deposit(100) }, nil},
		{"overdraw", func() error { return a.Withdraw(101) }, ErrInsufficient},
		{"close non-empty", func() error { return a.Close() }, ErrInvalid},
		{"withdraw all", func() error { return a.Withdraw(100) }, nil},
		{"close", func() error { return a.Close() }, nil},
		{"deposit after close", func() error { return a.Deposit(1) }, ErrInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.run(); !errors.Is(err, tt.want) {
				t.Fatalf("got %v, want %v", err, tt.want)
			}
		})
	}
	if n := len(a.Pending()); n != 4 {
		t.Fatalf("pending=%d, want 4 (only successful commands emit events)", n)
	}
}

func TestSaveLoadRebuildsState(t *testing.T) {
	_, repo := newRepo(t, 0)
	a := NewAccount("acc")
	_ = a.Open("ada")
	_ = a.Deposit(100)
	_ = a.Withdraw(30)
	if err := repo.Save(a); err != nil {
		t.Fatal(err)
	}
	got, err := repo.Load("acc")
	if err != nil {
		t.Fatal(err)
	}
	want := State{ID: "acc", Owner: "ada", Balance: 70, Opened: true, Version: 3}
	if got.State != want {
		t.Fatalf("got %+v want %+v", got.State, want)
	}
	if _, err := repo.Load("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestOptimisticConcurrency(t *testing.T) {
	_, repo := newRepo(t, 0)
	a := NewAccount("acc")
	_ = a.Open("ada")
	_ = a.Deposit(100)
	_ = repo.Save(a)

	// Two "requests" load the same version and both try to withdraw 80.
	r1, _ := repo.Load("acc")
	r2, _ := repo.Load("acc")
	if err := r1.Withdraw(80); err != nil {
		t.Fatal(err)
	}
	if err := r2.Withdraw(80); err != nil {
		t.Fatal(err) // valid against r2's stale view
	}
	if err := repo.Save(r1); err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(r2); !errors.Is(err, ErrConcurrency) {
		t.Fatalf("second save: want ErrConcurrency, got %v", err)
	}
	// Retry: reload and re-run the command against fresh state.
	r2, _ = repo.Load("acc")
	if err := r2.Withdraw(80); !errors.Is(err, ErrInsufficient) {
		t.Fatalf("retry: want ErrInsufficient, got %v", err)
	}
}

func TestSnapshotsReduceReplay(t *testing.T) {
	tests := []struct {
		every    int
		wantRead uint64 // events read by a Load after 105 events saved one at a time
	}{
		{0, 105},   // no snapshots: full replay
		{10, 5},    // snapshot at v100, replay 101..105
		{50, 5},    // snapshot at v100
		{200, 105}, // boundary never crossed
	}
	for _, tt := range tests {
		s, repo := newRepo(t, tt.every)
		a := NewAccount("acc")
		_ = a.Open("ada")
		_ = repo.Save(a)
		for range 104 {
			_ = a.Deposit(1)
			if err := repo.Save(a); err != nil {
				t.Fatal(err)
			}
		}
		before := s.EventsRead()
		got, err := repo.Load("acc")
		if err != nil {
			t.Fatal(err)
		}
		if read := s.EventsRead() - before; read != tt.wantRead {
			t.Errorf("every=%d: read %d events, want %d", tt.every, read, tt.wantRead)
		}
		if got.Balance != 104 || got.Version != 105 {
			t.Errorf("every=%d: state %+v", tt.every, got.State)
		}
	}
}

func TestLoadAtTemporalQuery(t *testing.T) {
	_, repo := newRepo(t, 2)
	a := NewAccount("acc")
	_ = a.Open("ada")  // v1
	_ = a.Deposit(100) // v2
	_ = a.Withdraw(40) // v3
	_ = a.Deposit(5)   // v4
	_ = repo.Save(a)
	tests := []struct {
		version int
		balance int64
		wantErr bool
	}{
		{0, 0, true}, {1, 0, false}, {2, 100, false}, {3, 60, false}, {4, 65, false}, {99, 65, false},
	}
	for _, tt := range tests {
		got, err := repo.LoadAt("acc", tt.version)
		if (err != nil) != tt.wantErr {
			t.Fatalf("v%d: err=%v", tt.version, err)
		}
		if err == nil && got.Balance != tt.balance {
			t.Fatalf("v%d: balance %d want %d", tt.version, got.Balance, tt.balance)
		}
	}
}
