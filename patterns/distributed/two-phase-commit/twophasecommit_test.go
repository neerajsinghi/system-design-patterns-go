package twophasecommit

import (
	"context"
	"errors"
	"testing"
)

func setup() (*Store, *Store, *Coordinator) {
	a := NewStore("bank-a", map[string]int{"alice": 100})
	b := NewStore("bank-b", map[string]int{"bob": 0})
	c := NewCoordinator(map[string]Participant{"bank-a": a, "bank-b": b})
	return a, b, c
}

func transfer(amount int) map[string]map[string]int {
	return map[string]map[string]int{
		"bank-a": {"alice": 100 - amount},
		"bank-b": {"bob": amount},
	}
}

func get(t *testing.T, s *Store, k string) int {
	t.Helper()
	v, err := s.Get(k)
	if err != nil {
		t.Fatalf("get %s: %v", k, err)
	}
	return v
}

func TestExecute(t *testing.T) {
	tests := []struct {
		name      string
		amount    int
		downB     bool
		want      Decision
		wantAlice int
		wantBob   int
	}{
		{"all vote yes -> commit", 30, false, Commit, 70, 30},
		{"constraint violation -> abort", 150, false, Abort, 100, 0},
		{"participant unreachable in phase 1 -> abort", 30, true, Abort, 100, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, b, c := setup()
			b.SetDown(tt.downB)
			d, _ := c.Execute(context.Background(), "tx1", transfer(tt.amount))
			if d != tt.want {
				t.Fatalf("decision=%v want %v", d, tt.want)
			}
			b.SetDown(false)
			if err := c.Recover(context.Background()); err != nil {
				t.Fatal(err)
			}
			if get(t, a, "alice") != tt.wantAlice || get(t, b, "bob") != tt.wantBob {
				t.Fatalf("alice=%d bob=%d", get(t, a, "alice"), get(t, b, "bob"))
			}
			if len(a.InDoubt())+len(b.InDoubt()) != 0 {
				t.Fatal("nothing should remain in doubt")
			}
		})
	}
}

func TestCoordinatorCrashBlocksParticipants(t *testing.T) {
	a, b, c := setup()
	c.CrashAfterPrepare = func(tx string) bool { return tx == "tx1" }
	d, err := c.Execute(context.Background(), "tx1", transfer(30))
	if d != Undecided || !errors.Is(err, ErrCoordinatorCrashed) {
		t.Fatalf("d=%v err=%v", d, err)
	}
	// Both participants voted yes and now wait: they must not decide alone.
	if got := a.InDoubt(); len(got) != 1 || got[0] != "tx1" {
		t.Fatalf("a in doubt: %v", got)
	}
	if _, err := a.Get("alice"); !errors.Is(err, ErrLocked) {
		t.Fatalf("read of in-doubt key should block, got %v", err)
	}
	// Another transaction on the same key cannot proceed.
	d2, _ := c.Execute(context.Background(), "tx2", map[string]map[string]int{"bank-a": {"alice": 90}})
	if d2 != Abort {
		t.Fatalf("conflicting tx should abort, got %v", d2)
	}
	// Coordinator restarts: no decision logged -> presumed abort.
	if err := c.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.Decision("tx1") != Abort {
		t.Fatalf("tx1 decision=%v", c.Decision("tx1"))
	}
	if get(t, a, "alice") != 100 || get(t, b, "bob") != 0 || len(a.InDoubt()) != 0 {
		t.Fatal("recovery should abort and release locks")
	}
	// A late duplicate prepare for the aborted tx must vote no.
	if yes, _ := a.Prepare(context.Background(), "tx1", map[string]int{"alice": 70}); yes {
		t.Fatal("prepare after abort must vote no")
	}
}

func TestCommitDecisionSurvivesParticipantOutage(t *testing.T) {
	a, b, c := setup()
	// b prepares, then goes down before phase 2.
	c.CrashAfterPrepare = func(string) bool { b.SetDown(true); return false }
	d, err := c.Execute(context.Background(), "tx1", transfer(40))
	if d != Commit || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("d=%v err=%v", d, err)
	}
	if get(t, a, "alice") != 60 {
		t.Fatal("a should have committed")
	}
	b.SetDown(false)
	if got := b.InDoubt(); len(got) != 1 {
		t.Fatalf("b should still be in doubt: %v", got)
	}
	if err := c.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if get(t, b, "bob") != 40 {
		t.Fatal("recovery must re-deliver COMMIT, never abort a committed tx")
	}
}

func TestValidation(t *testing.T) {
	_, _, c := setup()
	ctx := context.Background()
	if _, err := c.Execute(ctx, "x", nil); err == nil {
		t.Fatal("empty tx")
	}
	if _, err := c.Execute(ctx, "x", map[string]map[string]int{"nope": {}}); !errors.Is(err, ErrUnknownParticipant) {
		t.Fatalf("unknown participant: %v", err)
	}
	if d, _ := c.Execute(ctx, "dup", transfer(1)); d != Commit {
		t.Fatal("first")
	}
	if _, err := c.Execute(ctx, "dup", transfer(1)); err == nil {
		t.Fatal("duplicate tx id must be rejected")
	}
	s := NewStore("s", nil)
	if err := s.Commit(ctx, "never-prepared"); !errors.Is(err, ErrUnknownTx) {
		t.Fatalf("commit unknown: %v", err)
	}
}
