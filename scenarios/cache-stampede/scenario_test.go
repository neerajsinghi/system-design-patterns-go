package cachestampede

import "testing"

// The invariant: after warm-up, the database is never asked for more than it
// can serve, and no user read fails.
func TestBrokenViolatesInvariant(t *testing.T) {
	r := Broken()
	if r.Peak() <= DBCapacity || r.FailedReads == 0 {
		t.Fatalf("broken run should overload the DB: peak %d, failed reads %d", r.Peak(), r.FailedReads)
	}
}

func TestFixedHoldsInvariant(t *testing.T) {
	r := Fixed()
	if r.Peak() > DBCapacity {
		t.Fatalf("fixed run peaked at %d queries/s, capacity is %d", r.Peak(), DBCapacity)
	}
	if r.FailedReads != 0 {
		t.Fatalf("fixed run failed %d reads", r.FailedReads)
	}
	if r.WaitedReads > Products/100 {
		t.Fatalf("fixed run made %d reads wait on the DB; early refresh should keep that near zero", r.WaitedReads)
	}
}
