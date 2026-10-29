package videopipeline

import (
	"os"
	"testing"
	"time"
)

// The invariant: every upload is encoded exactly once, no upload request is
// held until the load balancer cuts it, a long video finishes within
// LongVideoBound × (its encode work / Workers), and a crash only redoes the
// chunks that were in flight.
func TestBrokenViolatesInvariant(t *testing.T) {
	r := Broken()
	if r.DuplicateEncodes() == 0 {
		t.Error("broken run should start duplicate encodes when the app retries")
	}
	if r.Timeouts == 0 || r.MaxHeld < LBTimeout {
		t.Errorf("broken run should hit the LB timeout: %d timeouts, longest held %v", r.Timeouts, r.MaxHeld)
	}
	if r.LongWorst <= LongVideoBound {
		t.Errorf("broken long videos took %.1fx their ideal, want > %.1fx", r.LongWorst, LongVideoBound)
	}
	if r.KeynoteRedone < 5*time.Hour-time.Minute {
		t.Errorf("broken keynote crash lost %v, want about 5h (restart from zero)", r.KeynoteRedone)
	}
}

func TestFixedHoldsInvariant(t *testing.T) {
	r := Fixed()
	if r.Encodes != Uploads || r.DuplicateEncodes() != 0 {
		t.Errorf("fixed run started %d encodes for %d uploads", r.Encodes, Uploads)
	}
	if r.Requests <= Uploads {
		t.Errorf("some acks should have been lost and retried: %d requests", r.Requests)
	}
	if r.Timeouts != 0 || r.MaxHeld >= LBTimeout {
		t.Errorf("fixed run: %d timeouts, longest request held %v", r.Timeouts, r.MaxHeld)
	}
	if r.LongWorst > LongVideoBound {
		t.Errorf("a long video took %.2fx its ideal, want <= %.1fx", r.LongWorst, LongVideoBound)
	}
	// A crash redoes at most the chunks in flight on the dead worker(s): one
	// per worker crash, and at most one per worker for the coordinator crash.
	if r.ChunksRedone > len(Crashes) {
		t.Errorf("%d chunks lost to %d worker crashes", r.ChunksRedone, len(Crashes))
	}
	if r.KeynoteInFlightAtCrash > Workers || r.KeynoteRedone > time.Duration(Workers)*maxAttempt() {
		t.Errorf("keynote coordinator crash redid %d chunks (%v)", r.KeynoteInFlightAtCrash, r.KeynoteRedone)
	}
	if r.KeynoteChunksResumed+r.KeynoteInFlightAtCrash > r.KeynoteChunks || r.KeynoteChunksResumed == 0 {
		t.Errorf("keynote checkpoint: %d of %d chunks resumed", r.KeynoteChunksResumed, r.KeynoteChunks)
	}
	if r.CallbacksSent != Uploads {
		t.Errorf("callbacks sent: %d, want %d", r.CallbacksSent, Uploads)
	}
}

func TestFixedIsFasterForEveryPercentile(t *testing.T) {
	b, f := Broken(), Fixed()
	for _, p := range []float64{50, 95, 100} {
		if Percentile(f.Ready, p) >= Percentile(b.Ready, p) {
			t.Errorf("p%.0f time to ready: fixed %v, broken %v", p, Percentile(f.Ready, p), Percentile(b.Ready, p))
		}
	}
}

// The fix's costs: polling clients learn late, chunking adds per-chunk
// overhead and a merge, and speculation spends worker time on copies.
func TestFixedCosts(t *testing.T) {
	r := Fixed()
	if max := Percentile(r.PollLatency, 100); max <= 0 || max > PollMax {
		t.Errorf("worst polling latency %v, want in (0, %v]", max, PollMax)
	}
	if r.Polls <= r.Requests {
		t.Errorf("polling should cost more requests than the submits: %d polls", r.Polls)
	}
	// Per-chunk overhead: the cluster spends more than the bare encode work.
	if r.Busy <= r.Useful {
		t.Errorf("busy %v should exceed useful work %v (per-chunk overhead)", r.Busy, r.Useful)
	}
	if r.MergeTail <= 0 || r.KeynoteProcessing <= r.KeynoteIdeal {
		t.Errorf("keynote: merge tail %v, processing %v vs ideal %v", r.MergeTail, r.KeynoteProcessing, r.KeynoteIdeal)
	}

	noSpec := fixed(false)
	if r.Backups == 0 || noSpec.Backups != 0 {
		t.Fatalf("backups: %d with speculation, %d without", r.Backups, noSpec.Backups)
	}
	if r.Busy <= noSpec.Busy {
		t.Errorf("speculation should cost worker time: busy %v with, %v without", r.Busy, noSpec.Busy)
	}
	if noSpec.LongWorst <= r.LongWorst {
		t.Errorf("speculation should help long videos: worst %.2fx with, %.2fx without", r.LongWorst, noSpec.LongWorst)
	}
	if testing.Verbose() {
		noSpec.Print(os.Stdout)
	}
}

func TestDeterministic(t *testing.T) {
	a, b := Fixed(), Fixed()
	for i := range a.Ready {
		if a.Ready[i] != b.Ready[i] || a.PollLatency[i] != b.PollLatency[i] {
			t.Fatalf("upload %d differs between runs", i)
		}
	}
	if a.Polls != b.Polls || a.Busy != b.Busy {
		t.Fatalf("runs differ: polls %d/%d, busy %v/%v", a.Polls, b.Polls, a.Busy, b.Busy)
	}
}
