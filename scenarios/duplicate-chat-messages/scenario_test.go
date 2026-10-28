package duplicatechat

import (
	"reflect"
	"testing"
	"time"

	snowflakeid "github.com/neerajsinghi/system-design-patterns-go/patterns/data/snowflake-id"
	idempotentconsumer "github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/idempotent-consumer"
)

// The invariant: every message Alice sends appears exactly once in Bob's
// conversation, and none is lost.
func TestBrokenViolatesInvariant(t *testing.T) {
	r := Broken()
	if r.Duplicates == 0 || r.MaxCopies < 3 {
		t.Errorf("broken run: %d duplicate bubbles, at most %d copies of one message; want duplicates and some message shown 3+ times", r.Duplicates, r.MaxCopies)
	}
	if r.Lost == 0 || r.LostBeforeServer == 0 || r.Lost == r.LostBeforeServer {
		t.Errorf("broken run lost %d messages (%d before the server); want losses on both legs", r.Lost, r.LostBeforeServer)
	}
	if r.ServerDedupHits != 0 || r.DeviceDedupHits != 0 {
		t.Errorf("broken run has no dedup, got %d server and %d device hits", r.ServerDedupHits, r.DeviceDedupHits)
	}
}

func TestFixedHoldsInvariant(t *testing.T) {
	r := Fixed()
	if r.Duplicates != 0 || r.Lost != 0 || r.Shown != Messages {
		t.Fatalf("fixed run: %d bubbles for %d messages, %d duplicates, %d lost", r.Shown, Messages, r.Duplicates, r.Lost)
	}
	if r.OutOfOrder != 0 {
		t.Errorf("fixed run shows %d messages out of send order", r.OutOfOrder)
	}
	if r.ClientGaveUp != 0 {
		t.Errorf("fixed run: %d messages never acked", r.ClientGaveUp)
	}
	// Both dedup layers did real work: the network really did duplicate.
	if r.ServerDedupHits == 0 || r.DeviceDedupHits == 0 {
		t.Errorf("fixed run: %d server and %d device dedup hits, want both > 0", r.ServerDedupHits, r.DeviceDedupHits)
	}
	if r.ServerStored != Messages || r.ServerReceived != Messages+r.ServerDedupHits {
		t.Errorf("fixed server stored %d of %d received (%d dedup hits), want exactly %d", r.ServerStored, r.ServerReceived, r.ServerDedupHits, Messages)
	}
}

// Backoff costs latency and some extra attempts: the fixed client makes more
// attempts than the broken one, because it keeps going instead of giving up.
func TestFixedRetriesLongerInsteadOfGivingUp(t *testing.T) {
	b, f := Broken(), Fixed()
	if f.ClientRetransmits <= b.ClientRetransmits || b.ClientGaveUp == 0 {
		t.Errorf("broken: %d retransmits, %d gave up; fixed: %d retransmits", b.ClientRetransmits, b.ClientGaveUp, f.ClientRetransmits)
	}
}

// The cost of server-side dedup is a store of recent IDs, and it only
// protects for as long as it remembers. A retransmission at exactly the end
// of the window is caught; one millisecond later the ID has been pruned and
// the copy is stored and fanned out again.
func TestDedupWindowBoundary(t *testing.T) {
	ingest := newServerInbox()
	m := idempotentconsumer.Message{ID: "42", Key: "alice->bob", Value: 7}
	at := func(d time.Duration) arrival { return arrival{at: start.Add(d), msg: m} }

	if stored, hit, size := ingest(at(0)); stored == nil || hit || size != 1 {
		t.Fatalf("first arrival: stored=%v hit=%v inbox=%d, want stored, no hit, inbox 1", stored != nil, hit, size)
	}
	if stored, hit, _ := ingest(at(DedupWindow)); stored != nil || !hit {
		t.Fatalf("retransmit at the window edge: stored=%v hit=%v, want deduplicated", stored != nil, hit)
	}
	if stored, hit, _ := ingest(at(DedupWindow + time.Millisecond)); stored == nil || hit {
		t.Fatalf("retransmit just past the window: stored=%v hit=%v, want it to slip through", stored != nil, hit)
	}
}

// So the window has to outlast the longest retransmit span, and the dedup
// store's size grows with window × message rate.
func TestDedupWindowCoversRetrySpan(t *testing.T) {
	if span := worstCaseRetrySpan(); span >= DedupWindow {
		t.Fatalf("a client can retransmit for %v, longer than the %v dedup window", span, DedupWindow)
	}
	r := Fixed()
	maxRate := float64(time.Second) / float64(MinGap)
	if limit := int(DedupWindow.Seconds()*maxRate) + 1; r.PeakServerInbox == 0 || r.PeakServerInbox > limit {
		t.Errorf("peak dedup store %d IDs, want 1..%d (window × max send rate)", r.PeakServerInbox, limit)
	}
}

// Sorting by Snowflake ID orders by the sender's clock. With one sender
// that is send order; across senders, a phone whose clock runs slow puts its
// reply above the message it answers.
func TestSnowflakeOrderTrustsSenderClocks(t *testing.T) {
	now := start
	aliceClock := func() time.Time { return now }
	bobClock := func() time.Time { return now.Add(-3 * time.Second) } // Bob's phone is 3s slow
	alice, err := snowflakeid.New(1, snowflakeid.WithClock(aliceClock))
	if err != nil {
		t.Fatal(err)
	}
	bob, err := snowflakeid.New(2, snowflakeid.WithClock(bobClock))
	if err != nil {
		t.Fatal(err)
	}
	question, _ := alice.Next()
	now = now.Add(time.Second) // Bob replies a second later
	reply, _ := bob.Next()
	if reply >= question {
		t.Fatalf("expected the skewed reply %v to sort before the question %v", reply, question)
	}
}

func TestDeterministic(t *testing.T) {
	if a, b := Broken(), Broken(); !reflect.DeepEqual(a, b) {
		t.Error("Broken is not deterministic")
	}
	if a, b := Fixed(), Fixed(); !reflect.DeepEqual(a, b) {
		t.Error("Fixed is not deterministic")
	}
}
