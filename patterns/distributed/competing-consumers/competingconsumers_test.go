package competingconsumers

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func newQ(t *testing.T, maxReceives int) (*Queue, *ManualClock) {
	t.Helper()
	clk := NewManualClock(time.Unix(0, 0))
	q, err := NewQueue(Config{VisibilityTimeout: 30 * time.Second, MaxReceives: maxReceives, Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	return q, clk
}

func TestConfigValidation(t *testing.T) {
	for _, cfg := range []Config{{}, {VisibilityTimeout: time.Second}, {MaxReceives: 1}} {
		if _, err := NewQueue(cfg); !errors.Is(err, ErrBadConfig) {
			t.Errorf("NewQueue(%+v) err=%v", cfg, err)
		}
	}
}

func TestReceiveHidesMessageUntilTimeout(t *testing.T) {
	q, clk := newQ(t, 5)
	q.Enqueue("job")
	d1, ok := q.Receive()
	if !ok || d1.Attempt != 1 {
		t.Fatalf("first receive: %+v %v", d1, ok)
	}
	if _, ok := q.Receive(); ok {
		t.Fatal("in-flight message must be invisible to other consumers")
	}
	clk.Advance(29 * time.Second)
	if _, ok := q.Receive(); ok {
		t.Fatal("still within visibility timeout")
	}
	clk.Advance(time.Second) // consumer 1 "crashed": timeout elapses
	d2, ok := q.Receive()
	if !ok || d2.Attempt != 2 || d2.MessageID != d1.MessageID {
		t.Fatalf("expected redelivery, got %+v %v", d2, ok)
	}
	if err := q.Ack(d1.Receipt); !errors.Is(err, ErrStaleReceipt) {
		t.Fatalf("stale consumer ack should fail, got %v", err)
	}
	if err := q.Ack(d2.Receipt); err != nil {
		t.Fatalf("current ack: %v", err)
	}
	if err := q.Ack(d2.Receipt); !errors.Is(err, ErrStaleReceipt) {
		t.Fatalf("double ack should fail, got %v", err)
	}
	if q.Len() != 0 {
		t.Fatalf("queue should be empty, len=%d", q.Len())
	}
}

func TestNackAndDeadLetter(t *testing.T) {
	tests := []struct {
		name        string
		maxReceives int
		failures    int
		wantDLQ     bool
	}{
		{"succeeds on last allowed attempt", 3, 2, false},
		{"poison message goes to DLQ", 3, 3, true},
		{"single attempt", 1, 1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, _ := newQ(t, tt.maxReceives)
			q.Enqueue("poison?")
			for i := 0; ; i++ {
				d, ok := q.Receive()
				if !ok {
					break
				}
				if i < tt.failures {
					if err := q.Nack(d.Receipt); err != nil {
						t.Fatal(err)
					}
					continue
				}
				if err := q.Ack(d.Receipt); err != nil {
					t.Fatal(err)
				}
			}
			dlq := q.DeadLetters()
			if (len(dlq) == 1) != tt.wantDLQ {
				t.Fatalf("dlq=%+v wantDLQ=%v", dlq, tt.wantDLQ)
			}
			if tt.wantDLQ && dlq[0].Receives != tt.maxReceives {
				t.Fatalf("dlq receives=%d", dlq[0].Receives)
			}
			if q.Len() != 0 {
				t.Fatalf("len=%d", q.Len())
			}
		})
	}
}

func TestFIFOAndSkipInvisible(t *testing.T) {
	q, _ := newQ(t, 3)
	q.Enqueue("a")
	q.Enqueue("b")
	da, _ := q.Receive()
	db, _ := q.Receive()
	if da.Body != "a" || db.Body != "b" {
		t.Fatalf("got %q, %q", da.Body, db.Body)
	}
	if err := q.Nack("bogus"); !errors.Is(err, ErrStaleReceipt) {
		t.Fatal("unknown receipt must be rejected")
	}
}

func TestWorkersProcessEachMessageOnce(t *testing.T) {
	q, err := NewQueue(Config{VisibilityTimeout: time.Minute, MaxReceives: 3})
	if err != nil {
		t.Fatal(err)
	}
	const n = 200
	for i := 0; i < n; i++ {
		q.Enqueue("job")
	}
	var mu sync.Mutex
	seen := map[string]int{}
	perWorker := map[int]int{}
	ctx, cancel := context.WithCancel(context.Background())
	wait := RunWorkers(ctx, q, 4, time.Millisecond, func(w int, d Delivery) error {
		mu.Lock()
		defer mu.Unlock()
		seen[d.MessageID]++
		perWorker[w]++
		if len(seen) == n {
			cancel()
		}
		return nil
	})
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Error("workers did not finish in time")
	}
	cancel()
	wait()
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("%s processed %d times", id, c)
		}
	}
	if len(seen) != n {
		t.Fatalf("processed %d of %d", len(seen), n)
	}
}

func TestWorkersRetryFailures(t *testing.T) {
	q, _ := NewQueue(Config{VisibilityTimeout: time.Minute, MaxReceives: 2})
	q.Enqueue("always-fails")
	q.Enqueue("ok")
	ctx, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	attempts := map[string]int{}
	wait := RunWorkers(ctx, q, 2, time.Millisecond, func(_ int, d Delivery) error {
		mu.Lock()
		attempts[d.Body]++
		mu.Unlock()
		if d.Body == "always-fails" {
			return errors.New("boom")
		}
		return nil
	})
	deadline := time.Now().Add(time.Second)
	for len(q.DeadLetters()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	wait()
	if dlq := q.DeadLetters(); len(dlq) != 1 || dlq[0].Body != "always-fails" {
		t.Fatalf("dlq=%+v", dlq)
	}
	if attempts["always-fails"] != 2 || attempts["ok"] != 1 {
		t.Fatalf("attempts=%v", attempts)
	}
}
