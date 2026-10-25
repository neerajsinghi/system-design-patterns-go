package transactionaloutbox

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// flakyBroker fails the first `failures` publishes, then forwards to next.
type flakyBroker struct {
	mu       sync.Mutex
	failures int
	next     Publisher
}

func (b *flakyBroker) Publish(ctx context.Context, m Message) error {
	b.mu.Lock()
	if b.failures > 0 {
		b.failures--
		b.mu.Unlock()
		return errors.New("broker unavailable")
	}
	b.mu.Unlock()
	return b.next.Publish(ctx, m)
}

func TestTransactionIsAtomic(t *testing.T) {
	tests := []struct {
		name        string
		order       Order
		wantErr     bool
		wantOrders  int
		wantPending int
	}{
		{"valid order writes row and event", Order{"o1", 10}, false, 1, 1},
		{"invalid amount writes nothing", Order{"o2", 0}, true, 0, 0},
		{"empty id writes nothing", Order{"", 5}, true, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := NewDB()
			err := PlaceOrder(db, tt.order)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err=%v", err)
			}
			_, ok := db.Order(tt.order.ID)
			if ok != (tt.wantOrders == 1) || len(db.Pending(10)) != tt.wantPending {
				t.Fatalf("order present=%v pending=%d", ok, len(db.Pending(10)))
			}
		})
	}
}

func TestRollbackDiscardsOutbox(t *testing.T) {
	db := NewDB()
	if err := PlaceOrder(db, Order{"o1", 10}); err != nil {
		t.Fatal(err)
	}
	if err := PlaceOrder(db, Order{"o1", 99}); !errors.Is(err, ErrDuplicateOrder) {
		t.Fatalf("want duplicate error, got %v", err)
	}
	err := db.WithTx(func(tx *Tx) error {
		tx.PutOrder(Order{"o3", 1})
		tx.AddOutbox("order.placed", "o3", "{}")
		return errors.New("business rule failed")
	})
	if err == nil {
		t.Fatal("expected rollback error")
	}
	if _, ok := db.Order("o3"); ok || len(db.Pending(10)) != 1 {
		t.Fatalf("rolled-back tx leaked writes: pending=%d", len(db.Pending(10)))
	}
	if o, _ := db.Order("o1"); o.Amount != 10 {
		t.Fatalf("duplicate overwrote order: %+v", o)
	}
}

func TestRelayAtLeastOnce(t *testing.T) {
	tests := []struct {
		name          string
		brokerFails   int
		crashOnFirst  bool
		passes        int
		wantProcessed int
		wantDupes     int
	}{
		{"happy path", 0, false, 1, 3, 0},
		{"broker down then recovers", 2, false, 3, 3, 0},
		{"crash after publish causes duplicate", 0, true, 2, 3, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := NewDB()
			for _, id := range []string{"a", "b", "c"} {
				if err := PlaceOrder(db, Order{id, 1}); err != nil {
					t.Fatal(err)
				}
			}
			consumer := &DedupConsumer{}
			crashed := false
			r := &Relay{DB: db, Publisher: &flakyBroker{failures: tt.brokerFails, next: consumer}}
			if tt.crashOnFirst {
				r.AfterPublish = func(Message) error {
					if !crashed {
						crashed = true
						return errors.New("relay crashed before marking")
					}
					return nil
				}
			}
			for i := 0; i < tt.passes; i++ {
				_, _ = r.RelayOnce(context.Background())
			}
			got, dupes := consumer.Snapshot()
			if len(got) != tt.wantProcessed || dupes != tt.wantDupes {
				t.Fatalf("processed=%d dupes=%d", len(got), dupes)
			}
			for i, m := range got {
				if m.ID != int64(i+1) {
					t.Fatalf("out of order: %+v", got)
				}
			}
			if p := db.Pending(10); len(p) != 0 {
				t.Fatalf("pending left: %+v", p)
			}
		})
	}
}

func TestRelayRunStopsOnCancel(t *testing.T) {
	db := NewDB()
	consumer := &DedupConsumer{}
	r := &Relay{DB: db, Publisher: consumer}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx, time.Millisecond, nil); close(done) }()

	_ = PlaceOrder(db, Order{"late", 7})
	deadline := time.Now().Add(time.Second)
	for {
		if got, _ := consumer.Snapshot(); len(got) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("relay did not publish")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
}
