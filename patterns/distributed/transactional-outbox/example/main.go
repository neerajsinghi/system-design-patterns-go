// Command example: orders are written with their events in one transaction;
// the relay survives a broker outage and a crash that causes a duplicate.
package main

import (
	"context"
	"errors"
	"fmt"

	outbox "github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/transactional-outbox"
)

// broker is a toggleable broker in front of an idempotent consumer.
type broker struct {
	down     bool
	consumer *outbox.DedupConsumer
}

func (b *broker) Publish(ctx context.Context, m outbox.Message) error {
	if b.down {
		return errors.New("broker unreachable")
	}
	fmt.Printf("    broker delivers outbox#%d %s key=%s\n", m.ID, m.Topic, m.Key)
	return b.consumer.Publish(ctx, m)
}

func main() {
	ctx := context.Background()
	db := outbox.NewDB()
	consumer := &outbox.DedupConsumer{}
	b := &broker{down: true, consumer: consumer}
	relay := &outbox.Relay{DB: db, Publisher: b}

	for _, id := range []string{"order-1", "order-2"} {
		_ = outbox.PlaceOrder(db, outbox.Order{ID: id, Amount: 100})
		fmt.Printf("tx committed: %s row + outbox row\n", id)
	}
	if err := outbox.PlaceOrder(db, outbox.Order{ID: "order-1", Amount: 5}); err != nil {
		fmt.Printf("tx rolled back (%v): no row, no event\n", err)
	}

	fmt.Println("relay pass 1 (broker is down):")
	n, err := relay.RelayOnce(ctx)
	fmt.Printf("    published=%d err=%v; still pending=%d (nothing lost)\n", n, err, len(db.Pending(10)))

	b.down = false
	crashOnce := true
	relay.AfterPublish = func(m outbox.Message) error {
		if crashOnce {
			crashOnce = false
			return fmt.Errorf("relay crashed after publishing outbox#%d but before marking it", m.ID)
		}
		return nil
	}
	fmt.Println("relay pass 2 (broker up):")
	n, err = relay.RelayOnce(ctx)
	fmt.Printf("    published=%d err=%v\n", n, err)

	fmt.Println("relay restarts, pass 3:")
	n, err = relay.RelayOnce(ctx)
	fmt.Printf("    published=%d err=%v; pending=%d\n", n, err, len(db.Pending(10)))

	got, dupes := consumer.Snapshot()
	fmt.Printf("consumer processed %d unique events, ignored %d duplicate (at-least-once + dedup = effectively once)\n", len(got), dupes)
}
