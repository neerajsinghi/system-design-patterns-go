// Command example shows fan-out and the three slow-subscriber policies.
package main

import (
	"fmt"

	pubsub "github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/pub-sub"
)

func main() {
	b := pubsub.NewBroker()
	defer b.Close()

	email, _ := b.Subscribe("order.placed", 10, pubsub.DropNewest)
	dashboard, _ := b.Subscribe("order.placed", 2, pubsub.DropOldest)
	legacy, _ := b.Subscribe("order.placed", 2, pubsub.Disconnect)

	fmt.Println("3 subscribers on order.placed: email(buf 10), dashboard(buf 2, drop-oldest), legacy(buf 2, disconnect)")
	fmt.Println("publishing 5 orders while nobody is reading...")
	for i := 1; i <= 5; i++ {
		st, _ := b.Publish("order.placed", fmt.Sprintf("order-%d", i))
		fmt.Printf("  publish order-%d -> delivered=%d dropped=%d evicted=%d\n", i, st.Delivered, st.Dropped, st.Evicted)
	}

	report := func(name string, s *pubsub.Subscription) {
		var got []string
		for {
			select {
			case m, ok := <-s.C():
				if !ok {
					fmt.Printf("%-9s received %v, then was disconnected (evicted=%v)\n", name, got, s.Evicted())
					return
				}
				got = append(got, fmt.Sprintf("#%d", m.Seq))
				continue
			default:
			}
			fmt.Printf("%-9s received %v, dropped=%d\n", name, got, s.Dropped())
			return
		}
	}
	report("email", email)
	report("dashboard", dashboard)
	report("legacy", legacy)
	fmt.Printf("active subscribers now: %d (the publisher was never blocked)\n", b.Subscribers("order.placed"))
}
