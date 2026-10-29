// Command example shows fan-out and the three slow-subscriber policies, then
// event versioning: producers on v1 and v2 of an event schema publishing to
// the same topic while a v3 consumer upcasts both.
package main

import (
	"encoding/json"
	"fmt"
	"math"

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

	versioning()
}

// order.placed schema history:
//
//	v1 {order_id, amount}                          amount in dollars
//	v2 {order_id, amount, amount_cents, currency}  expand: dual-write both amounts
//	v3 {order_id, amount_cents, currency}          contract: amount removed
type orderV3 struct {
	OrderID     string `json:"order_id"`
	AmountCents int64  `json:"amount_cents"`
	Currency    string `json:"currency"`
}

func centsFromAmount(f map[string]json.RawMessage) error {
	if _, ok := f["amount_cents"]; ok {
		return nil // a v2 producer dual-wrote it: keep the producer's value
	}
	var dollars float64
	if err := json.Unmarshal(f["amount"], &dollars); err != nil {
		return err
	}
	f["amount_cents"] = json.RawMessage(fmt.Sprint(int64(math.Round(dollars * 100))))
	return nil
}

func v1ToV2(f map[string]json.RawMessage) error {
	if _, ok := f["currency"]; !ok {
		f["currency"] = json.RawMessage(`"USD"`) // default for the added field
	}
	return centsFromAmount(f)
}

func v2ToV3(f map[string]json.RawMessage) error {
	if err := centsFromAmount(f); err != nil {
		return err
	}
	delete(f, "amount")
	return nil
}

func versioning() {
	fmt.Println("\n--- Event versioning: mixed producers during a rollout ---")
	b := pubsub.NewBroker()
	defer b.Close()
	upgraded, _ := b.Subscribe("order.placed", 16, pubsub.DropNewest)
	stale, _ := b.Subscribe("order.placed", 16, pubsub.DropNewest)

	full := pubsub.NewRegistry() // consumer on v3 with the whole chain
	_ = full.Declare("order.placed", 3)
	_ = full.Register("order.placed", 1, v1ToV2)
	_ = full.Register("order.placed", 2, v2ToV3)
	partial := pubsub.NewRegistry() // consumer on v3 that only shipped the v2->v3 upcaster
	_ = partial.Declare("order.placed", 3)
	_ = partial.Register("order.placed", 2, v2ToV3)

	fmt.Println("1. checkout-old (still v1) and checkout-new (v2, dual-writing amount and amount_cents) publish 6 orders:")
	for i := 1; i <= 6; i++ {
		var raw []byte
		if i%2 == 1 {
			raw, _ = pubsub.Encode("order.placed", 1, map[string]any{"order_id": fmt.Sprintf("o%d", i), "amount": 10.25 * float64(i)})
		} else {
			raw, _ = pubsub.Encode("order.placed", 2, map[string]any{
				"order_id": fmt.Sprintf("o%d", i), "amount": 10.25 * float64(i),
				"amount_cents": 1025 * i, "currency": "EUR", "coupon": "FALL10", // coupon: a field v3 does not know
			})
		}
		fmt.Printf("   %s\n", raw)
		_, _ = b.Publish("order.placed", raw)
	}

	consume := func(name string, sub *pubsub.Subscription, reg *pubsub.Registry) {
		var total int64
		for {
			select {
			case m := <-sub.C():
				e, steps, err := reg.Decode(m.Payload.([]byte))
				if err != nil {
					fmt.Printf("   %-8s #%d error: %v\n", name, m.Seq, err)
					continue
				}
				o := orderV3{Currency: "USD"} // tolerant reader: default first, unknown fields ignored
				_ = pubsub.ReadTolerant(e.Data, &o)
				total += o.AmountCents
				fmt.Printf("   %-8s #%d %s %d cents %s (upcast steps: %d)\n", name, m.Seq, o.OrderID, o.AmountCents, o.Currency, steps)
				continue
			default:
			}
			st := reg.Stats()
			fmt.Printf("   %s: decoded=%d upcasted=%d steps=%d errors=%d, total=%d cents\n", name, st.Decoded, st.Upcasted, st.Steps, st.Failed, total)
			return
		}
	}
	fmt.Println("2. consumer on v3 with upcasters v1->v2->v3:")
	consume("upgraded", upgraded, full)
	fmt.Println("3. consumer on v3 that forgot the v1->v2 upcaster:")
	consume("stale", stale, partial)

	fmt.Println("4. replaying a v1 event from last year's archive, and an event from a future v4 producer:")
	old, _ := pubsub.Encode("order.placed", 1, map[string]any{"order_id": "o-2025", "amount": 0.1})
	e, steps, err := full.Decode(old)
	fmt.Printf("   replayed v1 -> v%d after %d steps: %s (err=%v)\n", e.Version, steps, e.Data, err)
	again, steps, _ := full.Upcast(e)
	fmt.Printf("   upcasting it again: %d steps, unchanged=%v\n", steps, string(again.Data) == string(e.Data))
	future, _ := pubsub.Encode("order.placed", 4, map[string]any{"order_id": "o-next"})
	_, _, err = full.Decode(future)
	fmt.Printf("   v4 event: %v\n", err)
}
