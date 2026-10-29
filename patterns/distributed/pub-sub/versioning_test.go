package pubsub

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
)

// The order.placed schema history used by these tests:
//
//	v1: {order_id, amount}                          amount in dollars (float)
//	v2: {order_id, amount, amount_cents, currency}  expand: dual-write amount + amount_cents
//	v3: {order_id, amount_cents, currency}          contract: amount removed
type orderV3 struct {
	OrderID     string `json:"order_id"`
	AmountCents int64  `json:"amount_cents"`
	Currency    string `json:"currency"`
}

func centsFromAmount(f map[string]json.RawMessage) error {
	if _, ok := f["amount_cents"]; ok {
		return nil // producer already wrote it; never overwrite
	}
	var dollars float64
	if err := json.Unmarshal(f["amount"], &dollars); err != nil {
		return fmt.Errorf("amount: %w", err)
	}
	f["amount_cents"] = json.RawMessage(fmt.Sprint(int64(math.Round(dollars * 100))))
	return nil
}

func v1ToV2(f map[string]json.RawMessage) error {
	if _, ok := f["currency"]; !ok {
		f["currency"] = json.RawMessage(`"USD"`)
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

func orderRegistry(t *testing.T, withV1 bool) *Registry {
	t.Helper()
	r := NewRegistry()
	if err := r.Declare("order.placed", 3); err != nil {
		t.Fatal(err)
	}
	if withV1 {
		if err := r.Register("order.placed", 1, v1ToV2); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Register("order.placed", 2, v2ToV3); err != nil {
		t.Fatal(err)
	}
	return r
}

func mustEncode(t *testing.T, version int, payload any) []byte {
	t.Helper()
	b, err := Encode("order.placed", version, payload)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestUpcastChainToCurrent(t *testing.T) {
	r := orderRegistry(t, true)
	tests := []struct {
		name      string
		raw       []byte
		wantSteps int
		want      orderV3
	}{
		{"v1 goes through two steps", mustEncode(t, 1, map[string]any{"order_id": "o1", "amount": 12.5}),
			2, orderV3{"o1", 1250, "USD"}},
		{"v2 dual-write keeps producer's cents and currency", mustEncode(t, 2, map[string]any{
			"order_id": "o2", "amount": 9.99, "amount_cents": 999, "currency": "EUR"}),
			1, orderV3{"o2", 999, "EUR"}},
		{"v3 is already current", mustEncode(t, 3, orderV3{"o3", 500, "INR"}),
			0, orderV3{"o3", 500, "INR"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, steps, err := r.Decode(tt.raw)
			if err != nil {
				t.Fatal(err)
			}
			if steps != tt.wantSteps || e.Version != 3 {
				t.Fatalf("steps=%d version=%d", steps, e.Version)
			}
			var got orderV3
			if err := ReadStrict(e.Data, &got); err != nil { // v3 shape exactly: "amount" is gone
				t.Fatalf("upcast output is not clean v3: %v (%s)", err, e.Data)
			}
			if got != tt.want {
				t.Fatalf("got %+v want %+v", got, tt.want)
			}
		})
	}
	if st := r.Stats(); st.Decoded != 3 || st.Upcasted != 2 || st.Steps != 3 || st.Failed != 0 {
		t.Fatalf("stats %+v", st)
	}
}

func TestUnknownVersionTypeAndMissingUpcaster(t *testing.T) {
	full := orderRegistry(t, true)
	noV1 := orderRegistry(t, false)
	v1 := mustEncode(t, 1, map[string]any{"order_id": "o1", "amount": 1})
	v4 := mustEncode(t, 4, map[string]any{"order_id": "o4"})
	other, _ := Encode("refund.issued", 1, map[string]any{})
	tests := []struct {
		name string
		reg  *Registry
		raw  []byte
		want error
	}{
		{"future version from a newer producer", full, v4, ErrUnknownVersion},
		{"zero version", full, []byte(`{"type":"order.placed","version":0,"data":{}}`), ErrBadEnvelope},
		{"undeclared type", full, other, ErrUnknownEventType},
		{"consumer without v1 upcaster", noV1, v1, ErrMissingUpcaster},
		{"not json", full, []byte("nope"), ErrBadEnvelope},
		{"data not an object", full, []byte(`{"type":"order.placed","version":1,"data":[1]}`), ErrBadEnvelope},
	}
	for _, tt := range tests {
		if _, _, err := tt.reg.Decode(tt.raw); !errors.Is(err, tt.want) {
			t.Errorf("%s: got %v want %v", tt.name, err, tt.want)
		}
	}
	if st := noV1.Stats(); st.Failed != 1 {
		t.Fatalf("noV1 failed=%d", st.Failed)
	}
}

func TestRegistrationValidation(t *testing.T) {
	r := NewRegistry()
	if err := r.Declare("", 1); !errors.Is(err, ErrBadRegistration) {
		t.Errorf("empty type: %v", err)
	}
	if err := r.Register("x", 1, v1ToV2); !errors.Is(err, ErrUnknownEventType) {
		t.Errorf("undeclared: %v", err)
	}
	_ = r.Declare("x", 2)
	cases := []struct {
		from int
		up   Upcaster
	}{{0, v1ToV2}, {1, nil}, {2, v1ToV2}}
	for _, c := range cases {
		if err := r.Register("x", c.from, c.up); !errors.Is(err, ErrBadRegistration) {
			t.Errorf("Register(from=%d): %v", c.from, err)
		}
	}
	if err := r.Register("x", 1, v1ToV2); err != nil {
		t.Fatal(err)
	}
	if err := r.Register("x", 1, v1ToV2); !errors.Is(err, ErrBadRegistration) {
		t.Errorf("duplicate: %v", err)
	}
	if _, err := Encode("", 1, nil); !errors.Is(err, ErrBadEnvelope) {
		t.Errorf("encode empty type: %v", err)
	}
	if r.Current("x") != 2 || r.Current("y") != 0 {
		t.Fatal("Current")
	}
}

func TestUpcastIsIdempotent(t *testing.T) {
	r := orderRegistry(t, true)
	first, _, err := r.Decode(mustEncode(t, 1, map[string]any{"order_id": "o1", "amount": 3.1}))
	if err != nil {
		t.Fatal(err)
	}
	again, steps, err := r.Upcast(first)
	if err != nil || steps != 0 || string(again.Data) != string(first.Data) {
		t.Fatalf("re-upcast changed the event: steps=%d err=%v %s vs %s", steps, err, again.Data, first.Data)
	}
	// Running a step on fields that already have the new value is a no-op too.
	f := map[string]json.RawMessage{"order_id": []byte(`"o"`), "amount_cents": []byte(`7`), "currency": []byte(`"EUR"`)}
	_ = v1ToV2(f)
	_ = v2ToV3(f)
	if string(f["amount_cents"]) != "7" || string(f["currency"]) != `"EUR"` {
		t.Fatalf("upcaster overwrote producer fields: %v", f)
	}
}

func TestTolerantReader(t *testing.T) {
	// A v3 consumer receives a payload from a producer that has already added
	// a field (gift_wrap) and has not set currency.
	data := []byte(`{"order_id":"o9","amount_cents":100,"gift_wrap":true}`)
	got := orderV3{Currency: "USD"} // default for a missing field
	if err := ReadTolerant(data, &got); err != nil {
		t.Fatalf("tolerant reader rejected an extra field: %v", err)
	}
	if got != (orderV3{"o9", 100, "USD"}) {
		t.Fatalf("got %+v", got)
	}
	if err := ReadStrict(data, &orderV3{}); err == nil {
		t.Fatal("strict reader should reject the unknown field")
	}
	// Unknown fields also survive upcasting, so a downstream tolerant reader
	// that does know them can still use them.
	r := orderRegistry(t, true)
	e, _, err := r.Decode(mustEncode(t, 1, map[string]any{"order_id": "o1", "amount": 1, "gift_wrap": true}))
	if err != nil {
		t.Fatal(err)
	}
	var withExtra struct {
		GiftWrap bool `json:"gift_wrap"`
	}
	if err := json.Unmarshal(e.Data, &withExtra); err != nil || !withExtra.GiftWrap {
		t.Fatalf("unknown field lost in upcast: %s", e.Data)
	}
}

func TestVersionedEventsThroughBroker(t *testing.T) {
	b := NewBroker()
	defer b.Close()
	sub, _ := b.Subscribe("order.placed", 16, DropNewest)
	_, _ = b.Publish("order.placed", mustEncode(t, 1, map[string]any{"order_id": "a", "amount": 1}))
	_, _ = b.Publish("order.placed", mustEncode(t, 2, map[string]any{"order_id": "b", "amount": 2, "amount_cents": 200, "currency": "USD"}))
	r := orderRegistry(t, true)
	var sum int64
	for _, m := range drain(sub) {
		e, _, err := r.Decode(m.Payload.([]byte))
		if err != nil {
			t.Fatal(err)
		}
		var o orderV3
		_ = ReadTolerant(e.Data, &o)
		sum += o.AmountCents
	}
	if sum != 300 {
		t.Fatalf("sum=%d", sum)
	}
}

func TestConcurrentDecode(t *testing.T) {
	r := orderRegistry(t, true)
	raws := [][]byte{
		mustEncode(t, 1, map[string]any{"order_id": "a", "amount": 1}),
		mustEncode(t, 2, map[string]any{"order_id": "b", "amount": 2}),
		mustEncode(t, 3, orderV3{"c", 3, "USD"}),
	}
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 100 {
				if _, _, err := r.Decode(raws[(g+i)%3]); err != nil {
					t.Error(err)
					return
				}
			}
			_ = r.Declare("order.placed", 3) // concurrent re-declare is safe
		}()
	}
	wg.Wait()
	if st := r.Stats(); st.Decoded != 800 || st.Failed != 0 {
		t.Fatalf("stats %+v", st)
	}
}
