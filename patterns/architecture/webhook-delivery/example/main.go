// Command example delivers a signed "payment.succeeded" webhook to a
// receiver that is briefly down, then shows a duplicate delivery and a
// forged request being handled safely.
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	wh "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/webhook-delivery"
)

func main() {
	secret := []byte("whsec_demo_shared_secret")
	var outage atomic.Int32
	outage.Store(2) // the receiver's database is down for the first 2 attempts

	receiver := &wh.Receiver{
		Secrets: [][]byte{secret},
		Handle: func(_ context.Context, id string, body []byte) error {
			if outage.Add(-1) >= 0 {
				return errors.New("database unavailable")
			}
			fmt.Printf("    receiver: processed %s %s (credit the merchant once)\n", id, body)
			return nil
		},
	}
	srv := httptest.NewServer(receiver)
	defer srv.Close()

	sender := &wh.Sender{
		Secret:    secret,
		BaseDelay: 20 * time.Millisecond,
		MaxDelay:  100 * time.Millisecond,
		Jitter:    func(d time.Duration) time.Duration { return d }, // no jitter, so output is predictable
		OnAttempt: func(attempt, status int, err error) {
			fmt.Printf("    sender: attempt %d -> status %d err=%v\n", attempt, status, err)
		},
	}
	body := []byte(`{"type":"payment.succeeded","amount":4200}`)

	fmt.Println("1) Deliver while the receiver is failing:")
	n, err := sender.Deliver(context.Background(), srv.URL, "evt_001", body)
	fmt.Printf("   delivered after %d attempts, err=%v\n\n", n, err)

	fmt.Println("2) Same event delivered again (for example, the 200 was lost on the way back):")
	n, err = sender.Deliver(context.Background(), srv.URL, "evt_001", body)
	fmt.Printf("   acknowledged in %d attempt, err=%v. Handler not called again.\n\n", n, err)

	fmt.Println("3) Attacker forges an event signed with a guessed secret:")
	req, _ := http.NewRequest(http.MethodPost, srv.URL, bytes.NewReader([]byte(`{"type":"payment.succeeded","amount":999999}`)))
	req.Header.Set(wh.HeaderID, "evt_evil")
	req.Header.Set(wh.HeaderSignature, wh.Sign([]byte("guessed-secret"), "evt_evil", time.Now().Unix(), body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	resp.Body.Close()
	fmt.Printf("   -> %d (rejected. A 4xx tells a real sender not to retry)\n\n", resp.StatusCode)

	fmt.Println("4) Captured, validly signed request replayed 10 minutes later:")
	stale := time.Now().Add(-10 * time.Minute).Unix()
	req, _ = http.NewRequest(http.MethodPost, srv.URL, bytes.NewReader(body))
	req.Header.Set(wh.HeaderID, "evt_002")
	req.Header.Set(wh.HeaderSignature, wh.Sign(secret, "evt_002", stale, body))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	resp.Body.Close()
	fmt.Printf("   -> %d (timestamp outside the 5 minute tolerance)\n", resp.StatusCode)
}
