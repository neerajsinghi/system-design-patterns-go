// Command example runs an in-process payments API behind the idempotency
// middleware and replays the classic failure: the client's charge succeeds,
// the response is lost, and the client retries.
package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	idem "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/idempotency-key"
)

func main() {
	balance := 1000
	charges := 0
	api := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var amount int
		_, _ = fmt.Sscanf(readAll(r), `{"amount":%d}`, &amount)
		balance -= amount
		charges++
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, `{"charge_id":"ch_%d","amount":%d}`, charges, amount)
	})
	server := idem.Middleware(idem.NewStore(24*time.Hour, nil), 0, api)

	call := func(label, key, body string, lose bool) {
		req := httptest.NewRequest(http.MethodPost, "/v1/charges", strings.NewReader(body))
		if key != "" {
			req.Header.Set(idem.HeaderName, key)
		}
		rec := httptest.NewRecorder() // in-process: no network
		server.ServeHTTP(rec, req)
		if lose {
			fmt.Printf("%-34s -> server answered %d, but the response was lost (client timeout)\n", label, rec.Code)
			return
		}
		replayed := ""
		if rec.Header().Get("Idempotent-Replayed") == "true" {
			replayed = " [replayed]"
		}
		fmt.Printf("%-34s -> %d %s%s\n", label, rec.Code, strings.TrimSpace(rec.Body.String()), replayed)
	}

	fmt.Printf("starting balance: %d\n\n", balance)
	call("POST charge 100 (key=order-42)", "order-42", `{"amount":100}`, true)
	call("retry, same key", "order-42", `{"amount":100}`, false)
	call("retry again, same key", "order-42", `{"amount":100}`, false)
	call("same key, different amount (bug)", "order-42", `{"amount":500}`, false)
	call("new order (key=order-43)", "order-43", `{"amount":50}`, false)
	fmt.Printf("\nbalance: %d after %d real charges (without keys the retries would have charged 3x)\n", balance, charges)

	fmt.Println("\nWithout a key, every retry is a new charge:")
	call("POST charge 10, no key", "", `{"amount":10}`, false)
	call("retry, no key", "", `{"amount":10}`, false)
	fmt.Printf("balance: %d\n", balance)
}

func readAll(r *http.Request) string {
	b, _ := io.ReadAll(r.Body)
	return string(b)
}
