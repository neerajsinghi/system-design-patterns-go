// Command example propagates one end-to-end deadline through three in-process
// "services" (gateway -> orders -> inventory), passing the remaining budget in
// a header the way gRPC passes grpc-timeout.
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/timeout"
)

// headers stands in for HTTP/gRPC metadata.
type headers map[string]string

func inventory(h headers, latency time.Duration) error {
	ctx, cancel := timeout.FromHeader(context.Background(), h[timeout.HeaderName])
	defer cancel()
	fmt.Printf("    inventory: received budget ~%sms, needs %v\n", roundMs(h[timeout.HeaderName]), latency)
	_, err := timeout.Run(ctx, func(ctx context.Context) (struct{}, error) {
		select {
		case <-time.After(latency):
			return struct{}{}, nil
		case <-ctx.Done():
			return struct{}{}, ctx.Err()
		}
	})
	return err
}

func orders(h headers, localWork, invLatency time.Duration) error {
	ctx, cancel := timeout.FromHeader(context.Background(), h[timeout.HeaderName])
	defer cancel()
	fmt.Printf("  orders: received budget ~%sms, doing %v of local work\n", roundMs(h[timeout.HeaderName]), localWork)
	time.Sleep(localWork)

	// Keep 20ms for building the response; refuse the hop if <10ms would remain.
	hctx, hcancel, err := timeout.WithHop(ctx, timeout.HopPolicy{Reserve: 20 * time.Millisecond, Min: 10 * time.Millisecond})
	defer hcancel()
	if err != nil {
		fmt.Println("  orders: under 10ms would remain for inventory, so not calling it")
		return err
	}
	v, _ := timeout.FormatRemaining(hctx, time.Now())
	return inventory(headers{timeout.HeaderName: v}, invLatency)
}

func gateway(name string, budget, localWork, invLatency time.Duration) {
	fmt.Printf("== %s: client allows %v end to end\n", name, budget)
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	v, _ := timeout.FormatRemaining(ctx, time.Now())
	start := time.Now()
	err := orders(headers{timeout.HeaderName: v}, localWork, invLatency)
	switch {
	case err == nil:
		fmt.Println("  gateway: 200 OK")
	case errors.Is(err, timeout.ErrInsufficientBudget):
		fmt.Println("  gateway: 504, failed fast without calling inventory")
	case errors.Is(err, context.DeadlineExceeded):
		fmt.Println("  gateway: 504, inventory exceeded its share of the budget")
	default:
		fmt.Println("  gateway: error:", err)
	}
	fmt.Printf("  answered within the client's %v budget: %v\n\n", budget, time.Since(start) <= budget+5*time.Millisecond)
}

// roundMs rounds a millisecond header value to the nearest 10ms for display
// only, so the printed narrative is stable across runs. The real value is what
// gets propagated: rounding it up would silently extend the deadline.
func roundMs(v string) string {
	var ms int
	_, _ = fmt.Sscan(v, &ms)
	return fmt.Sprint((ms + 5) / 10 * 10)
}

func main() {
	gateway("healthy", 200*time.Millisecond, 30*time.Millisecond, 20*time.Millisecond)
	gateway("slow inventory", 200*time.Millisecond, 30*time.Millisecond, 500*time.Millisecond)
	gateway("budget nearly spent", 60*time.Millisecond, 40*time.Millisecond, 20*time.Millisecond)
}
