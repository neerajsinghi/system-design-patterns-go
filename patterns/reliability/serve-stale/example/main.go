// Command example simulates a product catalogue whose origin goes down for
// two minutes. It compares a plain TTL cache with a serve-stale cache, and
// shows how a retry interval keeps the failing origin from being hammered.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	servestale "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/serve-stale"
)

const (
	products  = 20
	ttl       = 30 * time.Second
	outageAt  = 45 * time.Second
	outageEnd = 165 * time.Second
	duration  = 240 * time.Second
)

type tally struct{ fresh, stale, errs int }

func simulate(staleIfError time.Duration, opts ...servestale.Option) ([]tally, int) {
	var mu sync.Mutex
	now := time.Unix(0, 0)
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	elapsed := func() time.Duration { return clock().Sub(time.Unix(0, 0)) }

	originCallsDuringOutage := 0
	load := func(_ context.Context, id int) (string, error) {
		if t := elapsed(); t >= outageAt && t < outageEnd {
			originCallsDuringOutage++
			return "", errors.New("origin: 503 service unavailable")
		}
		return fmt.Sprintf("product %d", id), nil
	}
	opts = append([]servestale.Option{servestale.WithClock(clock)}, opts...)
	cache, err := servestale.New(load, ttl, staleIfError, opts...)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	buckets := make([]tally, duration/(30*time.Second))
	for elapsed() < duration {
		b := &buckets[elapsed()/(30*time.Second)]
		for id := range products {
			r, err := cache.Get(ctx, id)
			switch {
			case err != nil:
				b.errs++
			case r.Stale:
				b.stale++
			default:
				b.fresh++
			}
		}
		mu.Lock()
		now = now.Add(time.Second)
		mu.Unlock()
	}
	return buckets, originCallsDuringOutage
}

func report(title string, buckets []tally, originCalls int) {
	fmt.Println(title)
	for i, b := range buckets {
		start, end := time.Duration(i)*30*time.Second, time.Duration(i+1)*30*time.Second
		marker := ""
		if start < outageEnd && end > outageAt {
			marker = "  <- origin down"
		}
		fmt.Printf("   t=%3d-%3ds  ok %4d  stale %4d  errors %4d%s\n",
			int(start.Seconds()), int(end.Seconds()), b.fresh, b.stale, b.errs, marker)
	}
	fmt.Printf("   origin calls during the outage: %d\n\n", originCalls)
}

func main() {
	fmt.Printf("%d products, each read once a second, TTL %v; the origin is down from %v to %v\n\n",
		products, ttl, outageAt, outageEnd)

	b, calls := simulate(0)
	report("1) Plain TTL cache: entries expire during the outage and users get errors", b, calls)

	b, calls = simulate(5 * time.Minute)
	report("2) Serve-stale (5m grace): users keep seeing the last good page, flagged stale", b, calls)

	b, calls = simulate(5*time.Minute, servestale.WithRetryInterval(10*time.Second))
	report("3) Serve-stale + 10s retry interval: same answers, far fewer calls to the failing origin", b, calls)
}
