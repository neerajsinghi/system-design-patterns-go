// Command example reproduces the classic failure: client A takes the lock,
// stalls in a long GC pause, its lease expires, client B takes the lock and
// writes, then A wakes up and writes. The storage rejects A by fencing token.
package main

import (
	"fmt"
	"time"

	fencing "github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/distributed-lock-fencing"
)

func main() {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := fencing.NewManualClock(start)
	locks := fencing.NewLockService(clk)
	store := fencing.NewStorage()
	const ttl = 10 * time.Second
	at := func() string { return fmt.Sprintf("t=%2ds", int(clk.Now().Sub(start).Seconds())) }

	// Warm up so tokens look realistic.
	for i := 0; i < 32; i++ {
		l, _ := locks.Acquire("report.csv", "warmup", ttl)
		_ = locks.Release(l)
	}

	a, _ := locks.Acquire("report.csv", "client-A", ttl)
	fmt.Printf("%s client-A acquires lock, fencing token %d, lease until +%ds\n", at(), a.Token, int(a.Expires.Sub(start).Seconds()))
	fmt.Printf("%s client-A starts a stop-the-world GC pause...\n", at())

	clk.Advance(5 * time.Second)
	if _, err := locks.Acquire("report.csv", "client-B", ttl); err != nil {
		fmt.Printf("%s client-B tries the lock: %v\n", at(), err)
	}
	clk.Advance(10 * time.Second)
	b, _ := locks.Acquire("report.csv", "client-B", ttl)
	fmt.Printf("%s client-A's lease expired; client-B acquires lock, token %d\n", at(), b.Token)
	fmt.Printf("%s client-B writes with token %d: %v\n", at(), b.Token, errText(store.Write("report.csv", "B's version", b.Token)))

	clk.Advance(2 * time.Second)
	fmt.Printf("%s client-A wakes up, still thinks it holds the lock, writes with token %d: %v\n",
		at(), a.Token, errText(store.Write("report.csv", "A's version", a.Token)))
	_, err := locks.Renew(a, ttl)
	fmt.Printf("%s client-A tries to renew: %v\n", at(), errText(err))

	v, _ := store.Read("report.csv")
	fmt.Printf("final content: %q (without fencing it would be A's stale version)\n", v)
}

func errText(err error) string {
	if err == nil {
		return "accepted"
	}
	return "REJECTED (" + err.Error() + ")"
}
