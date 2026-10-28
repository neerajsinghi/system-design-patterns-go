// Command example runs a payments stream through an at-least-once log with
// lost acks and consumer crashes, and compares three consumers: commit the
// offset before the side effect, commit it after, and the inbox pattern
// (side effect and processed-ID insert in one transaction). It then shows
// what a too-short dedup window does when old messages are replayed.
package main

import (
	"fmt"
	"log"
	"math/rand/v2"
	"time"

	ic "github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/idempotent-consumer"
)

const (
	payments   = 1000
	accounts   = 8
	partitions = 4
	crashEvery = 40 // on average, one crash per this many records
)

func credit(tx *ic.Tx, m ic.Message) error {
	tx.Add(m.Key, m.Value)
	return nil
}

func must[T any](v T, err error) T {
	if err != nil {
		log.Fatal(err)
	}
	return v
}

func main() {
	// 1) Producer side: client-assigned IDs and retries until acked.
	rng := rand.New(rand.NewPCG(7, 42))
	lg := must(ic.NewLog(partitions))
	lostAcks, lostReqs := 0, 0
	prod := must(ic.NewProducer(lg, "payments-api", ic.WithFaults(func(m ic.Message, attempt int) ic.Fault {
		switch r := rng.Float64(); {
		case r < 0.03:
			lostAcks++
			return ic.LoseAck // appended, but the producer never hears so
		case r < 0.05:
			lostReqs++
			return ic.LoseRequest
		}
		return ic.Delivered
	})))
	var ids []string
	var want int64
	for i := range payments {
		amount := int64(1 + rng.IntN(100))
		m := must(prod.Send(fmt.Sprintf("acct-%d", i%accounts), amount))
		ids = append(ids, m.ID)
		want += amount
	}
	records := int64(0)
	for p := range partitions {
		records += lg.End(p)
	}
	copies := map[string]int{}
	for p := range partitions {
		for _, m := range must(lg.Read(p, 0, int(lg.End(p)))) {
			copies[m.ID]++
		}
	}
	dupIDs := 0
	for _, n := range copies {
		if n > 1 {
			dupIDs++
		}
	}
	ps := prod.Stats()
	fmt.Printf("1) Producer sent %d payments (IDs like %q) in %d attempts: %d lost requests, %d lost acks.\n",
		ps.Sent, ids[0], ps.Attempts, lostReqs, lostAcks)
	fmt.Printf("   Every lost ack made the producer resend a message the log already had:\n")
	fmt.Printf("   the log holds %d records for %d payments: %d payments appear more than once\n",
		records, payments, dupIDs)
	fmt.Printf("   (some lost their ack twice) before any consumer has run.\n\n")

	// 2) The same crash schedule for every strategy.
	var crashes []ic.Crash
	points := []ic.CrashPoint{ic.BeforeEffect, ic.AfterEffect, ic.BeforeCommit, ic.AfterCommit}
	crashRng := rand.New(rand.NewPCG(1, 2))
	for p := range partitions {
		for off := range lg.End(p) {
			if crashRng.IntN(crashEvery) == 0 {
				crashes = append(crashes, ic.Crash{Partition: p, Offset: off, Point: points[crashRng.IntN(len(points))]})
			}
		}
	}
	count := map[ic.CrashPoint]int{}
	for _, c := range crashes {
		count[c.Point]++
	}
	fmt.Printf("2) %d injected consumer crashes: %d before-effect, %d after-effect, %d before-commit, %d after-commit.\n",
		len(crashes), count[ic.BeforeEffect], count[ic.AfterEffect], count[ic.BeforeCommit], count[ic.AfterCommit])
	fmt.Printf("   Expected: each of %d payments applied once, total credited %d.\n\n", payments, want)
	fmt.Printf("   %-14s %9s %7s %6s %11s %9s %14s\n", "strategy", "delivered", "exact", "lost", "duplicated", "skipped", "credited")
	for _, s := range []ic.Strategy{ic.CommitBefore, ic.CommitAfter, ic.Inbox} {
		store := must(ic.NewStore())
		c := must(ic.NewConsumer(lg, store, "ledger-"+s.String(), s, credit, ic.WithCrashes(crashes...)))
		if err := c.Drain(); err != nil {
			log.Fatal(err)
		}
		a := store.Audit(ids)
		var got int64
		for k := range accounts {
			got += store.Balance(fmt.Sprintf("acct-%d", k))
		}
		st := c.Stats()
		fmt.Printf("   %-14s %9d %7d %6d %11d %9d %7d (%+d)\n",
			s, st.Delivered, a.Exact, a.Lost, a.Duplicated, st.Skipped, got, got-want)
	}
	fmt.Println()
	fmt.Println("   commit-before: the 10 crashes between commit and effect (after-commit = before-effect) lose")
	fmt.Println("   8 payments; the other 2 hit a payment that also had a retransmitted copy, so it landed once by")
	fmt.Println("   luck. The producer's retransmits are still applied twice.")
	fmt.Println("   commit-after: nothing is lost, but the 13 crashes between effect and commit add redeliveries")
	fmt.Println("   on top of the retransmits (some hit already-duplicated payments, so 48 rather than 38+13).")
	fmt.Println("   inbox: 53 skipped = 40 retransmitted records + 13 crash redeliveries; every payment lands once.")
	fmt.Println()

	// 3) Dedup window: an offset reset replays the last 10 minutes.
	fmt.Println("3) Dedup window: one payment a second for 10 minutes, consumed live, then the group's offset")
	fmt.Println("   is reset to 0 (a bad deploy, a replay to rebuild a projection) and all 600 are delivered again.")
	for _, window := range []time.Duration{24 * time.Hour, 5 * time.Minute, time.Minute} {
		now := time.Unix(0, 0)
		clock := func() time.Time { return now }
		l := must(ic.NewLog(1))
		store := must(ic.NewStore(ic.WithClock(clock), ic.WithRetention(window)))
		p := must(ic.NewProducer(l, "chat"))
		c := must(ic.NewConsumer(l, store, "g", ic.Inbox, credit))
		var replayIDs []string
		for range 600 {
			m := must(p.Send("room-1", 1))
			replayIDs = append(replayIDs, m.ID)
			must(c.Poll(10))
			now = now.Add(time.Second)
		}
		if err := l.Commit("g", 0, 0); err != nil {
			log.Fatal(err)
		}
		store.Prune() // what is still remembered when the replay starts
		inbox := store.InboxSize()
		replay := must(ic.NewConsumer(l, store, "g", ic.Inbox, credit))
		if err := replay.Drain(); err != nil {
			log.Fatal(err)
		}
		a := store.Audit(replayIDs)
		fmt.Printf("   window %-8v inbox holds %3d IDs at replay time -> %3d skipped, %3d applied twice\n",
			window, inbox, replay.Stats().Skipped, a.Duplicated)
	}
	fmt.Println("   A duplicate that arrives after its ID was pruned is indistinguishable from a new message.")
}
