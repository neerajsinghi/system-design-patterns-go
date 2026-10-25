// Command example event-sources a bank account: it rebuilds state from
// history, answers a temporal query, catches a concurrent double-spend, and
// shows snapshots cutting replay cost.
package main

import (
	"fmt"
	"log"

	"github.com/neerajsinghi/system-design-patterns-go/patterns/data/event-sourcing"
)

func outcome(err error) string {
	if err != nil {
		return err.Error()
	}
	return "ok"
}

func main() {
	store := eventsourcing.NewStore()
	repo, err := eventsourcing.NewRepository(store, 50)
	if err != nil {
		log.Fatal(err)
	}

	acc := eventsourcing.NewAccount("acc-1")
	_ = acc.Open("Ada")
	_ = acc.Deposit(500)
	_ = acc.Withdraw(120)
	_ = acc.Deposit(80)
	fmt.Println("Events recorded for acc-1:")
	for i, e := range acc.Pending() {
		fmt.Printf("  v%d %T%+v\n", i+1, e, e)
	}
	if err := repo.Save(acc); err != nil {
		log.Fatal(err)
	}

	cur, _ := repo.Load("acc-1")
	fmt.Printf("\nCurrent state (fold over 4 events): balance=%d version=%d\n", cur.Balance, cur.Version)
	past, _ := repo.LoadAt("acc-1", 2)
	fmt.Printf("Temporal query, state at v2:       balance=%d\n", past.Balance)

	fmt.Println("\nTwo requests load v4 and both try to withdraw 400:")
	r1, _ := repo.Load("acc-1")
	r2, _ := repo.Load("acc-1")
	_ = r1.Withdraw(400)
	_ = r2.Withdraw(400)
	fmt.Printf("  request 1 save: %s\n", outcome(repo.Save(r1)))
	fmt.Printf("  request 2 save: %s\n", outcome(repo.Save(r2)))
	r2, _ = repo.Load("acc-1")
	fmt.Printf("  request 2 reloads and retries: %s\n", outcome(r2.Withdraw(400)))

	fmt.Println("\nA busy account: 1,000 small deposits, saved one by one (snapshot every 50 events)")
	busy := eventsourcing.NewAccount("acc-2")
	_ = busy.Open("Grace")
	_ = repo.Save(busy)
	for range 1000 {
		_ = busy.Deposit(1)
		_ = repo.Save(busy)
	}
	before := store.EventsRead()
	b, _ := repo.Load("acc-2")
	withSnap := store.EventsRead() - before

	noSnap, _ := eventsourcing.NewRepository(store, 0)
	before = store.EventsRead()
	b2, _ := noSnap.Load("acc-2")
	full := store.EventsRead() - before
	fmt.Printf("  load with snapshot:    balance=%d, replayed %d events\n", b.Balance, withSnap)
	fmt.Printf("  load without snapshot: balance=%d, replayed %d events\n", b2.Balance, full)
}
