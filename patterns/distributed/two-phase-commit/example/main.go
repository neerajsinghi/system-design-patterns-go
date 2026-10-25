// Command example moves money between two banks with 2PC: one clean commit,
// one abort on a no vote, and the blocking failure mode when the coordinator
// crashes between the phases.
package main

import (
	"context"
	"fmt"

	tpc "github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/two-phase-commit"
)

func balances(a, b *tpc.Store) string {
	show := func(s *tpc.Store, k string) string {
		v, err := s.Get(k)
		if err != nil {
			return "BLOCKED"
		}
		return fmt.Sprint(v)
	}
	return fmt.Sprintf("alice=%s bob=%s", show(a, "alice"), show(b, "bob"))
}

func main() {
	ctx := context.Background()
	a := tpc.NewStore("bank-a", map[string]int{"alice": 100})
	b := tpc.NewStore("bank-b", map[string]int{"bob": 0})
	c := tpc.NewCoordinator(map[string]tpc.Participant{"bank-a": a, "bank-b": b})

	d, _ := c.Execute(ctx, "tx1", map[string]map[string]int{"bank-a": {"alice": 70}, "bank-b": {"bob": 30}})
	fmt.Printf("tx1 transfer 30: both vote yes -> %s; %s\n", d, balances(a, b))

	d, _ = c.Execute(ctx, "tx2", map[string]map[string]int{"bank-a": {"alice": -30}, "bank-b": {"bob": 130}})
	fmt.Printf("tx2 transfer 100: bank-a votes no (would go negative) -> %s; %s\n", d, balances(a, b))

	fmt.Println()
	c.CrashAfterPrepare = func(tx string) bool { return tx == "tx3" }
	_, err := c.Execute(ctx, "tx3", map[string]map[string]int{"bank-a": {"alice": 50}, "bank-b": {"bob": 50}})
	fmt.Printf("tx3 transfer 20: both prepared, then %v\n", err)
	fmt.Printf("  in doubt: bank-a=%v bank-b=%v; %s\n", a.InDoubt(), b.InDoubt(), balances(a, b))
	fmt.Println("  participants voted yes, so they may NOT abort on their own: they hold locks and wait")
	d, _ = c.Execute(ctx, "tx4", map[string]map[string]int{"bank-a": {"alice": 69}})
	fmt.Printf("  tx4 (fee on alice) meanwhile -> %s because alice is locked by tx3\n", d)

	c.CrashAfterPrepare = nil
	_ = c.Recover(ctx)
	fmt.Printf("coordinator restarts, finds no decision for tx3 -> presumed %s; %s\n", c.Decision("tx3"), balances(a, b))
	fmt.Printf("  in doubt now: bank-a=%v bank-b=%v\n", a.InDoubt(), b.InDoubt())
}
