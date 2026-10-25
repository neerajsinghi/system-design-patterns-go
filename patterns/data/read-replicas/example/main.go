// Command example walks through the classic "I updated my profile and it
// reverted" bug caused by replication lag, and the read-your-writes fix.
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/neerajsinghi/system-design-patterns-go/patterns/data/read-replicas"
)

func main() {
	now := time.Unix(0, 0)
	clock := func() time.Time { return now }
	tick := func(d time.Duration) { now = now.Add(d); fmt.Printf("  ... %v later\n", d) }

	cluster, err := readreplicas.New(
		[]time.Duration{50 * time.Millisecond, 500 * time.Millisecond},
		readreplicas.WithClock(clock),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Leader + 2 followers (replica-1 lags 50ms, replica-2 lags 500ms)")
	cluster.Write("user:7:name", "Ada")
	now = now.Add(time.Second) // let the initial write replicate everywhere

	fmt.Println("\nWithout read-your-writes: Ada renames herself to Grace")
	v := cluster.Write("user:7:name", "Grace")
	fmt.Printf("  write -> leader, version %d\n", v)
	for range 2 {
		val, _, from := cluster.Read("user:7:name")
		fmt.Printf("  read  -> %-9s = %q\n", from, val)
	}
	fmt.Println("  Ada sees her old name and assumes the save failed.")

	tick(100 * time.Millisecond)
	for range 2 {
		val, _, from := cluster.Read("user:7:name")
		fmt.Printf("  read  -> %-9s = %q\n", from, val)
	}
	fmt.Printf("  replica versions: %v\n", cluster.ReplicaVersions())

	fmt.Println("\nWith read-your-writes: Ada uses a session that remembers her last write version")
	ada := cluster.NewSession()
	v = ada.Write("user:7:name", "Ada Lovelace")
	fmt.Printf("  write -> leader, version %d\n", v)
	val, _, from := ada.Read("user:7:name")
	fmt.Printf("  read  -> %-9s = %q (no replica has v%d yet, so the leader answers)\n", from, val, v)
	tick(60 * time.Millisecond)
	for range 3 {
		val, _, from = ada.Read("user:7:name")
		fmt.Printf("  read  -> %-9s = %q\n", from, val)
	}
	fmt.Println("  replica-2 is skipped until it catches up, and the leader stays mostly unloaded.")

	bob := cluster.NewSession()
	val, _, from = bob.Read("user:7:name")
	val2, _, from2 := bob.Read("user:7:name")
	fmt.Printf("\nBob (no writes of his own) may still read stale data: %s=%q, %s=%q\n", from, val, from2, val2)
	s := cluster.Stats()
	fmt.Printf("\nreads served by followers: %d, by leader: %d\n", s.FollowerReads, s.LeaderReads)
}
