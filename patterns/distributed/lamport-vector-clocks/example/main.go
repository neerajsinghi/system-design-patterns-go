// Command example runs three processes exchanging messages and shows what
// Lamport and vector clocks can and cannot tell you about the events.
package main

import (
	"fmt"

	lvc "github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/lamport-vector-clocks"
)

type event struct {
	name  string
	stamp lvc.Stamp
}

func main() {
	A, B, C := lvc.NewProcess("A"), lvc.NewProcess("B"), lvc.NewProcess("C")
	var log []event
	rec := func(name string, s lvc.Stamp) {
		log = append(log, event{name, s})
		fmt.Printf("  %-26s lamport=%d vector=%v\n", name, s.Lamport.Time, s.Vector)
	}

	fmt.Println("timeline:")
	rec("A: write x=1", A.Local())
	m1 := A.Send("x=1")
	rec("A: send m1 to B", m1.Stamp)
	rec("C: write x=7 (independent)", C.Local())
	rec("B: receive m1", B.Receive(m1))
	m2 := B.Send("ack")
	rec("B: send m2 to C", m2.Stamp)
	rec("C: receive m2", C.Receive(m2))

	pairs := [][2]int{{0, 5}, {2, 1}, {2, 3}, {0, 2}}
	fmt.Println("\ncomparisons:")
	for _, p := range pairs {
		x, y := log[p[0]], log[p[1]]
		lam := "<"
		if x.stamp.Lamport.Time > y.stamp.Lamport.Time {
			lam = ">"
		} else if x.stamp.Lamport.Time == y.stamp.Lamport.Time {
			lam = "="
		}
		fmt.Printf("  %-26s vs %-26s lamport %s, vector says %s\n", x.name, y.name, lam, x.stamp.Vector.Compare(y.stamp.Vector))
	}
	fmt.Println("\nLamport: C's write (1) < A's send (2), yet neither caused the other.")
	fmt.Println("Vector clocks detect that: concurrent writes to x are a conflict to resolve.")
}
