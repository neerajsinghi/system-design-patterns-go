// Command example compares a strong quorum (N=3, R=2, W=2) with a weak one
// (N=3, R=1, W=1) when a replica misses a write, and shows read repair.
package main

import (
	"fmt"
	"log"

	"github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/quorum"
)

func cluster(r, w int) (*quorum.Store, []*quorum.Replica) {
	reps := []*quorum.Replica{quorum.NewReplica("r0"), quorum.NewReplica("r1"), quorum.NewReplica("r2")}
	s, err := quorum.NewStore(quorum.Config{N: 3, R: r, W: w}, reps)
	if err != nil {
		log.Fatal(err)
	}
	return s, reps
}

func show(reps []*quorum.Replica) string {
	out := ""
	for _, r := range reps {
		v, _, err := r.Get("cart")
		switch {
		case err != nil:
			out += fmt.Sprintf(" %s=DOWN", r.ID)
		case v.Data == "":
			out += fmt.Sprintf(" %s=<none>", r.ID)
		default:
			out += fmt.Sprintf(" %s=%s@%d", r.ID, v.Data, v.Version.Counter)
		}
	}
	return out
}

func scenario(r, w int) {
	cfg := quorum.Config{N: 3, R: r, W: w}
	fmt.Printf("== N=3 R=%d W=%d (R+W>N: %v)\n", r, w, cfg.Strong())
	s, reps := cluster(r, w)
	_, _ = s.Put("alice", "cart", "[book]")
	fmt.Printf("put [book]                    ->%s\n", show(reps))

	down := []int{2}
	if w == 1 {
		down = []int{1, 2}
	}
	for _, i := range down {
		reps[i].SetDown(true)
	}
	_, err := s.Put("alice", "cart", "[book,pen]")
	fmt.Printf("put [book,pen] (some down)    ->%s  err=%v\n", show(reps), err)
	for _, i := range down {
		reps[i].SetDown(false)
	}

	v, err := s.GetFrom("cart", 2)
	fmt.Printf("get starting at r2            -> %s (err=%v)\n", v.Data, err)
	fmt.Printf("after read repair             ->%s  repairs=%d\n\n", show(reps), s.Repairs())
}

func main() {
	scenario(2, 2)
	scenario(1, 1)

	fmt.Println("== N=3 R=2 W=2 with two replicas down")
	s, reps := cluster(2, 2)
	reps[1].SetDown(true)
	reps[2].SetDown(true)
	_, err := s.Put("alice", "cart", "[x]")
	fmt.Printf("put [x] -> err=%v\n", err)
	_, err = s.Get("cart")
	fmt.Printf("get     -> err=%v (consistency is kept by refusing to answer)\n", err)
}
