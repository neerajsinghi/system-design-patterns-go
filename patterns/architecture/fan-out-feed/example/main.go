// Command example builds a small social graph with one celebrity
// (10,000 followers) and compares the cost of push, pull and hybrid
// timelines.
package main

import (
	"fmt"
	"time"

	feed "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/fan-out-feed"
)

func main() {
	t := time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC)
	clock := func() time.Time { t = t.Add(time.Minute); return t }

	const fans = 10_000
	fmt.Printf("Graph: %d users follow @celeb, and each follows 20 friends. 100 users post once each.\n", fans)
	fmt.Println("Workload: @celeb posts 3 times, then 1,000 users open their timelines.")
	fmt.Println()
	fmt.Printf("%-8s %14s %14s   %s\n", "strategy", "inbox writes", "outbox reads", "user0's top 4")

	for _, s := range []feed.Strategy{feed.Push, feed.Pull, feed.Hybrid} {
		f := feed.New(s, 1_000, 200, clock) // 1,000+ followers counts as a celebrity
		for i := 0; i < fans; i++ {
			u := fmt.Sprintf("user%d", i)
			f.Follow(u, "celeb")
			for k := 1; k <= 20; k++ {
				f.Follow(u, fmt.Sprintf("user%d", (i+k)%fans))
			}
		}
		for i := 0; i < 100; i++ { // a few friends post
			f.Publish(fmt.Sprintf("user%d", i), fmt.Sprintf("friend post by user%d", i))
		}
		for i := 1; i <= 3; i++ {
			f.Publish("celeb", fmt.Sprintf("celeb post #%d", i))
		}
		var top []feed.Post
		for i := 0; i < 1000; i++ {
			tl := f.Timeline(fmt.Sprintf("user%d", i), 4)
			if i == 0 {
				top = tl
			}
		}
		st := f.Stats()
		fmt.Printf("%-8s %14d %14d   ", s, st.InboxWrites, st.OutboxReads)
		for _, p := range top {
			fmt.Printf("%q ", p.Text)
		}
		fmt.Println()
	}

	fmt.Println()
	fmt.Println("push:   each celeb post costs 10,000 writes. Reads touch only one inbox.")
	fmt.Println("pull:   posting is free, but each read merges 21 outboxes.")
	fmt.Println("hybrid: friends are pushed and only @celeb is pulled, so writes stay small and each read adds one outbox.")
}
