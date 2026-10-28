// Command example builds a small social graph with one celebrity
// (10,000 followers) and compares the cost of push, pull and hybrid
// timelines. It then follows the fan-out-on-write path into push
// notifications: a celebrity with 200,000 followers posts while OTPs keep
// arriving, first through a single FIFO queue and then through priority
// lanes, both behind per-provider rate limits. Finally it shows how
// coalescing shrinks a like storm.
package main

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	feed "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/fan-out-feed"
)

func main() {
	timelines()
	fmt.Println()
	notifications()
	fmt.Println()
	coalescing()
}

func timelines() {
	t := time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC)
	clock := func() time.Time { t = t.Add(time.Minute); return t }

	const fans = 10_000
	fmt.Println("== 1. Timelines: push vs pull vs hybrid ==")
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

// Simulated provider limits. Real APNs/FCM limits are not published as
// fixed numbers; these just make the throttling visible.
var providers = []struct {
	name  string
	rate  float64
	burst int
}{{"apns", 2_000, 20}, {"fcm", 3_000, 30}}

const tick = 10 * time.Millisecond

// fanNames is shared by steps 2 and 3 so the example doesn't spend its time
// formatting 200,000 strings twice.
var fanNames = func() []string {
	out := make([]string, 200_000)
	for i := range out {
		out[i] = "fan" + strconv.Itoa(i)
	}
	return out
}()

func providerFor(i int) string { return providers[i%2].name }

type simClock struct{ t time.Time }

func (c *simClock) now() time.Time { return c.t }

func newDispatcher(c *simClock, extra ...feed.DispatcherOption) *feed.Dispatcher {
	opts := []feed.DispatcherOption{feed.WithDispatchClock(c.now)}
	for _, p := range providers {
		opts = append(opts, feed.WithProvider(p.name, p.rate, p.burst))
	}
	d, err := feed.NewDispatcher(append(opts, extra...)...)
	if err != nil {
		panic(err)
	}
	return d
}

type result struct {
	otpP50, otpP99, otpMax time.Duration
	socialDone             time.Duration
	maxPerSec              map[string]int
}

// runStorm enqueues one "new post" push per follower at t=0 and an OTP
// every 100 ms for 30 s, ticking the dispatcher every 10 ms.
func runStorm(followers []string, extra ...feed.DispatcherOption) result {
	start := time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)
	c := &simClock{t: start}
	d := newDispatcher(c, extra...)
	for i, u := range followers {
		_ = d.Enqueue(feed.Notification{User: u, Provider: providerFor(i), Lane: feed.Social, Actor: "@celeb", Text: "posted a photo"})
	}
	var otp []time.Duration
	perSec := map[string]map[int64]int{}
	res := result{maxPerSec: map[string]int{}}
	for step := 0; d.Pending() > 0 || step <= 3000; step++ {
		if step%10 == 0 && step <= 3000 { // one OTP every 100 ms for 30 s
			_ = d.Enqueue(feed.Notification{User: fmt.Sprint("rider", step), Provider: providerFor(step / 10), Lane: feed.Transactional, Text: "your code is 123456"})
		}
		for _, del := range d.Dispatch() {
			if perSec[del.Provider] == nil {
				perSec[del.Provider] = map[int64]int{}
			}
			perSec[del.Provider][del.SentAt.Unix()]++
			if del.Lane == feed.Transactional {
				otp = append(otp, del.Delay())
			} else {
				res.socialDone = del.SentAt.Sub(start)
			}
		}
		c.t = c.t.Add(tick)
	}
	sort.Slice(otp, func(i, j int) bool { return otp[i] < otp[j] })
	res.otpP50 = otp[len(otp)/2]
	res.otpP99 = otp[len(otp)*99/100]
	res.otpMax = otp[len(otp)-1]
	for name, secs := range perSec {
		for _, n := range secs {
			res.maxPerSec[name] = max(res.maxPerSec[name], n)
		}
	}
	return res
}

func notifications() {
	followers := fanNames // @celeb's followers: one push each on the fan-out-on-write path

	fmt.Println("== 2. Push notifications: a celebrity post vs OTPs ==")
	fmt.Printf("@celeb has %d followers, so one post enqueues %d social pushes at t=0.\n", len(followers), len(followers))
	fmt.Println("Meanwhile an OTP arrives every 100 ms for 30 s (301 OTPs).")
	fmt.Printf("Simulated provider limits: apns %.0f/s (burst %d), fcm %.0f/s (burst %d). Dispatcher ticks every 10 ms.\n",
		providers[0].rate, providers[0].burst, providers[1].rate, providers[1].burst)
	fmt.Println()

	fifo := runStorm(followers, feed.WithSingleFIFO())
	lanes := runStorm(followers) // default starvation limit 20
	fmt.Printf("%-16s %9s %9s %9s %18s %11s %11s\n", "queueing", "OTP p50", "OTP p99", "OTP max", "storm drained at", "apns max/s", "fcm max/s")
	for _, r := range []struct {
		name string
		res  result
	}{{"single FIFO", fifo}, {"priority lanes", lanes}} {
		fmt.Printf("%-16s %8.2fs %8.2fs %8.2fs %17.2fs %11d %11d\n", r.name,
			r.res.otpP50.Seconds(), r.res.otpP99.Seconds(), r.res.otpMax.Seconds(), r.res.socialDone.Seconds(),
			r.res.maxPerSec["apns"], r.res.maxPerSec["fcm"])
	}
	fmt.Println()
	fmt.Println("FIFO: an OTP waits behind every celeb push queued ahead of it on its provider.")
	fmt.Println("Lanes: the OTP lane is drained first, so an OTP leaves on the tick it arrives.")
	fmt.Println("The storm drains at about the same time either way (lanes ~0.1 s later, because the OTPs now go ahead of it):")
	fmt.Println("apns carries half the storm at 2,000/s, so the provider limits, not the queueing, set the pace.")
	fmt.Println("max/s is counted per calendar second; the token bucket caps any one-second window at rate + burst.")
}

func coalescing() {
	fmt.Println("== 3. Coalescing a like storm ==")
	const creators, likesEach = 1_000, 200
	fmt.Printf("@celeb reshares %d creators' posts; fans send %d likes to each (%d like pushes) over 20 s.\n",
		creators, likesEach, creators*likesEach)
	fmt.Println()
	fmt.Printf("%-24s %12s %14s   %s\n", "mode", "pushes sent", "last push at", "creator0000's first pushes")
	for _, window := range []time.Duration{0, 5 * time.Second} {
		start := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
		c := &simClock{t: start}
		var extra []feed.DispatcherOption
		if window > 0 {
			extra = append(extra, feed.WithCoalesceWindow(window))
		}
		d := newDispatcher(c, extra...)
		names := make([]string, creators)
		for i := range names {
			names[i] = fmt.Sprintf("creator%04d", i)
		}
		total := creators * likesEach
		perTick := total / 2000 // 20 s of 10 ms ticks
		sent, like := 0, 0
		var last time.Duration
		var first []string
		for like < total || d.Pending() > 0 {
			for k := 0; k < perTick && like < total; k++ {
				cr := like % creators
				_ = d.Enqueue(feed.Notification{
					User: names[cr], Provider: providerFor(cr), Lane: feed.Social,
					Actor: fanNames[like], Text: "liked your post", CollapseKey: "like:reshared",
				})
				like++
			}
			for _, del := range d.Dispatch() {
				sent++
				last = del.SentAt.Sub(start)
				if del.User == "creator0000" && len(first) < 2 {
					first = append(first, fmt.Sprintf("%q", del.Summary()))
				}
			}
			c.t = c.t.Add(tick)
		}
		name := "no coalescing"
		if window > 0 {
			name = fmt.Sprintf("coalesce, %v window", window)
		}
		fmt.Printf("%-24s %12d %13.2fs   %v\n", name, sent, last.Seconds(), first)
	}
}
