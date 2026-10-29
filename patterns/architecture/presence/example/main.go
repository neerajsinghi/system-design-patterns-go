// Command example simulates a presence service in front of a fleet of
// gateway nodes: 1,000 users, some of them on flaky mobile networks that
// drop the socket every ~20s. It compares the events emitted with and
// without an offline grace period, crashes a gateway node to show lease
// expiry, walks through a multi-device user, and rejects stale rider
// positions for live tracking. All time is simulated.
package main

import (
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/presence"
)

const (
	users     = 1000
	flaky     = 200 // users on a bad network
	hbEvery   = 10 * time.Second
	ttl       = 30 * time.Second
	grace     = 15 * time.Second
	simLength = 10 * time.Minute
)

var epoch = time.Unix(1_700_000_000, 0)

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func uid(i int) string { return fmt.Sprintf("u%04d", i) }

func newTracker(c *clock, g time.Duration) *presence.Tracker {
	tr, err := presence.New(presence.WithClock(c.now), presence.WithTTL(ttl),
		presence.WithOfflineGrace(g), presence.WithAwayAfter(0)) // away is shown in step 4
	if err != nil {
		log.Fatal(err)
	}
	return tr
}

type flapResult struct {
	drops, changes, delivered, longOutages int
	heartbeats                             uint64
}

// flapping runs 10 minutes of traffic. Flaky users lose their socket every
// 15-25s; most reconnect within 1-8s, one drop in 20 is a real 25-40s outage.
// A watcher subscribed to everyone drains every 5s.
func flapping(g time.Duration) flapResult {
	rng := rand.New(rand.NewPCG(7, 11))
	c := &clock{t: epoch}
	tr := newTracker(c, g)
	all := make([]string, users)
	for i := range users {
		all[i] = uid(i)
	}
	w, err := tr.Watch(all...)
	if err != nil {
		log.Fatal(err)
	}

	type dev struct {
		conn      string
		gen       int
		connected bool
		nextEvent time.Duration // next drop (if connected) or reconnect
	}
	devs := make([]dev, users)
	for i := range users {
		devs[i] = dev{conn: "c0", connected: true, nextEvent: time.Duration(15+rng.IntN(11)) * time.Second}
		if err := tr.Connect(presence.Connection{User: uid(i), ID: "c0", Node: fmt.Sprintf("gw-%d", i%3+1)}); err != nil {
			log.Fatal(err)
		}
	}
	tr.Sweep()
	w.Drain()
	base := tr.Stats()

	var r flapResult
	for t := time.Second; t <= simLength; t += time.Second {
		c.t = epoch.Add(t)
		for i := range users {
			d := &devs[i]
			if i < flaky && t >= d.nextEvent {
				if d.connected {
					tr.Disconnect(uid(i), d.conn)
					d.connected = false
					r.drops++
					gap := time.Duration(1+rng.IntN(8)) * time.Second
					if rng.IntN(20) == 0 {
						gap = time.Duration(25+rng.IntN(16)) * time.Second
						r.longOutages++
					}
					d.nextEvent = t + gap
				} else {
					d.gen++
					d.conn = fmt.Sprintf("c%d", d.gen)
					_ = tr.Connect(presence.Connection{User: uid(i), ID: d.conn, Node: fmt.Sprintf("gw-%d", rng.IntN(3)+1)})
					d.connected = true
					d.nextEvent = t + time.Duration(15+rng.IntN(11))*time.Second
				}
			}
			if d.connected && (t+time.Duration(i)*time.Second)%hbEvery == 0 {
				_ = tr.Heartbeat(uid(i), d.conn)
			}
		}
		tr.Sweep()
		if t%(5*time.Second) == 0 {
			w.Drain()
		}
	}
	w.Drain()
	s := tr.Stats()
	r.changes = int(s.StateChanges - base.StateChanges)
	r.delivered = int(s.Delivered - base.Delivered)
	r.heartbeats = s.Heartbeats - base.Heartbeats
	return r
}

func main() {
	fmt.Println("1) Cost model: why presence lives in memory (or Redis with TTL), not the primary DB")
	for _, n := range []int{users, 1_000_000, 50_000_000} {
		fmt.Printf("   %11d users x 1 heartbeat / %v = %9.0f writes/s\n", n, hbEvery, float64(n)/hbEvery.Seconds())
	}
	fmt.Println()

	fmt.Printf("2) Flapping: %d users, %d on flaky networks dropping every 15-25s, %v simulated\n", users, flaky, simLength)
	without, with := flapping(0), flapping(grace)
	fmt.Printf("   %-28s %12s %12s\n", "", "no grace", fmt.Sprintf("%v grace", grace))
	fmt.Printf("   %-28s %12d %12d\n", "socket drops", without.drops, with.drops)
	fmt.Printf("   %-28s %12d %12d\n", "  of which 25-40s outages", without.longOutages, with.longOutages)
	fmt.Printf("   %-28s %12d %12d\n", "presence state changes", without.changes, with.changes)
	fmt.Printf("   %-28s %12d %12d\n", "delivered (5s coalescing)", without.delivered, with.delivered)
	fmt.Printf("   %-28s %12d %12d\n", "heartbeats (memory only)", without.heartbeats, with.heartbeats)
	fmt.Printf("   Grace cuts state changes by %.0f%%: only the %d real outages are announced (offline +\n",
		100*(1-float64(with.changes)/float64(without.changes)), with.longOutages)
	fmt.Printf("   online, minus outages still open when the run ends). Coalescing alone hides flaps that\n")
	fmt.Printf("   fit between two drains, but not the rest. Each offline event is also the one moment\n")
	fmt.Printf("   worth persisting last-seen to the DB, instead of %d heartbeat writes.\n\n", with.heartbeats)

	nodeCrash()
	multiDevice()
	liveTracking()
}

// nodeCrash kills gw-2 at t=60s. Its sockets stop heartbeating and nobody
// calls Disconnect, so only lease expiry can notice.
func nodeCrash() {
	c := &clock{t: epoch}
	tr := newTracker(c, grace)
	all := make([]string, users)
	for i := range users {
		all[i] = uid(i)
		if err := tr.Connect(presence.Connection{User: uid(i), ID: "c", Node: fmt.Sprintf("gw-%d", i%3+1)}); err != nil {
			log.Fatal(err)
		}
	}
	w, _ := tr.Watch(all...)
	w.Drain()
	crash := 60 * time.Second
	onCrashed := 0
	var offlineAt []time.Duration
	routable := map[time.Duration]int{}
	for t := time.Second; t <= 2*time.Minute; t += time.Second {
		c.t = epoch.Add(t)
		for i := range users {
			node := i%3 + 1
			if node == 2 && t >= crash {
				continue // dead node: no heartbeats
			}
			if (t+time.Duration(i)*time.Second)%hbEvery == 0 {
				_ = tr.Heartbeat(uid(i), "c")
			}
		}
		tr.Sweep()
		for _, ev := range w.Drain() {
			if ev.State == presence.Offline {
				offlineAt = append(offlineAt, ev.At.Sub(epoch)-crash)
			}
		}
		if t == crash {
			for i := 1; i < users; i += 3 {
				if len(tr.Lookup(uid(i))) > 0 {
					onCrashed++
				}
			}
		}
		if d := t - crash; d == 10*time.Second || d == 21*time.Second || d == 30*time.Second || d == 40*time.Second {
			n := 0
			for i := 1; i < users; i += 3 {
				if l := tr.Lookup(uid(i)); len(l) > 0 && l[0].Node == "gw-2" {
					n++
				}
			}
			routable[d] = n
		}
	}
	sort.Slice(offlineAt, func(i, j int) bool { return offlineAt[i] < offlineAt[j] })
	fmt.Printf("3) Gateway gw-2 crashes at t=%v holding %d users (heartbeat %v, TTL %v, grace %v)\n", crash, onCrashed, hbEvery, ttl, grace)
	for _, d := range []time.Duration{10 * time.Second, 21 * time.Second, 30 * time.Second, 40 * time.Second} {
		fmt.Printf("   crash+%-3v Lookup still routes %3d users to dead gw-2\n", d, routable[d])
	}
	fmt.Printf("   offline events: %d, first at crash+%v, last at crash+%v\n", len(offlineAt), offlineAt[0], offlineAt[len(offlineAt)-1])
	fmt.Printf("   (last heartbeats were 1-10s before the crash, so leases ran out at crash+20s..29s:\n")
	fmt.Printf("   Lookup stops routing there at TTL, the offline event follows %v of grace later)\n", grace)
	fmt.Printf("   users still online on gw-1/gw-3: %d\n\n", tr.Count(presence.Online))
}

func multiDevice() {
	c := &clock{t: epoch}
	tr, _ := presence.New(presence.WithClock(c.now), presence.WithTTL(ttl),
		presence.WithOfflineGrace(grace), presence.WithAwayAfter(2*time.Minute))
	w, _ := tr.Watch("alice")
	at := func(d time.Duration) { c.t = epoch.Add(d); tr.Sweep() }
	show := func(what string) {
		p := tr.Status("alice")
		evs := ""
		for _, ev := range w.Drain() {
			evs += fmt.Sprintf(" [event %v -> %v]", ev.Prev, ev.State)
		}
		fmt.Printf("   t=%-5v %-34s %-7v devices=%d last seen t=%v%s\n",
			c.t.Sub(epoch), what, p.State, p.Connections, p.LastSeen.Sub(epoch), evs)
	}
	fmt.Println("4) Multi-device: alice on phone and laptop (away after 2m idle)")
	_ = tr.Connect(presence.Connection{User: "alice", ID: "phone", Device: "phone", Node: "gw-1"})
	_ = tr.Connect(presence.Connection{User: "alice", ID: "laptop", Device: "web", Node: "gw-3"})
	show("phone + laptop connect")
	routes := ""
	for _, cn := range tr.Lookup("alice") {
		routes += cn.ID + "@" + cn.Node + " "
	}
	fmt.Printf("   Lookup(alice) = %s(a message is pushed to both)\n", routes)
	at(10 * time.Second)
	tr.Disconnect("alice", "phone")
	show("phone closes the app")
	for d := 20 * time.Second; d <= 3*time.Minute; d += 10 * time.Second {
		at(d)
		_ = tr.Heartbeat("alice", "laptop")
	}
	show("laptop idle, still heartbeating")
	at(3*time.Minute + 5*time.Second)
	_ = tr.Activity("alice", "laptop")
	show("types on laptop")
	at(3*time.Minute + 10*time.Second)
	tr.Disconnect("alice", "laptop")
	show("closes laptop (grace starts)")
	at(3*time.Minute + 25*time.Second)
	show("grace over")
	at(8*time.Minute + 25*time.Second)
	p := tr.Status("alice")
	fmt.Printf("   t=%v friends see: last seen %v ago\n\n", c.t.Sub(epoch), c.t.Sub(p.LastSeen))
}

func liveTracking() {
	c := &clock{t: epoch}
	tr := newTracker(c, grace)
	_ = tr.Connect(presence.Connection{User: "rider-42", ID: "app", Node: "gw-1"})
	maxAge := 15 * time.Second
	fmt.Printf("5) Live tracking: rider-42 sends a GPS fix every 4s; the customer app accepts fixes up to %v old\n", maxAge)
	accepted, rejected := 0, 0
	send := func(measured time.Duration, lat float64) {
		err := tr.UpdatePosition("rider-42", "app", presence.Position{Lat: lat, Lng: 77.6, At: epoch.Add(measured)})
		switch {
		case errors.Is(err, presence.ErrStalePosition):
			rejected++
			fmt.Printf("   t=%-4v fix measured at t=%v arrives late: rejected (would move the rider backwards)\n", c.t.Sub(epoch), measured)
		case err == nil:
			accepted++
		}
	}
	for d := time.Duration(0); d <= 24*time.Second; d += 4 * time.Second {
		c.t = epoch.Add(d)
		if d == 12*time.Second {
			continue // this fix is stuck in a retry queue
		}
		send(d, 12.9+float64(d/time.Second)/1000)
	}
	c.t = epoch.Add(26 * time.Second)
	send(12*time.Second, 12.912)
	fmt.Printf("   fixes accepted %d, rejected %d\n", accepted, rejected)
	for _, d := range []time.Duration{30 * time.Second, 45 * time.Second} {
		c.t = epoch.Add(d) // rider entered a tunnel after the t=24s fix
		p, err := tr.LatestPosition("rider-42", maxAge)
		verdict := "show on map"
		if errors.Is(err, presence.ErrStalePosition) {
			verdict = "stale: show \"location updating...\" instead of a frozen pin"
		}
		fmt.Printf("   t=%v read: lat %.3f measured %v ago -> %s\n", d, p.Lat, c.t.Sub(p.At), verdict)
	}
}
