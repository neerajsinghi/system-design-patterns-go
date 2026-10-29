// Command example runs three WebSocket hub nodes on loopback, connects 200
// users plus a few named ones, and walks through the four things a
// real-time gateway has to get right: routing a chat message to a user on
// another node (and all their devices), broadcasting, evicting a slow
// client without delaying anyone else, and detecting a client that has
// silently gone away.
package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	wsh "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/websocket-hub"
)

const (
	users       = 200
	watchers    = 30   // users 000-029 follow a rider's live location
	updates     = 200  // location updates sent to them
	updateBytes = 4096 // each update carries a route polyline
	queueSize   = 16
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	f.t = f.t.Add(d)
	f.mu.Unlock()
}

// client is a simulated user: it reads its socket and sorts what arrives.
type client struct {
	conn  *wsh.Conn
	inbox chan string
}

var (
	broadcastsSeen atomic.Int64
	latencies      = make(chan time.Duration, watchers*updates)
)

func connect(url, user string) *client {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := wsh.Dial(ctx, url+"/?user="+user)
	if err != nil {
		panic(err)
	}
	return &client{conn: conn, inbox: make(chan string, 16)}
}

func (c *client) readLoop() {
	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		switch {
		case bytes.HasPrefix(data, []byte("loc|")):
			sent, _ := strconv.ParseInt(string(data[4:23]), 10, 64)
			latencies <- time.Since(time.Unix(0, sent))
		case bytes.HasPrefix(data, []byte("broadcast|")):
			broadcastsSeen.Add(1)
		default:
			c.inbox <- string(data)
		}
	}
}

func waitUntil(cond func() bool) {
	deadline := time.Now().Add(2 * time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(200 * time.Microsecond)
	}
}

func pct(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	return d[int(float64(len(d)-1)*p)]
}

func main() {
	clock := &fakeClock{t: time.Date(2026, 10, 28, 9, 0, 0, 0, time.UTC)}
	cluster := wsh.NewCluster()
	reg := cluster.Registry()

	// Chat handler: a client sends "@user text"; the node it is connected
	// to routes it, wherever the recipient is.
	onMessage := func(h *wsh.Hub, from string, _ wsh.Opcode, data []byte) {
		to, text, ok := strings.Cut(string(data), " ")
		if !ok || !strings.HasPrefix(to, "@") {
			return
		}
		_, _ = h.SendToUser(to[1:], []byte(from+": "+text))
	}

	names := []string{"node-a", "node-b", "node-c"}
	hubs := map[string]*wsh.Hub{}
	urls := map[string]string{}
	var servers []*httptest.Server
	for _, n := range names {
		h, err := wsh.New(n,
			wsh.WithCluster(cluster),
			wsh.WithQueueSize(queueSize),
			wsh.WithSlowPolicy(wsh.Disconnect),
			wsh.WithHeartbeat(15*time.Second, 20*time.Second),
			wsh.WithClock(clock.Now),
			wsh.WithOnMessage(onMessage),
		)
		if err != nil {
			panic(err)
		}
		srv := httptest.NewServer(h)
		hubs[n], urls[n] = h, "ws"+strings.TrimPrefix(srv.URL, "http")
		servers = append(servers, srv)
	}
	written := func() map[string]uint64 {
		m := map[string]uint64{}
		for n, h := range hubs {
			m[n] = h.Stats().Written
		}
		return m
	}
	total := func() int {
		t := 0
		for _, h := range hubs {
			t += h.Connections()
		}
		return t
	}

	fmt.Println("1. Connect 200 users round-robin over 3 nodes, plus alice, bob (2 devices), slowpoke and sleepy")
	for i := range users {
		go connect(urls[names[i%3]], fmt.Sprintf("user-%03d", i)).readLoop()
	}
	alice := connect(urls["node-a"], "alice")
	bobPhone := connect(urls["node-c"], "bob")
	bobLaptop := connect(urls["node-b"], "bob")
	// slowpoke is attached over net.Pipe, which has no buffer: the first
	// frame node-a writes to it blocks, as on a phone that has stopped
	// reading. (A real socket would first soak up hundreds of KiB of kernel
	// buffer, an amount that differs by OS.)
	slowSide, slowpoke := net.Pipe()
	if _, err := hubs["node-a"].Register("slowpoke", wsh.NewConn(slowSide, true)); err != nil {
		panic(err)
	}
	sleepy := connect(urls["node-c"], "sleepy")
	sleepy.conn.SetPingHandler(func([]byte) error { return nil }) // phone asleep: never pongs
	for _, c := range []*client{alice, bobPhone, bobLaptop, sleepy} {
		go c.readLoop()
	}
	waitUntil(func() bool { return total() == users+5 })
	for _, n := range names {
		fmt.Printf("   %s holds %d connections\n", n, hubs[n].Connections())
	}
	fmt.Printf("   registry: %d users; bob is on %v\n\n", reg.Users(), reg.Nodes("bob"))

	fmt.Println(`2. Chat: alice (node-a) sends "@bob are you free?" over her WebSocket`)
	before := written()
	start := time.Now()
	if err := alice.conn.WriteMessage(wsh.OpText, []byte("@bob are you free?")); err != nil {
		panic(err)
	}
	gotPhone, gotLaptop := <-bobPhone.inbox, <-bobLaptop.inbox
	chatLatency := time.Since(start)
	after := written()
	fmt.Printf("   bob's phone  (node-c) got %q\n", gotPhone)
	fmt.Printf("   bob's laptop (node-b) got %q\n", gotLaptop)
	fmt.Printf("   both devices within %v; socket writes per node: a=%d b=%d c=%d\n",
		chatLatency.Round(time.Microsecond), after["node-a"]-before["node-a"], after["node-b"]-before["node-b"], after["node-c"]-before["node-c"])
	fmt.Println("   node-a looked bob up in the registry and published to node-b and node-c; only the owners wrote")
	fmt.Println()

	fmt.Println(`3. Broadcast from node-b: "maintenance at 02:00"`)
	queued, _ := hubs["node-b"].Broadcast([]byte("broadcast|maintenance at 02:00"))
	want := int64(total() - 1) // every connection except slowpoke, which never reads
	waitUntil(func() bool { return broadcastsSeen.Load() == want })
	fmt.Printf("   node-b queued it for its %d local connections and published it once on the bus\n", queued)
	fmt.Printf("   %d of %d connections read it (node-a's write to slowpoke is stuck: it never reads)\n\n", broadcastsSeen.Load(), total())

	fmt.Printf("4. Slow consumer: %d watchers follow a rider; slowpoke (node-a) watches too but never reads\n", watchers)
	fmt.Printf("   %d updates of %d bytes each, queue %d per connection, policy %s\n", updates, updateBytes, queueSize, wsh.Disconnect)
	pad := strings.Repeat("~", updateBytes-24)
	recipients := []string{"slowpoke"}
	for i := range watchers {
		recipients = append(recipients, fmt.Sprintf("user-%03d", i))
	}
	evictedAt := 0
	var slowestSend time.Duration
	var lat, latWithSlow []time.Duration
	for u := 1; u <= updates; u++ {
		msg := []byte(fmt.Sprintf("loc|%019d|%s", time.Now().UnixNano(), pad))
		for _, r := range recipients {
			t0 := time.Now()
			_, _ = hubs["node-a"].SendToUser(r, msg) // watchers on node-b and node-c go via the bus
			slowestSend = max(slowestSend, time.Since(t0))
		}
		for range watchers { // lockstep: every watcher has update u before u+1 is sent
			l := <-latencies
			lat = append(lat, l)
			if evictedAt == 0 {
				latWithSlow = append(latWithSlow, l)
			}
		}
		if evictedAt == 0 && hubs["node-a"].Stats().EvictedSlow == 1 {
			evictedAt = u
		}
	}
	latAfter := slices.Clone(lat[len(latWithSlow):])
	slices.Sort(lat)
	slices.Sort(latWithSlow)
	slices.Sort(latAfter)
	fmt.Printf("   slowpoke evicted at update %d: its writer was stuck on the broadcast, so its %d-slot queue filled\n",
		evictedAt, queueSize)
	fmt.Printf("   the other %d watchers got all %d updates (%d deliveries): latency p50 %v, p99 %v, max %v\n",
		watchers, updates, len(lat), pct(lat, 0.50).Round(time.Microsecond), pct(lat, 0.99).Round(time.Microsecond), lat[len(lat)-1].Round(time.Microsecond))
	fmt.Printf("   p50 while slowpoke was stuck (updates 1-%d): %v; after it was gone: %v\n",
		evictedAt, pct(latWithSlow, 0.50).Round(time.Microsecond), pct(latAfter, 0.50).Round(time.Microsecond))
	fmt.Printf("   slowest single SendToUser call: %v (it enqueues and never waits on a client's socket)\n", slowestSend.Round(time.Microsecond))
	fmt.Printf("   registry still lists slowpoke? %v\n\n", len(reg.Nodes("slowpoke")) > 0)

	fmt.Println("5. Heartbeat (fake clock, ping every 15s, timeout 20s): sleepy's phone went to sleep and stops answering")
	conns := total()
	pongs := func() uint64 {
		var s uint64
		for _, h := range hubs {
			s += h.Stats().Pongs
		}
		return s
	}
	clock.Advance(15 * time.Second)
	pinged := 0
	for _, n := range names {
		p, _ := hubs[n].Heartbeat()
		pinged += p
	}
	waitUntil(func() bool { return pongs() == uint64(conns-1) })
	fmt.Printf("   t=+15s: pinged %d connections, %d pongs came back\n", pinged, pongs())
	clock.Advance(15 * time.Second)
	evicted := 0
	for _, n := range names {
		_, e := hubs[n].Heartbeat()
		evicted += e
	}
	fmt.Printf("   t=+30s: removed %d connection (silent 30s > 20s); the rest were last heard at +15s\n", evicted)
	fmt.Printf("   registry still lists sleepy? %v; connections now %d\n\n", len(reg.Nodes("sleepy")) > 0, total())

	fmt.Println("6. Per-node totals")
	fmt.Printf("   %-7s %6s %8s %9s %8s %9s %9s\n", "node", "conns", "written", "forwarded", "from-bus", "slow-kick", "dead-kick")
	for _, n := range names {
		s := hubs[n].Stats()
		fmt.Printf("   %-7s %6d %8d %9d %8d %9d %9d\n", n, s.Connections, s.Written, s.Forwarded, s.FromBus, s.EvictedSlow, s.EvictedDead)
	}

	for _, n := range names {
		hubs[n].Close() // 1001 close handshake with every remaining client
	}
	for _, s := range servers {
		s.Close()
	}
	cluster.Close()
	_ = slowpoke.Close()
}
