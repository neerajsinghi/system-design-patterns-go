// Command example walks through the classic "I updated my profile and it
// reverted" bug caused by replication lag, and the read-your-writes fix. It
// then carries the session across stateless app servers in a signed token,
// shows a monotonic-reads violation and its fix, and compares version
// tokens with the simpler "read from the leader for a while after a write".
package main

import (
	"fmt"
	"log"
	"strings"
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

	fmt.Println("\n1. Without read-your-writes: Ada renames herself to Grace")
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

	fmt.Println("\n2. With read-your-writes: Ada uses a session that remembers her last write version")
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
	fmt.Printf("\n3. Bob (no writes of his own) may still read stale data: %s=%q, %s=%q\n", from, val, from2, val2)
	s := cluster.Stats()
	fmt.Printf("  reads so far served by followers: %d, by leader: %d\n", s.FollowerReads, s.LeaderReads)

	crossServer()
	monotonic()
	compareStrategies()
}

// newCluster builds a fresh leader + 50ms + 500ms cluster on its own fake clock.
func newCluster(opts ...readreplicas.Option) (*readreplicas.Cluster, func(time.Duration)) {
	now := time.Unix(0, 0)
	clock := func() time.Time { return now }
	c, err := readreplicas.New([]time.Duration{50 * time.Millisecond, 500 * time.Millisecond},
		append([]readreplicas.Option{readreplicas.WithClock(clock)}, opts...)...)
	if err != nil {
		log.Fatal(err)
	}
	return c, func(d time.Duration) { now = now.Add(d) }
}

// crossServer: the session lives in memory on app server A, but the next
// request is load-balanced to app server B.
func crossServer() {
	fmt.Println("\n4. Profile update, then refresh lands on a different app server")
	c, advance := newCluster(readreplicas.WithTokenKey([]byte("shared-secret-for-all-app-servers")))
	c.Write("user:7:name", "Ada")
	advance(time.Second)

	serverA := c.NewSession()
	v := serverA.Write("user:7:name", "Grace")
	cookie := serverA.Token()
	fmt.Printf("  server A: write version %d, sets cookie rw=%s... (%d chars, HMAC-signed)\n", v, cookie[:12], len(cookie))

	fmt.Println("  server B, session kept only in server A's memory (B starts a blank session):")
	blank := c.NewSession()
	for range 2 {
		val, _, from := blank.Read("user:7:name")
		fmt.Printf("    read -> %-9s = %q\n", from, val)
	}

	fmt.Println("  server B, resuming the session from the cookie:")
	for _, wait := range []time.Duration{0, 60 * time.Millisecond} {
		advance(wait)
		serverB, err := c.ResumeSession(cookie)
		if err != nil {
			log.Fatal(err)
		}
		for range 2 {
			val, _, from := serverB.Read("user:7:name")
			fmt.Printf("    +%-4v read -> %-9s = %q\n", wait, from, val)
		}
	}

	// A client edits the cookie to change its version (base64 chars 1-11 encode lastWrite).
	tampered := []byte(cookie)
	if tampered[10] == 'A' {
		tampered[10] = 'B'
	} else {
		tampered[10] = 'A'
	}
	_, err := c.ResumeSession(string(tampered))
	fmt.Printf("  edited cookie: %v\n", err)
	_, err = c.ResumeSession("garbage")
	fmt.Printf("  garbage cookie: %v\n", err)
}

// monotonic: a reader with no writes of their own bounces between a fresh
// and a stale replica.
func monotonic() {
	fmt.Println("\n5. Monotonic reads: a live score page polled by a user who never writes")
	c, advance := newCluster()
	c.Write("match:score", "1-0")
	advance(time.Second)
	c.Write("match:score", "2-0")
	advance(100 * time.Millisecond)
	fmt.Printf("  replica versions now: %v (leader head %d)\n", c.ReplicaVersions(), c.Head())

	for _, tc := range []struct {
		name string
		opts []readreplicas.SessionOption
	}{
		{"plain session    ", nil},
		{"monotonic session", []readreplicas.SessionOption{readreplicas.WithMonotonicReads()}},
	} {
		s := c.NewSession(tc.opts...)
		var seen []string
		backwards := 0
		prev := ""
		for range 4 {
			val, _, from := s.Read("match:score")
			seen = append(seen, fmt.Sprintf("%s(%s)", val, from))
			if prev == "2-0" && val == "1-0" {
				backwards++
			}
			prev = val
		}
		fmt.Printf("  %s: %s  -> went back in time %d times\n", tc.name, strings.Join(seen, " "), backwards)
	}
	fmt.Println("  The monotonic session remembers it has read version 2 and skips replica-2 until it catches up.")
}

// compareStrategies: version tokens vs "stick to the leader for a window".
func compareStrategies() {
	fmt.Println("\n6. Version tokens vs leader-for-200ms-after-a-write (one write, then a read every 50ms from 0 to 550ms)")
	for _, tc := range []struct {
		name string
		opts []readreplicas.SessionOption
	}{
		{"version token          ", nil},
		{"leader for 200ms window", []readreplicas.SessionOption{readreplicas.WithLeaderAfterWrite(200 * time.Millisecond)}},
	} {
		c, advance := newCluster()
		c.Write("user:7:name", "Ada")
		advance(time.Second)
		s := c.NewSession(tc.opts...)
		s.Write("user:7:name", "Grace")
		before := c.Stats()
		stale := 0
		reads := 0
		for range 12 {
			if val, _, _ := s.Read("user:7:name"); val != "Grace" {
				stale++
			}
			reads++
			advance(50 * time.Millisecond)
		}
		st := c.Stats()
		fmt.Printf("  %s: %d reads, %2d on the leader, %d stale (own write missing)\n",
			tc.name, reads, st.LeaderReads-before.LeaderReads, stale)
	}
	fmt.Println("  The window sends every early read to the leader even though replica-1 caught up after 50ms,")
	fmt.Println("  and once it expires replica-2 (500ms lag) can still serve the old name.")
}
