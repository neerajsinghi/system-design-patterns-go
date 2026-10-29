// Command example walks one instance through startup, a dependency outage
// and a graceful shutdown, showing how liveness and readiness diverge, then
// runs a heartbeat failure detector over a small cluster, and finally a
// per-connection heartbeat monitor over three client connections.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	healthcheck "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/health-check"
)

func probe(h http.Handler) string {
	rec := httptest.NewRecorder() // in-process: no network
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	return fmt.Sprint(rec.Code)
}

func main() {
	var dbUp, warm atomic.Bool
	c := healthcheck.NewChecker(100 * time.Millisecond)
	c.AddLiveness("goroutines", func(context.Context) error { return nil })
	c.AddReadiness("database", func(context.Context) error {
		if !dbUp.Load() {
			return errors.New("connection refused")
		}
		return nil
	})
	c.AddReadiness("cache-warm", func(context.Context) error {
		if !warm.Load() {
			return errors.New("still loading")
		}
		return nil
	})

	step := func(label string) {
		ready := c.Ready(context.Background())
		var failing []string
		for name, status := range ready.Checks {
			if status != "ok" {
				failing = append(failing, name+": "+status)
			}
		}
		sort.Strings(failing)
		note := ""
		if len(failing) > 0 {
			note = "  (" + strings.Join(failing, "; ") + ")"
		}
		fmt.Printf("%-28s /livez=%s /readyz=%s%s\n", label, probe(c.LiveHandler()), probe(c.ReadyHandler()), note)
	}

	fmt.Println("== Liveness vs readiness for one instance")
	step("1. booting")
	dbUp.Store(true)
	warm.Store(true)
	step("2. warmed up")
	dbUp.Store(false)
	step("3. database outage")
	fmt.Println("   -> LB stops routing here, but the pod is NOT restarted (a restart cannot fix the DB)")
	dbUp.Store(true)
	step("4. database back")
	c.SetDraining(true)
	step("5. SIGTERM: draining")
	fmt.Println("   -> new traffic goes elsewhere while in-flight requests finish")

	fmt.Println("\n== Heartbeat failure detector (suspect after 2s, dead after 5s)")
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	d := healthcheck.NewDetector(2*time.Second, 5*time.Second, func() time.Time { return now })
	nodes := []string{"node-a", "node-b", "node-c"}
	for sec := 0; sec <= 6; sec++ {
		for _, n := range nodes {
			switch {
			case n == "node-b" && sec >= 1: // node-b crashes after t=0
			case n == "node-c" && sec >= 2 && sec <= 3: // node-c has a 2s GC pause
			default:
				d.Heartbeat(n)
			}
		}
		var line []string
		for _, s := range d.Snapshot() {
			line = append(line, fmt.Sprintf("%s=%-7s", s.Node, s.State))
		}
		fmt.Printf("t=%ds  %s\n", sec, strings.TrimRight(strings.Join(line, " "), " "))
		now = now.Add(time.Second)
	}
	fmt.Println("node-c was only suspected during its pause; node-b was declared dead.")

	connectionHeartbeats()
}

// connectionHeartbeats simulates a server holding three client connections
// (think WebSocket) that must each send a heartbeat every second.
func connectionHeartbeats() {
	fmt.Println("\n== Per-connection heartbeats (beat every 1s, dead after 3 missed intervals)")
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := start
	var events []string
	m, err := healthcheck.NewHeartbeatMonitor(time.Second, 3,
		healthcheck.WithHeartbeatClock(func() time.Time { return now }),
		healthcheck.WithOnDead(func(d healthcheck.DeadConn) {
			events = append(events, fmt.Sprintf("%s DEAD (missed %d, last beat t=%ds) -> close socket, free its buffers",
				d.ID, d.Missed, int(d.LastBeat.Sub(start).Seconds())))
		}),
		healthcheck.WithOnLate(func(id string, missed int) {
			events = append(events, fmt.Sprintf("%s late by %d -> send an extra ping", id, missed))
		}),
	)
	if err != nil {
		panic(err)
	}
	conns := []string{"browser", "half-open", "mobile"}
	for sec := 0; sec <= 7; sec++ {
		for _, c := range conns {
			switch {
			case c == "half-open" && sec >= 2: // client's Wi-Fi vanished; no FIN, server socket still "ESTABLISHED"
			case c == "mobile" && (sec == 3 || sec == 4): // two missed beats in a tunnel, then back
			default:
				m.Beat(c)
			}
		}
		events = events[:0]
		m.Sweep()
		var line []string
		for _, c := range conns {
			if n, ok := m.Missed(c); ok {
				line = append(line, fmt.Sprintf("%s=missed %d", c, n))
			} else {
				line = append(line, c+"=gone")
			}
		}
		fmt.Printf("t=%ds  %s\n", sec, strings.Join(line, "  "))
		for _, e := range events {
			fmt.Printf("       %s\n", e)
		}
		now = now.Add(time.Second)
	}
	fmt.Println("mobile missed 2 in a row and survived; half-open was reaped 3s after its last beat.")
	fmt.Println("Linux TCP keepalive defaults (7200s idle + 9 probes x 75s) would have taken 2h11m15s,")
	fmt.Println("and would never notice a client whose TCP stack is fine but whose app is hung.")
}
