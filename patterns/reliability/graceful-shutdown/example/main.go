// Command example shuts down one of two backends behind a simulated load
// balancer while clients keep sending traffic, first naively (close
// everything the moment SIGTERM arrives) and then with a Drainer. Everything
// runs over real net/http on loopback, with durations scaled to milliseconds.
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gracefulshutdown "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/graceful-shutdown"
	healthcheck "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/health-check"
)

const (
	clients      = 4
	workTime     = 3 * time.Millisecond   // a normal request
	slowTime     = 80 * time.Millisecond  // slow, but fits in the drain deadline
	exportTime   = 400 * time.Millisecond // longer than any deadline
	msgTime      = 15 * time.Millisecond  // one queue message
	lbPoll       = 10 * time.Millisecond  // load balancer health-check interval
	sigtermAt    = 60 * time.Millisecond
	deregDelay   = 40 * time.Millisecond
	drainTimeout = 150 * time.Millisecond
	tail         = 40 * time.Millisecond // traffic keeps flowing after the shutdown
)

type backend struct {
	name    string
	url     string
	ln      net.Listener
	srv     *http.Server
	checker *healthcheck.Checker
	drainer *gracefulshutdown.Drainer
	ready   atomic.Bool // the load balancer's view
	// set by the SSE handler to tell long-lived streams to wrap up
	draining func() <-chan struct{}
}

func newBackend(name string) *backend {
	b := &backend{name: name, checker: healthcheck.NewChecker(50 * time.Millisecond)}
	b.draining = func() <-chan struct{} { return nil } // never, unless a Drainer is attached
	mux := http.NewServeMux()
	mux.Handle("/readyz", b.checker.ReadyHandler())
	sleepHandler := func(d time.Duration) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-time.After(d):
				_, _ = io.WriteString(w, "ok")
			case <-r.Context().Done():
			}
		}
	}
	mux.Handle("/work", sleepHandler(workTime))
	mux.Handle("/slow", sleepHandler(slowTime))
	mux.Handle("/export", sleepHandler(exportTime))
	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				_, _ = io.WriteString(w, "data: tick\n\n")
				w.(http.Flusher).Flush()
			case <-b.draining():
				// Ask the client to reconnect now; the LB will send it elsewhere.
				_, _ = io.WriteString(w, "event: reconnect\nretry: 0\ndata: server draining\n\n")
				return
			case <-r.Context().Done():
				return
			}
		}
	})
	var err error
	if b.ln, err = net.Listen("tcp", "127.0.0.1:0"); err != nil {
		log.Fatal(err)
	}
	b.url = "http://" + b.ln.Addr().String()
	b.srv = &http.Server{Handler: mux}
	return b
}

// results of one run
type results struct {
	workOK, workFailed       int64
	slowOK, slowFailed       int
	exportOK                 bool
	sseHint                  bool
	acked, abandoned, queued int64
	report                   *gracefulshutdown.Report
}

func run(graceful bool) results {
	var res results
	a, b := newBackend("A"), newBackend("B")
	backends := []*backend{a, b}

	// Queue with plenty of messages; only A consumes in this demo.
	queue := make(chan int, 1000)
	for i := range 1000 {
		queue <- i
	}
	var acked, abandoned atomic.Int64
	kill := make(chan struct{}) // naive mode: the process just dies

	sigCtx, sigterm := context.WithCancel(context.Background()) // stands in for signal.NotifyContext
	runDone := make(chan gracefulshutdown.Report, 1)
	if graceful {
		d, err := gracefulshutdown.New(a.srv,
			gracefulshutdown.WithReadiness(a.checker),
			gracefulshutdown.WithDeregistrationDelay(deregDelay),
			gracefulshutdown.WithDrainTimeout(drainTimeout))
		if err != nil {
			log.Fatal(err)
		}
		a.drainer = d
		a.draining = d.Draining
		// Startup order: queue connection first, then the metrics exporter.
		must(d.OnStop("queue connection", 20*time.Millisecond, func(context.Context) error {
			time.Sleep(2 * time.Millisecond)
			return nil
		}))
		must(d.OnStop("metrics exporter", 20*time.Millisecond, func(context.Context) error {
			time.Sleep(time.Millisecond)
			return nil
		}))
		must(d.Go("consumer", func(ctx context.Context) error {
			for ctx.Err() == nil {
				select {
				case <-ctx.Done():
				case <-queue:
					time.Sleep(msgTime) // finish the message even if ctx is cancelled now
					acked.Add(1)
				}
			}
			return nil
		}))
		go func() {
			rep, err := d.Run(sigCtx, a.ln)
			if err != nil {
				log.Fatal(err)
			}
			runDone <- rep
		}()
	} else {
		go func() { _ = a.srv.Serve(a.ln) }()
		go func() {
			for {
				select {
				case <-kill:
					return
				case <-queue:
					select {
					case <-time.After(msgTime):
						acked.Add(1)
					case <-kill:
						abandoned.Add(1) // never acked: the broker redelivers it
						return
					}
				}
			}
		}()
	}
	go func() { _ = b.srv.Serve(b.ln) }()
	a.ready.Store(true)
	b.ready.Store(true)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Load balancer: polls /readyz on every backend.
	probe := &http.Client{Timeout: 50 * time.Millisecond, Transport: &http.Transport{DisableKeepAlives: true}}
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(lbPoll)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
			}
			for _, be := range backends {
				resp, err := probe.Get(be.url + "/readyz")
				ok := err == nil && resp.StatusCode == http.StatusOK
				if err == nil {
					_, _ = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
				be.ready.Store(ok)
			}
		}
	}()

	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: clients}}
	get := func(url string) error {
		resp, err := client.Get(url)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if _, err := io.ReadAll(resp.Body); err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			return errors.New(resp.Status)
		}
		return nil
	}

	// Steady load through the load balancer.
	var workOK, workFailed atomic.Int64
	var rr atomic.Uint64
	for range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// Round robin over the backends the LB currently thinks are ready.
				i := rr.Add(1)
				be := backends[i%2]
				if !be.ready.Load() {
					be = backends[(i+1)%2]
				}
				if get(be.url+"/work") == nil {
					workOK.Add(1)
				} else {
					workFailed.Add(1)
				}
			}
		}()
	}

	// A long-lived SSE stream that happens to be on A.
	sseDone := make(chan bool, 1)
	go func() {
		resp, err := client.Get(a.url + "/events")
		if err != nil {
			sseDone <- false
			return
		}
		defer resp.Body.Close()
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			if strings.HasPrefix(sc.Text(), "event: reconnect") {
				sseDone <- true
				return
			}
		}
		sseDone <- false // reset or EOF without a hint
	}()

	// Requests already routed to A shortly before SIGTERM.
	time.Sleep(sigtermAt - 10*time.Millisecond)
	slowErrs := make(chan error, 3)
	for range 3 {
		go func() { slowErrs <- get(a.url + "/slow") }()
	}
	exportErr := make(chan error, 1)
	go func() { exportErr <- get(a.url + "/export") }()
	time.Sleep(10 * time.Millisecond)

	// SIGTERM.
	sigterm()
	if graceful {
		rep := <-runDone
		res.report = &rep
	} else {
		close(kill)
		_ = a.srv.Close() // what os.Exit does to every open connection
	}
	time.Sleep(tail)
	close(stop)
	wg.Wait()

	for range 3 {
		if <-slowErrs == nil {
			res.slowOK++
		} else {
			res.slowFailed++
		}
	}
	res.exportOK = <-exportErr == nil
	res.sseHint = <-sseDone
	res.workOK, res.workFailed = workOK.Load(), workFailed.Load()
	res.acked, res.abandoned, res.queued = acked.Load(), abandoned.Load(), int64(len(queue))
	_ = a.srv.Close()
	_ = b.srv.Close()
	return res
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func yesNo(b bool, yes, no string) string {
	if b {
		return yes
	}
	return no
}

func main() {
	fmt.Println("Two backends (A, B) behind a load balancer that polls /readyz every 10ms.")
	fmt.Printf("%d clients send steady traffic; at t=%v A receives SIGTERM.\n\n", clients, sigtermAt)

	fmt.Println("1. Naive shutdown: close the server and stop the consumer at once")
	n := run(false)
	fmt.Printf("   normal requests:   %d ok, %d failed (in flight on A, or routed to A before the LB noticed)\n", n.workOK, n.workFailed)
	fmt.Printf("   slow requests:     %d ok, %d failed (connection reset)\n", n.slowOK, n.slowFailed)
	fmt.Printf("   export request:    %s\n", yesNo(n.exportOK, "ok", "failed"))
	fmt.Printf("   SSE stream:        %s\n", yesNo(n.sseHint, "got a reconnect hint", "dropped without a reconnect hint"))
	fmt.Printf("   queue consumer:    %d acked, %d abandoned mid-message (will be redelivered)\n\n", n.acked, n.abandoned)

	fmt.Printf("2. Graceful shutdown: readiness off, wait %v, drain up to %v, then stop hooks\n", deregDelay, drainTimeout)
	g := run(true)
	fmt.Printf("   normal requests:   %d ok, %d failed\n", g.workOK, g.workFailed)
	fmt.Printf("   slow requests:     %d ok, %d failed (finished inside the drain deadline)\n", g.slowOK, g.slowFailed)
	fmt.Printf("   export request:    %s (it needs %v, more than the deadline)\n", yesNo(g.exportOK, "ok", "cut"), exportTime)
	fmt.Printf("   SSE stream:        %s\n", yesNo(g.sseHint, "got a reconnect hint", "dropped without a reconnect hint"))
	fmt.Printf("   queue consumer:    %d acked, %d abandoned; %d messages left for other consumers\n", g.acked, g.abandoned, g.queued)

	rep := g.report
	fmt.Println("\n3. The Drainer's report for A")
	for _, e := range rep.Timeline {
		fmt.Printf("   +%-4dms %s\n", e.At.Milliseconds(), e.Phase)
	}
	fmt.Printf("   in flight when the listener closed: %d\n", rep.InFlightAtDrain)
	fmt.Printf("   forced close: %v, requests cut: %v, connections dropped: %d\n", rep.Forced, rep.Cut, rep.ForcedConns)
	for _, w := range rep.Workers {
		fmt.Printf("   worker %q: %s\n", w.Name, yesNo(w.Cut, "cut at the deadline", "stopped cleanly"))
	}
	for i, h := range rep.Hooks {
		fmt.Printf("   stop hook %d: %-17s err=%v (reverse of startup order)\n", i+1, h.Name, h.Err)
	}

	fmt.Println("\nSummary")
	fmt.Printf("   failed requests (excluding the export): naive %d, graceful %d\n",
		n.workFailed+int64(n.slowFailed), g.workFailed+int64(g.slowFailed))
	fmt.Println("   The export was cut either way: no deadline saves a request longer than the deadline,")
	fmt.Println("   but the graceful version names it in the report instead of failing silently.")
}
