// Command example compares short polling with long polling for a sparse
// stream of updates over loopback HTTP.
//
// Both clients watch the same topic at the same time, so they see identical
// traffic. Time is scaled down so the run takes well under a second:
// 1 simulated second = 8ms of real time. Short polling asks every 2
// simulated seconds; long polling lets the server hold each request for up
// to 25 simulated seconds.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	longpolling "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/long-polling"
)

const scale = 8 * time.Millisecond // one simulated second

// sim converts simulated seconds to real time.
func sim(s float64) time.Duration { return time.Duration(s * float64(scale)) }

// When (in simulated seconds) each update is published. Sparse and bursty,
// with one 28s silence that is longer than the long-poll hold.
var schedule = []float64{1.5, 4.2, 4.4, 11.0, 18.3, 19.0, 47.5, 52.0, 56.0}

const runFor = 60.0 // simulated seconds

type result struct {
	stats              longpolling.ClientStats
	delivered          int
	avgLatency, maxLat float64 // simulated seconds
}

func main() {
	topic, err := longpolling.New(longpolling.WithMaxWait(sim(25)))
	if err != nil {
		log.Fatal(err)
	}
	srv := httptest.NewServer(topic)
	defer srv.Close()

	fmt.Printf("Time is scaled: 1 simulated second = %v real. All times below are simulated.\n", scale)
	fmt.Printf("%d updates over %.0fs; the longest silence is 28.5s.\n\n", len(schedule), runFor)

	// Step 1: run both clients against the same stream.
	fmt.Println("1. Short polling (every 2s) and long polling (hold up to 25s) watch the same topic")
	var mu sync.Mutex
	published := map[uint64]time.Time{}

	short, _ := longpolling.NewClient(srv.URL, 0)
	short.Hold = longpolling.NoHold
	short.Interval = sim(2)
	long, _ := longpolling.NewClient(srv.URL, 0)
	// long.Hold = 0: the server's 25s maximum

	ctx, cancel := context.WithTimeout(context.Background(), sim(runFor))
	defer cancel()
	var wg sync.WaitGroup
	results := make([]result, 2)
	for i, c := range []*longpolling.Client{short, long} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var total, worst time.Duration
			n := 0
			_ = c.Run(ctx, func(ev longpolling.Event) {
				mu.Lock()
				lat := time.Since(published[ev.Cursor])
				mu.Unlock()
				total += lat
				worst = max(worst, lat)
				n++
			})
			r := result{stats: c.Stats(), delivered: n}
			if n > 0 {
				r.avgLatency = float64(total) / float64(n) / float64(scale)
				r.maxLat = float64(worst) / float64(scale)
			}
			results[i] = r
		}()
	}

	start := time.Now()
	for i, at := range schedule {
		time.Sleep(time.Until(start.Add(sim(at))))
		mu.Lock() // record the time before publishing, so latency is never negative
		published[uint64(i+1)] = time.Now()
		mu.Unlock()
		if _, err := topic.Publish(fmt.Sprintf("update %d", i+1)); err != nil {
			log.Fatal(err)
		}
	}
	wg.Wait()

	fmt.Printf("   %-14s %9s %9s %7s %13s %13s\n", "", "requests", "delivered", "empty", "avg latency", "max latency")
	for i, name := range []string{"short polling", "long polling"} {
		r := results[i]
		fmt.Printf("   %-14s %9d %9d %7d %12.2fs %12.2fs\n",
			name, r.stats.Requests, r.delivered, r.stats.Empty, r.avgLatency, r.maxLat)
	}
	s := topic.Stats()
	fmt.Printf("   server: %d long polls woken by a publish, %d timed out after 25s with 204 and was re-sent\n\n",
		s.Woken, s.TimedOut)
	fmt.Println("   Short polling pays ~1 request per 2s whether or not anything happened, and an")
	fmt.Println("   update waits for the next tick. Long polling sends one request per update, plus")
	fmt.Println("   one per 25s of silence, and delivers almost at once: its latency is one wakeup")
	fmt.Println("   and one loopback response, not a wait for the next tick.")
	fmt.Println("   The last long poll was still held when the run ended; it counts as a request.")
	fmt.Println("   Real timers drive this run, so counts shift by one or two and latencies by a")
	fmt.Println("   fraction of a second between runs. On a busy machine a few ms of scheduling")
	fmt.Println("   delay is already a simulated second, so long polling's max can jump.")

	// Step 2: a publish while the client is between polls is not lost.
	fmt.Println("\n2. An update published between two polls")
	c, _ := longpolling.NewClient(srv.URL, topic.Head())
	if _, err := topic.Publish("published while nobody was waiting"); err != nil {
		log.Fatal(err)
	}
	t0 := time.Now()
	b, err := c.Poll(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   next poll with cursor=%d returned at once (%s held) with %d event: cursor %d %q\n",
		topic.Head()-1, heldFor(time.Since(t0)), len(b.Events), b.Events[0].Cursor, b.Events[0].Data)

	// Step 3: the held-request cap.
	fmt.Println("\n3. Capping held requests at 3 on a fresh topic, then sending 5 long polls")
	capped, _ := longpolling.New(longpolling.WithMaxHeld(3), longpolling.WithMaxWait(sim(25)))
	csrv := httptest.NewServer(capped)
	defer csrv.Close()
	cctx, ccancel := context.WithCancel(context.Background())
	for range 3 {
		go func() {
			hc, _ := longpolling.NewClient(csrv.URL, 0)
			_, _ = hc.Poll(cctx)
		}()
	}
	for capped.Held() < 3 {
		time.Sleep(time.Millisecond)
	}
	for i := 4; i <= 5; i++ {
		resp, err := http.Get(csrv.URL + "?cursor=0")
		if err != nil {
			log.Fatal(err)
		}
		resp.Body.Close()
		fmt.Printf("   poll %d: %d %s, Retry-After: %ss\n", i, resp.StatusCode, http.StatusText(resp.StatusCode), resp.Header.Get("Retry-After"))
	}
	fmt.Printf("   held: %d, rejected: %d\n", capped.Held(), capped.Stats().Rejected)
	ccancel() // the three held clients hang up
	for capped.Held() > 0 {
		time.Sleep(time.Millisecond)
	}
	fmt.Printf("   after the held clients disconnect: held %d, cancelled %d\n", capped.Held(), capped.Stats().Cancelled)
	capped.Close()
	topic.Close()
}

func heldFor(d time.Duration) string {
	if d < sim(1) {
		return "under 1s"
	}
	return fmt.Sprintf("%.1fs", float64(d)/float64(scale))
}
