// Command example compares a synchronous "encode this video" endpoint behind
// a load balancer timeout with the async request-reply version of the same
// endpoint: 202 Accepted, polling with backoff, a 303 to the result, an
// idempotent resubmit, a completion callback, a cancel and retention expiry.
//
// The synchronous half runs at 1/100 speed in real time (a 5s encode takes
// 50ms). The async half runs on a simulated clock: the encoder advances one
// step per simulated second, and the client's sleeps move that clock, so the
// printed numbers are the same on every run.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	arr "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/async-request-reply"
	wh "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/webhook-delivery"
)

const (
	speedup   = 100             // synchronous half: 1 simulated second = 10ms
	encode    = 5 * time.Second // an encode takes 5 steps of 1s
	steps     = 5
	lbTimeout = 2 * time.Second
	retries   = 3
	retention = 24 * time.Hour
)

func main() {
	fmt.Printf("Encoding a video takes %v. The load balancer cuts requests after %v.\n\n", encode, lbTimeout)
	syncStarted := synchronous()
	asyncStarted, polls := asynchronous()

	fmt.Println("Summary for one logical \"encode video-42\" request:")
	fmt.Printf("   %-34s %6s %6s\n", "", "sync", "async")
	fmt.Printf("   %-34s %6d %6d\n", "encodes started", syncStarted, asyncStarted)
	fmt.Printf("   %-34s %6s %6s\n", "client received the result", "no", "yes")
	fmt.Printf("   %-34s %5.0fs %5.0fs\n", "server time spent encoding", (time.Duration(syncStarted) * encode).Seconds(), (time.Duration(asyncStarted) * encode).Seconds())
	fmt.Printf("   %-34s %6d %6d\n", "request goroutines held for 5s", syncStarted, 0)
	fmt.Printf("   %-34s %6s %6d\n", "status polls", "-", polls)
}

// synchronous: the handler encodes inline. The client's timeout stands in for
// the load balancer, and the client retries on timeout as most SDKs do.
func synchronous() int64 {
	fmt.Println("1) Synchronous POST /encode, client retries on timeout:")
	var started, finished atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started.Add(1)
		time.Sleep(encode / speedup) // the handler goroutine is held the whole time
		finished.Add(1)
		fmt.Fprint(w, "encoded video-42")
	}))
	client := &http.Client{Timeout: lbTimeout / speedup}
	for attempt := 1; attempt <= 1+retries; attempt++ {
		resp, err := client.Post(srv.URL+"/encode", "text/plain", strings.NewReader("video-42"))
		if err == nil {
			resp.Body.Close()
			fmt.Printf("   attempt %d: %s\n", attempt, resp.Status)
			break
		}
		fmt.Printf("   attempt %d: gateway timeout after %v (the encode keeps running)\n", attempt, lbTimeout)
	}
	srv.Close() // waits for the orphaned handlers to finish
	fmt.Printf("   encodes started: %d, finished with nobody listening: %d, results delivered: 0\n\n",
		started.Load(), finished.Load())
	return started.Load()
}

// sim drives the async half deterministically. Each tick lets the encoder
// finish one step; Sleep advances the clock and sends one tick per simulated
// second, then waits until the server has recorded that step.
type sim struct {
	mu      sync.Mutex
	now     time.Time
	start   time.Time
	tick    chan struct{}
	updates chan arr.Status
	ticked  int
	done    bool
	encodes map[string]int // Work calls per input
}

func newSim() *sim {
	t := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	return &sim{now: t, start: t, tick: make(chan struct{}), updates: make(chan arr.Status, 256), encodes: map[string]int{}}
}

func (s *sim) Now() time.Time          { s.mu.Lock(); defer s.mu.Unlock(); return s.now }
func (s *sim) advance(d time.Duration) { s.mu.Lock(); s.now = s.now.Add(d); s.mu.Unlock() }
func (s *sim) elapsed() time.Duration  { return s.Now().Sub(s.start) }
func (s *sim) onUpdate(st arr.Status)  { s.updates <- st }

func (s *sim) work(ctx context.Context, in []byte, progress func(float64)) ([]byte, error) {
	s.mu.Lock()
	s.encodes[string(in)]++
	s.mu.Unlock()
	for i := 1; i <= steps; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.tick:
		}
		progress(float64(i) / steps)
	}
	return []byte("encoded " + string(in) + " (1080p, 720p, 480p)"), nil
}

// Sleep is the client's sleep: simulated time passes and the encoder moves.
func (s *sim) Sleep(_ context.Context, d time.Duration) error {
	for range int(d / time.Second) {
		s.advance(time.Second)
		if s.done {
			continue
		}
		s.tick <- struct{}{}
		s.ticked++
		for st := range s.updates {
			if st.State.Terminal() {
				s.done = true
				break
			}
			if st.State == arr.Running && s.ticked < steps && st.Progress >= float64(s.ticked)/steps {
				break
			}
		}
	}
	return nil
}

func (s *sim) waitFor(id string, state arr.State) {
	for st := range s.updates {
		if st.ID == id && st.State == state {
			return
		}
	}
}

func asynchronous() (executions int64, polls int) {
	ctx := context.Background()
	sm := newSim()

	// The customer's endpoint for completion callbacks.
	secret := []byte("whsec_demo")
	callbacks := make(chan arr.Status, 4)
	receiver := httptest.NewServer(&wh.Receiver{
		Secrets: [][]byte{secret},
		Handle: func(_ context.Context, _ string, body []byte) error {
			var st arr.Status
			if err := json.Unmarshal(body, &st); err != nil {
				return err
			}
			callbacks <- st
			return nil
		},
	})
	defer receiver.Close()

	srv, err := arr.New(sm.work,
		arr.WithWorkers(2),
		arr.WithClock(sm.Now),
		arr.WithRetention(retention),
		arr.WithRetryAfter(time.Second),
		arr.WithCallbacks(&wh.Sender{Secret: secret}),
		arr.WithOnUpdate(sm.onUpdate),
	)
	if err != nil {
		log.Fatal(err)
	}
	api := httptest.NewServer(srv)
	defer api.Close()
	defer srv.Close()

	client := &arr.Client{
		BaseURL:   api.URL,
		BaseDelay: time.Second,
		MaxDelay:  4 * time.Second,
		Sleep:     sm.Sleep,
		OnPoll: func(n int, st arr.Status, next time.Duration) {
			line := fmt.Sprintf("   poll %d at t=%v: %s %3.0f%%", n, sm.elapsed(), st.State, st.Progress*100)
			switch {
			case next > 0:
				line += fmt.Sprintf(", next poll in %v", next)
			case st.State == arr.Succeeded:
				line += ", 303 See Other -> " + st.ResultURL
			}
			fmt.Println(line)
		},
	}

	fmt.Println("2) Async: POST /jobs answers at once, the client polls with backoff:")
	sub, err := client.Submit(ctx, []byte("video-42"), "encode-video-42", receiver.URL)
	if err != nil {
		log.Fatal(err)
	}
	loc := strings.TrimPrefix(sub.Location, api.URL)
	fmt.Printf("   202 Accepted, Location: %s, Retry-After: %v, state %s\n", loc, sub.RetryAfter, sub.Status.State)
	res, err := client.Await(ctx, sub)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   result: %q after %d polls at t=%v (the encode itself took %v)\n", res.Body, res.Polls, sm.elapsed(), encode)
	cb := <-callbacks
	fmt.Printf("   callback POSTed to the customer: %s %s\n\n", cb.ID, cb.State)

	fmt.Println("3) The client lost the 202 and resubmits with the same Idempotency-Key:")
	again, err := client.Submit(ctx, []byte("video-42"), "encode-video-42", receiver.URL)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   202 again, same job %s (Idempotent-Replayed: %v), encodes started so far: %d\n",
		again.Status.ID, again.Replayed, srv.Stats().Executions)
	_, err = client.Submit(ctx, []byte("video-43"), "encode-video-42", receiver.URL)
	fmt.Printf("   same key, different video: %v (HTTP 422)\n\n", err)

	fmt.Println("4) A second job is canceled while it runs:")
	subB, err := client.Submit(ctx, []byte("video-99"), "", "")
	if err != nil {
		log.Fatal(err)
	}
	sm.waitFor(subB.Status.ID, arr.Running)
	st, err := client.Cancel(ctx, subB.Location)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   DELETE %s -> 202, cancelRequested=%v\n", strings.TrimPrefix(subB.Location, api.URL), st.CancelRequested)
	sm.waitFor(subB.Status.ID, arr.Canceled)
	_, err = client.Wait(ctx, subB.Location)
	fmt.Printf("   polling it now: %v\n\n", err)

	fmt.Println("5) 24h later, finished jobs are past retention:")
	sm.advance(retention)
	fmt.Printf("   Sweep removed %d jobs\n", srv.Sweep())
	_, err = client.Wait(ctx, sub.Location)
	fmt.Printf("   GET %s: %v\n\n", loc, err)

	srv.Close() // waits for any pending callbacks
	stats := srv.Stats()
	fmt.Printf("   server stats: %d jobs submitted, %d encodes started, %d callback sent\n\n",
		stats.Submitted, stats.Executions, stats.CallbacksSent)
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return int64(sm.encodes["video-42"]), res.Polls
}
