package longpolling

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func newTopic(t *testing.T, opts ...Option) *Topic {
	t.Helper()
	tp, err := New(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tp.Close)
	return tp
}

func publish(t *testing.T, tp *Topic, data ...string) {
	t.Helper()
	for _, d := range data {
		if _, err := tp.Publish(d); err != nil {
			t.Fatal(err)
		}
	}
}

// waitHeld waits until the topic holds exactly n requests.
func waitHeld(t *testing.T, tp *Topic, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if tp.Held() == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("held = %d, want %d", tp.Held(), n)
}

func datas(b Batch) string {
	s := fmt.Sprint(b.Cursor, ":")
	for _, ev := range b.Events {
		s += fmt.Sprintf(" %d=%s", ev.Cursor, ev.Data)
	}
	if b.Reset {
		s += " reset"
	}
	return s
}

func TestNewValidates(t *testing.T) {
	tests := []struct {
		name string
		opt  Option
	}{
		{"zero max wait", WithMaxWait(0)},
		{"zero max held", WithMaxHeld(0)},
		{"zero history", WithHistory(0)},
		{"negative batch", WithMaxBatch(-1)},
		{"zero retry-after", WithRetryAfter(0)},
	}
	for _, tc := range tests {
		if _, err := New(tc.opt); err == nil {
			t.Errorf("%s: expected error", tc.name)
		}
	}
	if _, err := New(); err != nil {
		t.Errorf("defaults: %v", err)
	}
	for _, u := range []string{"", "/poll", "not a url"} {
		if _, err := NewClient(u, 0); err == nil {
			t.Errorf("NewClient(%q): expected error", u)
		}
	}
}

func TestImmediateAnswers(t *testing.T) {
	tests := []struct {
		name    string
		history int
		batch   int
		publish int
		cursor  uint64
		want    string
	}{
		{"from start", 10, 0, 3, 0, "3: 1=e1 2=e2 3=e3"},
		{"only newer", 10, 0, 3, 1, "3: 2=e2 3=e3"},
		{"batch cap", 10, 2, 5, 0, "2: 1=e1 2=e2"},
		{"fell behind history", 2, 0, 5, 1, "5: 4=e4 5=e5 reset"},
		{"exactly at history edge", 2, 0, 5, 3, "5: 4=e4 5=e5"},
		{"cursor from the future", 10, 0, 2, 99, "2: 1=e1 2=e2 reset"},
		{"future cursor, empty log", 10, 0, 0, 7, "0: reset"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tp := newTopic(t, WithHistory(tc.history), WithMaxBatch(tc.batch))
			for i := 1; i <= tc.publish; i++ {
				publish(t, tp, fmt.Sprint("e", i))
			}
			b, err := tp.Wait(context.Background(), tc.cursor, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if got := datas(b); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestWaiterWakesOnPublish(t *testing.T) {
	tp := newTopic(t)
	got := make(chan Batch, 1)
	go func() {
		b, _ := tp.Wait(context.Background(), 0, 5*time.Second)
		got <- b
	}()
	waitHeld(t, tp, 1)
	publish(t, tp, "hello")
	select {
	case b := <-got:
		if datas(b) != "1: 1=hello" {
			t.Errorf("got %s", datas(b))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter was not woken")
	}
	waitHeld(t, tp, 0)
	if s := tp.Stats(); s.Woken != 1 || s.TimedOut != 0 {
		t.Errorf("stats = %+v", s)
	}
}

func TestTimeoutReturnsEmpty(t *testing.T) {
	tp := newTopic(t, WithMaxWait(20*time.Millisecond))
	start := time.Now()
	b, err := tp.Wait(context.Background(), 0, time.Hour) // capped at max wait
	if err != nil || len(b.Events) != 0 || b.Cursor != 0 {
		t.Fatalf("got %s, %v", datas(b), err)
	}
	if el := time.Since(start); el < 20*time.Millisecond {
		t.Errorf("returned after %v, before the hold", el)
	}
	if s := tp.Stats(); s.TimedOut != 1 || tp.Held() != 0 {
		t.Errorf("stats = %+v, held = %d", s, tp.Held())
	}

	// The same over HTTP: 204 No Content.
	srv := httptest.NewServer(tp)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "?cursor=0&wait=5")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("status = %d", resp.StatusCode)
	}
}

func TestShortPollDoesNotHold(t *testing.T) {
	tp := newTopic(t)
	b, err := tp.Wait(context.Background(), 0, 0)
	if err != nil || len(b.Events) != 0 || tp.Held() != 0 {
		t.Fatalf("got %s, %v", datas(b), err)
	}
	if s := tp.Stats(); s.TimedOut != 0 {
		t.Errorf("a short poll is not a timeout: %+v", s)
	}
}

func TestNoLostUpdateBetweenPolls(t *testing.T) {
	tp := newTopic(t, WithMaxWait(time.Second))
	srv := httptest.NewServer(tp)
	defer srv.Close()
	c, err := NewClient(srv.URL, 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	go func() { // publish "a" once the first poll is being held
		for tp.Held() != 1 {
			time.Sleep(time.Millisecond)
		}
		_, _ = tp.Publish("a")
	}()
	b, err := c.Poll(ctx) // held, then woken by "a"
	if err != nil || datas(b) != "1: 1=a" {
		t.Fatalf("first poll: %s, %v", datas(b), err)
	}

	// The client is between requests. Nobody is waiting.
	if tp.Held() != 0 {
		t.Fatal("no request should be held between polls")
	}
	publish(t, tp, "b", "c")

	start := time.Now()
	b, err = c.Poll(ctx) // must return at once with both
	if err != nil || datas(b) != "3: 2=b 3=c" {
		t.Fatalf("second poll: %s, %v", datas(b), err)
	}
	if el := time.Since(start); el > 500*time.Millisecond {
		t.Errorf("second poll was held for %v", el)
	}
	if c.Cursor() != 3 {
		t.Errorf("cursor = %d", c.Cursor())
	}
	if s := tp.Stats(); s.Woken != 1 || s.Immediate != 1 {
		t.Errorf("stats = %+v", s)
	}
}

func TestMultipleWaitersAllWake(t *testing.T) {
	const n = 50
	tp := newTopic(t)
	var wg sync.WaitGroup
	results := make(chan string, n)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, err := tp.Wait(context.Background(), 0, 5*time.Second)
			if err != nil {
				results <- err.Error()
				return
			}
			results <- datas(b)
		}()
	}
	waitHeld(t, tp, n)
	publish(t, tp, "broadcast")
	wg.Wait()
	close(results)
	for r := range results {
		if r != "1: 1=broadcast" {
			t.Errorf("waiter got %s", r)
		}
	}
	if s := tp.Stats(); s.Woken != n || tp.Held() != 0 {
		t.Errorf("stats = %+v, held = %d", s, tp.Held())
	}
}

func TestClientCancelRemovesWaiter(t *testing.T) {
	tp := newTopic(t)

	// Directly: cancelling the context ends the wait and unregisters it.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := tp.Wait(ctx, 0, 5*time.Second)
		done <- err
	}()
	waitHeld(t, tp, 1)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v", err)
	}
	waitHeld(t, tp, 0)

	// Over HTTP: the client hangs up, the server notices via r.Context().
	srv := httptest.NewServer(tp)
	defer srv.Close()
	c, _ := NewClient(srv.URL, 0)
	ctx, cancel = context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(ctx, func(Event) {}) }()
	waitHeld(t, tp, 1)
	cancel()
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Errorf("run err = %v", err)
	}
	waitHeld(t, tp, 0)
	if s := tp.Stats(); s.Cancelled != 2 {
		t.Errorf("stats = %+v", s)
	}
}

func TestHeldRequestCap(t *testing.T) {
	tp := newTopic(t, WithMaxHeld(2), WithRetryAfter(1500*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for range 2 {
		go func() { _, _ = tp.Wait(ctx, 0, 5*time.Second) }()
	}
	waitHeld(t, tp, 2)

	if _, err := tp.Wait(context.Background(), 0, time.Second); !errors.Is(err, ErrTooManyWaiters) {
		t.Errorf("third waiter: err = %v", err)
	}
	// A poll that can be answered at once is not held, so the cap doesn't apply.
	other := newTopic(t, WithMaxHeld(1))
	publish(t, other, "x")
	if _, err := other.Wait(context.Background(), 0, time.Second); err != nil {
		t.Errorf("immediate answer rejected: %v", err)
	}

	srv := httptest.NewServer(tp)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "?cursor=0")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") != "2" {
		t.Errorf("status = %d, Retry-After = %q", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	c, _ := NewClient(srv.URL, 0)
	_, err = c.Poll(context.Background())
	var be *BusyError
	if !errors.As(err, &be) || be.RetryAfter != 2*time.Second || !errors.Is(err, ErrTooManyWaiters) {
		t.Errorf("client err = %v", err)
	}
	if s := tp.Stats(); s.Rejected != 3 {
		t.Errorf("stats = %+v", s)
	}
}

func TestCloseWakesWaitersAndRefusesNew(t *testing.T) {
	tp, _ := New()
	done := make(chan error, 1)
	go func() {
		_, err := tp.Wait(context.Background(), 0, 5*time.Second)
		done <- err
	}()
	waitHeld(t, tp, 1)
	tp.Close()
	if err := <-done; !errors.Is(err, ErrClosed) {
		t.Errorf("err = %v", err)
	}
	if _, err := tp.Publish("x"); !errors.Is(err, ErrClosed) {
		t.Errorf("publish after close: %v", err)
	}
	tp.Close() // idempotent
}

func TestHTTPBadRequests(t *testing.T) {
	tp := newTopic(t)
	srv := httptest.NewServer(tp)
	defer srv.Close()
	tests := []struct {
		method, query string
		want          int
	}{
		{http.MethodGet, "?cursor=abc", http.StatusBadRequest},
		{http.MethodGet, "?cursor=-1", http.StatusBadRequest},
		{http.MethodGet, "?wait=-5", http.StatusBadRequest},
		{http.MethodGet, "?wait=soon", http.StatusBadRequest},
		{http.MethodPost, "", http.StatusMethodNotAllowed},
		{http.MethodGet, "?cursor=0&wait=0", http.StatusNoContent},
	}
	for _, tc := range tests {
		req, _ := http.NewRequest(tc.method, srv.URL+tc.query, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Errorf("%s %s: status %d, want %d", tc.method, tc.query, resp.StatusCode, tc.want)
		}
	}
}

func TestClientRunDeliversEverythingInOrder(t *testing.T) {
	const total = 200
	tp := newTopic(t, WithMaxWait(50*time.Millisecond), WithMaxBatch(7))
	srv := httptest.NewServer(tp)
	defer srv.Close()

	got := make(chan Event, total)
	var clients []*Client
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for range 3 {
		c, _ := NewClient(srv.URL, 0)
		clients = append(clients, c)
		wg.Add(1)
		go func() {
			defer wg.Done()
			var last uint64
			_ = c.Run(ctx, func(ev Event) {
				if ev.Cursor != last+1 {
					t.Errorf("gap or duplicate: %d after %d", ev.Cursor, last)
				}
				last = ev.Cursor
				got <- ev
			})
		}()
	}
	var pubs sync.WaitGroup
	pubs.Add(1)
	go func() { // concurrent publisher
		defer pubs.Done()
		for i := range total {
			if _, err := tp.Publish(fmt.Sprint(i)); err != nil {
				t.Error(err)
			}
		}
	}()
	pubs.Wait()
	for range total * len(clients) {
		select {
		case <-got:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for events")
		}
	}
	cancel()
	wg.Wait()
	for _, c := range clients {
		if s := c.Stats(); c.Cursor() != total || s.Events != total || s.Resets != 0 {
			t.Errorf("cursor = %d, stats = %+v", c.Cursor(), c.Stats())
		}
	}
}
