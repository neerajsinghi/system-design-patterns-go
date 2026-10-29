package asyncrequestreply

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	webhookdelivery "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/webhook-delivery"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (f *fakeClock) Now() time.Time          { f.mu.Lock(); defer f.mu.Unlock(); return f.t }
func (f *fakeClock) Advance(d time.Duration) { f.mu.Lock(); f.t = f.t.Add(d); f.mu.Unlock() }

// gated is a Work whose jobs block until released, so tests control timing.
type gated struct {
	release chan struct{} // close or send to let a job finish
	started chan string   // receives each job's input when it starts
	calls   atomic.Int64
	fail    bool
}

func newGated() *gated {
	return &gated{release: make(chan struct{}), started: make(chan string, 64)}
}

func (g *gated) work(ctx context.Context, in []byte, progress func(float64)) ([]byte, error) {
	g.calls.Add(1)
	g.started <- string(in)
	progress(0.5)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-g.release:
	}
	if g.fail {
		return nil, errors.New("encoder crashed")
	}
	return []byte("done:" + string(in)), nil
}

// updates collects OnUpdate callbacks so tests can wait for a state.
type updates struct{ ch chan Status }

func newUpdates() *updates        { return &updates{ch: make(chan Status, 1024)} }
func (u *updates) hook(st Status) { u.ch <- st }
func (u *updates) waitFor(t *testing.T, id string, s State) Status {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case st := <-u.ch:
			if st.ID == id && st.State == s {
				return st
			}
		case <-timeout:
			t.Fatalf("timed out waiting for %s to become %s", id, s)
		}
	}
}

func start(t *testing.T, w Work, opts ...Option) (*Server, *httptest.Server) {
	t.Helper()
	s, err := New(w, opts...)
	if err != nil {
		t.Fatal(err)
	}
	hs := httptest.NewServer(s)
	t.Cleanup(func() { s.Close(); hs.Close() })
	return s, hs
}

func noRedirect() *http.Client {
	return &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

func do(t *testing.T, method, url, body string, hdr map[string]string) (*http.Response, Status) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := noRedirect().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var st Status
	if strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
		_ = json.NewDecoder(resp.Body).Decode(&st)
	}
	return resp, st
}

func TestNewValidation(t *testing.T) {
	w := newGated().work
	tests := []struct {
		name string
		work Work
		opts []Option
	}{
		{"nil work", nil, nil},
		{"zero workers", w, []Option{WithWorkers(0)}},
		{"zero queue", w, []Option{WithQueueSize(0)}},
		{"zero retention", w, []Option{WithRetention(0)}},
		{"negative retry-after", w, []Option{WithRetryAfter(-time.Second)}},
		{"zero max body", w, []Option{WithMaxBody(0)}},
		{"nil clock", w, []Option{WithClock(nil)}},
		{"nil sender", w, []Option{WithCallbacks(nil)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if s, err := New(tt.work, tt.opts...); err == nil {
				s.Close()
				t.Fatal("expected error")
			}
		})
	}
}

func TestStateTransitions(t *testing.T) {
	g, u := newGated(), newUpdates()
	_, hs := start(t, g.work, WithWorkers(1), WithOnUpdate(u.hook))

	// Job 1 occupies the only worker, so job 2 stays queued.
	resp, st1 := do(t, "POST", hs.URL+"/jobs", "a", nil)
	if resp.StatusCode != 202 || resp.Header.Get("Location") != "/jobs/"+st1.ID || resp.Header.Get("Retry-After") != "1" {
		t.Fatalf("submit: %d Location=%q Retry-After=%q", resp.StatusCode, resp.Header.Get("Location"), resp.Header.Get("Retry-After"))
	}
	<-g.started
	u.waitFor(t, st1.ID, Running)
	_, st2 := do(t, "POST", hs.URL+"/jobs", "b", nil)

	resp, st := do(t, "GET", hs.URL+"/jobs/"+st2.ID, "", nil)
	if resp.StatusCode != 200 || st.State != Queued || resp.Header.Get("Retry-After") != "1" {
		t.Fatalf("job 2: %d %+v", resp.StatusCode, st)
	}
	resp, st = do(t, "GET", hs.URL+"/jobs/"+st1.ID, "", nil)
	if resp.StatusCode != 200 || st.State != Running || st.Progress != 0.5 {
		t.Fatalf("job 1 running: %d %+v", resp.StatusCode, st)
	}
	resp, _ = do(t, "GET", hs.URL+"/jobs/"+st1.ID+"/result", "", nil)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("result before done: %d, want 409", resp.StatusCode)
	}

	g.release <- struct{}{}
	u.waitFor(t, st1.ID, Succeeded)
	resp, st = do(t, "GET", hs.URL+"/jobs/"+st1.ID, "", nil)
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/jobs/"+st1.ID+"/result" {
		t.Fatalf("done: %d Location=%q", resp.StatusCode, resp.Header.Get("Location"))
	}
	if st.State != Succeeded || st.Progress != 1 || resp.Header.Get("Retry-After") != "" {
		t.Fatalf("done status: %+v Retry-After=%q", st, resp.Header.Get("Retry-After"))
	}
	r, err := http.Get(hs.URL + resp.Header.Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if r.StatusCode != 200 || string(body) != "done:a" {
		t.Fatalf("result: %d %q", r.StatusCode, body)
	}

	// The default client follows the 303 straight to the result.
	r, _ = http.Get(hs.URL + "/jobs/" + st1.ID)
	body, _ = io.ReadAll(r.Body)
	r.Body.Close()
	if string(body) != "done:a" {
		t.Fatalf("followed redirect: %q", body)
	}

	// Job 2 now runs and succeeds.
	<-g.started
	close(g.release)
	u.waitFor(t, st2.ID, Succeeded)

	resp, _ = do(t, "GET", hs.URL+"/jobs/nope", "", nil)
	if resp.StatusCode != 404 {
		t.Fatalf("unknown job: %d", resp.StatusCode)
	}
}

func TestFailedJob(t *testing.T) {
	g, u := newGated(), newUpdates()
	g.fail = true
	close(g.release)
	_, hs := start(t, g.work, WithOnUpdate(u.hook))
	_, st := do(t, "POST", hs.URL+"/jobs", "x", nil)
	u.waitFor(t, st.ID, Failed)
	resp, st := do(t, "GET", hs.URL+"/jobs/"+st.ID, "", nil)
	if resp.StatusCode != 200 || st.State != Failed || st.Error != "encoder crashed" {
		t.Fatalf("%d %+v", resp.StatusCode, st)
	}
}

func TestPanicFailsJob(t *testing.T) {
	u := newUpdates()
	_, hs := start(t, func(context.Context, []byte, func(float64)) ([]byte, error) { panic("boom") }, WithOnUpdate(u.hook))
	_, st := do(t, "POST", hs.URL+"/jobs", "x", nil)
	if got := u.waitFor(t, st.ID, Failed); !strings.Contains(got.Error, "boom") {
		t.Fatalf("error = %q", got.Error)
	}
}

func TestIdempotentSubmit(t *testing.T) {
	g := newGated()
	close(g.release)
	s, hs := start(t, g.work)
	key := map[string]string{HeaderIdempotencyKey: "encode-42"}

	r1, st1 := do(t, "POST", hs.URL+"/jobs", "video-42", key)
	r2, st2 := do(t, "POST", hs.URL+"/jobs", "video-42", key)
	if r1.StatusCode != 202 || r2.StatusCode != 202 || st1.ID != st2.ID {
		t.Fatalf("same key: %d %s / %d %s", r1.StatusCode, st1.ID, r2.StatusCode, st2.ID)
	}
	if r1.Header.Get(HeaderReplayed) != "" || r2.Header.Get(HeaderReplayed) != "true" {
		t.Fatalf("replayed header: %q %q", r1.Header.Get(HeaderReplayed), r2.Header.Get(HeaderReplayed))
	}
	if r2.Header.Get("Location") != "/jobs/"+st1.ID {
		t.Fatalf("replay Location %q", r2.Header.Get("Location"))
	}
	r3, _ := do(t, "POST", hs.URL+"/jobs", "video-43", key)
	if r3.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("mismatched body: %d, want 422", r3.StatusCode)
	}
	_, a := do(t, "POST", hs.URL+"/jobs", "video-42", nil)
	_, b := do(t, "POST", hs.URL+"/jobs", "video-42", nil)
	if a.ID == b.ID || a.ID == st1.ID {
		t.Fatal("submits without a key must create separate jobs")
	}
	s.Close()
	if got := s.Stats(); got.Submitted != 3 || got.Executions != 3 {
		t.Fatalf("stats %+v, want 3 submitted and 3 executions", got)
	}
}

func TestConcurrentSameKeyCreatesOneJob(t *testing.T) {
	g := newGated()
	close(g.release)
	s, hs := start(t, g.work)
	var wg sync.WaitGroup
	ids := make([]string, 16)
	for i := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, st := do(t, "POST", hs.URL+"/jobs", "same", map[string]string{HeaderIdempotencyKey: "k"})
			ids[i] = st.ID
		}()
	}
	wg.Wait()
	for _, id := range ids {
		if id != ids[0] || id == "" {
			t.Fatalf("ids differ: %v", ids)
		}
	}
	s.Close()
	if n := s.Stats().Executions; n != 1 {
		t.Fatalf("executions = %d, want 1", n)
	}
}

func TestRetentionExpiry(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	g, u := newGated(), newUpdates()
	close(g.release)
	s, hs := start(t, g.work, WithClock(clk.Now), WithRetention(time.Hour), WithOnUpdate(u.hook))
	key := map[string]string{HeaderIdempotencyKey: "report-7"}

	_, st := do(t, "POST", hs.URL+"/jobs", "r", key)
	u.waitFor(t, st.ID, Succeeded)

	clk.Advance(time.Hour - time.Nanosecond)
	if resp, _ := do(t, "GET", hs.URL+"/jobs/"+st.ID, "", nil); resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("inside retention: %d", resp.StatusCode)
	}
	if n := s.Sweep(); n != 0 {
		t.Fatalf("swept %d inside retention", n)
	}
	clk.Advance(time.Nanosecond)
	if n := s.Sweep(); n != 1 {
		t.Fatalf("swept %d, want 1", n)
	}
	for _, path := range []string{"/jobs/" + st.ID, "/jobs/" + st.ID + "/result"} {
		if resp, _ := do(t, "GET", hs.URL+path, "", nil); resp.StatusCode != 404 {
			t.Fatalf("GET %s after expiry: %d", path, resp.StatusCode)
		}
	}
	// The key expired with the job, so the same key now starts new work.
	_, again := do(t, "POST", hs.URL+"/jobs", "r", key)
	if again.ID == st.ID {
		t.Fatal("expired key should create a new job")
	}
}

func TestRunningJobNeverExpires(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	g := newGated()
	s, hs := start(t, g.work, WithClock(clk.Now), WithRetention(time.Minute))
	_, st := do(t, "POST", hs.URL+"/jobs", "slow", nil)
	<-g.started
	clk.Advance(time.Hour)
	if n := s.Sweep(); n != 0 {
		t.Fatalf("swept a running job")
	}
	if _, ok := s.Get(st.ID); !ok {
		t.Fatal("running job disappeared")
	}
	close(g.release)
}

func TestCancel(t *testing.T) {
	g, u := newGated(), newUpdates()
	s, hs := start(t, g.work, WithWorkers(1), WithOnUpdate(u.hook))

	_, running := do(t, "POST", hs.URL+"/jobs", "running", nil)
	<-g.started
	u.waitFor(t, running.ID, Running)
	_, queued := do(t, "POST", hs.URL+"/jobs", "queued", nil)

	// Queued: canceled immediately, Work never called for it.
	resp, st := do(t, "DELETE", hs.URL+"/jobs/"+queued.ID, "", nil)
	if resp.StatusCode != 200 || st.State != Canceled {
		t.Fatalf("cancel queued: %d %+v", resp.StatusCode, st)
	}
	// Running: 202 while the worker stops, then canceled.
	resp, st = do(t, "DELETE", hs.URL+"/jobs/"+running.ID, "", nil)
	if resp.StatusCode != 202 || st.State != Running || !st.CancelRequested {
		t.Fatalf("cancel running: %d %+v", resp.StatusCode, st)
	}
	u.waitFor(t, running.ID, Canceled)
	// Idempotent DELETE.
	if resp, _ := do(t, "DELETE", hs.URL+"/jobs/"+running.ID, "", nil); resp.StatusCode != 200 {
		t.Fatalf("second delete: %d", resp.StatusCode)
	}
	if resp, _ := do(t, "DELETE", hs.URL+"/jobs/missing", "", nil); resp.StatusCode != 404 {
		t.Fatalf("delete unknown: %d", resp.StatusCode)
	}

	// A finished job cannot be canceled.
	close(g.release)
	_, done := do(t, "POST", hs.URL+"/jobs", "done", nil)
	u.waitFor(t, done.ID, Succeeded)
	if resp, _ := do(t, "DELETE", hs.URL+"/jobs/"+done.ID, "", nil); resp.StatusCode != http.StatusConflict {
		t.Fatalf("delete succeeded job: %d", resp.StatusCode)
	}
	s.Close()
	if n := g.calls.Load(); n != 2 {
		t.Fatalf("work calls = %d, want 2 (queued job never ran)", n)
	}
}

func TestCallbackFiredOnce(t *testing.T) {
	secret := []byte("whsec_test")
	var calls atomic.Int64
	var got Status
	var mu sync.Mutex
	rcv := httptest.NewServer(&webhookdelivery.Receiver{
		Secrets: [][]byte{secret},
		Handle: func(_ context.Context, id string, body []byte) error {
			calls.Add(1)
			mu.Lock()
			defer mu.Unlock()
			return json.Unmarshal(body, &got)
		},
	})
	defer rcv.Close()

	g, u := newGated(), newUpdates()
	sender := &webhookdelivery.Sender{Secret: secret, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond}
	s, hs := start(t, g.work, WithCallbacks(sender), WithOnUpdate(u.hook))
	hdr := map[string]string{HeaderCallbackURL: rcv.URL, HeaderIdempotencyKey: "cb"}
	_, st := do(t, "POST", hs.URL+"/jobs", "v", hdr)
	do(t, "POST", hs.URL+"/jobs", "v", hdr) // duplicate submit
	<-g.started
	close(g.release)
	u.waitFor(t, st.ID, Succeeded)
	s.Close() // waits for pending callbacks

	if calls.Load() != 1 {
		t.Fatalf("callback handled %d times, want 1", calls.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if got.ID != st.ID || got.State != Succeeded || got.ResultURL == "" {
		t.Fatalf("callback body %+v", got)
	}
	if stats := s.Stats(); stats.CallbacksSent != 1 || stats.CallbacksFailed != 0 {
		t.Fatalf("stats %+v", stats)
	}
}

func TestSubmitValidation(t *testing.T) {
	g := newGated()
	close(g.release)
	small, plain := start(t, g.work, WithMaxBody(8))
	_, withCB := start(t, g.work, WithCallbacks(&webhookdelivery.Sender{Secret: []byte("s")}))
	tests := []struct {
		name string
		url  string
		body string
		hdr  map[string]string
		want int
	}{
		{"callback not enabled", plain.URL, "x", map[string]string{HeaderCallbackURL: "http://example.test/hook"}, 400},
		{"relative callback", withCB.URL, "x", map[string]string{HeaderCallbackURL: "/hook"}, 400},
		{"non-http callback", withCB.URL, "x", map[string]string{HeaderCallbackURL: "ftp://example.test/hook"}, 400},
		{"ok", withCB.URL, "x", nil, 202},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if resp, _ := do(t, "POST", tt.url+"/jobs", tt.body, tt.hdr); resp.StatusCode != tt.want {
				t.Fatalf("status %d, want %d", resp.StatusCode, tt.want)
			}
		})
	}
	// Served in-process: over a real socket net/http lingers ~500ms before
	// closing a connection whose body it refused to read.
	rec := httptest.NewRecorder()
	small.ServeHTTP(rec, httptest.NewRequest("POST", "/jobs", strings.NewReader("123456789")))
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("body too large: %d", rec.Code)
	}
	if resp, _ := do(t, "PUT", plain.URL+"/jobs", "", nil); resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("PUT /jobs: %d", resp.StatusCode)
	}
}

func TestQueueFullAndClosed(t *testing.T) {
	g := newGated()
	s, hs := start(t, g.work, WithWorkers(1), WithQueueSize(1))
	do(t, "POST", hs.URL+"/jobs", "1", nil) // running
	<-g.started
	do(t, "POST", hs.URL+"/jobs", "2", nil) // queued
	resp, _ := do(t, "POST", hs.URL+"/jobs", "3", map[string]string{HeaderIdempotencyKey: "k3"})
	if resp.StatusCode != 503 || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("queue full: %d Retry-After=%q", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	s.Close() // cancels job 1, fails job 2 without running it
	if resp, _ := do(t, "POST", hs.URL+"/jobs", "4", nil); resp.StatusCode != 503 {
		t.Fatalf("after close: %d", resp.StatusCode)
	}
	if st, _ := s.Get("job-2"); st.State != Failed {
		t.Fatalf("queued job after close: %+v", st)
	}
	if n := g.calls.Load(); n != 1 {
		t.Fatalf("work calls = %d, want 1", n)
	}
	s.Close() // idempotent
}

func TestClientWaitHonoursRetryAfterAndBackoff(t *testing.T) {
	tests := []struct {
		name       string
		retryAfter time.Duration
		want       []time.Duration
	}{
		{"backoff grows past Retry-After", time.Second, []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 4 * time.Second}},
		{"Retry-After is the floor", 3 * time.Second, []time.Duration{3 * time.Second, 3 * time.Second, 4 * time.Second, 4 * time.Second}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g, u := newGated(), newUpdates()
			_, hs := start(t, g.work, WithRetryAfter(tt.retryAfter), WithOnUpdate(u.hook))
			c := &Client{BaseURL: hs.URL, BaseDelay: time.Second, MaxDelay: 4 * time.Second}
			sub, err := c.Submit(context.Background(), []byte("in"), "", "")
			if err != nil {
				t.Fatal(err)
			}
			st, loc := sub.Status, sub.Location
			if sub.RetryAfter != (tt.retryAfter+time.Second-1)/time.Second*time.Second {
				t.Fatalf("submission Retry-After %v", sub.RetryAfter)
			}
			<-g.started
			var delays []time.Duration
			c.Sleep = func(_ context.Context, d time.Duration) error {
				delays = append(delays, d)
				if len(delays) == 4 { // let the job finish during the 4th wait
					close(g.release)
					u.waitFor(t, st.ID, Succeeded)
				}
				return nil
			}
			res, err := c.Wait(context.Background(), loc)
			if err != nil {
				t.Fatal(err)
			}
			if string(res.Body) != "done:in" || res.Polls != 5 || res.Status.State != Succeeded {
				t.Fatalf("result %+v body %q", res, res.Body)
			}
			if fmt.Sprint(delays) != fmt.Sprint(tt.want) {
				t.Fatalf("delays %v, want %v", delays, tt.want)
			}
		})
	}
}

func TestClientErrors(t *testing.T) {
	g := newGated()
	g.fail = true
	close(g.release)
	_, hs := start(t, g.work)
	c := &Client{BaseURL: hs.URL, Sleep: func(context.Context, time.Duration) error { return nil }}
	ctx := context.Background()

	sub, err := c.Submit(ctx, []byte("x"), "", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Await(ctx, sub); !errors.Is(err, ErrJobFailed) {
		t.Fatalf("failed job: %v", err)
	}
	if _, err := c.Wait(ctx, hs.URL+"/jobs/nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown job: %v", err)
	}
	first, err := c.Submit(ctx, []byte("a"), "k", "")
	if err != nil {
		t.Fatal(err)
	}
	if again, err := c.Submit(ctx, []byte("a"), "k", ""); err != nil || !again.Replayed || again.Status.ID != first.Status.ID {
		t.Fatalf("replay: %+v %v", again, err)
	}
	if _, err := c.Submit(ctx, []byte("b"), "k", ""); !errors.Is(err, ErrKeyMismatch) {
		t.Fatalf("mismatch: %v", err)
	}
	if _, err := (&Client{}).Submit(ctx, nil, "", ""); err == nil {
		t.Fatal("missing BaseURL should fail")
	}

	slow := newGated()
	_, hs2 := start(t, slow.work)
	c2 := &Client{BaseURL: hs2.URL, MaxPolls: 2, Sleep: c.Sleep}
	sub, _ = c2.Submit(ctx, []byte("y"), "", "")
	loc := sub.Location
	if _, err := c2.Wait(ctx, loc); !errors.Is(err, ErrTooManyPolls) {
		t.Fatalf("max polls: %v", err)
	}
	if st, err := c2.Cancel(ctx, loc); err != nil || !(st.State == Canceled || st.CancelRequested) {
		t.Fatalf("cancel: %+v %v", st, err)
	}
}

func TestConcurrentClients(t *testing.T) {
	var calls atomic.Int64
	work := func(ctx context.Context, in []byte, progress func(float64)) ([]byte, error) {
		calls.Add(1)
		for i := 1; i <= 4; i++ {
			progress(float64(i) / 4)
		}
		return append([]byte("ok:"), in...), nil
	}
	s, hs := start(t, work, WithWorkers(4), WithQueueSize(64))
	c := &Client{BaseURL: hs.URL, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond,
		Sleep: func(context.Context, time.Duration) error { time.Sleep(time.Millisecond); return nil }}
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			in := fmt.Sprint(i)
			sub, err := c.Submit(context.Background(), []byte(in), "key-"+in, "")
			if err != nil {
				errs <- err
				return
			}
			res, err := c.Await(context.Background(), sub)
			if err == nil && string(res.Body) != "ok:"+in {
				err = fmt.Errorf("job %s got %q", in, res.Body)
			}
			if err != nil {
				errs <- err
			}
			_, _ = http.Get(hs.URL + "/jobs/missing")
			s.Sweep()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if calls.Load() != 32 {
		t.Fatalf("executions = %d, want 32", calls.Load())
	}
}
