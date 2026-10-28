package gracefulshutdown

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	healthcheck "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/health-check"
)

// start serves h on a loopback port through a Drainer.
func start(t *testing.T, h http.Handler, opts ...Option) (*Drainer, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	d, err := New(&http.Server{Handler: h}, opts...)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = d.srv.Serve(ln) }()
	t.Cleanup(func() { _ = d.srv.Close() })
	return d, "http://" + ln.Addr().String()
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(time.Millisecond)
	}
}

func get(client *http.Client, url string) error {
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

func TestNewValidation(t *testing.T) {
	srv := &http.Server{}
	tests := []struct {
		name string
		srv  *http.Server
		opts []Option
	}{
		{"nil server", nil, nil},
		{"negative delay", srv, []Option{WithDeregistrationDelay(-time.Second)}},
		{"zero drain", srv, []Option{WithDrainTimeout(0)}},
		{"nil clock", srv, []Option{WithClock(nil)}},
		{"nil sleep", srv, []Option{WithSleep(nil)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.srv, tt.opts...); err == nil {
				t.Fatal("expected error")
			}
		})
	}
	d, err := New(&http.Server{})
	if err != nil {
		t.Fatal(err)
	}
	if d.OnStop("x", 0, func(context.Context) error { return nil }) == nil {
		t.Error("zero hook timeout accepted")
	}
	if d.OnStop("x", time.Second, nil) == nil || d.Go("w", nil) == nil || d.OnDrain(nil) == nil {
		t.Error("nil func accepted")
	}
}

func TestNoRequestLostInsideDrainDeadline(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		_, _ = io.WriteString(w, "ok")
	})
	d, url := start(t, mux, WithDeregistrationDelay(0), WithDrainTimeout(5*time.Second))

	const n = 20
	errs := make(chan error, n)
	client := &http.Client{Transport: &http.Transport{}}
	for range n {
		go func() { errs <- get(client, url+"/slow") }()
	}
	waitFor(t, func() bool { return d.InFlight() == n })

	rep := d.Shutdown(context.Background())
	for range n {
		if err := <-errs; err != nil {
			t.Errorf("request lost: %v", err)
		}
	}
	if rep.Forced || len(rep.Cut) != 0 {
		t.Errorf("forced=%v cut=%v, want clean drain", rep.Forced, rep.Cut)
	}
	if rep.InFlightAtDrain != n {
		t.Errorf("InFlightAtDrain = %d, want %d", rep.InFlightAtDrain, n)
	}
	if _, ok := rep.At(PhaseDrained); !ok {
		t.Error("no drained phase in timeline")
	}
	if d.Served() != n {
		t.Errorf("Served = %d, want %d", d.Served(), n)
	}
}

func TestForcedCloseAfterDeadline(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	mux := http.NewServeMux()
	mux.HandleFunc("/hang", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	mux.HandleFunc("/quick", func(w http.ResponseWriter, r *http.Request) {})
	d, url := start(t, mux, WithDeregistrationDelay(0), WithDrainTimeout(50*time.Millisecond))

	errc := make(chan error, 1)
	go func() { errc <- get(&http.Client{Transport: &http.Transport{}}, url+"/hang") }()
	waitFor(t, func() bool { return d.InFlight() == 1 })

	t0 := time.Now()
	rep := d.Shutdown(context.Background())
	if took := time.Since(t0); took > 2*time.Second {
		t.Errorf("shutdown took %v, want about the 50ms deadline", took)
	}
	if !rep.Forced || len(rep.Cut) != 1 || rep.Cut[0] != "GET /hang" || rep.ForcedConns != 1 {
		t.Fatalf("forced=%v cut=%v conns=%d, want forced with GET /hang cut on 1 conn", rep.Forced, rep.Cut, rep.ForcedConns)
	}
	if err := <-errc; err == nil {
		t.Error("cut request reported success to the client")
	}
	if _, ok := rep.At(PhaseForced); !ok {
		t.Error("no forced phase in timeline")
	}
}

func TestReadinessFlipsBeforeListenerCloses(t *testing.T) {
	checker := healthcheck.NewChecker(time.Second)
	mux := http.NewServeMux()
	mux.Handle("/readyz", checker.ReadyHandler())
	mux.HandleFunc("/work", func(w http.ResponseWriter, r *http.Request) {})

	var url string
	// One connection per request, so no spare keep-alive connection is
	// left for Shutdown to wait on (see Report.ForcedConns).
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	var duringDelay struct{ ready, work int }
	// The injected sleep runs while the instance is "deregistering": not
	// ready, but still accepting and serving.
	sleep := func(ctx context.Context, _ time.Duration) {
		for path, dst := range map[string]*int{"/readyz": &duringDelay.ready, "/work": &duringDelay.work} {
			resp, err := client.Get(url + path)
			if err != nil {
				t.Errorf("%s during delay: %v", path, err)
				continue
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			*dst = resp.StatusCode
		}
	}
	d, u := start(t, mux, WithReadiness(checker), WithDeregistrationDelay(time.Hour), WithSleep(sleep))
	url = u
	if err := get(client, url+"/readyz"); err != nil {
		t.Fatalf("ready before shutdown: %v", err)
	}

	rep := d.Shutdown(context.Background())
	if duringDelay.ready != http.StatusServiceUnavailable || duringDelay.work != http.StatusOK {
		t.Errorf("during delay: /readyz %d /work %d, want 503 and 200", duringDelay.ready, duringDelay.work)
	}
	notReady, _ := rep.At(PhaseNotReady)
	closed, ok := rep.At(PhaseStopAccept)
	if !ok || notReady > closed {
		t.Errorf("readiness at %v, listener closed at %v", notReady, closed)
	}
	var phases []string
	for _, e := range rep.Timeline {
		phases = append(phases, e.Phase)
	}
	want := []string{PhaseRequested, PhaseNotReady, PhaseDeregistered, PhaseStopAccept, PhaseDrained, PhaseHooksDone}
	if strings.Join(phases, "|") != strings.Join(want, "|") {
		t.Errorf("timeline %v, want %v", phases, want)
	}
	if _, err := net.DialTimeout("tcp", strings.TrimPrefix(url, "http://"), time.Second); err == nil {
		t.Error("listener still accepting after shutdown")
	}
}

func TestHooksRunInReverseWithTimeouts(t *testing.T) {
	d, _ := start(t, http.NewServeMux(), WithDeregistrationDelay(0), WithDrainTimeout(time.Second))
	var mu sync.Mutex
	var order []string
	add := func(name string, timeout time.Duration, fn func(ctx context.Context) error) {
		if err := d.OnStop(name, timeout, func(ctx context.Context) error {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			return fn(ctx)
		}); err != nil {
			t.Fatal(err)
		}
	}
	stuck := make(chan struct{})
	defer close(stuck)
	add("db", time.Second, func(context.Context) error { return nil })
	add("cache", 20*time.Millisecond, func(context.Context) error { <-stuck; return nil }) // ignores ctx
	add("metrics", time.Second, func(context.Context) error { return errors.New("flush failed") })

	rep := d.Shutdown(context.Background())
	mu.Lock()
	got := strings.Join(order, ",")
	mu.Unlock()
	if got != "metrics,cache,db" {
		t.Errorf("hook order %s, want metrics,cache,db", got)
	}
	if len(rep.Hooks) != 3 {
		t.Fatalf("hooks = %+v", rep.Hooks)
	}
	if h := rep.Hooks[0]; h.Name != "metrics" || h.Err == nil || h.TimedOut {
		t.Errorf("metrics hook = %+v", h)
	}
	if h := rep.Hooks[1]; h.Name != "cache" || !h.TimedOut || h.Took > 500*time.Millisecond {
		t.Errorf("cache hook = %+v, want timed out after ~20ms", h)
	}
	if h := rep.Hooks[2]; h.Name != "db" || h.Err != nil {
		t.Errorf("db hook = %+v", h)
	}
	if d.OnStop("late", time.Second, func(context.Context) error { return nil }) != ErrShuttingDown {
		t.Error("hook registered after shutdown")
	}
}

func TestWorkersCancelledAndFinishCurrentItem(t *testing.T) {
	d, _ := start(t, http.NewServeMux(), WithDeregistrationDelay(0), WithDrainTimeout(150*time.Millisecond))
	queue := make(chan int, 100)
	for i := range 100 {
		queue <- i
	}
	var acked atomic.Int64
	consumer := func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case msg := <-queue:
				_ = msg
				time.Sleep(5 * time.Millisecond) // process; not interrupted
				acked.Add(1)
			}
		}
	}
	for range 3 {
		if err := d.Go("consumer", consumer); err != nil {
			t.Fatal(err)
		}
	}
	stuckRelease := make(chan struct{})
	defer close(stuckRelease)
	if err := d.Go("ignores-ctx", func(context.Context) error { <-stuckRelease; return nil }); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return acked.Load() >= 3 })

	rep := d.Shutdown(context.Background())
	if left := len(queue); left == 0 {
		t.Error("consumers drained the whole queue: they did not stop pulling")
	}
	if got := acked.Load() + int64(len(queue)); got != 100 {
		t.Errorf("acked+left = %d, want 100: a message was lost mid-processing", got)
	}
	cut := 0
	for _, w := range rep.Workers {
		switch {
		case w.Cut:
			cut++
			if w.Name != "ignores-ctx" {
				t.Errorf("worker %s cut", w.Name)
			}
		case !errors.Is(w.Err, context.Canceled):
			t.Errorf("worker %s returned %v, want context.Canceled", w.Name, w.Err)
		}
	}
	if cut != 1 || len(rep.Workers) != 4 {
		t.Errorf("workers = %+v, want 4 with 1 cut", rep.Workers)
	}
	if d.Go("late", consumer) != ErrShuttingDown {
		t.Error("worker started after shutdown")
	}
}

func TestDrainingNotifiesLongLivedHandlers(t *testing.T) {
	var d *Drainer
	ready := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		<-ready
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: hello\n\n")
		w.(http.Flusher).Flush()
		<-d.Draining()
		_, _ = io.WriteString(w, "event: reconnect\nretry: 0\n\n")
	})
	d, url := start(t, mux, WithDeregistrationDelay(0), WithDrainTimeout(5*time.Second))
	close(ready)
	var hubNotified atomic.Bool
	if err := d.OnDrain(func() { hubNotified.Store(true) }); err != nil {
		t.Fatal(err)
	}

	resp, err := (&http.Client{Transport: &http.Transport{}}).Get(url + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	sc.Scan() // "data: hello"
	rep := d.Shutdown(context.Background())
	var rest []string
	for sc.Scan() {
		rest = append(rest, sc.Text())
	}
	if !strings.Contains(strings.Join(rest, "\n"), "event: reconnect") {
		t.Errorf("stream ended without reconnect hint: %q", rest)
	}
	if rep.Forced {
		t.Error("SSE stream held the drain until the deadline")
	}
	waitFor(t, hubNotified.Load)
}

func TestShutdownIsIdempotent(t *testing.T) {
	var hookRuns, readyFlips atomic.Int64
	d, _ := start(t, http.NewServeMux(), WithDeregistrationDelay(10*time.Millisecond), WithDrainTimeout(time.Second),
		WithReadiness(readinessFunc(func(bool) { readyFlips.Add(1) })))
	_ = d.OnStop("once", time.Second, func(context.Context) error { hookRuns.Add(1); return nil })

	var wg sync.WaitGroup
	reps := make([]Report, 5)
	for i := range reps {
		wg.Add(1)
		go func() { defer wg.Done(); reps[i] = d.Shutdown(context.Background()) }()
	}
	wg.Wait()
	again := d.Shutdown(context.Background())
	if hookRuns.Load() != 1 || readyFlips.Load() != 1 {
		t.Errorf("hook ran %d times, readiness flipped %d times; want 1 and 1", hookRuns.Load(), readyFlips.Load())
	}
	for _, r := range append(reps, again) {
		if r.Elapsed != reps[0].Elapsed || len(r.Timeline) != len(reps[0].Timeline) {
			t.Errorf("callers got different reports")
		}
	}
}

type readinessFunc func(bool)

func (f readinessFunc) SetDraining(b bool) { f(b) }

func TestRunShutsDownOnSignal(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {})
	d, err := New(&http.Server{Handler: mux}, WithDeregistrationDelay(5*time.Millisecond), WithDrainTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	// Stands in for signal.NotifyContext: cancel is the "SIGTERM".
	sigCtx, sigterm := context.WithCancel(context.Background())
	type result struct {
		rep Report
		err error
	}
	done := make(chan result, 1)
	go func() { rep, err := d.Run(sigCtx, ln); done <- result{rep, err} }()
	if err := get(&http.Client{Transport: &http.Transport{}}, "http://"+ln.Addr().String()+"/"); err != nil {
		t.Fatal(err)
	}
	sigterm()
	res := <-done
	if res.err != nil || res.rep.Forced {
		t.Errorf("Run = %+v, %v", res.rep, res.err)
	}
	if _, ok := res.rep.At(PhaseHooksDone); !ok {
		t.Error("shutdown did not complete")
	}
}

func TestConcurrentLoadDuringShutdown(t *testing.T) {
	checker := healthcheck.NewChecker(time.Second)
	mux := http.NewServeMux()
	mux.HandleFunc("/work", func(w http.ResponseWriter, r *http.Request) { time.Sleep(2 * time.Millisecond) })
	d, url := start(t, mux, WithReadiness(checker), WithDeregistrationDelay(30*time.Millisecond), WithDrainTimeout(200*time.Millisecond))

	// Clients stop sending once readiness fails, as a load balancer would.
	var failed, ok atomic.Int64
	var wg sync.WaitGroup
	client := &http.Client{Transport: &http.Transport{}}
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for checker.Ready(context.Background()).Healthy {
				if err := get(client, url+"/work"); err != nil {
					failed.Add(1)
				} else {
					ok.Add(1)
				}
			}
		}()
	}
	waitFor(t, func() bool { return ok.Load() > 20 })
	rep := d.Shutdown(context.Background())
	wg.Wait()
	// rep.Forced may be true here: the client transport can leave a spare
	// connection that never sent a request, which Shutdown waits on. What
	// matters is that no request failed or was cut.
	if failed.Load() != 0 || len(rep.Cut) != 0 {
		t.Errorf("failed=%d cut=%v under load", failed.Load(), rep.Cut)
	}
}
