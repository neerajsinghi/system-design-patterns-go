package healthcheck

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func ok(context.Context) error   { return nil }
func fail(context.Context) error { return errors.New("db unreachable") }

func TestLiveVsReady(t *testing.T) {
	tests := []struct {
		name      string
		live      CheckFunc
		ready     CheckFunc
		draining  bool
		wantLive  bool
		wantReady bool
	}{
		{"all healthy", ok, ok, false, true, true},
		{"dependency down: not ready but alive", ok, fail, false, true, false},
		{"process stuck: neither", fail, ok, false, false, false},
		{"draining: alive but not ready", ok, ok, true, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewChecker(50 * time.Millisecond)
			c.AddLiveness("loop", tt.live)
			c.AddReadiness("db", tt.ready)
			c.SetDraining(tt.draining)
			if got := c.Live(context.Background()).Healthy; got != tt.wantLive {
				t.Fatalf("live = %v, want %v", got, tt.wantLive)
			}
			if got := c.Ready(context.Background()).Healthy; got != tt.wantReady {
				t.Fatalf("ready = %v, want %v", got, tt.wantReady)
			}
		})
	}
}

func TestSlowCheckTimesOut(t *testing.T) {
	c := NewChecker(5 * time.Millisecond)
	block := make(chan struct{})
	defer close(block) // release the misbehaving check so its goroutine exits
	c.AddReadiness("ignores-ctx", func(context.Context) error { <-block; return nil })
	c.AddReadiness("honours-ctx", func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })
	start := time.Now()
	r := c.Ready(context.Background())
	if r.Healthy {
		t.Fatal("slow checks must fail readiness")
	}
	if time.Since(start) > 200*time.Millisecond {
		t.Fatalf("Ready took %v; checks must run concurrently with timeouts", time.Since(start))
	}
	if r.Checks["ignores-ctx"] != context.DeadlineExceeded.Error() {
		t.Fatalf("report = %v", r.Checks)
	}
}

func TestHandlers(t *testing.T) {
	c := NewChecker(time.Second)
	c.AddLiveness("loop", ok)
	c.AddReadiness("db", fail)
	tests := []struct {
		name     string
		h        http.Handler
		wantCode int
	}{
		{"live", c.LiveHandler(), http.StatusOK},
		{"ready", c.ReadyHandler(), http.StatusServiceUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			tt.h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
			if rec.Code != tt.wantCode {
				t.Fatalf("code = %d, want %d", rec.Code, tt.wantCode)
			}
			var rep Report
			if err := json.Unmarshal(rec.Body.Bytes(), &rep); err != nil {
				t.Fatal(err)
			}
			if rep.Checks["loop"] != "ok" {
				t.Fatalf("report = %+v", rep)
			}
		})
	}
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Add(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func TestDetector(t *testing.T) {
	tests := []struct {
		name    string
		silence time.Duration
		want    NodeState
	}{
		{"fresh", 0, Alive},
		{"just under suspect", 999 * time.Millisecond, Alive},
		{"suspect", time.Second, Suspect},
		{"still suspect", 2999 * time.Millisecond, Suspect},
		{"dead", 3 * time.Second, Dead},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clk := &fakeClock{t: time.Unix(0, 0)}
			d := NewDetector(time.Second, 3*time.Second, clk.Now)
			d.Heartbeat("n1")
			clk.Add(tt.silence)
			if got := d.State("n1"); got != tt.want {
				t.Fatalf("state = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDetectorRecoveryAndForget(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	d := NewDetector(time.Second, 500*time.Millisecond, clk.Now) // deadAfter raised to 1s
	if d.State("ghost") != Unknown {
		t.Fatal("unknown node")
	}
	d.Heartbeat("a")
	d.Heartbeat("b")
	clk.Add(time.Second)
	if d.State("a") != Dead {
		t.Fatalf("deadAfter should be raised to suspectAfter; got %v", d.State("a"))
	}
	d.Heartbeat("a")
	snap := d.Snapshot()
	if len(snap) != 2 || snap[0].Node != "a" || snap[0].State != Alive || snap[1].State != Dead {
		t.Fatalf("snapshot = %+v", snap)
	}
	d.Forget("b")
	if d.State("b") != Unknown {
		t.Fatal("forgotten node should be unknown")
	}
}

func TestNodeStateString(t *testing.T) {
	if Suspect.String() != "suspect" || NodeState(-1).String() != "unknown" || NodeState(99).String() != "unknown" {
		t.Fatal("bad String")
	}
}
