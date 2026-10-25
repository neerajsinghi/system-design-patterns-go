package webhookdelivery

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var (
	secret = []byte("whsec_test_secret")
	t0     = time.Unix(1_800_000_000, 0)
)

func TestSignVerify(t *testing.T) {
	body := []byte(`{"amount":100}`)
	good := Sign(secret, "evt_1", t0.Unix(), body)
	tests := []struct {
		name    string
		secrets [][]byte
		id      string
		header  string
		body    []byte
		now     time.Time
		want    error
	}{
		{"valid", [][]byte{secret}, "evt_1", good, body, t0, nil},
		{"rotated: new secret first, old still accepted", [][]byte{[]byte("new"), secret}, "evt_1", good, body, t0, nil},
		{"wrong secret", [][]byte{[]byte("other")}, "evt_1", good, body, t0, ErrBadSignature},
		{"tampered body", [][]byte{secret}, "evt_1", good, []byte(`{"amount":999}`), t0, ErrBadSignature},
		{"id swapped to dodge dedup", [][]byte{secret}, "evt_2", good, body, t0, ErrBadSignature},
		{"replayed after tolerance", [][]byte{secret}, "evt_1", good, body, t0.Add(6 * time.Minute), ErrStale},
		{"timestamp from the future", [][]byte{secret}, "evt_1", good, body, t0.Add(-6 * time.Minute), ErrStale},
		{"just inside tolerance", [][]byte{secret}, "evt_1", good, body, t0.Add(5 * time.Minute), nil},
		{"malformed header", [][]byte{secret}, "evt_1", "garbage", body, t0, ErrBadSignature},
		{"bad timestamp", [][]byte{secret}, "evt_1", "t=abc,v1=00", body, t0, ErrBadSignature},
		{"missing v1", [][]byte{secret}, "evt_1", "t=1800000000", body, t0, ErrBadSignature},
		{"missing id", [][]byte{secret}, "", good, body, t0, ErrBadSignature},
		{"extra v1 entries are fine", [][]byte{secret}, "evt_1", good + ",v1=deadbeef", body, t0, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := Verify(tc.secrets, tc.id, tc.header, tc.body, tc.now, 5*time.Minute)
			if !errors.Is(err, tc.want) {
				t.Errorf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

// scripted returns a server that replies with statuses in order, then 200.
func scripted(t *testing.T, statuses ...int) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i := int(n.Add(1)) - 1
		if i < len(statuses) {
			if statuses[i] == 429 {
				w.Header().Set("Retry-After", "3")
			}
			w.WriteHeader(statuses[i])
			return
		}
		w.WriteHeader(200)
	}))
	t.Cleanup(srv.Close)
	return srv, &n
}

func TestSenderRetries(t *testing.T) {
	const ms = time.Millisecond
	tests := []struct {
		name         string
		statuses     []int
		maxAttempts  int
		wantAttempts int
		wantErr      error
		wantDelays   []time.Duration
	}{
		{"first try", nil, 5, 1, nil, nil},
		{"recovers after 5xx", []int{500, 503}, 5, 3, nil, []time.Duration{100 * ms, 200 * ms}},
		{"backoff capped at max", []int{500, 500, 500, 500}, 5, 5, nil, []time.Duration{100 * ms, 200 * ms, 300 * ms, 300 * ms}},
		{"exhausted", []int{500, 500, 500}, 3, 3, ErrExhausted, []time.Duration{100 * ms, 200 * ms}},
		{"4xx is permanent", []int{400}, 5, 1, ErrPermanent, nil},
		{"410 gone is permanent", []int{410}, 5, 1, ErrPermanent, nil},
		{"429 honours Retry-After (capped)", []int{429}, 5, 2, nil, []time.Duration{300 * ms}},
		{"408 is retried", []int{408}, 5, 2, nil, []time.Duration{100 * ms}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := scripted(t, tc.statuses...)
			var delays []time.Duration
			s := &Sender{
				Secret: secret, MaxAttempts: tc.maxAttempts,
				BaseDelay: 100 * ms, MaxDelay: 300 * ms,
				Jitter: func(d time.Duration) time.Duration { return d },
				Sleep:  func(_ context.Context, d time.Duration) error { delays = append(delays, d); return nil },
			}
			n, err := s.Deliver(context.Background(), srv.URL, "evt_1", []byte(`{}`))
			if n != tc.wantAttempts || !errors.Is(err, tc.wantErr) {
				t.Errorf("attempts=%d err=%v, want %d %v", n, err, tc.wantAttempts, tc.wantErr)
			}
			if len(delays) != len(tc.wantDelays) {
				t.Fatalf("delays = %v, want %v", delays, tc.wantDelays)
			}
			for i := range delays {
				if delays[i] != tc.wantDelays[i] {
					t.Errorf("delays = %v, want %v", delays, tc.wantDelays)
				}
			}
		})
	}
}

func TestSenderResignsEachAttempt(t *testing.T) {
	var mu sync.Mutex
	var sigs []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		sigs = append(sigs, r.Header.Get(HeaderSignature))
		n := len(sigs)
		mu.Unlock()
		if n == 1 {
			w.WriteHeader(503)
		}
	}))
	defer srv.Close()
	clock := t0
	s := &Sender{
		Secret: secret, Now: func() time.Time { return clock },
		Sleep: func(_ context.Context, d time.Duration) error { clock = clock.Add(time.Hour); return nil },
	}
	if _, err := s.Deliver(context.Background(), srv.URL, "evt_1", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if len(sigs) != 2 || sigs[0] == sigs[1] || !strings.HasPrefix(sigs[1], "t=1800003600,") {
		t.Errorf("retry should carry a fresh timestamp: %v", sigs)
	}
}

func TestSenderCancellation(t *testing.T) {
	srv, _ := scripted(t, 500, 500, 500)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	s := &Sender{Secret: secret, BaseDelay: time.Hour}
	start := time.Now()
	_, err := s.Deliver(ctx, srv.URL, "evt_1", nil)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > time.Second {
		t.Errorf("err = %v after %v", err, time.Since(start))
	}
}

func TestEndToEndIdempotentReceiver(t *testing.T) {
	var processed atomic.Int32
	var failOnce atomic.Bool
	failOnce.Store(true)
	rc := &Receiver{Secrets: [][]byte{secret}, Handle: func(_ context.Context, id string, body []byte) error {
		if failOnce.Swap(false) {
			return errors.New("db down") // first try fails, so the sender must retry
		}
		processed.Add(1)
		return nil
	}}
	srv := httptest.NewServer(rc)
	defer srv.Close()
	s := &Sender{Secret: secret, Sleep: func(context.Context, time.Duration) error { return nil }}

	n, err := s.Deliver(context.Background(), srv.URL, "evt_42", []byte(`{"x":1}`))
	if err != nil || n != 2 {
		t.Fatalf("attempts=%d err=%v", n, err)
	}
	// The sender re-sends (lost ack, operator replay, ...). It is acknowledged, not reprocessed.
	if _, err := s.Deliver(context.Background(), srv.URL, "evt_42", []byte(`{"x":1}`)); err != nil {
		t.Fatal(err)
	}
	if processed.Load() != 1 {
		t.Errorf("processed %d times, want 1", processed.Load())
	}
	if rc.Forget(time.Now().Add(time.Second)) != 1 {
		t.Error("forget should drop the completed id")
	}
}

func TestConcurrentDuplicatesProcessedOnce(t *testing.T) {
	var processed atomic.Int32
	rc := &Receiver{Secrets: [][]byte{secret}, Handle: func(context.Context, string, []byte) error {
		processed.Add(1)
		time.Sleep(20 * time.Millisecond)
		return nil
	}}
	srv := httptest.NewServer(rc)
	defer srv.Close()
	s := &Sender{Secret: secret, BaseDelay: 5 * time.Millisecond, MaxDelay: 10 * time.Millisecond, MaxAttempts: 20}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.Deliver(context.Background(), srv.URL, "evt_dup", []byte(`{}`)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if processed.Load() != 1 {
		t.Errorf("processed %d times, want 1", processed.Load())
	}
}

func TestReceiverRejects(t *testing.T) {
	now := t0
	rc := &Receiver{Secrets: [][]byte{secret}, Now: func() time.Time { return now }}
	body := []byte(`{"ok":true}`)
	post := func(method, id, sig string, b []byte) int {
		req := httptest.NewRequest(method, "/hook", bytes.NewReader(b))
		req.Header.Set(HeaderID, id)
		req.Header.Set(HeaderSignature, sig)
		rec := httptest.NewRecorder()
		rc.ServeHTTP(rec, req)
		return rec.Code
	}
	sig := Sign(secret, "evt_1", t0.Unix(), body)
	if c := post(http.MethodGet, "evt_1", sig, body); c != 405 {
		t.Errorf("GET = %d", c)
	}
	if c := post(http.MethodPost, "evt_1", "t=1,v1=00", body); c != 400 {
		t.Errorf("bad sig = %d", c)
	}
	if c := post(http.MethodPost, "evt_1", sig, bytes.Repeat([]byte("a"), 2<<20)); c != 413 {
		t.Errorf("huge body = %d", c)
	}
	now = t0.Add(time.Hour) // captured request replayed an hour later
	if c := post(http.MethodPost, "evt_1", sig, body); c != 400 {
		t.Errorf("replay = %d", c)
	}
}
