package idempotencykey

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Add(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func TestDo(t *testing.T) {
	ok := func(context.Context) (Response, error) { return Response{Status: 201, Body: []byte("charged")}, nil }
	tests := []struct {
		name        string
		firstKey    string
		firstFP     string
		firstFn     func(context.Context) (Response, error)
		advance     time.Duration
		secondKey   string
		secondFP    string
		wantOutcome Outcome
		wantErr     error
		wantExec    int32 // total executions of the second fn
	}{
		{"retry replays", "k1", "fp", ok, 0, "k1", "fp", Replayed, nil, 0},
		{"different key executes", "k1", "fp", ok, 0, "k2", "fp", Executed, nil, 1},
		{"key reuse with different body", "k1", "fp", ok, 0, "k1", "other", Replayed, ErrMismatch, 0},
		{"expired record executes again", "k1", "fp", ok, 25 * time.Hour, "k1", "fp", Executed, nil, 1},
		{"failed first attempt frees key", "k1", "fp", func(context.Context) (Response, error) { return Response{}, errors.New("db down") }, 0, "k1", "fp", Executed, nil, 1},
		{"4xx responses are replayed too", "k1", "fp", func(context.Context) (Response, error) { return Response{Status: 402, Body: []byte("declined")}, nil }, 0, "k1", "fp", Replayed, nil, 0},
		{"missing key", "k1", "fp", ok, 0, "", "fp", Executed, ErrMissingKey, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clk := &fakeClock{t: time.Unix(0, 0)}
			s := NewStore(24*time.Hour, clk.Now)
			first, _, _ := s.Do(context.Background(), tt.firstKey, tt.firstFP, tt.firstFn)
			clk.Add(tt.advance)
			var execs atomic.Int32
			resp, outcome, err := s.Do(context.Background(), tt.secondKey, tt.secondFP, func(context.Context) (Response, error) {
				execs.Add(1)
				return Response{Status: 201, Body: []byte("charged again")}, nil
			})
			if !errors.Is(err, tt.wantErr) || (tt.wantErr == nil && err != nil) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err == nil && outcome != tt.wantOutcome {
				t.Fatalf("outcome = %v, want %v", outcome, tt.wantOutcome)
			}
			if execs.Load() != tt.wantExec {
				t.Fatalf("executions = %d, want %d", execs.Load(), tt.wantExec)
			}
			if outcome == Replayed && err == nil && string(resp.Body) != string(first.Body) {
				t.Fatalf("replayed %q, want %q", resp.Body, first.Body)
			}
		})
	}
}

func TestConcurrentDuplicatesExecuteOnce(t *testing.T) {
	s := NewStore(time.Hour, nil)
	release := make(chan struct{})
	var execs, turnedAway atomic.Int32
	var wg sync.WaitGroup
	results := make([]error, 10)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, err := s.Do(context.Background(), "same", "fp", func(context.Context) (Response, error) {
				execs.Add(1)
				<-release
				return Response{Status: 200}, nil
			})
			if errors.Is(err, ErrInProgress) {
				turnedAway.Add(1)
			}
			results[i] = err
		}(i)
	}
	// Wait until one call is executing and the other nine have been turned away.
	for execs.Load() != 1 || turnedAway.Load() != 9 {
		time.Sleep(100 * time.Microsecond)
	}
	close(release)
	wg.Wait()
	conflicts := 0
	for _, err := range results {
		if errors.Is(err, ErrInProgress) {
			conflicts++
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if execs.Load() != 1 || conflicts != 9 {
		t.Fatalf("executions = %d, conflicts = %d; want 1 and 9", execs.Load(), conflicts)
	}
}

func TestStoredResponseIsIsolated(t *testing.T) {
	s := NewStore(time.Hour, nil)
	body := []byte("original")
	_, _, _ = s.Do(context.Background(), "k", "fp", func(context.Context) (Response, error) { return Response{Status: 200, Body: body}, nil })
	body[0] = 'X' // caller mutates its slice
	r, _, _ := s.Do(context.Background(), "k", "fp", nil)
	r.Body[1] = 'Y' // and the replayed copy
	r2, _, _ := s.Do(context.Background(), "k", "fp", nil)
	if string(r2.Body) != "original" {
		t.Fatalf("stored body was mutated: %q", r2.Body)
	}
}

func TestSweep(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	s := NewStore(time.Minute, clk.Now)
	ok := func(context.Context) (Response, error) { return Response{Status: 200}, nil }
	_, _, _ = s.Do(context.Background(), "old", "fp", ok)
	clk.Add(2 * time.Minute)
	_, _, _ = s.Do(context.Background(), "new", "fp", ok)
	if n := s.Sweep(); n != 1 {
		t.Fatalf("swept %d, want 1", n)
	}
}

func TestFingerprint(t *testing.T) {
	if Fingerprint([]byte("ab"), []byte("c")) == Fingerprint([]byte("a"), []byte("bc")) {
		t.Fatal("fingerprint must be unambiguous across part boundaries")
	}
	if Fingerprint([]byte("x")) != Fingerprint([]byte("x")) {
		t.Fatal("fingerprint must be deterministic")
	}
}

func TestMiddleware(t *testing.T) {
	var charges atomic.Int32
	h := Middleware(NewStore(time.Hour, nil), 64, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := charges.Add(1)
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, "charge #%d", n)
	}))
	do := func(method, key, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "/charges", strings.NewReader(body))
		if key != "" {
			req.Header.Set(HeaderName, key)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	tests := []struct {
		name, method, key, body string
		wantCode                int
		wantBody                string
		wantReplay              bool
	}{
		{"first", http.MethodPost, "abc", `{"amount":100}`, 201, "charge #1", false},
		{"retry", http.MethodPost, "abc", `{"amount":100}`, 201, "charge #1", true},
		{"mismatch", http.MethodPost, "abc", `{"amount":999}`, 422, "", false},
		{"no key passes through", http.MethodPost, "", `{}`, 201, "charge #2", false},
		{"GET ignores key", http.MethodGet, "abc", "", 201, "charge #3", false},
		{"body too large", http.MethodPost, "big", strings.Repeat("x", 100), 413, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(tt.method, tt.key, tt.body)
			if rec.Code != tt.wantCode {
				t.Fatalf("code = %d, want %d (%s)", rec.Code, tt.wantCode, rec.Body)
			}
			if tt.wantBody != "" && rec.Body.String() != tt.wantBody {
				t.Fatalf("body = %q, want %q", rec.Body, tt.wantBody)
			}
			if got := rec.Header().Get("Idempotent-Replayed") == "true"; got != tt.wantReplay {
				t.Fatalf("replayed header = %v, want %v", got, tt.wantReplay)
			}
		})
	}
}
