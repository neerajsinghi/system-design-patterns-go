// Package idempotencykey makes retried writes safe. The client generates a
// unique key per logical operation (for example "charge order 123") and sends
// it with every attempt. The server executes the operation once, stores the
// response under the key, and replays that response for every retry, so a
// timeout followed by a retry can never charge a customer twice.
package idempotencykey

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"
)

var (
	// ErrMissingKey: the request carried no idempotency key.
	ErrMissingKey = errors.New("idempotency: missing key")
	// ErrInProgress: another request with the same key is still executing (HTTP 409).
	ErrInProgress = errors.New("idempotency: request with this key is in progress")
	// ErrMismatch: the key was reused with a different request (HTTP 422).
	ErrMismatch = errors.New("idempotency: key reused with a different request")
)

// Response is what gets stored and replayed.
type Response struct {
	Status int
	Body   []byte
}

// Outcome says whether a call executed the operation or replayed it.
type Outcome int

const (
	Executed Outcome = iota
	Replayed
)

type record struct {
	fingerprint string
	done        bool
	resp        Response
	at          time.Time
}

// Store remembers responses by key for TTL. It is in-memory for teaching; a
// real deployment keeps records in the same database transaction as the side
// effect, or in a shared store such as Redis, so every replica sees them.
// Safe for concurrent use.
type Store struct {
	ttl time.Duration
	now func() time.Time

	mu      sync.Mutex
	records map[string]*record
}

// NewStore keeps records for ttl (<=0 means 24h). now may be nil.
func NewStore(ttl time.Duration, now func() time.Time) *Store {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	if now == nil {
		now = time.Now
	}
	return &Store{ttl: ttl, now: now, records: map[string]*record{}}
}

// Do executes fn at most once per key while the record lives.
//
//   - New key: fn runs. A returned Response (any status) is stored and
//     replayed later; a returned error stores nothing and frees the key, so the
//     client may retry. Only return an error when fn made no durable change.
//   - Same key and fingerprint, finished: the stored Response is replayed.
//   - Same key, still running: ErrInProgress.
//   - Same key, different fingerprint: ErrMismatch.
func (s *Store) Do(ctx context.Context, key, fingerprint string, fn func(context.Context) (Response, error)) (Response, Outcome, error) {
	if key == "" {
		return Response{}, Executed, ErrMissingKey
	}
	s.mu.Lock()
	now := s.now()
	if r, ok := s.records[key]; ok && now.Sub(r.at) < s.ttl {
		defer s.mu.Unlock()
		switch {
		case r.fingerprint != fingerprint:
			return Response{}, Replayed, ErrMismatch
		case !r.done:
			return Response{}, Replayed, ErrInProgress
		default:
			return cloneResp(r.resp), Replayed, nil
		}
	}
	rec := &record{fingerprint: fingerprint, at: now}
	s.records[key] = rec // claim the key before running, so duplicates see it
	s.mu.Unlock()

	resp, err := fn(ctx)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.records[key] != rec {
		// Our record expired and was replaced while fn ran; still answer the caller.
		return resp, Executed, err
	}
	if err != nil {
		delete(s.records, key)
		return Response{}, Executed, err
	}
	rec.done, rec.resp, rec.at = true, cloneResp(resp), s.now()
	return resp, Executed, nil
}

// Sweep deletes finished records older than TTL and returns how many it
// removed. In-progress records are kept, since their owner is still running.
func (s *Store) Sweep() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now, n := s.now(), 0
	for k, r := range s.records {
		if r.done && now.Sub(r.at) >= s.ttl {
			delete(s.records, k)
			n++
		}
	}
	return n
}

func cloneResp(r Response) Response {
	return Response{Status: r.Status, Body: bytes.Clone(r.Body)}
}

// Fingerprint hashes the parts of a request that must match on retry
// (method, path, body, caller identity). Parts are length-prefixed so that
// ("ab","c") and ("a","bc") differ.
func Fingerprint(parts ...[]byte) string {
	h := sha256.New()
	var n [8]byte
	for _, p := range parts {
		binary.BigEndian.PutUint64(n[:], uint64(len(p)))
		h.Write(n[:])
		h.Write(p)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// HeaderName is the conventional request header for the key.
const HeaderName = "Idempotency-Key"

// Middleware applies the store to non-safe HTTP methods. Requests without a
// key pass straight through; replays carry "Idempotent-Replayed: true".
// Only the status and body are stored and replayed, not response headers.
// Bodies are limited to maxBody bytes (<=0 means 1 MiB).
func Middleware(s *Store, maxBody int64, next http.Handler) http.Handler {
	if maxBody <= 0 {
		maxBody = 1 << 20
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get(HeaderName)
		if key == "" || r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodOptions {
			next.ServeHTTP(w, r)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, maxBody+1))
		if err != nil || int64(len(body)) > maxBody {
			http.Error(w, "request body too large or unreadable", http.StatusRequestEntityTooLarge)
			return
		}
		fp := Fingerprint([]byte(r.Method), []byte(r.URL.Path), body)
		resp, outcome, err := s.Do(r.Context(), key, fp, func(context.Context) (Response, error) {
			rec := &recorder{header: w.Header(), status: http.StatusOK}
			r.Body = io.NopCloser(bytes.NewReader(body))
			next.ServeHTTP(rec, r)
			return Response{Status: rec.status, Body: rec.buf.Bytes()}, nil
		})
		switch {
		case errors.Is(err, ErrInProgress):
			http.Error(w, err.Error(), http.StatusConflict)
			return
		case errors.Is(err, ErrMismatch):
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		case err != nil:
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if outcome == Replayed {
			w.Header().Set("Idempotent-Replayed", "true")
		}
		w.WriteHeader(resp.Status)
		_, _ = w.Write(resp.Body)
	})
}

// recorder buffers a handler's output so it can be stored before sending.
type recorder struct {
	header http.Header
	status int
	buf    bytes.Buffer
}

func (r *recorder) Header() http.Header         { return r.header }
func (r *recorder) Write(b []byte) (int, error) { return r.buf.Write(b) }
func (r *recorder) WriteHeader(code int)        { r.status = code }
