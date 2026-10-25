// Package webhookdelivery implements both sides of reliable webhooks.
//
// The Sender signs each payload with HMAC-SHA256 over id.timestamp.body and
// retries with capped exponential backoff and jitter. It re-signs every
// attempt with a fresh timestamp.
//
// The Receiver verifies the signature in constant time, rejects stale
// timestamps (replay protection), and deduplicates by event ID, so
// at-least-once delivery becomes effectively-once processing.
package webhookdelivery

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Headers sent with every delivery.
const (
	HeaderID        = "Webhook-Id"
	HeaderSignature = "Webhook-Signature" // "t=<unix>,v1=<hex hmac>"
)

// Sign returns the signature header value. The event ID is part of the
// signed content. Otherwise an attacker could replay a captured body under
// a new ID and slip past deduplication.
func Sign(secret []byte, id string, ts int64, body []byte) string {
	return "t=" + strconv.FormatInt(ts, 10) + ",v1=" + hex.EncodeToString(mac(secret, id, ts, body))
}

func mac(secret []byte, id string, ts int64, body []byte) []byte {
	m := hmac.New(sha256.New, secret)
	fmt.Fprintf(m, "%s.%d.", id, ts)
	m.Write(body)
	return m.Sum(nil)
}

// Verification errors.
var (
	ErrBadSignature = errors.New("webhookdelivery: signature mismatch or malformed header")
	ErrStale        = errors.New("webhookdelivery: timestamp outside tolerance")
)

// Verify checks header against any of secrets (several secrets allow key
// rotation) and requires the timestamp to be within tolerance of now.
func Verify(secrets [][]byte, id, header string, body []byte, now time.Time, tolerance time.Duration) error {
	var ts int64
	var sigs [][]byte
	haveTS := false
	for _, part := range strings.Split(header, ",") {
		k, v, _ := strings.Cut(strings.TrimSpace(part), "=")
		switch k {
		case "t":
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return ErrBadSignature
			}
			ts, haveTS = n, true
		case "v1":
			if b, err := hex.DecodeString(v); err == nil {
				sigs = append(sigs, b)
			}
		}
	}
	if !haveTS || len(sigs) == 0 || id == "" {
		return ErrBadSignature
	}
	if age := now.Sub(time.Unix(ts, 0)); age > tolerance || age < -tolerance {
		return ErrStale
	}
	for _, secret := range secrets {
		want := mac(secret, id, ts, body)
		for _, got := range sigs {
			if hmac.Equal(got, want) {
				return nil
			}
		}
	}
	return ErrBadSignature
}

// Delivery errors.
var (
	ErrPermanent = errors.New("webhookdelivery: receiver rejected the event permanently")
	ErrExhausted = errors.New("webhookdelivery: retries exhausted")
)

// Sender delivers signed webhooks with retries.
type Sender struct {
	Client      *http.Client // nil means a client with a 10s timeout
	Secret      []byte
	MaxAttempts int           // 0 means 5
	BaseDelay   time.Duration // 0 means 500ms. Doubles each attempt
	MaxDelay    time.Duration // 0 means 1m
	Now         func() time.Time
	Sleep       func(context.Context, time.Duration) error
	Jitter      func(time.Duration) time.Duration // nil means "full jitter": rand in [0, d]
	OnAttempt   func(attempt, status int, err error)
}

// Deliver POSTs body to url until it gets a 2xx, a permanent failure (4xx
// other than 408/429), or runs out of attempts. It returns the number of
// attempts made.
func (s *Sender) Deliver(ctx context.Context, url, id string, body []byte) (int, error) {
	client := s.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	now, sleep, jitter := s.Now, s.Sleep, s.Jitter
	if now == nil {
		now = time.Now
	}
	if sleep == nil {
		sleep = sleepCtx
	}
	if jitter == nil {
		jitter = func(d time.Duration) time.Duration { return rand.N(d + 1) }
	}
	maxAttempts := s.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 5
	}
	base, maxDelay := orDefault(s.BaseDelay, 500*time.Millisecond), orDefault(s.MaxDelay, time.Minute)

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		status, retryAfter, err := s.attempt(ctx, client, url, id, body, now())
		if s.OnAttempt != nil {
			s.OnAttempt(attempt, status, err)
		}
		switch {
		case err == nil && status >= 200 && status < 300:
			return attempt, nil
		case err == nil && status >= 400 && status < 500 && status != 408 && status != 429:
			return attempt, fmt.Errorf("%w: status %d", ErrPermanent, status)
		case err != nil:
			lastErr = err
		default:
			lastErr = fmt.Errorf("status %d", status)
		}
		if attempt == maxAttempts || ctx.Err() != nil {
			break
		}
		delay := jitter(backoff(base, maxDelay, attempt))
		if retryAfter > delay {
			delay = min(retryAfter, maxDelay)
		}
		if err := sleep(ctx, delay); err != nil {
			return attempt, err
		}
	}
	return maxAttempts, fmt.Errorf("%w: %v", ErrExhausted, lastErr)
}

func (s *Sender) attempt(ctx context.Context, c *http.Client, url, id string, body []byte, now time.Time) (status int, retryAfter time.Duration, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(HeaderID, id)
	req.Header.Set(HeaderSignature, Sign(s.Secret, id, now.Unix(), body))
	resp, err := c.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if secs, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && secs > 0 {
		retryAfter = time.Duration(secs) * time.Second
	}
	return resp.StatusCode, retryAfter, nil
}

// backoff returns base*2^(attempt-1), capped at maxDelay without overflowing.
func backoff(base, maxDelay time.Duration, attempt int) time.Duration {
	d := base
	for i := 1; i < attempt && d < maxDelay; i++ {
		d *= 2
	}
	return min(d, maxDelay)
}

func orDefault(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return d
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Receiver is an idempotent webhook endpoint.
type Receiver struct {
	Secrets   [][]byte
	Tolerance time.Duration // 0 means 5 minutes
	Now       func() time.Time
	// Handle processes a verified event. Returning an error responds 500,
	// so the sender retries.
	Handle func(ctx context.Context, id string, body []byte) error

	mu   sync.Mutex
	seen map[string]entry
}

type entry struct {
	done bool
	at   time.Time
}

func (rc *Receiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
		return
	}
	now := time.Now
	if rc.Now != nil {
		now = rc.Now
	}
	id := r.Header.Get(HeaderID)
	if err := Verify(rc.Secrets, id, r.Header.Get(HeaderSignature), body, now(), orDefault(rc.Tolerance, 5*time.Minute)); err != nil {
		// 400, not 5xx: a bad signature will never succeed on retry.
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rc.mu.Lock()
	if rc.seen == nil {
		rc.seen = make(map[string]entry)
	}
	e, ok := rc.seen[id]
	if ok && e.done {
		rc.mu.Unlock()
		w.WriteHeader(http.StatusOK) // duplicate: acknowledge, don't reprocess
		return
	}
	if ok {
		rc.mu.Unlock()
		w.Header().Set("Retry-After", "1") // another delivery of this ID is in progress
		http.Error(w, "in progress", http.StatusServiceUnavailable)
		return
	}
	rc.seen[id] = entry{at: now()}
	rc.mu.Unlock()

	if rc.Handle != nil {
		err = rc.Handle(r.Context(), id, body)
	}

	rc.mu.Lock()
	if err != nil {
		delete(rc.seen, id) // release the claim so a retry can process it
	} else {
		rc.seen[id] = entry{done: true, at: now()}
	}
	rc.mu.Unlock()
	if err != nil {
		http.Error(w, "processing failed", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// Forget drops completed IDs recorded before cutoff. Keep IDs for longer
// than the sender's whole retry window. Retries carry fresh timestamps, so
// the timestamp check alone does not stop a late duplicate.
func (rc *Receiver) Forget(cutoff time.Time) int {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	n := 0
	for id, e := range rc.seen {
		if e.done && e.at.Before(cutoff) {
			delete(rc.seen, id)
			n++
		}
	}
	return n
}
