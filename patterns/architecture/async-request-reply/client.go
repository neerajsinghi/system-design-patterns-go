package asyncrequestreply

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// Client errors.
var (
	ErrNotFound     = errors.New("asyncrequestreply: job not found (unknown or expired)")
	ErrKeyMismatch  = errors.New("asyncrequestreply: idempotency key reused with a different request")
	ErrJobFailed    = errors.New("asyncrequestreply: job failed")
	ErrJobCanceled  = errors.New("asyncrequestreply: job canceled")
	ErrTooManyPolls = errors.New("asyncrequestreply: gave up polling")
)

// Client submits jobs to a Server and polls them to completion. The zero
// value is not usable: BaseURL is required. Safe for concurrent use as long
// as its fields are not changed.
type Client struct {
	BaseURL   string        // e.g. "http://127.0.0.1:8080"
	HTTP      *http.Client  // nil means a client with a 10s timeout
	BaseDelay time.Duration // first poll delay; 0 means 500ms. Doubles per poll
	MaxDelay  time.Duration // cap on the backoff; 0 means 30s
	MaxPolls  int           // 0 means poll until ctx is done
	// Sleep waits between polls; nil sleeps for real. Tests and simulations
	// inject their own.
	Sleep func(ctx context.Context, d time.Duration) error
	// OnPoll, if set, is called after every status poll with the poll
	// number, the status, and the delay before the next poll (0 when done).
	OnPoll func(poll int, st Status, next time.Duration)
}

// Result is what Wait returns.
type Result struct {
	Status Status
	Body   []byte // the job's result; nil unless it succeeded
	Polls  int    // status requests made
}

// Submission is the server's answer to a submit.
type Submission struct {
	Status     Status
	Location   string        // absolute status URL from the Location header
	RetryAfter time.Duration // the server's hint before the first poll (0 if none)
	Replayed   bool          // true when the key matched an existing job
}

// Submit POSTs input to /jobs. key may be "" (no idempotency) and
// callbackURL may be "" (no callback).
func (c *Client) Submit(ctx context.Context, input []byte, key, callbackURL string) (Submission, error) {
	target, err := c.resolve("/jobs")
	if err != nil {
		return Submission{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(input))
	if err != nil {
		return Submission{}, err
	}
	if key != "" {
		req.Header.Set(HeaderIdempotencyKey, key)
	}
	if callbackURL != "" {
		req.Header.Set(HeaderCallbackURL, callbackURL)
	}
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return Submission{}, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusAccepted:
	case http.StatusUnprocessableEntity:
		return Submission{}, ErrKeyMismatch
	default:
		return Submission{}, unexpected(resp)
	}
	var sub Submission
	if err := json.NewDecoder(resp.Body).Decode(&sub.Status); err != nil {
		return Submission{}, err
	}
	sub.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
	sub.Replayed = resp.Header.Get(HeaderReplayed) == "true"
	sub.Location, err = c.resolve(resp.Header.Get("Location"))
	return sub, err
}

// Await waits the submission's Retry-After before the first poll (the 202
// already said the job is not ready), then calls Wait.
func (c *Client) Await(ctx context.Context, sub Submission) (Result, error) {
	if sub.RetryAfter > 0 && !sub.Status.State.Terminal() {
		sleep := c.Sleep
		if sleep == nil {
			sleep = sleepCtx
		}
		if err := sleep(ctx, sub.RetryAfter); err != nil {
			return Result{}, err
		}
	}
	return c.Wait(ctx, sub.Location)
}

// Wait polls location until the job finishes. Each delay is the exponential
// backoff (BaseDelay doubling up to MaxDelay) or the server's Retry-After,
// whichever is longer: the client never polls sooner than the server asked.
// On 303 See Other it fetches the result from the Location it names.
func (c *Client) Wait(ctx context.Context, location string) (Result, error) {
	base, maxDelay := c.BaseDelay, c.MaxDelay
	if base <= 0 {
		base = 500 * time.Millisecond
	}
	if maxDelay <= 0 {
		maxDelay = 30 * time.Second
	}
	sleep := c.Sleep
	if sleep == nil {
		sleep = sleepCtx
	}
	hc := c.httpClient()
	backoff := base
	var res Result
	for {
		if c.MaxPolls > 0 && res.Polls >= c.MaxPolls {
			return res, ErrTooManyPolls
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, location, nil)
		if err != nil {
			return res, err
		}
		resp, err := hc.Do(req)
		if err != nil {
			return res, err
		}
		res.Polls++
		var st Status
		code := resp.StatusCode
		retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
		next := resp.Header.Get("Location")
		if code == http.StatusOK || code == http.StatusSeeOther {
			err = json.NewDecoder(resp.Body).Decode(&st)
		} else {
			err = unexpected(resp)
		}
		resp.Body.Close()
		switch {
		case code == http.StatusNotFound:
			return res, ErrNotFound
		case err != nil:
			return res, err
		}
		res.Status = st

		delay := time.Duration(0)
		if !st.State.Terminal() {
			delay = max(backoff, retryAfter)
			backoff = min(backoff*2, maxDelay)
		}
		if c.OnPoll != nil {
			c.OnPoll(res.Polls, st, delay)
		}
		switch {
		case code == http.StatusSeeOther:
			res.Body, err = c.fetch(ctx, hc, location, next)
			return res, err
		case st.State == Failed:
			return res, fmt.Errorf("%w: %s", ErrJobFailed, st.Error)
		case st.State == Canceled:
			return res, ErrJobCanceled
		case st.State.Terminal():
			return res, fmt.Errorf("asyncrequestreply: terminal state %q without a result", st.State)
		}
		if err := sleep(ctx, delay); err != nil {
			return res, err
		}
	}
}

// Cancel sends DELETE to location and returns the status the server reports.
func (c *Client) Cancel(ctx context.Context, location string) (Status, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, location, nil)
	if err != nil {
		return Status{}, err
	}
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return Status{}, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusAccepted, http.StatusConflict:
		var st Status
		err := json.NewDecoder(resp.Body).Decode(&st)
		return st, err
	case http.StatusNotFound:
		return Status{}, ErrNotFound
	default:
		return Status{}, unexpected(resp)
	}
}

func (c *Client) fetch(ctx context.Context, hc *http.Client, from, location string) ([]byte, error) {
	base, err := url.Parse(from)
	if err != nil {
		return nil, err
	}
	ref, err := url.Parse(location)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base.ResolveReference(ref).String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, unexpected(resp)
	}
	return io.ReadAll(resp.Body)
}

// httpClient returns a copy of the configured client that does not follow
// redirects, so Wait sees the 303 itself.
func (c *Client) httpClient() *http.Client {
	hc := http.Client{Timeout: 10 * time.Second}
	if c.HTTP != nil {
		hc = *c.HTTP
	}
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &hc
}

func (c *Client) resolve(ref string) (string, error) {
	if c.BaseURL == "" {
		return "", errors.New("asyncrequestreply: Client.BaseURL is required")
	}
	base, err := url.Parse(c.BaseURL)
	if err != nil {
		return "", err
	}
	r, err := url.Parse(ref)
	if err != nil {
		return "", err
	}
	return base.ResolveReference(r).String(), nil
}

// parseRetryAfter reads the delta-seconds form. The HTTP-date form is not
// used by this server and is ignored.
func parseRetryAfter(v string) time.Duration {
	if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return 0
}

func unexpected(resp *http.Response) error {
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return fmt.Errorf("asyncrequestreply: unexpected status %d: %s", resp.StatusCode, bytes.TrimSpace(msg))
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
