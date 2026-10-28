package conditionalrequests

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
)

// StatusError is returned for an unexpected HTTP status. errors.Is maps 412,
// 428 and 404 to ErrPreconditionFailed, ErrPreconditionRequired and
// ErrNotFound.
type StatusError struct {
	Code int
	// ETag is the server's current ETag, if the response carried one.
	ETag string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("conditionalrequests: unexpected status %d %s", e.Code, http.StatusText(e.Code))
}

// Is lets errors.Is match the sentinel errors.
func (e *StatusError) Is(target error) bool {
	switch target {
	case ErrPreconditionFailed:
		return e.Code == http.StatusPreconditionFailed
	case ErrPreconditionRequired:
		return e.Code == http.StatusPreconditionRequired
	case ErrNotFound:
		return e.Code == http.StatusNotFound
	}
	return false
}

// Fetched is the result of Client.Get.
type Fetched struct {
	Body         []byte
	ETag         string
	LastModified string
	// Revalidated is true when the server answered 304 and Body came from
	// the client's cache.
	Revalidated bool
}

// ClientStats counts what the client did.
type ClientStats struct {
	Requests    uint64 // HTTP requests sent
	Full        uint64 // 200 responses with a body
	NotModified uint64 // 304 responses served from cache
	BodyBytes   int64  // body bytes received
	Conflicts   uint64 // 412 responses to writes
}

type cached struct {
	etag, lastModified string
	body               []byte
}

// Client is a concurrency-safe HTTP client that caches GET bodies by URL
// with their ETag and revalidates them with If-None-Match.
type Client struct {
	hc         *http.Client
	revalidate bool

	mu    sync.Mutex
	cache map[string]cached
	stats ClientStats
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithRevalidation turns ETag caching and revalidation on (the default) or
// off. With it off, every Get downloads the full body: the baseline.
func WithRevalidation(on bool) ClientOption {
	return func(c *Client) { c.revalidate = on }
}

// NewClient wraps hc (http.DefaultClient if nil).
func NewClient(hc *http.Client, opts ...ClientOption) *Client {
	if hc == nil {
		hc = http.DefaultClient
	}
	c := &Client{hc: hc, revalidate: true, cache: make(map[string]cached)}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Stats returns a snapshot of the counters.
func (c *Client) Stats() ClientStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

// Get fetches url. If a cached copy exists it sends If-None-Match and, on
// 304, returns the cached body without downloading it again.
func (c *Client) Get(ctx context.Context, url string) (Fetched, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Fetched{}, err
	}
	c.mu.Lock()
	entry, have := c.cache[url]
	c.stats.Requests++
	c.mu.Unlock()
	if c.revalidate && have {
		req.Header.Set("If-None-Match", entry.etag)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return Fetched{}, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNotModified:
		_, _ = io.Copy(io.Discard, resp.Body)
		if !have {
			return Fetched{}, errors.New("conditionalrequests: 304 without a cached copy")
		}
		c.mu.Lock()
		c.stats.NotModified++
		c.mu.Unlock()
		return Fetched{Body: append([]byte(nil), entry.body...), ETag: entry.etag,
			LastModified: entry.lastModified, Revalidated: true}, nil
	case http.StatusOK:
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return Fetched{}, err
		}
		f := Fetched{Body: body, ETag: resp.Header.Get("ETag"), LastModified: resp.Header.Get("Last-Modified")}
		c.mu.Lock()
		c.stats.Full++
		c.stats.BodyBytes += int64(len(body))
		if c.revalidate && f.ETag != "" {
			c.cache[url] = cached{etag: f.ETag, lastModified: f.LastModified, body: append([]byte(nil), body...)}
		}
		c.mu.Unlock()
		return f, nil
	default:
		_, _ = io.Copy(io.Discard, resp.Body)
		c.forget(url)
		return Fetched{}, &StatusError{Code: resp.StatusCode, ETag: resp.Header.Get("ETag")}
	}
}

func (c *Client) forget(url string) {
	c.mu.Lock()
	delete(c.cache, url)
	c.mu.Unlock()
}

// Put replaces the resource at url with body. If ifMatch is non-empty it is
// sent as If-Match, so the write only succeeds if the resource is still that
// version; a mismatch returns an error matching ErrPreconditionFailed. It
// returns the new ETag.
func (c *Client) Put(ctx context.Context, url string, body []byte, ifMatch string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	if ifMatch != "" {
		req.Header.Set("If-Match", ifMatch)
	}
	c.mu.Lock()
	c.stats.Requests++
	c.mu.Unlock()
	resp, err := c.hc.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	// The cached copy is out of date either way. (The server may normalise
	// what we sent, so we don't assume our body is the new representation.)
	c.forget(url)
	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusNoContent:
		return resp.Header.Get("ETag"), nil
	case http.StatusPreconditionFailed:
		c.mu.Lock()
		c.stats.Conflicts++
		c.mu.Unlock()
	}
	return "", &StatusError{Code: resp.StatusCode, ETag: resp.Header.Get("ETag")}
}

// UpdateResult describes a successful Update.
type UpdateResult struct {
	Attempts int    // 1 means no conflict
	ETag     string // the ETag after the write
}

// Update performs a safe read-modify-write: GET the resource, compute the
// new body with modify, PUT it with If-Match set to the ETag that was read.
// On 412 someone else wrote in between, so it starts over from a fresh read,
// up to maxAttempts times. modify must be a pure function of the current
// body, because it can run more than once.
func (c *Client) Update(ctx context.Context, url string, maxAttempts int, modify func(current []byte) ([]byte, error)) (UpdateResult, error) {
	if maxAttempts < 1 {
		return UpdateResult{}, errors.New("conditionalrequests: maxAttempts must be at least 1")
	}
	if modify == nil {
		return UpdateResult{}, errors.New("conditionalrequests: nil modify function")
	}
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		f, err := c.Get(ctx, url)
		if err != nil {
			return UpdateResult{Attempts: attempt}, err
		}
		next, err := modify(f.Body)
		if err != nil {
			return UpdateResult{Attempts: attempt}, err
		}
		etag, err := c.Put(ctx, url, next, f.ETag)
		if errors.Is(err, ErrPreconditionFailed) {
			continue
		}
		if err != nil {
			return UpdateResult{Attempts: attempt}, err
		}
		return UpdateResult{Attempts: attempt, ETag: etag}, nil
	}
	return UpdateResult{Attempts: maxAttempts}, fmt.Errorf("%w (%d attempts)", ErrConflict, maxAttempts)
}
