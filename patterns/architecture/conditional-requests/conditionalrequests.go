// Package conditionalrequests implements HTTP conditional requests (RFC 9110
// §13): validators (ETag and Last-Modified) on every response, and the
// If-None-Match, If-Modified-Since and If-Match preconditions that use them.
//
// Two different problems share one mechanism:
//
//   - Wasted transfers. A client polling a resource, or a cache whose copy
//     has gone stale, downloads the whole body again even when nothing
//     changed. With validators it sends "If-None-Match: <etag>" instead, and
//     the server answers 304 Not Modified with no body when the ETag still
//     matches.
//   - Lost updates. Two editors read version 1, both edit, both PUT, and the
//     second PUT silently erases the first. With "If-Match: <etag>" the write
//     only happens if the resource is still the version the client read;
//     otherwise the server answers 412 Precondition Failed and the client
//     re-reads, re-applies its change and tries again. This is optimistic
//     concurrency control expressed in HTTP headers.
//
// The package has four parts: a versioned in-memory Store whose Mutate
// evaluates preconditions and writes under one lock (so check-then-write has
// no race), an http.Handler that serves the store with validators and
// preconditions, ETag parsing and the strong/weak comparison functions, and
// a Client that caches bodies by URL, revalidates them, and offers a
// read-modify-write Update helper that retries on 412.
package conditionalrequests

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Errors returned by the store, handler helpers and client.
var (
	// ErrPreconditionFailed means an If-Match / If-None-Match condition was
	// false (HTTP 412).
	ErrPreconditionFailed = errors.New("conditionalrequests: precondition failed")
	// ErrPreconditionRequired means the server requires a conditional write
	// and the request had none (HTTP 428).
	ErrPreconditionRequired = errors.New("conditionalrequests: precondition required")
	// ErrNotFound means the resource does not exist (HTTP 404).
	ErrNotFound = errors.New("conditionalrequests: not found")
	// ErrConflict means Client.Update kept losing the race and gave up.
	ErrConflict = errors.New("conditionalrequests: update conflicted on every attempt")
)

// Resource is one stored representation and its validators.
type Resource struct {
	Key         string
	Body        []byte
	ContentType string
	// Version is unique across the whole store and grows on every write,
	// so an ETag is never reused, even after delete and re-create.
	Version uint64
	// ModTime is when the resource was last written. Last-Modified only has
	// one-second resolution, which is one reason ETags are preferred.
	ModTime time.Time
	// ETag is a strong entity tag, quoted: "<version>-<content hash>".
	ETag string
}

func (r Resource) clone() Resource {
	r.Body = append([]byte(nil), r.Body...)
	return r
}

// Change is what a Mutation wants done to a resource.
type Change struct {
	Body []byte
	// ContentType of the new body; empty keeps the current one (or
	// application/octet-stream for a new resource).
	ContentType string
	// Delete removes the resource; Body and ContentType are ignored.
	Delete bool
}

// Mutation decides, given the current resource (nil if absent), what to
// write. Returning an error aborts the write. It runs under the store lock,
// so it must be quick and must not call back into the store.
type Mutation func(cur *Resource) (Change, error)

// Store is a concurrency-safe, versioned key/value store of resources.
type Store struct {
	now func() time.Time

	mu      sync.RWMutex
	items   map[string]Resource
	version uint64
}

// StoreOption configures a Store.
type StoreOption func(*Store)

// WithClock injects the clock used for ModTime / Last-Modified.
func WithClock(now func() time.Time) StoreOption {
	return func(s *Store) {
		if now != nil {
			s.now = now
		}
	}
}

// NewStore returns an empty store.
func NewStore(opts ...StoreOption) *Store {
	s := &Store{now: time.Now, items: make(map[string]Resource)}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Get returns a copy of the resource stored under key.
func (s *Store) Get(key string) (Resource, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.items[key]
	if !ok {
		return Resource{}, false
	}
	return r.clone(), true
}

// Put writes body unconditionally and returns the new resource.
func (s *Store) Put(key string, body []byte, contentType string) Resource {
	r, _, _ := s.Mutate(key, func(*Resource) (Change, error) {
		return Change{Body: body, ContentType: contentType}, nil
	})
	return r
}

// Mutate atomically reads the resource under key, lets m decide what to do,
// and applies the change. Because the precondition check inside m and the
// write happen under the same lock, two writers holding the same ETag cannot
// both succeed. It returns the resulting resource (for a delete, the one that
// was removed) and whether the key existed before the call.
func (s *Store) Mutate(key string, m Mutation) (Resource, bool, error) {
	if m == nil {
		return Resource{}, false, errors.New("conditionalrequests: nil mutation")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, existed := s.items[key]
	var curPtr *Resource
	if existed {
		c := cur.clone()
		curPtr = &c
	}
	ch, err := m(curPtr)
	if err != nil {
		return Resource{}, existed, err
	}
	if ch.Delete {
		if !existed {
			return Resource{}, false, ErrNotFound
		}
		delete(s.items, key)
		return cur.clone(), true, nil
	}
	ct := ch.ContentType
	if ct == "" {
		ct = cur.ContentType
	}
	if ct == "" {
		ct = "application/octet-stream"
	}
	s.version++
	r := Resource{
		Key:         key,
		Body:        append([]byte(nil), ch.Body...),
		ContentType: ct,
		Version:     s.version,
		ModTime:     s.now().UTC(),
	}
	r.ETag = makeETag(r.Version, r.Body)
	s.items[key] = r
	return r.clone(), existed, nil
}

func makeETag(version uint64, body []byte) string {
	sum := sha256.Sum256(body)
	return fmt.Sprintf(`"%d-%s"`, version, hex.EncodeToString(sum[:6]))
}

// ETag is a parsed entity tag.
type ETag struct {
	// Opaque is the value between the quotes.
	Opaque string
	// Weak is true for W/"..." tags: "semantically equivalent", not
	// byte-for-byte identical.
	Weak bool
}

// String formats the tag as it appears in a header.
func (e ETag) String() string {
	if e.Weak {
		return `W/"` + e.Opaque + `"`
	}
	return `"` + e.Opaque + `"`
}

// ParseETag parses a single entity tag such as "abc" or W/"abc".
func ParseETag(s string) (ETag, error) {
	tags, star, err := ParseETagList(s)
	if err != nil || star || len(tags) != 1 {
		return ETag{}, fmt.Errorf("conditionalrequests: invalid entity tag %q", s)
	}
	return tags[0], nil
}

// ParseETagList parses an If-Match / If-None-Match field value: either "*"
// or a comma-separated list of entity tags. Commas are legal inside an
// opaque tag, so it scans quotes rather than splitting on commas. On a
// malformed value it returns the tags parsed so far and an error.
func ParseETagList(s string) (tags []ETag, star bool, err error) {
	i := 0
	for {
		for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == ',') {
			i++
		}
		if i == len(s) {
			break
		}
		if s[i] == '*' {
			star = true
			i++
			continue
		}
		weak := false
		if strings.HasPrefix(s[i:], "W/") {
			weak = true
			i += 2
		}
		if i >= len(s) || s[i] != '"' {
			return tags, star, fmt.Errorf("conditionalrequests: malformed entity tag list %q", s)
		}
		end := strings.IndexByte(s[i+1:], '"')
		if end < 0 {
			return tags, star, fmt.Errorf("conditionalrequests: unterminated entity tag in %q", s)
		}
		tags = append(tags, ETag{Opaque: s[i+1 : i+1+end], Weak: weak})
		i += end + 2
	}
	if star && len(tags) > 0 {
		return tags, star, fmt.Errorf("conditionalrequests: \"*\" mixed with entity tags in %q", s)
	}
	if !star && len(tags) == 0 {
		return nil, false, fmt.Errorf("conditionalrequests: empty entity tag list")
	}
	return tags, star, nil
}

// StrongMatch reports whether a and b match under strong comparison: both
// must be strong and have the same opaque value. If-Match uses it, because a
// write must be based on exactly the bytes the client saw.
func StrongMatch(a, b ETag) bool { return !a.Weak && !b.Weak && a.Opaque == b.Opaque }

// WeakMatch reports whether a and b match under weak comparison: the opaque
// values are equal, whatever the W/ prefixes. If-None-Match uses it, because
// "you already have an equivalent copy" is good enough to skip a download.
func WeakMatch(a, b ETag) bool { return a.Opaque == b.Opaque }
