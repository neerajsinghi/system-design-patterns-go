package conditionalrequests

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// maxBody caps request bodies accepted by the handler.
const maxBody = 1 << 20

// Handler serves a Store over HTTP with validators and preconditions.
// The request path is the resource key. Supported methods:
//
//   - GET, HEAD: 200 with ETag and Last-Modified, or 304 Not Modified.
//   - PUT: replace the body (201 when created, 204 when updated).
//   - PATCH: shallow JSON merge patch of a JSON object (RFC 7396 at the top
//     level only; a null value deletes a field). 204 on success.
//   - DELETE: 204.
//
// Successful writes return the new ETag. A 412 response carries the current
// ETag so a client can tell what it lost to.
type Handler struct {
	store          *Store
	requireIfMatch bool
}

// HandlerOption configures a Handler.
type HandlerOption func(*Handler)

// WithRequireIfMatch makes the handler reject unconditional PUT, PATCH and
// DELETE with 428 Precondition Required (RFC 6585). A write counts as
// conditional if it carries If-Match or If-None-Match, so "If-None-Match: *"
// (create only if absent) is still allowed.
func WithRequireIfMatch(require bool) HandlerOption {
	return func(h *Handler) { h.requireIfMatch = require }
}

// NewHandler returns a handler for store.
func NewHandler(store *Store, opts ...HandlerOption) (*Handler, error) {
	if store == nil {
		return nil, errors.New("conditionalrequests: nil store")
	}
	h := &Handler{store: store}
	for _, o := range opts {
		o(h)
	}
	return h, nil
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		h.serveRead(w, r)
	case http.MethodPut, http.MethodPatch, http.MethodDelete:
		h.serveWrite(w, r)
	default:
		w.Header().Set("Allow", "GET, HEAD, PUT, PATCH, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func setValidators(w http.ResponseWriter, res Resource) {
	w.Header().Set("ETag", res.ETag)
	w.Header().Set("Last-Modified", res.ModTime.UTC().Format(http.TimeFormat))
	// no-cache: caches may store it but must revalidate before each reuse,
	// which is exactly the conditional GET this package demonstrates.
	w.Header().Set("Cache-Control", "no-cache")
}

func (h *Handler) serveRead(w http.ResponseWriter, r *http.Request) {
	res, ok := h.store.Get(r.URL.Path)
	var cur *Resource
	if ok {
		cur = &res
	}
	switch Evaluate(r, cur) {
	case http.StatusNotModified:
		setValidators(w, res)
		w.WriteHeader(http.StatusNotModified)
		return
	case http.StatusPreconditionFailed:
		preconditionFailed(w, cur)
		return
	}
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	setValidators(w, res)
	w.Header().Set("Content-Type", res.ContentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(res.Body)))
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(res.Body)
	}
}

func preconditionFailed(w http.ResponseWriter, cur *Resource) {
	if cur != nil {
		w.Header().Set("ETag", cur.ETag)
	}
	http.Error(w, "precondition failed", http.StatusPreconditionFailed)
}

func (h *Handler) serveWrite(w http.ResponseWriter, r *http.Request) {
	conditional := len(r.Header.Values("If-Match")) > 0 || len(r.Header.Values("If-None-Match")) > 0
	if h.requireIfMatch && !conditional {
		http.Error(w, "this resource requires If-Match", http.StatusPreconditionRequired)
		return
	}
	var body []byte
	if r.Method != http.MethodDelete {
		b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
		if err != nil {
			http.Error(w, "request body too large or unreadable", http.StatusRequestEntityTooLarge)
			return
		}
		body = b
	}

	var failedWith *Resource
	res, existed, err := h.store.Mutate(r.URL.Path, func(cur *Resource) (Change, error) {
		if st := Evaluate(r, cur); st == http.StatusPreconditionFailed {
			failedWith = cur
			return Change{}, ErrPreconditionFailed
		}
		switch r.Method {
		case http.MethodPut:
			return Change{Body: body, ContentType: r.Header.Get("Content-Type")}, nil
		case http.MethodPatch:
			if cur == nil {
				return Change{}, ErrNotFound
			}
			merged, err := mergePatch(cur.Body, body)
			if err != nil {
				return Change{}, err
			}
			return Change{Body: merged}, nil
		default: // DELETE
			if cur == nil {
				return Change{}, ErrNotFound
			}
			return Change{Delete: true}, nil
		}
	})
	switch {
	case errors.Is(err, ErrPreconditionFailed):
		preconditionFailed(w, failedWith)
	case errors.Is(err, ErrNotFound):
		http.Error(w, "not found", http.StatusNotFound)
	case err != nil:
		http.Error(w, err.Error(), http.StatusBadRequest)
	case r.Method == http.MethodDelete:
		w.WriteHeader(http.StatusNoContent)
	default:
		setValidators(w, res)
		if existed {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusCreated)
		}
	}
}

// Evaluate applies the RFC 9110 §13.2.2 precedence of preconditions to a
// request, given the current resource (nil if it does not exist). It returns
// 0 when the request should proceed, 304 when a GET/HEAD can be answered Not
// Modified, or 412 when a precondition failed.
//
//  1. If-Match present: proceed only on a strong match ("*" matches any
//     existing resource). Otherwise 412.
//  2. If-None-Match present: if it matches (weak comparison; "*" matches any
//     existing resource), 304 for GET/HEAD and 412 for other methods.
//  3. Otherwise, for GET/HEAD with If-Modified-Since: 304 if the resource
//     has not changed since that time.
//
// If-Unmodified-Since and If-Range are not implemented. A malformed If-Match
// never matches (fail safe: no write); a malformed If-None-Match is treated
// as not matching (the client simply gets a full response).
func Evaluate(r *http.Request, cur *Resource) int {
	safe := r.Method == http.MethodGet || r.Method == http.MethodHead
	if v := r.Header.Values("If-Match"); len(v) > 0 {
		if !ifMatch(strings.Join(v, ","), cur) {
			return http.StatusPreconditionFailed
		}
	}
	if v := r.Header.Values("If-None-Match"); len(v) > 0 {
		if ifNoneMatchMatches(strings.Join(v, ","), cur) {
			if safe {
				return http.StatusNotModified
			}
			return http.StatusPreconditionFailed
		}
		return 0
	}
	if safe && cur != nil {
		if ims := r.Header.Get("If-Modified-Since"); ims != "" {
			if t, err := http.ParseTime(ims); err == nil && !cur.ModTime.Truncate(time.Second).After(t) {
				return http.StatusNotModified
			}
		}
	}
	return 0
}

func ifMatch(header string, cur *Resource) bool {
	if cur == nil {
		return false
	}
	tags, star, err := ParseETagList(header)
	if err != nil {
		return false
	}
	if star {
		return true
	}
	current, err := ParseETag(cur.ETag)
	if err != nil {
		return false
	}
	for _, t := range tags {
		if StrongMatch(t, current) {
			return true
		}
	}
	return false
}

func ifNoneMatchMatches(header string, cur *Resource) bool {
	if cur == nil {
		return false
	}
	tags, star, _ := ParseETagList(header) // keep whatever parsed
	if star {
		return true
	}
	current, err := ParseETag(cur.ETag)
	if err != nil {
		return false
	}
	for _, t := range tags {
		if WeakMatch(t, current) {
			return true
		}
	}
	return false
}

// mergePatch applies a top-level JSON merge patch to a JSON object.
func mergePatch(doc, patch []byte) ([]byte, error) {
	var base map[string]json.RawMessage
	if err := json.Unmarshal(doc, &base); err != nil {
		return nil, errors.New("conditionalrequests: current resource is not a JSON object")
	}
	var p map[string]json.RawMessage
	if err := json.Unmarshal(patch, &p); err != nil {
		return nil, errors.New("conditionalrequests: patch is not a JSON object")
	}
	if base == nil {
		base = map[string]json.RawMessage{}
	}
	for k, v := range p {
		if string(v) == "null" {
			delete(base, k)
		} else {
			base[k] = v
		}
	}
	return json.Marshal(base)
}
