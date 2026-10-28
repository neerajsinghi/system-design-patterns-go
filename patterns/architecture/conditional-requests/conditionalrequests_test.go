package conditionalrequests

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var t0 = time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)

func fixedClock() func() time.Time { return func() time.Time { return t0 } }

func newTestHandler(t *testing.T, opts ...HandlerOption) (*Store, *Handler) {
	t.Helper()
	s := NewStore(WithClock(fixedClock()))
	h, err := NewHandler(s, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return s, h
}

func do(h http.Handler, method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	var r *http.Request
	if body != "" {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	} else {
		r = httptest.NewRequest(method, path, nil)
	}
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestGetSetsValidators(t *testing.T) {
	s, h := newTestHandler(t)
	res := s.Put("/doc", []byte(`{"a":1}`), "application/json")

	w := do(h, http.MethodGet, "/doc", "", nil)
	if w.Code != http.StatusOK || w.Body.String() != `{"a":1}` {
		t.Fatalf("got %d %q", w.Code, w.Body)
	}
	etag := w.Header().Get("ETag")
	if etag != res.ETag || !strings.HasPrefix(etag, `"`) || !strings.HasSuffix(etag, `"`) {
		t.Fatalf("ETag = %q, want strong quoted %q", etag, res.ETag)
	}
	if got := w.Header().Get("Last-Modified"); got != t0.Format(http.TimeFormat) {
		t.Fatalf("Last-Modified = %q", got)
	}
	if w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("Content-Type = %q", w.Header().Get("Content-Type"))
	}
	if w := do(h, http.MethodGet, "/missing", "", nil); w.Code != http.StatusNotFound {
		t.Fatalf("missing: %d", w.Code)
	}
}

func TestIfNoneMatch(t *testing.T) {
	s, h := newTestHandler(t)
	old := s.Put("/doc", []byte("v1"), "text/plain").ETag
	cur := s.Put("/doc", []byte("v2"), "text/plain").ETag
	opaque := strings.Trim(cur, `"`)

	tests := []struct {
		name   string
		method string
		path   string
		inm    string
		want   int
	}{
		{"exact match", "GET", "/doc", cur, 304},
		{"stale tag", "GET", "/doc", old, 200},
		{"list containing current", "GET", "/doc", `"zzz", ` + old + `, ` + cur, 304},
		{"list without current", "GET", "/doc", `"zzz", ` + old, 200},
		{"star on existing", "GET", "/doc", "*", 304},
		{"star on missing", "GET", "/nope", "*", 404},
		{"weak form of current (weak comparison)", "GET", "/doc", `W/"` + opaque + `"`, 304},
		{"HEAD match", "HEAD", "/doc", cur, 304},
		{"malformed ignored", "GET", "/doc", `garbage`, 200},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := do(h, tc.method, tc.path, "", map[string]string{"If-None-Match": tc.inm})
			if w.Code != tc.want {
				t.Fatalf("status = %d, want %d", w.Code, tc.want)
			}
			if tc.want == 304 {
				if w.Body.Len() != 0 {
					t.Fatalf("304 had a body: %q", w.Body)
				}
				if w.Header().Get("ETag") != cur {
					t.Fatalf("304 ETag = %q", w.Header().Get("ETag"))
				}
			}
			if tc.want == 200 && tc.method == "GET" && w.Body.String() != "v2" {
				t.Fatalf("body = %q", w.Body)
			}
		})
	}
}

func TestIfNoneMatchMultipleHeaderLines(t *testing.T) {
	s, h := newTestHandler(t)
	cur := s.Put("/doc", []byte("x"), "").ETag
	r := httptest.NewRequest("GET", "/doc", nil)
	r.Header.Add("If-None-Match", `"other"`)
	r.Header.Add("If-None-Match", cur)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 304 {
		t.Fatalf("status = %d, want 304", w.Code)
	}
}

func TestIfModifiedSince(t *testing.T) {
	s, h := newTestHandler(t)
	cur := s.Put("/doc", []byte("x"), "").ETag
	tests := []struct {
		name    string
		headers map[string]string
		want    int
	}{
		{"same time", map[string]string{"If-Modified-Since": t0.Format(http.TimeFormat)}, 304},
		{"later", map[string]string{"If-Modified-Since": t0.Add(time.Hour).Format(http.TimeFormat)}, 304},
		{"earlier", map[string]string{"If-Modified-Since": t0.Add(-time.Second).Format(http.TimeFormat)}, 200},
		{"unparseable", map[string]string{"If-Modified-Since": "yesterday"}, 200},
		{"If-None-Match mismatch wins over IMS", map[string]string{
			"If-Modified-Since": t0.Format(http.TimeFormat), "If-None-Match": `"old"`}, 200},
		{"If-None-Match match", map[string]string{"If-None-Match": cur}, 304},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if w := do(h, "GET", "/doc", "", tc.headers); w.Code != tc.want {
				t.Fatalf("status = %d, want %d", w.Code, tc.want)
			}
		})
	}
}

func TestComparisonRules(t *testing.T) {
	// The table from RFC 9110 §8.8.3.2.
	tests := []struct {
		a, b         string
		strong, weak bool
	}{
		{`W/"1"`, `W/"1"`, false, true},
		{`W/"1"`, `W/"2"`, false, false},
		{`W/"1"`, `"1"`, false, true},
		{`"1"`, `"1"`, true, true},
		{`"1"`, `"2"`, false, false},
	}
	for _, tc := range tests {
		a, err := ParseETag(tc.a)
		if err != nil {
			t.Fatal(err)
		}
		b, err := ParseETag(tc.b)
		if err != nil {
			t.Fatal(err)
		}
		if got := StrongMatch(a, b); got != tc.strong {
			t.Errorf("StrongMatch(%s, %s) = %v", tc.a, tc.b, got)
		}
		if got := WeakMatch(a, b); got != tc.weak {
			t.Errorf("WeakMatch(%s, %s) = %v", tc.a, tc.b, got)
		}
	}
}

func TestParseETagList(t *testing.T) {
	tests := []struct {
		in      string
		want    []ETag
		star    bool
		wantErr bool
	}{
		{`"a"`, []ETag{{Opaque: "a"}}, false, false},
		{` "a" ,W/"b",, "c,d" `, []ETag{{Opaque: "a"}, {Opaque: "b", Weak: true}, {Opaque: "c,d"}}, false, false},
		{`*`, nil, true, false},
		{`""`, []ETag{{Opaque: ""}}, false, false},
		{`abc`, nil, false, true},
		{`"abc`, nil, false, true},
		{`*, "a"`, []ETag{{Opaque: "a"}}, true, true},
		{``, nil, false, true},
	}
	for _, tc := range tests {
		got, star, err := ParseETagList(tc.in)
		if (err != nil) != tc.wantErr {
			t.Errorf("%q: err = %v, wantErr %v", tc.in, err, tc.wantErr)
		}
		if star != tc.star || len(got) != len(tc.want) {
			t.Errorf("%q: got %v star=%v", tc.in, got, star)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("%q: tag %d = %v, want %v", tc.in, i, got[i], tc.want[i])
			}
		}
	}
	if e := (ETag{Opaque: "x", Weak: true}); e.String() != `W/"x"` {
		t.Errorf("String = %s", e)
	}
	if _, err := ParseETag(`"a", "b"`); err == nil {
		t.Error("ParseETag accepted a list")
	}
}

func TestIfMatchWrites(t *testing.T) {
	tests := []struct {
		name    string
		method  string
		body    string
		ifMatch func(cur, old string) string
		missing bool
		want    int
		changed bool
	}{
		{"PUT current", "PUT", `{"a":2}`, func(c, _ string) string { return c }, false, 204, true},
		{"PUT stale", "PUT", `{"a":2}`, func(_, o string) string { return o }, false, 412, false},
		{"PUT list with current", "PUT", `{"a":2}`, func(c, o string) string { return o + ", " + c }, false, 204, true},
		{"PUT weak form never matches", "PUT", `{"a":2}`, func(c, _ string) string { return "W/" + c }, false, 412, false},
		{"PUT star on existing", "PUT", `{"a":2}`, func(_, _ string) string { return "*" }, false, 204, true},
		{"PUT star on missing", "PUT", `{"a":2}`, func(_, _ string) string { return "*" }, true, 412, false},
		{"PUT tag on missing", "PUT", `{"a":2}`, func(c, _ string) string { return c }, true, 412, false},
		{"PUT malformed", "PUT", `{"a":2}`, func(_, _ string) string { return "nonsense" }, false, 412, false},
		{"PATCH current", "PATCH", `{"b":3}`, func(c, _ string) string { return c }, false, 204, true},
		{"PATCH stale", "PATCH", `{"b":3}`, func(_, o string) string { return o }, false, 412, false},
		{"DELETE current", "DELETE", "", func(c, _ string) string { return c }, false, 204, true},
		{"DELETE stale", "DELETE", "", func(_, o string) string { return o }, false, 412, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, h := newTestHandler(t)
			path := "/doc"
			old := s.Put(path, []byte(`{"a":0}`), "application/json").ETag
			cur := s.Put(path, []byte(`{"a":1}`), "application/json")
			if tc.missing {
				path = "/missing"
			}
			w := do(h, tc.method, path, tc.body, map[string]string{"If-Match": tc.ifMatch(cur.ETag, old)})
			if w.Code != tc.want {
				t.Fatalf("status = %d, want %d (%s)", w.Code, tc.want, w.Body)
			}
			after, ok := s.Get("/doc")
			if changed := !ok || after.ETag != cur.ETag; changed != tc.changed {
				t.Fatalf("resource changed = %v, want %v", changed, tc.changed)
			}
			if tc.want == 412 && !tc.missing && w.Header().Get("ETag") != cur.ETag {
				t.Fatalf("412 should carry current ETag, got %q", w.Header().Get("ETag"))
			}
			if tc.want == 204 && tc.method != "DELETE" && w.Header().Get("ETag") != after.ETag {
				t.Fatalf("write should return new ETag %q, got %q", after.ETag, w.Header().Get("ETag"))
			}
		})
	}
}

func TestPatchMerges(t *testing.T) {
	s, h := newTestHandler(t)
	cur := s.Put("/doc", []byte(`{"a":1,"b":2}`), "application/json").ETag
	w := do(h, "PATCH", "/doc", `{"b":null,"c":"x"}`, map[string]string{"If-Match": cur})
	if w.Code != 204 {
		t.Fatalf("status = %d", w.Code)
	}
	got, _ := s.Get("/doc")
	if string(got.Body) != `{"a":1,"c":"x"}` || got.ContentType != "application/json" {
		t.Fatalf("merged = %s (%s)", got.Body, got.ContentType)
	}
	if w := do(h, "PATCH", "/doc", `[1]`, nil); w.Code != 400 {
		t.Fatalf("bad patch: %d", w.Code)
	}
	if w := do(h, "PATCH", "/missing", `{}`, nil); w.Code != 404 {
		t.Fatalf("patch missing: %d", w.Code)
	}
	if w := do(h, "DELETE", "/missing", "", nil); w.Code != 404 {
		t.Fatalf("delete missing: %d", w.Code)
	}
}

func TestIfNoneMatchStarCreateOnly(t *testing.T) {
	_, h := newTestHandler(t)
	hdr := map[string]string{"If-None-Match": "*"}
	if w := do(h, "PUT", "/new", "first", hdr); w.Code != 201 || w.Header().Get("ETag") == "" {
		t.Fatalf("create: %d etag=%q", w.Code, w.Header().Get("ETag"))
	}
	if w := do(h, "PUT", "/new", "second", hdr); w.Code != 412 {
		t.Fatalf("second create: %d, want 412", w.Code)
	}
}

func TestRequireIfMatch(t *testing.T) {
	s, h := newTestHandler(t, WithRequireIfMatch(true))
	cur := s.Put("/doc", []byte("x"), "").ETag
	tests := []struct {
		name, method, path string
		headers            map[string]string
		want               int
	}{
		{"PUT without precondition", "PUT", "/doc", nil, 428},
		{"PATCH without precondition", "PATCH", "/doc", nil, 428},
		{"DELETE without precondition", "DELETE", "/doc", nil, 428},
		{"GET unaffected", "GET", "/doc", nil, 200},
		{"create-only allowed", "PUT", "/new", map[string]string{"If-None-Match": "*"}, 201},
		{"PUT with If-Match", "PUT", "/doc", map[string]string{"If-Match": cur}, 204},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if w := do(h, tc.method, tc.path, "y", tc.headers); w.Code != tc.want {
				t.Fatalf("status = %d, want %d", w.Code, tc.want)
			}
		})
	}
	_, lax := newTestHandler(t)
	if w := do(lax, "PUT", "/doc", "y", nil); w.Code != 201 {
		t.Fatalf("without the option an unconditional PUT should work, got %d", w.Code)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	_, h := newTestHandler(t)
	w := do(h, "POST", "/doc", "x", nil)
	if w.Code != 405 || w.Header().Get("Allow") == "" {
		t.Fatalf("got %d allow=%q", w.Code, w.Header().Get("Allow"))
	}
}

func TestETagChangesOnWrite(t *testing.T) {
	s := NewStore()
	seen := map[string]bool{}
	var last uint64
	for i := range 50 {
		body := []byte("same")
		if i%2 == 0 {
			body = []byte("different " + strconv.Itoa(i))
		}
		r := s.Put("/doc", body, "")
		if seen[r.ETag] {
			t.Fatalf("ETag %s reused", r.ETag)
		}
		if r.Version <= last {
			t.Fatalf("version did not grow: %d after %d", r.Version, last)
		}
		seen[r.ETag], last = true, r.Version
	}
	// Delete and re-create with identical content still gets a new ETag.
	before, _ := s.Get("/doc")
	if _, _, err := s.Mutate("/doc", func(*Resource) (Change, error) { return Change{Delete: true}, nil }); err != nil {
		t.Fatal(err)
	}
	if after := s.Put("/doc", before.Body, ""); after.ETag == before.ETag {
		t.Fatal("ETag reused after delete + re-create")
	}
}

func TestStoreReturnsCopies(t *testing.T) {
	s := NewStore()
	body := []byte("abc")
	s.Put("/k", body, "")
	body[0] = 'X'
	r, _ := s.Get("/k")
	r.Body[1] = 'Y'
	again, _ := s.Get("/k")
	if string(again.Body) != "abc" {
		t.Fatalf("store aliased caller memory: %q", again.Body)
	}
}

func TestStoreConcurrentCompareAndSet(t *testing.T) {
	s := NewStore()
	s.Put("/n", []byte("0"), "")
	const workers, each = 8, 200
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range each {
				for {
					read, _ := s.Get("/n")
					n, _ := strconv.Atoi(string(read.Body))
					_, _, err := s.Mutate("/n", func(cur *Resource) (Change, error) {
						if cur.ETag != read.ETag {
							return Change{}, ErrPreconditionFailed
						}
						return Change{Body: []byte(strconv.Itoa(n + 1))}, nil
					})
					if err == nil {
						break
					}
				}
			}
		}()
	}
	wg.Wait()
	got, _ := s.Get("/n")
	if string(got.Body) != strconv.Itoa(workers*each) {
		t.Fatalf("counter = %s, want %d", got.Body, workers*each)
	}
}

func newServer(t *testing.T, opts ...HandlerOption) (*Store, *httptest.Server) {
	t.Helper()
	s, h := newTestHandler(t, opts...)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return s, srv
}

func TestClientRevalidation(t *testing.T) {
	s, srv := newServer(t)
	s.Put("/doc", []byte(strings.Repeat("x", 1000)), "")
	ctx := context.Background()

	c := NewClient(srv.Client())
	for i := range 10 {
		f, err := c.Get(ctx, srv.URL+"/doc")
		if err != nil {
			t.Fatal(err)
		}
		if len(f.Body) != 1000 || f.Revalidated != (i > 0) {
			t.Fatalf("poll %d: len=%d revalidated=%v", i, len(f.Body), f.Revalidated)
		}
	}
	if st := c.Stats(); st.Full != 1 || st.NotModified != 9 || st.BodyBytes != 1000 {
		t.Fatalf("stats = %+v", st)
	}
	s.Put("/doc", []byte("new"), "")
	f, err := c.Get(ctx, srv.URL+"/doc")
	if err != nil || f.Revalidated || string(f.Body) != "new" {
		t.Fatalf("after change: %+v %v", f, err)
	}

	plain := NewClient(srv.Client(), WithRevalidation(false))
	for range 10 {
		if _, err := plain.Get(ctx, srv.URL+"/doc"); err != nil {
			t.Fatal(err)
		}
	}
	if st := plain.Stats(); st.Full != 10 || st.NotModified != 0 {
		t.Fatalf("plain stats = %+v", st)
	}
	if _, err := c.Get(ctx, srv.URL+"/missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
}

type doc struct {
	Title string   `json:"title"`
	Tags  []string `json:"tags"`
}

func edit(f func(*doc)) func([]byte) ([]byte, error) {
	return func(b []byte) ([]byte, error) {
		var d doc
		if err := json.Unmarshal(b, &d); err != nil {
			return nil, err
		}
		f(&d)
		return json.Marshal(d)
	}
}

func TestLostUpdateWithoutIfMatch(t *testing.T) {
	s, srv := newServer(t)
	s.Put("/doc", []byte(`{"title":"draft","tags":[]}`), "application/json")
	ctx := context.Background()
	alice, bob := NewClient(srv.Client()), NewClient(srv.Client())
	url := srv.URL + "/doc"

	fa, _ := alice.Get(ctx, url)
	fb, _ := bob.Get(ctx, url)
	na, _ := edit(func(d *doc) { d.Title = "final" })(fa.Body)
	nb, _ := edit(func(d *doc) { d.Tags = append(d.Tags, "go") })(fb.Body)
	if _, err := alice.Put(ctx, url, na, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := bob.Put(ctx, url, nb, ""); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get("/doc")
	if !strings.Contains(string(got.Body), `"draft"`) {
		t.Fatalf("expected Alice's title to be lost, got %s", got.Body)
	}
}

func TestLostUpdatePreventedWithIfMatch(t *testing.T) {
	s, srv := newServer(t, WithRequireIfMatch(true))
	s.Put("/doc", []byte(`{"title":"draft","tags":[]}`), "application/json")
	ctx := context.Background()
	alice, bob := NewClient(srv.Client()), NewClient(srv.Client())
	url := srv.URL + "/doc"

	// Bob reads; Alice writes before Bob's PUT; Bob's PUT gets 412.
	fb, _ := bob.Get(ctx, url)
	if _, err := alice.Update(ctx, url, 3, edit(func(d *doc) { d.Title = "final" })); err != nil {
		t.Fatal(err)
	}
	nb, _ := edit(func(d *doc) { d.Tags = append(d.Tags, "go") })(fb.Body)
	_, err := bob.Put(ctx, url, nb, fb.ETag)
	var se *StatusError
	if !errors.Is(err, ErrPreconditionFailed) || !errors.As(err, &se) || se.ETag == fb.ETag || se.ETag == "" {
		t.Fatalf("stale PUT: %v", err)
	}
	if _, err := bob.Put(ctx, url, nb, ""); !errors.Is(err, ErrPreconditionRequired) {
		t.Fatalf("unconditional PUT: %v", err)
	}

	// Update retries: on the first attempt Alice sneaks in another write.
	attempts := 0
	res, err := bob.Update(ctx, url, 3, func(b []byte) ([]byte, error) {
		attempts++
		if attempts == 1 {
			if _, err := alice.Update(ctx, url, 3, edit(func(d *doc) { d.Title = "final v2" })); err != nil {
				return nil, err
			}
		}
		return edit(func(d *doc) { d.Tags = append(d.Tags, "go") })(b)
	})
	if err != nil || res.Attempts != 2 {
		t.Fatalf("update: %+v %v", res, err)
	}
	got, _ := s.Get("/doc")
	var d doc
	_ = json.Unmarshal(got.Body, &d)
	if d.Title != "final v2" || len(d.Tags) != 1 || d.Tags[0] != "go" {
		t.Fatalf("final doc = %s", got.Body)
	}
	if res.ETag != got.ETag {
		t.Fatalf("returned ETag %q, current %q", res.ETag, got.ETag)
	}
	if bob.Stats().Conflicts != 2 {
		t.Fatalf("bob conflicts = %d", bob.Stats().Conflicts)
	}
}

func TestUpdateGivesUp(t *testing.T) {
	s, srv := newServer(t)
	s.Put("/doc", []byte(`{"title":"x"}`), "")
	ctx := context.Background()
	c := NewClient(srv.Client())
	res, err := c.Update(ctx, srv.URL+"/doc", 3, func(b []byte) ([]byte, error) {
		s.Put("/doc", []byte(`{"title":"interloper"}`), "") // always lose the race
		return b, nil
	})
	if !errors.Is(err, ErrConflict) || res.Attempts != 3 {
		t.Fatalf("got %+v %v", res, err)
	}
}

func TestConcurrentHTTPUpdates(t *testing.T) {
	s, srv := newServer(t, WithRequireIfMatch(true))
	s.Put("/counter", []byte(`{"n":0}`), "application/json")
	const editors, each = 6, 10
	var wg sync.WaitGroup
	errs := make(chan error, editors)
	for range editors {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := NewClient(srv.Client())
			for range each {
				_, err := c.Update(context.Background(), srv.URL+"/counter", 1000, func(b []byte) ([]byte, error) {
					var v struct{ N int }
					if err := json.Unmarshal(b, &v); err != nil {
						return nil, err
					}
					return json.Marshal(map[string]int{"n": v.N + 1})
				})
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	// Readers revalidating concurrently.
	reader := NewClient(srv.Client())
	for range 20 {
		if _, err := reader.Get(context.Background(), srv.URL+"/counter"); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	got, _ := s.Get("/counter")
	if string(got.Body) != `{"n":60}` {
		t.Fatalf("counter = %s, want 60 (no lost updates)", got.Body)
	}
}

func TestValidation(t *testing.T) {
	if _, err := NewHandler(nil); err == nil {
		t.Error("NewHandler(nil) accepted")
	}
	if _, _, err := NewStore().Mutate("/k", nil); err == nil {
		t.Error("nil mutation accepted")
	}
	if _, _, err := NewStore().Mutate("/k", func(*Resource) (Change, error) { return Change{Delete: true}, nil }); !errors.Is(err, ErrNotFound) {
		t.Errorf("delete missing: %v", err)
	}
	c := NewClient(nil)
	ok := func([]byte) ([]byte, error) { return nil, nil }
	if _, err := c.Update(context.Background(), "http://127.0.0.1/", 0, ok); err == nil {
		t.Error("maxAttempts 0 accepted")
	}
	if _, err := c.Update(context.Background(), "http://127.0.0.1/", 1, nil); err == nil {
		t.Error("nil modify accepted")
	}
	if _, err := c.Get(context.Background(), "://bad"); err == nil {
		t.Error("bad URL accepted")
	}
	NewStore(WithClock(nil)).Put("/k", nil, "") // nil clock ignored, must not panic
}
