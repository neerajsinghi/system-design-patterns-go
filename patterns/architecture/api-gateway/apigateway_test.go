package apigateway

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// echoBackend replies with the path, subject and request ID it received.
func echoBackend(t *testing.T, name string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"backend":   name,
			"path":      r.URL.Path,
			"subject":   r.Header.Get(HeaderSubject),
			"requestID": r.Header.Get(HeaderRequestID),
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func tokenAuth(r *http.Request) (string, error) {
	if tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "); tok == "good" {
		return "alice", nil
	}
	return "", errors.New("bad token")
}

func TestGatewayRouting(t *testing.T) {
	users := echoBackend(t, "users")
	admin := echoBackend(t, "admin")
	health := echoBackend(t, "health")
	gw, err := New(Config{
		Authenticate: tokenAuth,
		Routes: []Route{
			{Prefix: "/users", Target: mustURL(t, users.URL+"/v1")},
			{Prefix: "/users/admin", Target: mustURL(t, admin.URL)},
			{Prefix: "/health", Target: mustURL(t, health.URL), Public: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name        string
		path        string
		token       string
		headers     map[string]string
		wantStatus  int
		wantBackend string
		wantPath    string
		wantSubject string
	}{
		{"strip prefix and join target path", "/users/42", "good", nil, 200, "users", "/v1/42", "alice"},
		{"longest prefix wins", "/users/admin/x", "good", nil, 200, "admin", "/x", "alice"},
		{"prefix boundary respected", "/usersfoo", "good", nil, 404, "", "", ""},
		{"missing token rejected", "/users/42", "", nil, 401, "", "", ""},
		{"bad token rejected", "/users/42", "nope", nil, 401, "", "", ""},
		{"public route skips auth", "/health", "", nil, 200, "health", "/", ""},
		{"spoofed subject stripped", "/health", "", map[string]string{HeaderSubject: "root"}, 200, "health", "/", ""},
		{"unknown route", "/nope", "good", nil, 404, "", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			if tc.token != "" {
				req.Header.Set("Authorization", "Bearer "+tc.token)
			}
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			rec := httptest.NewRecorder()
			gw.ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (%s)", rec.Code, tc.wantStatus, rec.Body)
			}
			if rec.Header().Get(HeaderRequestID) == "" {
				t.Error("response missing request ID")
			}
			if tc.wantBackend == "" {
				return
			}
			var got map[string]string
			if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
				t.Fatal(err)
			}
			if got["backend"] != tc.wantBackend || got["path"] != tc.wantPath || got["subject"] != tc.wantSubject {
				t.Errorf("got %v, want backend=%s path=%s subject=%q", got, tc.wantBackend, tc.wantPath, tc.wantSubject)
			}
			if got["requestID"] != rec.Header().Get(HeaderRequestID) {
				t.Errorf("backend saw request ID %q, client got %q", got["requestID"], rec.Header().Get(HeaderRequestID))
			}
		})
	}
}

func TestRequestIDPropagation(t *testing.T) {
	backend := echoBackend(t, "b")
	gw, err := New(Config{Routes: []Route{{Prefix: "/", Target: mustURL(t, backend.URL)}}})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		incoming string
		keep     bool
	}{
		{"abc-123_X", true},
		{"", false},
		{"has space", false},
		{strings.Repeat("a", 65), false},
	}
	for _, tc := range tests {
		req := httptest.NewRequest(http.MethodGet, "/anything", nil)
		req.Header.Set(HeaderRequestID, tc.incoming)
		rec := httptest.NewRecorder()
		gw.ServeHTTP(rec, req)
		got := rec.Header().Get(HeaderRequestID)
		if tc.keep && got != tc.incoming {
			t.Errorf("incoming %q: got %q, want it kept", tc.incoming, got)
		}
		if !tc.keep && (got == tc.incoming || !validRequestID(got)) {
			t.Errorf("incoming %q: got %q, want a fresh valid ID", tc.incoming, got)
		}
	}
}

func TestAggregate(t *testing.T) {
	profile := echoBackend(t, "profile")
	orders := echoBackend(t, "orders")
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer broken.Close()

	gw, err := New(Config{
		Authenticate: tokenAuth,
		Aggregates: []Aggregate{
			{Path: "/dashboard", Parts: map[string]string{"profile": profile.URL + "/p", "orders": orders.URL + "/o"}},
			{Path: "/partial", Parts: map[string]string{"profile": profile.URL, "broken": broken.URL}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	do := func(path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer good")
		rec := httptest.NewRecorder()
		gw.ServeHTTP(rec, req)
		return rec
	}

	rec := do("/dashboard")
	if rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	var got map[string]map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got["profile"]["path"] != "/p" || got["orders"]["path"] != "/o" || got["orders"]["subject"] != "alice" {
		t.Errorf("unexpected aggregate: %v", got)
	}

	rec = do("/partial")
	body, _ := io.ReadAll(rec.Body)
	if rec.Code != http.StatusBadGateway || !strings.Contains(string(body), "broken") {
		t.Errorf("want 502 naming broken part, got %d %s", rec.Code, body)
	}
}

func TestNewValidation(t *testing.T) {
	cases := []Config{
		{Routes: []Route{{Prefix: "users", Target: &url.URL{}}}},
		{Routes: []Route{{Prefix: "/users"}}},
		{Aggregates: []Aggregate{{Path: "/x"}}},
	}
	for i, cfg := range cases {
		if _, err := New(cfg); err == nil {
			t.Errorf("case %d: expected error", i)
		}
	}
}

func TestUpstreamDown(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	addr := dead.URL
	dead.Close()
	gw, _ := New(Config{Routes: []Route{{Prefix: "/x", Target: mustURL(t, addr)}}})
	rec := httptest.NewRecorder()
	gw.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
	if rec.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", rec.Code)
	}
}
