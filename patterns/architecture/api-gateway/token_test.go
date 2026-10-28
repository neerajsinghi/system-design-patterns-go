package apigateway

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

var testSecret = []byte("0123456789abcdef-test-secret")

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Add(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func newClock() *fakeClock { return &fakeClock{t: time.Unix(1_800_000_000, 0)} }

func mustSign(t *testing.T, secret []byte, c Claims) string {
	t.Helper()
	tok, err := SignHS256(secret, c)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// rawToken builds a token from arbitrary header/payload JSON and signature.
func rawToken(header, payload string, sig []byte) string {
	return b64.EncodeToString([]byte(header)) + "." + b64.EncodeToString([]byte(payload)) + "." + b64.EncodeToString(sig)
}

func TestVerify(t *testing.T) {
	clk := newClock()
	v, err := NewTokenVerifier(testSecret, WithVerifierClock(clk.Now))
	if err != nil {
		t.Fatal(err)
	}
	exp := clk.Now().Add(time.Hour)
	good := mustSign(t, testSecret, Claims{Subject: "alice", Scopes: []string{"orders:read"}, ExpiresAt: exp})
	expired := mustSign(t, testSecret, Claims{Subject: "alice", ExpiresAt: clk.Now().Add(-time.Second)})
	otherKey := mustSign(t, []byte("a-completely-different-key"), Claims{Subject: "alice", ExpiresAt: exp})

	payload := `{"sub":"admin","exp":` + jsonInt(exp.Unix()) + `}`
	algNone := rawToken(`{"alg":"none","typ":"JWT"}`, payload, nil)
	algNoneCase := rawToken(`{"alg":"None"}`, payload, nil)
	algHS512 := rawToken(`{"alg":"HS512"}`, payload, []byte("x"))
	// Re-sign a tampered payload with the right key but keep the old signature.
	parts := strings.Split(good, ".")
	tampered := parts[0] + "." + b64.EncodeToString([]byte(payload)) + "." + parts[2]
	noExpHdr := b64.EncodeToString([]byte(`{"alg":"HS256"}`))
	noExpPayload := b64.EncodeToString([]byte(`{"sub":"alice"}`))
	noExp := noExpHdr + "." + noExpPayload + "." + b64.EncodeToString(mac(testSecret, noExpHdr+"."+noExpPayload))

	tests := []struct {
		name    string
		token   string
		wantErr error
	}{
		{"valid", good, nil},
		{"empty", "", ErrTokenMissing},
		{"expired", expired, ErrTokenExpired},
		{"signed with another key", otherKey, ErrTokenSignature},
		{"tampered payload", tampered, ErrTokenSignature},
		{"alg none", algNone, ErrTokenAlg},
		{"alg None (case)", algNoneCase, ErrTokenAlg},
		{"alg HS512", algHS512, ErrTokenAlg},
		{"two segments", "a.b", ErrTokenMalformed},
		{"garbage base64", "!!.??.**", ErrTokenMalformed},
		{"missing exp", noExp, ErrTokenMalformed},
		{"oversized", strings.Repeat("a", maxTokenLen+1), ErrTokenMalformed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, err := v.Verify(tc.token)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr == nil && (c.Subject != "alice" || len(c.Scopes) != 1 || !c.ExpiresAt.Equal(exp)) {
				t.Errorf("claims = %+v", c)
			}
		})
	}
}

func jsonInt(n int64) string { b, _ := json.Marshal(n); return string(b) }

func TestVerifyExpiryBoundaryAndLeeway(t *testing.T) {
	clk := newClock()
	exp := clk.Now().Add(10 * time.Second)
	tok := mustSign(t, testSecret, Claims{Subject: "a", ExpiresAt: exp})
	strict, _ := NewTokenVerifier(testSecret, WithVerifierClock(clk.Now))
	lenient, _ := NewTokenVerifier(testSecret, WithVerifierClock(clk.Now), WithLeeway(5*time.Second))

	clk.Add(10 * time.Second) // now == exp
	if _, err := strict.Verify(tok); !errors.Is(err, ErrTokenExpired) {
		t.Errorf("at exp: err = %v, want expired", err)
	}
	if _, err := lenient.Verify(tok); err != nil {
		t.Errorf("within leeway: err = %v", err)
	}
	clk.Add(5 * time.Second)
	if _, err := lenient.Verify(tok); !errors.Is(err, ErrTokenExpired) {
		t.Errorf("past leeway: err = %v, want expired", err)
	}
}

func TestTokenCacheNeverOutlivesExp(t *testing.T) {
	clk := newClock()
	v, _ := NewTokenVerifier(testSecret, WithVerifierClock(clk.Now), WithTokenCache(10, time.Hour))
	tok := mustSign(t, testSecret, Claims{Subject: "a", ExpiresAt: clk.Now().Add(30 * time.Second)})

	for range 5 {
		if _, err := v.Verify(tok); err != nil {
			t.Fatal(err)
		}
	}
	if s := v.Stats(); s.Verified != 1 || s.CacheHits != 4 {
		t.Fatalf("stats = %+v, want 1 verify + 4 cache hits", s)
	}
	clk.Add(30 * time.Second) // cache TTL is 1h, but the token's exp has passed
	if _, err := v.Verify(tok); !errors.Is(err, ErrTokenExpired) {
		t.Fatalf("cached token served past exp: err = %v", err)
	}
	// A forged token never enters the cache, even after a good one with the same subject.
	if _, err := v.Verify(tok[:len(tok)-2] + "AA"); err == nil {
		t.Fatal("forged token accepted")
	}
}

func TestTokenCacheBounded(t *testing.T) {
	clk := newClock()
	v, _ := NewTokenVerifier(testSecret, WithVerifierClock(clk.Now), WithTokenCache(2, time.Minute))
	for i := range 5 {
		tok := mustSign(t, testSecret, Claims{Subject: string(rune('a' + i)), ExpiresAt: clk.Now().Add(time.Hour)})
		if _, err := v.Verify(tok); err != nil {
			t.Fatal(err)
		}
	}
	v.mu.Lock()
	n := len(v.cache)
	v.mu.Unlock()
	if n != 2 {
		t.Errorf("cache size = %d, want 2", n)
	}
}

func TestVerifierValidation(t *testing.T) {
	if _, err := NewTokenVerifier([]byte("short")); err == nil {
		t.Error("short secret accepted")
	}
	if _, err := NewTokenVerifier(testSecret, WithTokenCache(10, 0)); err == nil {
		t.Error("cache without ttl accepted")
	}
	if _, err := NewTokenVerifier(testSecret, WithLeeway(-time.Second)); err == nil {
		t.Error("negative leeway accepted")
	}
	if _, err := SignHS256(nil, Claims{Subject: "a", ExpiresAt: time.Now()}); err == nil {
		t.Error("empty secret accepted by SignHS256")
	}
	v, _ := NewTokenVerifier(testSecret)
	if _, err := New(Config{Authenticate: tokenAuth, Verifier: v}); err == nil {
		t.Error("Authenticate and Verifier both accepted")
	}
}

// headerBackend echoes the identity-related headers it received.
func headerBackend(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"userID":        r.Header.Get(HeaderUserID),
			"scopes":        r.Header.Get(HeaderScopes),
			"subject":       r.Header.Get(HeaderSubject),
			"authorization": r.Header.Get("Authorization"),
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestGatewayAuthOffloading(t *testing.T) {
	clk := newClock()
	backend := headerBackend(t)
	v, _ := NewTokenVerifier(testSecret, WithVerifierClock(clk.Now))
	gw, err := New(Config{
		Verifier: v,
		Routes: []Route{
			{Prefix: "/api", Target: mustURL(t, backend.URL)},
			{Prefix: "/public", Target: mustURL(t, backend.URL), Public: true},
		},
		Aggregates: []Aggregate{{Path: "/home", Parts: map[string]string{"a": backend.URL}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	good := mustSign(t, testSecret, Claims{Subject: "alice", Scopes: []string{"orders:read", "orders:write"}, ExpiresAt: clk.Now().Add(time.Hour)})
	expired := mustSign(t, testSecret, Claims{Subject: "alice", ExpiresAt: clk.Now().Add(-time.Minute)})
	forged := mustSign(t, []byte("attacker-chosen-secret!"), Claims{Subject: "admin", ExpiresAt: clk.Now().Add(time.Hour)})
	algNone := rawToken(`{"alg":"none"}`, `{"sub":"admin","exp":`+jsonInt(clk.Now().Add(time.Hour).Unix())+`}`, nil)

	spoof := map[string]string{HeaderUserID: "admin", HeaderScopes: "admin:*", HeaderSubject: "admin"}
	tests := []struct {
		name       string
		path       string
		token      string
		headers    map[string]string
		wantStatus int
		want       map[string]string
	}{
		{"valid token: claims forwarded, Authorization stripped", "/api/x", good, nil, 200,
			map[string]string{"userID": "alice", "scopes": "orders:read orders:write", "subject": "alice", "authorization": ""}},
		{"spoofed identity headers overwritten", "/api/x", good, spoof, 200,
			map[string]string{"userID": "alice", "scopes": "orders:read orders:write", "subject": "alice"}},
		{"public route: spoofed headers removed, not trusted", "/public/x", "", spoof, 200,
			map[string]string{"userID": "", "scopes": "", "subject": ""}},
		{"no token", "/api/x", "", nil, 401, nil},
		{"no token but spoofed X-User-ID", "/api/x", "", spoof, 401, nil},
		{"expired", "/api/x", expired, nil, 401, nil},
		{"bad signature", "/api/x", forged, nil, 401, nil},
		{"alg none", "/api/x", algNone, nil, 401, nil},
		{"aggregate gets claims", "/home", good, spoof, 200, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			if tc.token != "" {
				req.Header.Set("Authorization", "Bearer "+tc.token)
			}
			for k, val := range tc.headers {
				req.Header.Set(k, val)
			}
			rec := httptest.NewRecorder()
			gw.ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (%s)", rec.Code, tc.wantStatus, rec.Body)
			}
			if tc.wantStatus == 401 && !strings.Contains(rec.Header().Get("WWW-Authenticate"), "invalid_token") {
				t.Error("401 without WWW-Authenticate")
			}
			if tc.path == "/home" {
				var got map[string]map[string]string
				_ = json.NewDecoder(rec.Body).Decode(&got)
				if got["a"]["userID"] != "alice" || got["a"]["scopes"] != "orders:read orders:write" {
					t.Errorf("aggregate part saw %v", got["a"])
				}
				return
			}
			if tc.want == nil {
				return
			}
			var got map[string]string
			if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
				t.Fatal(err)
			}
			for k, want := range tc.want {
				if got[k] != want {
					t.Errorf("backend saw %s=%q, want %q", k, got[k], want)
				}
			}
		})
	}
}

// Without a Verifier the gateway still strips client copies of the new
// trusted headers, so a backend can never be fooled by them.
func TestTrustedHeadersStrippedWithoutVerifier(t *testing.T) {
	backend := headerBackend(t)
	gw, _ := New(Config{Authenticate: tokenAuth, Routes: []Route{{Prefix: "/api", Target: mustURL(t, backend.URL)}}})
	req := httptest.NewRequest(http.MethodGet, "/api", nil)
	req.Header.Set("Authorization", "Bearer good")
	req.Header.Set(HeaderUserID, "admin")
	rec := httptest.NewRecorder()
	gw.ServeHTTP(rec, req)
	var got map[string]string
	_ = json.NewDecoder(rec.Body).Decode(&got)
	if got["userID"] != "" || got["subject"] != "alice" || got["authorization"] != "Bearer good" {
		t.Errorf("backend saw %v", got)
	}
}

func TestVerifierConcurrent(t *testing.T) {
	clk := newClock()
	v, _ := NewTokenVerifier(testSecret, WithVerifierClock(clk.Now), WithTokenCache(4, time.Minute))
	toks := make([]string, 8)
	for i := range toks {
		toks[i] = mustSign(t, testSecret, Claims{Subject: string(rune('a' + i)), ExpiresAt: clk.Now().Add(time.Hour)})
	}
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 200 {
				tok := toks[(g+i)%len(toks)]
				if i%7 == 0 {
					tok += "x" // invalid
				}
				_, _ = v.Verify(tok)
				if i%50 == 0 {
					clk.Add(time.Second)
				}
			}
		}()
	}
	wg.Wait()
	s := v.Stats()
	if s.Verified+s.CacheHits+s.Rejected != 8*200 {
		t.Errorf("stats %+v do not add up to 1600", s)
	}
}
