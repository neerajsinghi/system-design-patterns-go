package apigateway

// This file adds auth offloading: the gateway verifies a signed bearer token
// once at the edge and forwards the verified claims to backends in trusted
// headers, so no backend has to call an auth service on every request.
//
// The token is a minimal JWT using HS256 (HMAC-SHA256):
//
//	base64url(header) "." base64url(payload) "." base64url(signature)
//
// with header {"alg":"HS256","typ":"JWT"} and payload claims "sub" (subject),
// "exp" (expiry, Unix seconds) and "scopes" (array of strings). Only HS256 is
// accepted; "none" and every other algorithm are rejected before the
// signature is even looked at.

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Trusted headers the gateway sets after verifying a token. Any copies sent
// by the client are removed first, on every route.
const (
	HeaderUserID = "X-User-ID"
	HeaderScopes = "X-Scopes" // space-separated, like an OAuth scope string
)

// Token verification errors. Verify wraps nothing else, so errors.Is works.
var (
	ErrTokenMissing   = errors.New("apigateway: missing bearer token")
	ErrTokenMalformed = errors.New("apigateway: malformed token")
	ErrTokenAlg       = errors.New("apigateway: unsupported token algorithm")
	ErrTokenSignature = errors.New("apigateway: bad token signature")
	ErrTokenExpired   = errors.New("apigateway: token expired")
)

// maxTokenLen bounds the work an attacker can make the gateway do per request.
const maxTokenLen = 4096

// Claims are the verified contents of a token.
type Claims struct {
	Subject   string
	Scopes    []string
	ExpiresAt time.Time // second precision, as in the token
}

type tokenHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ,omitempty"`
}

type tokenPayload struct {
	Sub    string   `json:"sub"`
	Exp    int64    `json:"exp"`
	Scopes []string `json:"scopes,omitempty"`
}

var b64 = base64.RawURLEncoding

// SignHS256 issues a token for c signed with secret. In a real system the
// identity provider does this; the gateway only verifies.
func SignHS256(secret []byte, c Claims) (string, error) {
	if len(secret) == 0 {
		return "", errors.New("apigateway: empty signing secret")
	}
	if c.Subject == "" || c.ExpiresAt.IsZero() {
		return "", errors.New("apigateway: claims need a subject and an expiry")
	}
	h, _ := json.Marshal(tokenHeader{Alg: "HS256", Typ: "JWT"})
	p, err := json.Marshal(tokenPayload{Sub: c.Subject, Exp: c.ExpiresAt.Unix(), Scopes: c.Scopes})
	if err != nil {
		return "", err
	}
	signingInput := b64.EncodeToString(h) + "." + b64.EncodeToString(p)
	return signingInput + "." + b64.EncodeToString(mac(secret, signingInput)), nil
}

func mac(secret []byte, signingInput string) []byte {
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(signingInput))
	return m.Sum(nil)
}

// VerifierStats counts verification outcomes.
type VerifierStats struct {
	Verified  uint64 // full HMAC verifications that succeeded
	CacheHits uint64 // requests answered from the verified-token cache
	Rejected  uint64
}

// TokenVerifier checks HS256 tokens. It is safe for concurrent use.
type TokenVerifier struct {
	secret    []byte
	now       func() time.Time
	leeway    time.Duration
	cacheMax  int
	cacheTTL  time.Duration
	verified  atomic.Uint64
	cacheHits atomic.Uint64
	rejected  atomic.Uint64
	mu        sync.Mutex
	cache     map[[sha256.Size]byte]cachedToken
}

type cachedToken struct {
	claims Claims
	until  time.Time
}

// VerifierOption configures a TokenVerifier.
type VerifierOption func(*TokenVerifier)

// WithVerifierClock injects the clock used for expiry checks.
func WithVerifierClock(now func() time.Time) VerifierOption {
	return func(v *TokenVerifier) {
		if now != nil {
			v.now = now
		}
	}
}

// WithLeeway tolerates clock skew between issuer and gateway: a token is
// accepted until exp + leeway. Keep it small (seconds).
func WithLeeway(d time.Duration) VerifierOption {
	return func(v *TokenVerifier) { v.leeway = d }
}

// WithTokenCache keeps up to maxEntries verified tokens, keyed by the
// SHA-256 of the token, for at most ttl and never past the token's own exp.
// A repeat request then skips JSON decoding and the HMAC. maxEntries <= 0
// disables the cache (the default).
func WithTokenCache(maxEntries int, ttl time.Duration) VerifierOption {
	return func(v *TokenVerifier) { v.cacheMax, v.cacheTTL = maxEntries, ttl }
}

// NewTokenVerifier returns a verifier for tokens signed with secret.
func NewTokenVerifier(secret []byte, opts ...VerifierOption) (*TokenVerifier, error) {
	if len(secret) < 16 {
		return nil, errors.New("apigateway: HMAC secret must be at least 16 bytes")
	}
	v := &TokenVerifier{secret: append([]byte(nil), secret...), now: time.Now}
	for _, o := range opts {
		o(v)
	}
	if v.leeway < 0 {
		return nil, errors.New("apigateway: leeway must be >= 0")
	}
	if v.cacheMax > 0 {
		if v.cacheTTL <= 0 {
			return nil, errors.New("apigateway: token cache needs a positive ttl")
		}
		v.cache = make(map[[sha256.Size]byte]cachedToken)
	}
	return v, nil
}

// Stats returns a snapshot of the counters.
func (v *TokenVerifier) Stats() VerifierStats {
	return VerifierStats{Verified: v.verified.Load(), CacheHits: v.cacheHits.Load(), Rejected: v.rejected.Load()}
}

// Verify checks token and returns its claims.
func (v *TokenVerifier) Verify(token string) (Claims, error) {
	c, err := v.verify(token)
	if err != nil {
		v.rejected.Add(1)
	}
	return c, err
}

func (v *TokenVerifier) verify(token string) (Claims, error) {
	if token == "" {
		return Claims{}, ErrTokenMissing
	}
	if len(token) > maxTokenLen {
		return Claims{}, ErrTokenMalformed
	}
	now := v.now()
	var key [sha256.Size]byte
	if v.cache != nil {
		key = sha256.Sum256([]byte(token))
		v.mu.Lock()
		e, ok := v.cache[key]
		if ok && now.Before(e.until) {
			v.mu.Unlock()
			v.cacheHits.Add(1)
			return e.claims, nil
		}
		if ok {
			delete(v.cache, key)
		}
		v.mu.Unlock()
	}

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return Claims{}, ErrTokenMalformed
	}
	var hdr tokenHeader
	if err := decodeSegment(parts[0], &hdr); err != nil {
		return Claims{}, ErrTokenMalformed
	}
	// Check the algorithm before anything else: accepting "none" or letting
	// the token choose the algorithm is the classic JWT vulnerability.
	if hdr.Alg != "HS256" {
		return Claims{}, ErrTokenAlg
	}
	sig, err := b64.DecodeString(parts[2])
	if err != nil {
		return Claims{}, ErrTokenMalformed
	}
	if !hmac.Equal(sig, mac(v.secret, parts[0]+"."+parts[1])) {
		return Claims{}, ErrTokenSignature
	}
	var p tokenPayload
	if err := decodeSegment(parts[1], &p); err != nil || p.Sub == "" || p.Exp == 0 {
		return Claims{}, ErrTokenMalformed
	}
	exp := time.Unix(p.Exp, 0)
	if !now.Before(exp.Add(v.leeway)) {
		return Claims{}, ErrTokenExpired
	}
	c := Claims{Subject: p.Sub, Scopes: p.Scopes, ExpiresAt: exp}
	v.verified.Add(1)

	if v.cache != nil {
		until := now.Add(v.cacheTTL)
		if exp.Before(until) {
			until = exp // never serve a token from cache past its exp
		}
		v.mu.Lock()
		if len(v.cache) >= v.cacheMax {
			for k, e := range v.cache {
				if !now.Before(e.until) {
					delete(v.cache, k)
				}
			}
		}
		if len(v.cache) < v.cacheMax {
			v.cache[key] = cachedToken{claims: c, until: until}
		}
		v.mu.Unlock()
	}
	return c, nil
}

func decodeSegment(seg string, dst any) error {
	raw, err := b64.DecodeString(seg)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, dst)
}

// bearerToken extracts the token from "Authorization: Bearer <token>".
func bearerToken(authz string) string {
	const prefix = "bearer "
	if len(authz) < len(prefix) || !strings.EqualFold(authz[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(authz[len(prefix):])
}
