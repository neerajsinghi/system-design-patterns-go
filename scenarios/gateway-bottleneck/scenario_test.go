package gatewaybottleneck

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	apigateway "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/api-gateway"
)

var (
	once          sync.Once
	broken, fixed Report
)

func reports() (Report, Report) {
	once.Do(func() { broken, fixed = Broken(), Fixed() })
	return broken, fixed
}

// The invariant, during the sale: gateway CPU at most 80% in every second;
// p99 at most 100 ms for well-behaved clients; no auth service call on the
// request path; backend connections within the limit; and the noisy partner
// admitted no more than its quota.
func TestBrokenViolatesInvariant(t *testing.T) {
	r, _ := reports()
	normal := r.Classes[ClassNormal]
	switch {
	case r.PeakCPU <= CPUTarget:
		t.Errorf("peak gateway CPU %.0f%%, want it pinned above %.0f%%", 100*r.PeakCPU, 100*CPUTarget)
	case normal.P99 <= P99Target:
		t.Errorf("well-behaved p99 %s, want above %s", normal.P99, P99Target)
	case r.AuthCallsPerRequest < 0.99:
		t.Errorf("auth calls per request %.2f, want one per request", r.AuthCallsPerRequest)
	case r.NoisyAdmitted <= NoisyQuota():
		t.Errorf("noisy partner admitted %d, want more than its quota %d", r.NoisyAdmitted, NoisyQuota())
	case r.HandshakesPerSec < 100 || r.PeakTimeWait < 1000:
		t.Errorf("handshakes %.0f/s, peak TIME_WAIT %d: want connection churn", r.HandshakesPerSec, r.PeakTimeWait)
	case r.BackendRefused == 0:
		t.Error("want the backend's connection limit to refuse connections once the fleet scales out")
	case r.MaxGatewaysUsed <= Gateways:
		t.Error("want the autoscaler to have added gateways")
	}
	if ok := normal.Outcomes[OK]; ok*2 > normal.Requests {
		t.Errorf("well-behaved clients got %d of %d requests through; want most to fail", ok, normal.Requests)
	}
}

func TestFixedHoldsInvariant(t *testing.T) {
	_, r := reports()
	normal := r.Classes[ClassNormal]
	if r.PeakCPU > CPUTarget {
		t.Errorf("peak gateway CPU %.0f%% > %.0f%%", 100*r.PeakCPU, 100*CPUTarget)
	}
	if normal.P99 > P99Target {
		t.Errorf("well-behaved p99 %s > %s", normal.P99, P99Target)
	}
	if normal.Outcomes[OK] != normal.Requests {
		t.Errorf("well-behaved outcomes %v: want every request served", normal.Outcomes)
	}
	if r.AuthCallsPerSec != 0 {
		t.Errorf("auth service calls %.1f/s, want 0", r.AuthCallsPerSec)
	}
	if r.PeakBackendConns > BackendConnLimit || r.BackendRefused+r.PortExhausted != 0 {
		t.Errorf("backend connections peak %d (limit %d), refused %d", r.PeakBackendConns, BackendConnLimit, r.BackendRefused+r.PortExhausted)
	}
	if r.NoisyAdmitted > NoisyQuota() {
		t.Errorf("noisy partner admitted %d > quota %d", r.NoisyAdmitted, NoisyQuota())
	}
	if r.HandshakesPerSec > 1 || r.PeakTimeWait != 0 {
		t.Errorf("handshakes %.1f/s, TIME_WAIT %d: want reused connections", r.HandshakesPerSec, r.PeakTimeWait)
	}
	if r.TokenCacheHitRate < 0.99 {
		t.Errorf("token cache hit rate %.3f, want ≥ 0.99", r.TokenCacheHitRate)
	}
}

// Even a fully scaled-out fleet must stay under the backend's limit.
func TestPoolSizingFitsFullFleet(t *testing.T) {
	if MaxGateways*PoolPerGateway > BackendConnLimit {
		t.Fatalf("%d gateways × %d pooled connections > backend limit %d", MaxGateways, PoolPerGateway, BackendConnLimit)
	}
}

// Without per-client quotas, local auth and pooling are not enough: the noisy
// partner takes the pooled connections and well-behaved clients queue.
func TestQuotasAreNeeded(t *testing.T) {
	r := withoutQuotas()
	if r.AuthCallsPerSec != 0 || r.PeakCPU > CPUTarget {
		t.Fatalf("auth %.0f/s, CPU %.0f%%: the bottleneck should be the pool, not CPU or auth", r.AuthCallsPerSec, 100*r.PeakCPU)
	}
	if p := r.Classes[ClassNormal].P99; p <= P99Target {
		t.Fatalf("without quotas well-behaved p99 is %s; want the noisy partner to starve them", p)
	}
}

// Cost of the fix: the noisy partner gets 429s, and only the noisy partner.
func TestCostNoisyPartnerGets429s(t *testing.T) {
	_, r := reports()
	noisy := r.Classes[ClassNoisy]
	if noisy.Outcomes[TooMany] == 0 || r.TooManyByClient[ClientNoisy] != noisy.Outcomes[TooMany] {
		t.Fatalf("noisy 429s: %d in the sale, %d overall", noisy.Outcomes[TooMany], r.TooManyByClient[ClientNoisy])
	}
	if len(r.TooManyByClient) != 1 {
		t.Fatalf("429s by client %v: want only the noisy partner limited", r.TooManyByClient)
	}
	// Admitted ≈ quota: the limiter neither under- nor over-admits.
	if min := NoisyQuota() * 95 / 100; r.NoisyAdmitted < min {
		t.Fatalf("noisy admitted %d, want close to its quota %d", r.NoisyAdmitted, NoisyQuota())
	}
}

// Cost of the fix: a bounded pool means some requests wait for a connection
// during bursts. The wait must stay well inside the latency target.
func TestCostPoolWaitsUnderBurst(t *testing.T) {
	_, r := reports()
	if r.PoolWaits == 0 {
		t.Fatal("want some requests to wait for a pooled connection")
	}
	if r.PoolWaitP99 <= 0 || r.PoolWaitP99 >= P99Target {
		t.Fatalf("pool wait p99 %s, want between 0 and %s", r.PoolWaitP99, P99Target)
	}
}

// Cost of the fix: local verification cannot see a revocation. The revoked
// token keeps working until its exp, and the cache never extends that.
func TestCostRevocationDelay(t *testing.T) {
	b, f := reports()
	wantAfter := 0
	for _, rq := range traffic() {
		if rq.class == ClassRevoked && rq.at >= RevokeAt {
			wantAfter++
		}
	}
	if f.RevokedAcceptedAfter != wantAfter {
		t.Fatalf("fixed served %d of %d requests on the revoked token after revocation, want all", f.RevokedAcceptedAfter, wantAfter)
	}
	if b.RevokedAcceptedAfter != 0 {
		t.Fatalf("broken (remote check) served %d requests on the revoked token", b.RevokedAcceptedAfter)
	}

	// The verifier on its own, on a fake clock: accepted until exp, rejected
	// after it, even though the cache TTL is longer than the remaining life.
	now := epoch
	v, err := apigateway.NewTokenVerifier(secret, apigateway.WithVerifierClock(func() time.Time { return now }),
		apigateway.WithTokenCache(TokenCacheSize, time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	tok, err := apigateway.SignHS256(secret, apigateway.Claims{Subject: ClientRevoked, ExpiresAt: epoch.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range []struct {
		at      time.Duration
		wantErr error
	}{{0, nil}, {30 * time.Second, nil}, {59 * time.Second, nil}, {time.Minute, apigateway.ErrTokenExpired}} {
		now = epoch.Add(step.at)
		if _, err := v.Verify(tok); !errors.Is(err, step.wantErr) {
			t.Fatalf("at +%s: err %v, want %v", step.at, err, step.wantErr)
		}
	}
}

// Both designs see the same traffic, and runs are deterministic even when
// they execute concurrently.
func TestDeterministicAndConcurrent(t *testing.T) {
	b, f := reports()
	var wg sync.WaitGroup
	var b2, f2 Report
	wg.Add(2)
	go func() { defer wg.Done(); b2 = Broken() }()
	go func() { defer wg.Done(); f2 = Fixed() }()
	wg.Wait()
	if !reflect.DeepEqual(b, b2) || !reflect.DeepEqual(f, f2) {
		t.Fatal("reruns differ")
	}
	for c := range b.Classes {
		if b.Classes[c].Requests != f.Classes[c].Requests {
			t.Fatalf("class %s: broken saw %d requests, fixed %d", classNames[c], b.Classes[c].Requests, f.Classes[c].Requests)
		}
	}
}
