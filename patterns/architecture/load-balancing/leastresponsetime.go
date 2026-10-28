package loadbalancing

import (
	"errors"
	"sync"
	"time"
)

// LeastResponseTime picks the backend with the lowest
//
//	score = EWMA(response time) × (in-flight + 1)
//
// Least connections only sees how many requests a backend holds. It can't
// tell a backend that answers in 1 ms from one that takes 50 ms while both
// have one request in flight. Multiplying a smoothed latency by the queue
// length estimates how long a new request would wait. The "+1" stops an idle
// slow backend from looking free.
//
// Latency is measured from Pick to done with an injectable clock. A backend
// with no samples yet is scored at the mean EWMA of the backends that have
// samples. That avoids two failure modes: scoring it at 0 would send it every
// request until its first answer returned, and scoring it at infinity would
// keep it from ever being tried.
type LeastResponseTime struct {
	now   func() time.Time
	alpha float64

	mu       sync.Mutex
	backends []*Backend
	ewma     []float64 // seconds. Only meaningful where seen[i]
	seen     []bool
	start    int
}

// LRTConfig configures a LeastResponseTime balancer. The zero value is valid.
type LRTConfig struct {
	// Now is the clock used to time requests. nil means time.Now.
	Now func() time.Time
	// Alpha is the EWMA weight of the newest sample, in (0, 1]. Higher
	// values react faster to a backend slowing down, but they're noisier.
	// 0 means 0.3.
	Alpha float64
}

// NewLeastResponseTime returns a least-response-time balancer.
func NewLeastResponseTime(cfg LRTConfig, backends ...*Backend) (*LeastResponseTime, error) {
	if cfg.Alpha == 0 {
		cfg.Alpha = 0.3
	}
	if cfg.Alpha < 0 || cfg.Alpha > 1 {
		return nil, errors.New("loadbalancing: EWMA alpha must be in (0, 1]")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	for _, b := range backends {
		if b == nil {
			return nil, errors.New("loadbalancing: nil backend")
		}
	}
	return &LeastResponseTime{
		now:      cfg.Now,
		alpha:    cfg.Alpha,
		backends: backends,
		ewma:     make([]float64, len(backends)),
		seen:     make([]bool, len(backends)),
	}, nil
}

// Latency returns the backend's current EWMA latency and whether it has any
// samples yet.
func (l *LeastResponseTime) Latency(b *Backend) (time.Duration, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, x := range l.backends {
		if x == b && l.seen[i] {
			return time.Duration(l.ewma[i] * float64(time.Second)), true
		}
	}
	return 0, false
}

// Pick implements Balancer. The returned done records the request's
// latency, so call it when the response has arrived (or failed).
func (l *LeastResponseTime) Pick() (*Backend, func(), error) {
	n := len(l.backends)
	if n == 0 {
		return nil, nil, ErrNoBackends
	}
	l.mu.Lock()
	var sum float64
	var cnt int
	for i := range n {
		if l.seen[i] {
			sum += l.ewma[i]
			cnt++
		}
	}
	prior := 1.0 // any positive constant works when nothing has been measured
	if cnt > 0 && sum > 0 {
		prior = sum / float64(cnt)
	}
	score := func(i int) float64 {
		lat := prior
		if l.seen[i] {
			lat = l.ewma[i]
		}
		return lat * float64(l.backends[i].Active()+1)
	}
	best, bestScore := l.start, score(l.start)
	for k := 1; k < n; k++ {
		i := (l.start + k) % n
		if s := score(i); s < bestScore {
			best, bestScore = i, s
		}
	}
	l.start = (l.start + 1) % n
	b := l.backends[best]
	b.active.Add(1)
	l.mu.Unlock()

	began := l.now()
	var once sync.Once
	return b, func() {
		once.Do(func() {
			sample := l.now().Sub(began).Seconds()
			l.mu.Lock()
			if l.seen[best] {
				l.ewma[best] = l.alpha*sample + (1-l.alpha)*l.ewma[best]
			} else {
				l.ewma[best], l.seen[best] = sample, true
			}
			l.mu.Unlock()
			b.active.Add(-1)
		})
	}, nil
}
