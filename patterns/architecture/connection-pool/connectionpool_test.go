package connectionpool

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (f *fakeClock) Now() time.Time          { f.mu.Lock(); defer f.mu.Unlock(); return f.t }
func (f *fakeClock) Advance(d time.Duration) { f.mu.Lock(); f.t = f.t.Add(d); f.mu.Unlock() }

// conn is a fake connection; broken simulates a socket killed while idle.
type conn struct {
	id     int64
	broken atomic.Bool
	closed atomic.Bool
}

// backend counts dials, closes and the peak number of open connections.
type backend struct {
	mu            sync.Mutex
	next          int64
	open, peak    int
	dials, closes int
	all           []*conn
}

func (b *backend) dial(context.Context) (*conn, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.next++
	b.dials++
	b.open++
	b.peak = max(b.peak, b.open)
	c := &conn{id: b.next}
	b.all = append(b.all, c)
	return c, nil
}

func (b *backend) close(c *conn) {
	if c.closed.Swap(true) {
		panic("connection closed twice")
	}
	b.mu.Lock()
	b.open--
	b.closes++
	b.mu.Unlock()
}

func (b *backend) snapshot() (open, peak, dials, closes int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.open, b.peak, b.dials, b.closes
}

func validate(c *conn) error {
	if c.broken.Load() {
		return errors.New("broken pipe")
	}
	return nil
}

func newPool(t *testing.T, b *backend, opts ...Option) *Pool[*conn] {
	t.Helper()
	p, err := New(b.dial, b.close, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// waitFor spins until cond holds; used to line goroutines up deterministically.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition never became true")
		}
		runtime.Gosched()
	}
}

func TestNewValidation(t *testing.T) {
	b := &backend{}
	tests := []struct {
		name string
		dial DialFunc[*conn]
		opts []Option
	}{
		{"nil dial", nil, nil},
		{"negative max open", b.dial, []Option{WithMaxOpen(-1)}},
		{"negative max idle", b.dial, []Option{WithMaxIdle(-1)}},
		{"max idle above max open", b.dial, []Option{WithMaxOpen(2), WithMaxIdle(3)}},
		{"negative idle timeout", b.dial, []Option{WithIdleTimeout(-time.Second)}},
		{"negative max lifetime", b.dial, []Option{WithMaxLifetime(-time.Second)}},
		{"validate of another type", b.dial, []Option{WithValidate(func(string) error { return nil })}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.dial, b.close, tt.opts...); err == nil {
				t.Fatal("expected error")
			}
		})
	}
	if _, err := New(b.dial, nil, WithMaxOpen(4), WithMaxIdle(4), WithValidate(validate)); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}

func TestReuse(t *testing.T) {
	b := &backend{}
	p := newPool(t, b, WithMaxOpen(2))
	ctx := context.Background()
	for range 10 {
		c, err := p.Get(ctx)
		if err != nil {
			t.Fatal(err)
		}
		c.Release()
		c.Release() // second release is a no-op
	}
	s := p.Stats()
	if s.Opens != 1 || s.Reuses != 9 || s.Idle != 1 || s.Open != 1 {
		t.Fatalf("stats = %+v, want 1 open, 9 reuses, 1 idle", s)
	}
}

func TestNeverExceedsMaxOpen(t *testing.T) {
	const maxOpen = 8
	b := &backend{}
	p := newPool(t, b, WithMaxOpen(maxOpen), WithMaxIdle(4))
	var wg sync.WaitGroup
	var inUse, peakInUse atomic.Int64
	for g := range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 50 {
				c, err := p.Get(context.Background())
				if err != nil {
					t.Error(err)
					return
				}
				n := inUse.Add(1)
				for {
					old := peakInUse.Load()
					if n <= old || peakInUse.CompareAndSwap(old, n) {
						break
					}
				}
				runtime.Gosched()
				inUse.Add(-1)
				if (g+i)%7 == 0 {
					c.Discard()
				} else {
					c.Release()
				}
			}
		}()
	}
	wg.Wait()
	_, peak, dials, closes := b.snapshot()
	if peak > maxOpen || peakInUse.Load() > maxOpen {
		t.Fatalf("backend peak %d, in-use peak %d, want <= %d", peak, peakInUse.Load(), maxOpen)
	}
	s := p.Stats()
	if s.Open != dials-closes || s.Open > maxOpen || s.Waiting != 0 || s.InUse != 0 {
		t.Fatalf("stats %+v inconsistent with %d dials, %d closes", s, dials, closes)
	}
	if got := s.Reuses + s.Opens; got != 64*50 {
		t.Fatalf("reuses+opens = %d, want %d", got, 64*50)
	}
}

func TestWaitersServedFIFO(t *testing.T) {
	b := &backend{}
	p := newPool(t, b, WithMaxOpen(1))
	held, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	const n = 5
	order := make(chan int, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := p.Get(context.Background())
			if err != nil {
				t.Error(err)
				return
			}
			order <- i
			c.Release()
		}()
		waitFor(t, func() bool { return p.Stats().Waiting == i+1 }) // enqueue in order
	}
	held.Release()
	wg.Wait()
	close(order)
	want := 0
	for got := range order {
		if got != want {
			t.Fatalf("waiter %d served at position %d", got, want)
		}
		want++
	}
	s := p.Stats()
	if s.Opens != 1 || s.Waits != n || s.Reuses != n {
		t.Fatalf("stats = %+v, want one connection handed through %d waiters", s, n)
	}
}

func TestWaitTimesOutWithContext(t *testing.T) {
	b := &backend{}
	clk := &fakeClock{t: time.Unix(0, 0)}
	p := newPool(t, b, WithMaxOpen(1), WithClock(clk.Now))
	held, _ := p.Get(context.Background())

	// A real deadline ends the wait.
	tctx, tcancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer tcancel()
	if _, err := p.Get(tctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}

	// Cancellation after 20ms of (fake) waiting is counted in WaitTime.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		waitFor(t, func() bool { return p.Stats().Waiting == 1 })
		clk.Advance(20 * time.Millisecond)
		cancel()
	}()
	if _, err := p.Get(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want canceled", err)
	}
	s := p.Stats()
	if s.Timeouts != 2 || s.Waiting != 0 || s.Waits != 2 || s.WaitTime != 20*time.Millisecond {
		t.Fatalf("stats = %+v", s)
	}
	// The abandoned wait must not swallow the next release.
	held.Release()
	if s := p.Stats(); s.Idle != 1 {
		t.Fatalf("released connection not idle: %+v", s)
	}
	if _, err := p.Get(ctx); err == nil {
		t.Fatal("Get with a done context should fail")
	}
}

func TestBrokenIdleConnectionsDiscardedOnBorrow(t *testing.T) {
	b := &backend{}
	p := newPool(t, b, WithMaxOpen(3), WithValidate(validate))
	ctx := context.Background()
	var cs []*Conn[*conn]
	for range 3 {
		c, _ := p.Get(ctx)
		cs = append(cs, c)
	}
	for _, c := range cs {
		c.Release()
	}
	// The backend restarts: every idle connection is now dead.
	for _, c := range b.all {
		c.broken.Store(true)
	}
	c, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if c.Reused() || c.Value().broken.Load() {
		t.Fatal("got a broken or reused connection; want a fresh dial")
	}
	s := p.Stats()
	if s.Discards != 3 || s.Opens != 4 || s.Open != 1 {
		t.Fatalf("stats = %+v, want 3 discards and a 4th dial", s)
	}
	for _, old := range b.all[:3] {
		if !old.closed.Load() {
			t.Fatal("broken connection was not closed")
		}
	}
	// Discard on a connection the caller found broken.
	c.Discard()
	if s := p.Stats(); s.Discards != 4 || s.Open != 0 {
		t.Fatalf("after Discard: %+v", s)
	}
}

func TestDiscardHandsSlotToWaiter(t *testing.T) {
	b := &backend{}
	p := newPool(t, b, WithMaxOpen(1))
	held, _ := p.Get(context.Background())
	got := make(chan *Conn[*conn])
	go func() {
		c, err := p.Get(context.Background())
		if err != nil {
			t.Error(err)
		}
		got <- c
	}()
	waitFor(t, func() bool { return p.Stats().Waiting == 1 })
	held.Discard()
	c := <-got
	if c.Reused() {
		t.Fatal("waiter should have dialed a fresh connection")
	}
	if _, peak, dials, _ := b.snapshot(); peak != 1 || dials != 2 {
		t.Fatalf("peak %d dials %d, want 1 and 2", peak, dials)
	}
}

func TestIdleTimeout(t *testing.T) {
	b := &backend{}
	clk := &fakeClock{t: time.Unix(0, 0)}
	p := newPool(t, b, WithMaxOpen(4), WithIdleTimeout(time.Minute), WithClock(clk.Now))
	ctx := context.Background()
	c1, _ := p.Get(ctx)
	c2, _ := p.Get(ctx)
	c1.Release()
	clk.Advance(30 * time.Second)
	c2.Release()
	clk.Advance(40 * time.Second) // c1 idle 70s, c2 idle 40s

	c, _ := p.Get(ctx)
	if !c.Reused() || c.Value() != c2.Value() {
		t.Fatal("want the connection that is still within its idle timeout")
	}
	s := p.Stats()
	if s.ExpiredIdle != 1 || !c1.Value().closed.Load() || s.Open != 1 {
		t.Fatalf("stats = %+v, want c1 expired and closed", s)
	}
	c.Release()
	clk.Advance(2 * time.Minute)
	if n := p.Reap(); n != 1 || p.Stats().Open != 0 {
		t.Fatalf("Reap closed %d, open %d; want 1, 0", n, p.Stats().Open)
	}
}

func TestMaxLifetime(t *testing.T) {
	b := &backend{}
	clk := &fakeClock{t: time.Unix(0, 0)}
	p := newPool(t, b, WithMaxOpen(1), WithMaxLifetime(time.Minute), WithClock(clk.Now))
	ctx := context.Background()

	// Released after its lifetime: closed instead of kept.
	c, _ := p.Get(ctx)
	clk.Advance(61 * time.Second)
	c.Release()
	if s := p.Stats(); s.ExpiredLifetime != 1 || s.Idle != 0 || !c.Value().closed.Load() {
		t.Fatalf("stats = %+v", s)
	}

	// Expires while idle: the next Get dials a replacement.
	c, _ = p.Get(ctx)
	c.Release()
	clk.Advance(time.Minute)
	c2, _ := p.Get(ctx)
	if c2.Reused() || p.Stats().ExpiredLifetime != 2 {
		t.Fatalf("want a fresh connection after lifetime, stats %+v", p.Stats())
	}

	// Expires while a waiter is queued: the waiter gets a fresh dial.
	got := make(chan *Conn[*conn])
	go func() { w, _ := p.Get(ctx); got <- w }()
	waitFor(t, func() bool { return p.Stats().Waiting == 1 })
	clk.Advance(time.Minute)
	c2.Release()
	w := <-got
	if w.Reused() || w.Value() == c2.Value() {
		t.Fatal("waiter got the expired connection")
	}
	if _, peak, _, _ := b.snapshot(); peak != 1 {
		t.Fatalf("peak %d, want 1", peak)
	}
}

func TestMaxIdle(t *testing.T) {
	b := &backend{}
	p := newPool(t, b, WithMaxOpen(5), WithMaxIdle(2))
	ctx := context.Background()
	var cs []*Conn[*conn]
	for range 5 {
		c, _ := p.Get(ctx)
		cs = append(cs, c)
	}
	for _, c := range cs {
		c.Release()
	}
	s := p.Stats()
	if s.Idle != 2 || s.Open != 2 || s.ClosedMaxIdle != 3 {
		t.Fatalf("stats = %+v, want 2 idle, 3 closed", s)
	}
}

func TestCloseDrains(t *testing.T) {
	ctx := context.Background()

	// Idle connections are closed by Close; in-use ones when released.
	b := &backend{}
	p := newPool(t, b, WithMaxOpen(2))
	idle, _ := p.Get(ctx)
	inUse, _ := p.Get(ctx)
	idle.Release()
	p.Close()
	p.Close() // idempotent
	if !idle.Value().closed.Load() {
		t.Fatal("idle connection not closed by Close")
	}
	if inUse.Value().closed.Load() {
		t.Fatal("in-use connection closed under its user")
	}
	inUse.Release()
	if open, _, _, _ := b.snapshot(); open != 0 || p.Stats().Open != 0 {
		t.Fatalf("backend open %d, pool open %d after drain; want 0", open, p.Stats().Open)
	}
	if _, err := p.Get(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("Get after Close err = %v", err)
	}

	// Waiters are woken with ErrClosed.
	b2 := &backend{}
	p2 := newPool(t, b2, WithMaxOpen(1))
	held, _ := p2.Get(ctx)
	waitErr := make(chan error)
	go func() { _, err := p2.Get(ctx); waitErr <- err }()
	waitFor(t, func() bool { return p2.Stats().Waiting == 1 })
	p2.Close()
	if err := <-waitErr; !errors.Is(err, ErrClosed) {
		t.Fatalf("waiter err = %v, want ErrClosed", err)
	}
	held.Release()
	if open, _, _, _ := b2.snapshot(); open != 0 {
		t.Fatalf("backend open %d after drain", open)
	}
}

func TestDialErrorFreesSlot(t *testing.T) {
	fail := atomic.Bool{}
	b := &backend{}
	dial := func(ctx context.Context) (*conn, error) {
		if fail.Load() {
			return nil, errors.New("connection refused")
		}
		return b.dial(ctx)
	}
	p, err := New(dial, b.close, WithMaxOpen(1))
	if err != nil {
		t.Fatal(err)
	}
	fail.Store(true)
	if _, err := p.Get(context.Background()); err == nil {
		t.Fatal("want dial error")
	}
	fail.Store(false)
	c, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("slot leaked after dial error: %v", err)
	}
	c.Release()
	if s := p.Stats(); s.DialErrors != 1 || s.Opens != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestUnboundedPool(t *testing.T) {
	b := &backend{}
	p := newPool(t, b) // MaxOpen 0: no cap, never waits
	var cs []*Conn[*conn]
	for range 50 {
		c, err := p.Get(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		cs = append(cs, c)
	}
	for _, c := range cs {
		c.Release()
	}
	if s := p.Stats(); s.Open != 50 || s.Idle != 50 || s.Waits != 0 {
		t.Fatalf("stats = %+v", s)
	}
}
