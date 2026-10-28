package websockethub

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---- helpers ----

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)} }

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	f.t = f.t.Add(d)
	f.mu.Unlock()
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func wsURL(srv *httptest.Server, user string) string {
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/?user=" + user
}

func dial(t *testing.T, srv *httptest.Server, user string) *Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := Dial(ctx, wsURL(srv, user))
	if err != nil {
		t.Fatalf("dial %s: %v", user, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// reader drains text messages from c into a channel until the connection ends.
func reader(c *Conn) <-chan string {
	out := make(chan string, 1024)
	go func() {
		defer close(out)
		for {
			_, data, err := c.ReadMessage()
			if err != nil {
				return
			}
			out <- string(data)
		}
	}()
	return out
}

func recv(t *testing.T, ch <-chan string) string {
	t.Helper()
	select {
	case s, ok := <-ch:
		if !ok {
			t.Fatal("connection closed while waiting for a message")
		}
		return s
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for a message")
	}
	return ""
}

func newNode(t *testing.T, name string, opts ...Option) (*Hub, *httptest.Server) {
	t.Helper()
	h, err := New(name, opts...)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(func() { h.Close(); srv.Close() })
	return h, srv
}

// upgradeServer runs fn on the server side of every upgraded connection.
func upgradeServer(t *testing.T, fn func(*Conn)) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := Upgrade(w, r)
		if err != nil {
			return
		}
		fn(c)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// ---- handshake ----

func TestAcceptKeyRFCExample(t *testing.T) {
	if got := AcceptKey("dGhlIHNhbXBsZSBub25jZQ=="); got != "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" {
		t.Fatalf("AcceptKey = %q", got)
	}
}

func TestUpgradeRejectsBadHandshakes(t *testing.T) {
	good := func() *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("Upgrade", "websocket")
		r.Header.Set("Connection", "keep-alive, Upgrade")
		r.Header.Set("Sec-WebSocket-Version", "13")
		r.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
		return r
	}
	tests := []struct {
		name   string
		mutate func(*http.Request)
		status int
	}{
		{"POST", func(r *http.Request) { r.Method = http.MethodPost }, http.StatusMethodNotAllowed},
		{"no Upgrade", func(r *http.Request) { r.Header.Del("Upgrade") }, http.StatusBadRequest},
		{"no Connection: Upgrade", func(r *http.Request) { r.Header.Set("Connection", "keep-alive") }, http.StatusBadRequest},
		{"version 8", func(r *http.Request) { r.Header.Set("Sec-WebSocket-Version", "8") }, http.StatusUpgradeRequired},
		{"key not base64", func(r *http.Request) { r.Header.Set("Sec-WebSocket-Key", "not base64!") }, http.StatusBadRequest},
		{"key wrong length", func(r *http.Request) { r.Header.Set("Sec-WebSocket-Key", "c2hvcnQ=") }, http.StatusBadRequest},
		{"valid but recorder can't hijack", func(*http.Request) {}, http.StatusInternalServerError},
	}
	for _, tc := range tests {
		r := good()
		tc.mutate(r)
		w := httptest.NewRecorder()
		if _, err := Upgrade(w, r); !errors.Is(err, ErrBadHandshake) {
			t.Errorf("%s: err = %v, want ErrBadHandshake", tc.name, err)
		}
		if w.Code != tc.status {
			t.Errorf("%s: status %d, want %d", tc.name, w.Code, tc.status)
		}
		if tc.status == http.StatusUpgradeRequired && w.Header().Get("Sec-WebSocket-Version") != "13" {
			t.Errorf("%s: 426 must advertise Sec-WebSocket-Version: 13", tc.name)
		}
	}
}

func TestDialAndEcho(t *testing.T) {
	srv := upgradeServer(t, func(c *Conn) {
		for {
			op, data, err := c.ReadMessage()
			if err != nil {
				return
			}
			_ = c.WriteMessage(op, append([]byte("echo:"), data...))
		}
	})
	c := dial(t, srv, "x")
	for _, msg := range []string{"hi", strings.Repeat("a", 200), strings.Repeat("b", 70000)} {
		if err := c.WriteMessage(OpText, []byte(msg)); err != nil {
			t.Fatal(err)
		}
		_, got, err := c.ReadMessage()
		if err != nil || string(got) != "echo:"+msg {
			t.Fatalf("echo of %d bytes: got %d bytes, err %v", len(msg), len(got), err)
		}
	}
	if _, err := Dial(context.Background(), "wss://example.com/"); !errors.Is(err, ErrBadHandshake) {
		t.Errorf("wss:// should be rejected, got %v", err)
	}
}

// ---- framing ----

func TestFrameRoundTrip(t *testing.T) {
	key := [4]byte{0x37, 0xfa, 0x21, 0x3d}
	tests := []struct {
		op     Opcode
		size   int
		masked bool
		header int // bytes before the payload
	}{
		{OpText, 0, false, 2},
		{OpText, 5, true, 6},
		{OpBinary, 125, false, 2},
		{OpBinary, 126, true, 8},     // 16-bit length
		{OpText, 65535, false, 4},    // largest 16-bit
		{OpBinary, 65536, true, 14},  // 64-bit length
		{OpBinary, 70000, false, 10}, // 64-bit length
		{OpPing, 125, true, 6},
		{OpClose, 2, false, 2},
	}
	for _, tc := range tests {
		payload := bytes.Repeat([]byte{'x', 'y', 'z'}, tc.size/3+1)[:tc.size]
		orig := append([]byte(nil), payload...)
		f := Frame{Fin: true, Opcode: tc.op, Masked: tc.masked, MaskKey: key, Payload: payload}
		wire := AppendFrame(nil, f)
		if len(wire) != tc.header+tc.size {
			t.Errorf("%s/%d: wire length %d, want %d", tc.op, tc.size, len(wire), tc.header+tc.size)
		}
		if !bytes.Equal(payload, orig) {
			t.Errorf("%s/%d: AppendFrame modified the caller's payload", tc.op, tc.size)
		}
		if tc.masked && tc.size > 0 && bytes.Equal(wire[tc.header:], orig) {
			t.Errorf("%s/%d: payload not masked on the wire", tc.op, tc.size)
		}
		got, err := ReadFrame(bytes.NewReader(wire), 0)
		if err != nil {
			t.Fatalf("%s/%d: %v", tc.op, tc.size, err)
		}
		if !got.Fin || got.Opcode != tc.op || got.Masked != tc.masked || !bytes.Equal(got.Payload, orig) {
			t.Errorf("%s/%d: round trip mismatch: fin=%v op=%s masked=%v", tc.op, tc.size, got.Fin, got.Opcode, got.Masked)
		}
		if tc.masked && got.MaskKey != key {
			t.Errorf("%s/%d: mask key %v", tc.op, tc.size, got.MaskKey)
		}
	}
}

func TestMaskingRFCExample(t *testing.T) {
	// RFC 6455 section 5.7: a masked "Hello" from a client.
	want := []byte{0x81, 0x85, 0x37, 0xfa, 0x21, 0x3d, 0x7f, 0x9f, 0x4d, 0x51, 0x58}
	got := AppendFrame(nil, Frame{Fin: true, Opcode: OpText, Masked: true, MaskKey: [4]byte{0x37, 0xfa, 0x21, 0x3d}, Payload: []byte("Hello")})
	if !bytes.Equal(got, want) {
		t.Fatalf("got % x, want % x", got, want)
	}
}

func TestReadFrameRejects(t *testing.T) {
	tests := []struct {
		name string
		wire []byte
		want error
	}{
		{"reserved bit", []byte{0x81 | 0x40, 0x00}, ErrProtocol},
		{"reserved opcode", []byte{0x83, 0x00}, ErrProtocol},
		{"fragmented ping", []byte{0x09, 0x00}, ErrProtocol},
		{"ping over 125", append([]byte{0x89, 126, 0x00, 126}, make([]byte, 126)...), ErrProtocol},
		{"non-minimal 16-bit", append([]byte{0x81, 126, 0x00, 0x05}, "hello"...), ErrProtocol},
		{"non-minimal 64-bit", []byte{0x81, 127, 0, 0, 0, 0, 0, 0, 0x01, 0x00}, ErrProtocol},
		{"64-bit msb set", []byte{0x82, 127, 0x80, 0, 0, 0, 0, 0, 0, 0}, ErrProtocol},
		{"over limit", []byte{0x82, 127, 0, 0, 0, 0, 0x7f, 0, 0, 0}, ErrTooLarge},
		{"truncated payload", []byte{0x81, 0x05, 'h', 'i'}, io.ErrUnexpectedEOF},
	}
	for _, tc := range tests {
		if _, err := ReadFrame(bytes.NewReader(tc.wire), 1<<20); !errors.Is(err, tc.want) {
			t.Errorf("%s: err = %v, want %v", tc.name, err, tc.want)
		}
	}
}

func TestServerRejectsUnmaskedClientFrame(t *testing.T) {
	serverErr := make(chan error, 1)
	srv := upgradeServer(t, func(c *Conn) {
		_, _, err := c.ReadMessage()
		serverErr <- err
	})
	c := dial(t, srv, "x")
	// Bypass the client's masking and send a raw unmasked frame.
	if err := WriteFrame(c.nc, Frame{Fin: true, Opcode: OpText, Payload: []byte("hi")}); err != nil {
		t.Fatal(err)
	}
	if err := <-serverErr; !errors.Is(err, ErrUnmaskedFrame) {
		t.Fatalf("server error = %v, want ErrUnmaskedFrame", err)
	}
	_, _, err := c.ReadMessage()
	var ce *CloseError
	if !errors.As(err, &ce) || ce.Code != CloseProtocolError {
		t.Fatalf("client got %v, want close 1002", err)
	}
}

func TestFragmentationReassembly(t *testing.T) {
	type result struct {
		op   Opcode
		data string
		err  error
	}
	results := make(chan result, 4)
	srv := upgradeServer(t, func(c *Conn) {
		c.SetMaxMessageSize(20)
		for {
			op, data, err := c.ReadMessage()
			results <- result{op, string(data), err}
			if err != nil {
				return
			}
		}
	})
	c := dial(t, srv, "x")
	pongs := make(chan string, 1)
	c.SetPongHandler(func(p []byte) { pongs <- string(p) })
	go func() { _, _, _ = c.ReadMessage() }() // runs the client's control-frame handling

	raw := func(fin bool, op Opcode, p string) {
		t.Helper()
		if err := WriteFrame(c.nc, Frame{Fin: fin, Opcode: op, Masked: true, MaskKey: [4]byte{1, 2, 3, 4}, Payload: []byte(p)}); err != nil {
			t.Fatal(err)
		}
	}
	raw(false, OpText, "Hel")
	raw(true, OpPing, "mid-message ping") // control frames may interleave
	raw(false, OpContinuation, "lo, ")
	raw(true, OpContinuation, "world")
	if r := <-results; r.err != nil || r.op != OpText || r.data != "Hello, world" {
		t.Fatalf("reassembled %v %q %v", r.op, r.data, r.err)
	}
	if p := <-pongs; p != "mid-message ping" {
		t.Fatalf("pong payload %q", p)
	}
	// Fragments adding up to more than the 20-byte limit are refused.
	raw(false, OpBinary, "0123456789")
	raw(false, OpContinuation, "0123456789")
	raw(true, OpContinuation, "!")
	if r := <-results; !errors.Is(r.err, ErrTooLarge) {
		t.Fatalf("oversized reassembly: %v", r.err)
	}
}

func TestFragmentationProtocolErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		frames []Frame
	}{
		{"continuation without start", []Frame{{Fin: true, Opcode: OpContinuation}}},
		{"new message inside fragmented one", []Frame{{Opcode: OpText, Payload: []byte("a")}, {Fin: true, Opcode: OpText, Payload: []byte("b")}}},
		{"invalid UTF-8 text", []Frame{{Fin: true, Opcode: OpText, Payload: []byte{0xff, 0xfe}}}},
	} {
		serverErr := make(chan error, 1)
		srv := upgradeServer(t, func(c *Conn) {
			_, _, err := c.ReadMessage()
			serverErr <- err
		})
		c := dial(t, srv, "x")
		for _, f := range tc.frames {
			f.Masked = true
			_ = WriteFrame(c.nc, f)
		}
		if err := <-serverErr; !errors.Is(err, ErrProtocol) {
			t.Errorf("%s: %v, want ErrProtocol", tc.name, err)
		}
	}
}

func TestCloseHandshake(t *testing.T) {
	serverErr := make(chan error, 1)
	srv := upgradeServer(t, func(c *Conn) {
		if err := c.CloseHandshake(CloseGoingAway, "deploying"); err != nil {
			serverErr <- err
			return
		}
		_, _, err := c.ReadMessage() // waits for the client's echo
		serverErr <- err
	})
	c := dial(t, srv, "x")
	_, _, err := c.ReadMessage()
	var ce *CloseError
	if !errors.As(err, &ce) || ce.Code != CloseGoingAway || ce.Reason != "deploying" {
		t.Fatalf("client got %v, want 1001 deploying", err)
	}
	if err := <-serverErr; !errors.As(err, &ce) || ce.Code != CloseGoingAway {
		t.Fatalf("server got %v, want echoed 1001", err)
	}
	if err := c.WriteMessage(OpText, []byte("late")); err == nil {
		t.Fatal("write after close should fail")
	}
}

// ---- hub ----

func TestNewValidation(t *testing.T) {
	tests := []struct {
		name string
		node string
		opt  Option
	}{
		{"empty node", "", nil},
		{"queue 0", "a", WithQueueSize(0)},
		{"unknown policy", "a", WithSlowPolicy(SlowPolicy(9))},
		{"timeout <= interval", "a", WithHeartbeat(time.Second, time.Second)},
		{"zero interval", "a", WithHeartbeat(0, time.Second)},
		{"write timeout 0", "a", WithWriteTimeout(0)},
		{"max message 0", "a", WithMaxMessageSize(0)},
		{"bus buffer 0", "a", WithBusBuffer(0)},
		{"nil clock", "a", WithClock(nil)},
		{"nil cluster", "a", WithCluster(nil)},
		{"nil auth", "a", WithAuth(nil)},
	}
	for _, tc := range tests {
		var opts []Option
		if tc.opt != nil {
			opts = append(opts, tc.opt)
		}
		if _, err := New(tc.node, opts...); err == nil {
			t.Errorf("%s: expected error", tc.name)
		}
	}
	cl := NewCluster()
	h, err := New("a", WithCluster(cl))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if _, err := New("a", WithCluster(cl)); err == nil {
		t.Error("duplicate node name should be rejected")
	}
}

func TestServeHTTPRequiresUser(t *testing.T) {
	_, srv := newNode(t, "a")
	if _, err := Dial(context.Background(), "ws"+strings.TrimPrefix(srv.URL, "http")+"/"); !errors.Is(err, ErrBadHandshake) {
		t.Fatalf("dial without user: %v", err)
	}
}

func TestHeartbeatEvictsSilentClient(t *testing.T) {
	clock := newFakeClock()
	h, srv := newNode(t, "a", WithClock(clock.Now), WithHeartbeat(15*time.Second, 20*time.Second))
	alive := dial(t, srv, "alive")
	silent := dial(t, srv, "silent")
	silent.SetPingHandler(func([]byte) error { return nil }) // phone asleep: no pongs
	aliveMsgs, silentMsgs := reader(alive), reader(silent)
	waitFor(t, "2 connections", func() bool { return h.Connections() == 2 })

	clock.Advance(15 * time.Second)
	if pinged, evicted := h.Heartbeat(); pinged != 2 || evicted != 0 {
		t.Fatalf("first sweep pinged %d evicted %d, want 2 0", pinged, evicted)
	}
	waitFor(t, "alive's pong", func() bool { return h.Stats().Pongs == 1 })

	clock.Advance(15 * time.Second) // alive silent 15s, silent client 30s
	if pinged, evicted := h.Heartbeat(); pinged != 1 || evicted != 1 {
		t.Fatalf("second sweep pinged %d evicted %d, want 1 1", pinged, evicted)
	}
	if got := h.cl.registry.Nodes("silent"); len(got) != 0 {
		t.Fatalf("silent still registered on %v", got)
	}
	if _, ok := <-silentMsgs; ok {
		t.Fatal("silent client should see its connection closed")
	}
	if _, err := h.SendToUser("alive", []byte("still here")); err != nil {
		t.Fatal(err)
	}
	if got := recv(t, aliveMsgs); got != "still here" {
		t.Fatalf("alive got %q", got)
	}
	if s := h.Stats(); s.EvictedDead != 1 || s.Connections != 1 {
		t.Fatalf("stats %+v", s)
	}
}

func TestRunPingsOnRealTicker(t *testing.T) {
	h, srv := newNode(t, "a", WithHeartbeat(5*time.Millisecond, 40*time.Millisecond))
	silent := dial(t, srv, "silent")
	silent.SetPingHandler(func([]byte) error { return nil })
	reader(silent)
	waitFor(t, "connection", func() bool { return h.Connections() == 1 })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)
	waitFor(t, "heartbeat eviction", func() bool { return h.Stats().EvictedDead == 1 })
}

// pipeClient registers a connection over net.Pipe. net.Pipe is unbuffered,
// so a peer that never reads blocks the hub's writer on the first frame:
// a perfectly deterministic slow client.
func pipeClient(t *testing.T, h *Hub, user string) (peer *Conn) {
	t.Helper()
	serverSide, clientSide := net.Pipe()
	t.Cleanup(func() { _ = clientSide.Close() })
	if _, err := h.Register(user, newConn(serverSide, nil, true)); err != nil {
		t.Fatal(err)
	}
	return newConn(clientSide, nil, false)
}

func queueLen(h *Hub, user string) int {
	for _, c := range h.snapshotUser(user) {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.queue)
	}
	return -1
}

func TestSlowConsumerDisconnectedWithoutStallingOthers(t *testing.T) {
	h, srv := newNode(t, "a", WithQueueSize(4), WithSlowPolicy(Disconnect))
	pipeClient(t, h, "slow") // never reads
	var fast []<-chan string
	for i := range 3 {
		fast = append(fast, reader(dial(t, srv, fmt.Sprintf("fast-%d", i))))
	}
	waitFor(t, "4 connections", func() bool { return h.Connections() == 4 })

	// Lockstep: every fast client must receive message i before i+1 is
	// sent. If the stuck slow client could stall the hub, this would hang.
	const n = 50
	for i := range n {
		if _, err := h.Broadcast([]byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
		for _, ch := range fast {
			if got := recv(t, ch); got != fmt.Sprint(i) {
				t.Fatalf("fast client got %q, want %d", got, i)
			}
		}
	}
	s := h.Stats()
	if s.EvictedSlow != 1 || s.Connections != 3 {
		t.Fatalf("stats %+v, want 1 slow eviction and 3 connections", s)
	}
	if nodes := h.cl.registry.Nodes("slow"); len(nodes) != 0 {
		t.Fatalf("slow client still registered: %v", nodes)
	}
}

func TestSlowConsumerDropOldest(t *testing.T) {
	h, _ := newNode(t, "a", WithQueueSize(4), WithSlowPolicy(DropOldest))
	peer := pipeClient(t, h, "rider-watcher")
	if _, err := h.SendToUser("rider-watcher", []byte("loc-1")); err != nil {
		t.Fatal(err)
	}
	// The writer pops loc-1 and blocks writing it into the unread pipe.
	waitFor(t, "writer to take loc-1", func() bool { return queueLen(h, "rider-watcher") == 0 })
	for i := 2; i <= 20; i++ {
		if _, err := h.SendToUser("rider-watcher", []byte(fmt.Sprintf("loc-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	msgs := reader(peer) // the client finally catches up
	for _, want := range []string{"loc-1", "loc-17", "loc-18", "loc-19", "loc-20"} {
		if got := recv(t, msgs); got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
	if s := h.Stats(); s.Dropped != 15 || s.EvictedSlow != 0 {
		t.Fatalf("stats %+v, want 15 dropped and no eviction", s)
	}
}

func TestCrossNodeDelivery(t *testing.T) {
	cl := NewCluster()
	a, srvA := newNode(t, "a", WithCluster(cl))
	b, srvB := newNode(t, "b", WithCluster(cl))
	laptop := reader(dial(t, srvA, "bob")) // bob is on both nodes
	phone := reader(dial(t, srvB, "bob"))
	carol := reader(dial(t, srvB, "carol"))
	waitFor(t, "registrations", func() bool { return cl.Registry().Users() == 2 && len(cl.Registry().Nodes("bob")) == 2 })

	routed, err := a.SendToUser("bob", []byte("hi bob"))
	if err != nil || routed != 2 {
		t.Fatalf("routed %d err %v", routed, err)
	}
	if recv(t, laptop) != "hi bob" || recv(t, phone) != "hi bob" {
		t.Fatal("both of bob's devices should get the message")
	}

	writtenA := a.Stats().Written
	if _, err := a.SendToUser("carol", []byte("hi carol")); err != nil {
		t.Fatal(err)
	}
	if got := recv(t, carol); got != "hi carol" {
		t.Fatalf("carol got %q", got)
	}
	waitFor(t, "b's writes", func() bool { return b.Stats().Written == 2 })
	if a.Stats().Written != writtenA {
		t.Fatal("node a wrote to a connection it does not own")
	}
	if sa, sb := a.Stats(), b.Stats(); sa.Forwarded != 2 || sb.FromBus != 2 {
		t.Fatalf("a forwarded %d, b received %d from bus; want 2 and 2", sa.Forwarded, sb.FromBus)
	}

	if n, err := b.Broadcast([]byte("all")); err != nil || n != 2 {
		t.Fatalf("broadcast queued %d locally, err %v", n, err)
	}
	for _, ch := range []<-chan string{laptop, phone, carol} {
		if got := recv(t, ch); got != "all" {
			t.Fatalf("broadcast got %q", got)
		}
	}
	if _, err := a.SendToUser("nobody", []byte("x")); !errors.Is(err, ErrUserOffline) {
		t.Fatalf("offline user: %v", err)
	}
}

func TestRegistryCleanupOnDisconnect(t *testing.T) {
	cl := NewCluster()
	a, _ := newNode(t, "a", WithCluster(cl))
	b, srvB := newNode(t, "b", WithCluster(cl))
	c1 := dial(t, srvB, "dana")
	c2 := dial(t, srvB, "dana")
	waitFor(t, "two connections", func() bool { return b.Connections() == 2 })
	if got := cl.Registry().Nodes("dana"); len(got) != 1 || got[0] != "b" {
		t.Fatalf("registry %v", got)
	}

	_ = c1.Close() // abrupt: TCP gone, no close frame
	waitFor(t, "first cleanup", func() bool { return b.Connections() == 1 })
	if got := cl.Registry().Nodes("dana"); len(got) != 1 {
		t.Fatal("dana still has a device, so the registry must keep node b")
	}

	if err := c2.CloseHandshake(CloseNormal, "bye"); err != nil {
		t.Fatal(err)
	}
	go func() { _, _, _ = c2.ReadMessage() }() // receive the server's echo
	waitFor(t, "second cleanup", func() bool { return b.Connections() == 0 && cl.Registry().Users() == 0 })
	if _, err := a.SendToUser("dana", []byte("x")); !errors.Is(err, ErrUserOffline) {
		t.Fatalf("send after disconnect: %v", err)
	}
}

func TestHubCloseSendsGoingAway(t *testing.T) {
	h, err := New("a")
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	defer srv.Close()
	c := dial(t, srv, "eve")
	waitFor(t, "connection", func() bool { return h.Connections() == 1 })
	errs := make(chan error, 1)
	go func() {
		_, _, err := c.ReadMessage()
		errs <- err
	}()
	h.Close()
	var ce *CloseError
	if err := <-errs; !errors.As(err, &ce) || ce.Code != CloseGoingAway {
		t.Fatalf("client got %v, want 1001", err)
	}
	if _, err := h.Broadcast([]byte("x")); !errors.Is(err, ErrHubClosed) {
		t.Fatalf("broadcast after close: %v", err)
	}
}

func TestConcurrentSendsAndChurn(t *testing.T) {
	cl := NewCluster()
	a, srvA := newNode(t, "a", WithCluster(cl), WithQueueSize(8))
	b, srvB := newNode(t, "b", WithCluster(cl), WithQueueSize(8))
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			srv := srvA
			if i%2 == 1 {
				srv = srvB
			}
			for j := range 5 {
				c := dial(t, srv, fmt.Sprintf("u%d", (i+j)%4))
				reader(c)
				_ = c.Close()
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			h := a
			if i%2 == 1 {
				h = b
			}
			for j := range 50 {
				_, _ = h.SendToUser(fmt.Sprintf("u%d", j%4), []byte("m"))
				_, _ = h.Broadcast([]byte("b"))
				h.Heartbeat()
			}
		}()
	}
	wg.Wait()
	waitFor(t, "all connections cleaned up", func() bool {
		return a.Connections() == 0 && b.Connections() == 0 && cl.Registry().Users() == 0
	})
}
