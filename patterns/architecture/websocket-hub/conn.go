package websockethub

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// Close status codes (RFC 6455 section 7.4.1).
const (
	CloseNormal          uint16 = 1000
	CloseGoingAway       uint16 = 1001
	CloseProtocolError   uint16 = 1002
	CloseUnsupportedData uint16 = 1003
	CloseNoStatus        uint16 = 1005 // never sent on the wire: "close frame had no code"
	CloseInvalidPayload  uint16 = 1007
	ClosePolicyViolation uint16 = 1008
	CloseTooBig          uint16 = 1009
)

// CloseError is returned by ReadMessage when the peer sent a close frame.
type CloseError struct {
	Code   uint16
	Reason string
}

func (e *CloseError) Error() string {
	return fmt.Sprintf("websockethub: connection closed: %d %s", e.Code, e.Reason)
}

// DefaultMaxMessageSize bounds a single frame and a reassembled message.
const DefaultMaxMessageSize = 1 << 20

// closeWait is how long CloseHandshake waits for the peer's close reply
// before dropping the TCP connection anyway.
const closeWait = time.Second

// Conn is one WebSocket connection, server or client side. ReadMessage must
// be called from one goroutine at a time; writes are serialised internally,
// so the read loop (answering pings and closes) and a writer goroutine can
// share a Conn.
type Conn struct {
	nc     net.Conn
	br     *bufio.Reader
	server bool
	maxMsg int64

	wmu       sync.Mutex
	closeSent atomic.Bool

	pingHandler func(payload []byte) error
	pongHandler func(payload []byte)
}

func newConn(nc net.Conn, br *bufio.Reader, server bool) *Conn {
	if br == nil {
		br = bufio.NewReader(nc)
	}
	c := &Conn{nc: nc, br: br, server: server, maxMsg: DefaultMaxMessageSize}
	c.pingHandler = func(p []byte) error { return c.WriteControl(OpPong, p) }
	return c
}

// SetMaxMessageSize sets the largest frame and reassembled message the
// connection accepts. Call it before reading.
func (c *Conn) SetMaxMessageSize(n int64) {
	if n > 0 {
		c.maxMsg = n
	}
}

// SetPingHandler replaces the default ping handler, which replies with a
// pong carrying the same payload. A handler that does nothing simulates a
// peer that has silently gone away. Call it before reading.
func (c *Conn) SetPingHandler(h func(payload []byte) error) { c.pingHandler = h }

// SetPongHandler is called for every pong received. Call it before reading.
func (c *Conn) SetPongHandler(h func(payload []byte)) { c.pongHandler = h }

// SetWriteDeadline sets the deadline for subsequent writes.
func (c *Conn) SetWriteDeadline(t time.Time) error { return c.nc.SetWriteDeadline(t) }

// RemoteAddr returns the peer's network address.
func (c *Conn) RemoteAddr() net.Addr { return c.nc.RemoteAddr() }

// Close drops the TCP connection without a close handshake.
func (c *Conn) Close() error { return c.nc.Close() }

func (c *Conn) writeFrame(op Opcode, payload []byte) error {
	f := Frame{Fin: true, Opcode: op, Payload: payload}
	if !c.server { // client-to-server frames must be masked
		f.Masked = true
		if _, err := rand.Read(f.MaskKey[:]); err != nil {
			return err
		}
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return WriteFrame(c.nc, f)
}

// WriteMessage sends a text or binary message as a single frame.
func (c *Conn) WriteMessage(op Opcode, data []byte) error {
	if op != OpText && op != OpBinary {
		return fmt.Errorf("websockethub: WriteMessage needs text or binary, got %s", op)
	}
	if c.closeSent.Load() {
		return net.ErrClosed
	}
	return c.writeFrame(op, data)
}

// WriteControl sends a ping or pong (at most 125 bytes).
func (c *Conn) WriteControl(op Opcode, payload []byte) error {
	if op != OpPing && op != OpPong {
		return fmt.Errorf("websockethub: WriteControl needs ping or pong, got %s", op)
	}
	if len(payload) > maxControlPayload {
		return fmt.Errorf("%w: control payload over 125 bytes", ErrProtocol)
	}
	return c.writeFrame(op, payload)
}

// CloseHandshake starts the closing handshake: it sends a close frame with
// code and reason, and the read loop closes the TCP connection when the
// peer's close reply arrives. If no reply comes within a second, the
// connection is dropped anyway.
func (c *Conn) CloseHandshake(code uint16, reason string) error {
	if !c.closeSent.CompareAndSwap(false, true) {
		return nil
	}
	time.AfterFunc(closeWait, func() { _ = c.nc.Close() })
	_ = c.nc.SetWriteDeadline(time.Now().Add(closeWait))
	return c.writeFrame(OpClose, closePayload(code, reason))
}

func closePayload(code uint16, reason string) []byte {
	if code == CloseNoStatus {
		return nil
	}
	if len(reason) > maxControlPayload-2 {
		reason = reason[:maxControlPayload-2]
	}
	return append(binary.BigEndian.AppendUint16(nil, code), reason...)
}

// fail answers a protocol violation with a close frame carrying code, then
// drops the connection.
func (c *Conn) fail(code uint16, err error) error {
	if c.closeSent.CompareAndSwap(false, true) {
		_ = c.nc.SetWriteDeadline(time.Now().Add(closeWait))
		_ = c.writeFrame(OpClose, closePayload(code, err.Error()))
	}
	_ = c.nc.Close()
	return err
}

// ReadMessage returns the next complete text or binary message. It
// reassembles fragmented messages, answers pings, reports pongs to the pong
// handler, and finishes the closing handshake: a close frame is echoed (if
// this side has not sent one yet), the TCP connection is closed, and a
// *CloseError is returned. Protocol violations are answered with the right
// close code before the error is returned.
func (c *Conn) ReadMessage() (Opcode, []byte, error) {
	var (
		msgOp Opcode
		msg   []byte
		inMsg bool
	)
	for {
		f, err := ReadFrame(c.br, c.maxMsg)
		if err != nil {
			switch {
			case errors.Is(err, ErrTooLarge):
				return 0, nil, c.fail(CloseTooBig, err)
			case errors.Is(err, ErrProtocol):
				return 0, nil, c.fail(CloseProtocolError, err)
			}
			_ = c.nc.Close()
			return 0, nil, err
		}
		if c.server && !f.Masked {
			return 0, nil, c.fail(CloseProtocolError, ErrUnmaskedFrame)
		}
		if !c.server && f.Masked {
			return 0, nil, c.fail(CloseProtocolError, fmt.Errorf("%w: server frame is masked", ErrProtocol))
		}
		switch f.Opcode {
		case OpPing:
			if h := c.pingHandler; h != nil {
				if err := h(f.Payload); err != nil {
					_ = c.nc.Close()
					return 0, nil, err
				}
			}
			continue
		case OpPong:
			if h := c.pongHandler; h != nil {
				h(f.Payload)
			}
			continue
		case OpClose:
			return 0, nil, c.handleClose(f.Payload)
		case OpContinuation:
			if !inMsg {
				return 0, nil, c.fail(CloseProtocolError, fmt.Errorf("%w: continuation frame without a message to continue", ErrProtocol))
			}
		default: // text or binary
			if inMsg {
				return 0, nil, c.fail(CloseProtocolError, fmt.Errorf("%w: new %s frame inside a fragmented message", ErrProtocol, f.Opcode))
			}
			msgOp, inMsg = f.Opcode, true
		}
		if int64(len(msg))+int64(len(f.Payload)) > c.maxMsg {
			return 0, nil, c.fail(CloseTooBig, fmt.Errorf("%w: reassembled message exceeds %d bytes", ErrTooLarge, c.maxMsg))
		}
		msg = append(msg, f.Payload...)
		if !f.Fin {
			continue
		}
		if msgOp == OpText && !utf8.Valid(msg) {
			return 0, nil, c.fail(CloseInvalidPayload, fmt.Errorf("%w: text message is not valid UTF-8", ErrProtocol))
		}
		if msg == nil {
			msg = []byte{}
		}
		return msgOp, msg, nil
	}
}

func (c *Conn) handleClose(p []byte) error {
	ce := &CloseError{Code: CloseNoStatus}
	switch {
	case len(p) == 1:
		return c.fail(CloseProtocolError, fmt.Errorf("%w: 1-byte close payload", ErrProtocol))
	case len(p) >= 2:
		ce.Code = binary.BigEndian.Uint16(p)
		ce.Reason = string(p[2:])
		if !utf8.ValidString(ce.Reason) {
			return c.fail(CloseInvalidPayload, fmt.Errorf("%w: close reason is not valid UTF-8", ErrProtocol))
		}
	}
	if c.closeSent.CompareAndSwap(false, true) { // peer started it: echo the code
		_ = c.nc.SetWriteDeadline(time.Now().Add(closeWait))
		_ = c.writeFrame(OpClose, closePayload(ce.Code, ""))
	}
	_ = c.nc.Close()
	return ce
}

// websocketGUID is the fixed string from RFC 6455 section 1.3.
const websocketGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// AcceptKey computes Sec-WebSocket-Accept for a Sec-WebSocket-Key: the
// base64 of SHA-1(key + GUID). It proves the server understood the
// WebSocket handshake, so a cache or plain HTTP server can't fake one.
func AcceptKey(key string) string {
	h := sha1.Sum([]byte(key + websocketGUID))
	return base64.StdEncoding.EncodeToString(h[:])
}

// ErrBadHandshake wraps every handshake failure.
var ErrBadHandshake = errors.New("websockethub: bad handshake")

// headerHasToken reports whether a comma-separated header contains token,
// case-insensitively ("Connection: keep-alive, Upgrade" is valid).
func headerHasToken(h http.Header, name, token string) bool {
	for _, v := range h.Values(name) {
		for _, t := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(t), token) {
				return true
			}
		}
	}
	return false
}

// checkUpgrade validates a client handshake and returns the HTTP status to
// reject it with, or 0 if it is acceptable.
func checkUpgrade(r *http.Request) (int, error) {
	switch {
	case r.Method != http.MethodGet:
		return http.StatusMethodNotAllowed, fmt.Errorf("%w: method %s, want GET", ErrBadHandshake, r.Method)
	case !headerHasToken(r.Header, "Upgrade", "websocket"):
		return http.StatusBadRequest, fmt.Errorf("%w: missing Upgrade: websocket", ErrBadHandshake)
	case !headerHasToken(r.Header, "Connection", "upgrade"):
		return http.StatusBadRequest, fmt.Errorf("%w: missing Connection: Upgrade", ErrBadHandshake)
	case r.Header.Get("Sec-WebSocket-Version") != "13":
		return http.StatusUpgradeRequired, fmt.Errorf("%w: unsupported Sec-WebSocket-Version %q", ErrBadHandshake, r.Header.Get("Sec-WebSocket-Version"))
	}
	key, err := base64.StdEncoding.DecodeString(r.Header.Get("Sec-WebSocket-Key"))
	if err != nil || len(key) != 16 {
		return http.StatusBadRequest, fmt.Errorf("%w: Sec-WebSocket-Key must be 16 random bytes in base64", ErrBadHandshake)
	}
	return 0, nil
}

// Upgrade validates the client's opening handshake, takes over the TCP
// connection with http.Hijacker, and writes the 101 response. On failure it
// writes an HTTP error itself (426 with Sec-WebSocket-Version: 13 for a
// version mismatch) and returns an error wrapping ErrBadHandshake.
func Upgrade(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	if status, err := checkUpgrade(r); err != nil {
		if status == http.StatusUpgradeRequired {
			w.Header().Set("Sec-WebSocket-Version", "13")
		}
		http.Error(w, err.Error(), status)
		return nil, err
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "websocket: connection cannot be hijacked", http.StatusInternalServerError)
		return nil, fmt.Errorf("%w: ResponseWriter does not implement http.Hijacker", ErrBadHandshake)
	}
	nc, rw, err := hj.Hijack()
	if err != nil {
		return nil, fmt.Errorf("%w: hijack: %v", ErrBadHandshake, err)
	}
	_ = nc.SetDeadline(time.Time{}) // drop the HTTP server's read/write timeouts
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + AcceptKey(r.Header.Get("Sec-WebSocket-Key")) + "\r\n\r\n"
	if _, err := nc.Write([]byte(resp)); err != nil {
		_ = nc.Close()
		return nil, err
	}
	// rw.Reader may already hold the client's first frames.
	return newConn(nc, rw.Reader, true), nil
}

// Dial opens a client connection to a ws:// URL: it sends the opening
// handshake with a random Sec-WebSocket-Key and checks the server's
// Sec-WebSocket-Accept. Frames it sends are masked. No TLS (wss://).
func Dial(ctx context.Context, rawURL string) (*Conn, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "ws" {
		return nil, fmt.Errorf("%w: scheme %q not supported (only ws://)", ErrBadHandshake, u.Scheme)
	}
	host := u.Host
	if u.Port() == "" {
		host = net.JoinHostPort(u.Hostname(), "80")
	}
	var d net.Dialer
	nc, err := d.DialContext(ctx, "tcp", host)
	if err != nil {
		return nil, err
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = nc.SetDeadline(dl)
	}
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		_ = nc.Close()
		return nil, err
	}
	key := base64.StdEncoding.EncodeToString(raw[:])
	req := "GET " + u.RequestURI() + " HTTP/1.1\r\n" +
		"Host: " + u.Host + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + key + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n"
	if _, err := nc.Write([]byte(req)); err != nil {
		_ = nc.Close()
		return nil, err
	}
	br := bufio.NewReader(nc)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil {
		_ = nc.Close()
		return nil, fmt.Errorf("%w: %v", ErrBadHandshake, err)
	}
	_ = resp.Body.Close() // a 101 has no body; the stream that follows is frames
	switch {
	case resp.StatusCode != http.StatusSwitchingProtocols:
		_ = nc.Close()
		return nil, fmt.Errorf("%w: status %s", ErrBadHandshake, resp.Status)
	case !headerHasToken(resp.Header, "Upgrade", "websocket"), !headerHasToken(resp.Header, "Connection", "upgrade"):
		_ = nc.Close()
		return nil, fmt.Errorf("%w: missing Upgrade/Connection in response", ErrBadHandshake)
	case resp.Header.Get("Sec-WebSocket-Accept") != AcceptKey(key):
		_ = nc.Close()
		return nil, fmt.Errorf("%w: wrong Sec-WebSocket-Accept", ErrBadHandshake)
	}
	_ = nc.SetDeadline(time.Time{})
	return newConn(nc, br, false), nil
}
