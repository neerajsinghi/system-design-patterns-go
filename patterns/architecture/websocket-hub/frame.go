package websockethub

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Opcode is the 4-bit frame type from RFC 6455 section 5.2.
type Opcode byte

// Frame opcodes. Values 3-7 and 11-15 are reserved and rejected.
const (
	OpContinuation Opcode = 0x0
	OpText         Opcode = 0x1
	OpBinary       Opcode = 0x2
	OpClose        Opcode = 0x8
	OpPing         Opcode = 0x9
	OpPong         Opcode = 0xA
)

// String returns the opcode name.
func (o Opcode) String() string {
	switch o {
	case OpContinuation:
		return "continuation"
	case OpText:
		return "text"
	case OpBinary:
		return "binary"
	case OpClose:
		return "close"
	case OpPing:
		return "ping"
	case OpPong:
		return "pong"
	default:
		return fmt.Sprintf("opcode(%d)", byte(o))
	}
}

// IsControl reports whether o is a control opcode (close, ping, pong).
// Control frames may be interleaved with a fragmented message, must not be
// fragmented themselves, and carry at most 125 bytes.
func (o Opcode) IsControl() bool { return o >= 0x8 }

func (o Opcode) valid() bool {
	switch o {
	case OpContinuation, OpText, OpBinary, OpClose, OpPing, OpPong:
		return true
	}
	return false
}

// maxControlPayload is the RFC 6455 limit for close, ping and pong payloads.
const maxControlPayload = 125

// Errors returned by the framing layer.
var (
	// ErrProtocol wraps every violation of RFC 6455 framing rules. The
	// connection answers it with close code 1002.
	ErrProtocol = errors.New("websockethub: protocol error")
	// ErrUnmaskedFrame means a client sent a frame without a mask. RFC 6455
	// requires every client-to-server frame to be masked.
	ErrUnmaskedFrame = errors.New("websockethub: client frame is not masked")
	// ErrTooLarge means a frame or reassembled message exceeds the
	// configured limit. The connection answers it with close code 1009.
	ErrTooLarge = errors.New("websockethub: message too large")
)

// Frame is one WebSocket frame. Payload is always the unmasked data: ReadFrame
// unmasks it, and WriteFrame masks a copy when Masked is set.
type Frame struct {
	Fin     bool
	Opcode  Opcode
	Masked  bool
	MaskKey [4]byte
	Payload []byte
}

// maskBytes XORs b in place with the 4-byte key. Masking
// and unmasking are the same operation.
func maskBytes(key [4]byte, b []byte) {
	for i := range b {
		b[i] ^= key[i&3]
	}
}

// AppendFrame encodes f onto dst and returns the extended slice. It picks
// the shortest length form: 7-bit for 0-125, 16-bit for up to 65535,
// 64-bit beyond.
func AppendFrame(dst []byte, f Frame) []byte {
	b0 := byte(f.Opcode) & 0x0F
	if f.Fin {
		b0 |= 0x80
	}
	var b1 byte
	if f.Masked {
		b1 = 0x80
	}
	n := len(f.Payload)
	switch {
	case n <= 125:
		dst = append(dst, b0, b1|byte(n))
	case n <= 0xFFFF:
		dst = append(dst, b0, b1|126)
		dst = binary.BigEndian.AppendUint16(dst, uint16(n))
	default:
		dst = append(dst, b0, b1|127)
		dst = binary.BigEndian.AppendUint64(dst, uint64(n))
	}
	if f.Masked {
		dst = append(dst, f.MaskKey[:]...)
	}
	start := len(dst)
	dst = append(dst, f.Payload...)
	if f.Masked {
		maskBytes(f.MaskKey, dst[start:])
	}
	return dst
}

// WriteFrame encodes f and writes it with a single Write call. f.Payload is
// not modified.
func WriteFrame(w io.Writer, f Frame) error {
	_, err := w.Write(AppendFrame(make([]byte, 0, 14+len(f.Payload)), f))
	return err
}

// ReadFrame decodes one frame from r and unmasks its payload. A
// maxPayload > 0 rejects longer frames with ErrTooLarge before allocating
// memory for them. Framing violations return errors wrapping ErrProtocol:
// reserved bits set (no extension was negotiated), reserved opcodes,
// non-minimal length encodings, a 64-bit length with the top bit set, and
// control frames that are fragmented or longer than 125 bytes. Whether a
// mask is required depends on the reader's role and is checked by Conn.
func ReadFrame(r io.Reader, maxPayload int64) (Frame, error) {
	var hdr [8]byte
	if _, err := io.ReadFull(r, hdr[:2]); err != nil {
		return Frame{}, err
	}
	f := Frame{Fin: hdr[0]&0x80 != 0, Opcode: Opcode(hdr[0] & 0x0F), Masked: hdr[1]&0x80 != 0}
	if hdr[0]&0x70 != 0 {
		return Frame{}, fmt.Errorf("%w: reserved bits set without a negotiated extension", ErrProtocol)
	}
	if !f.Opcode.valid() {
		return Frame{}, fmt.Errorf("%w: reserved opcode %d", ErrProtocol, byte(f.Opcode))
	}
	n := uint64(hdr[1] & 0x7F)
	switch n {
	case 126:
		if _, err := io.ReadFull(r, hdr[:2]); err != nil {
			return Frame{}, unexpected(err)
		}
		n = uint64(binary.BigEndian.Uint16(hdr[:2]))
		if n < 126 {
			return Frame{}, fmt.Errorf("%w: non-minimal 16-bit length %d", ErrProtocol, n)
		}
	case 127:
		if _, err := io.ReadFull(r, hdr[:8]); err != nil {
			return Frame{}, unexpected(err)
		}
		n = binary.BigEndian.Uint64(hdr[:8])
		if n>>63 != 0 {
			return Frame{}, fmt.Errorf("%w: 64-bit length has the most significant bit set", ErrProtocol)
		}
		if n <= 0xFFFF {
			return Frame{}, fmt.Errorf("%w: non-minimal 64-bit length %d", ErrProtocol, n)
		}
	}
	if f.Opcode.IsControl() {
		if !f.Fin {
			return Frame{}, fmt.Errorf("%w: fragmented %s frame", ErrProtocol, f.Opcode)
		}
		if n > maxControlPayload {
			return Frame{}, fmt.Errorf("%w: %s payload of %d bytes exceeds 125", ErrProtocol, f.Opcode, n)
		}
	}
	if maxPayload > 0 && n > uint64(maxPayload) {
		return Frame{}, fmt.Errorf("%w: frame of %d bytes exceeds limit %d", ErrTooLarge, n, maxPayload)
	}
	if f.Masked {
		if _, err := io.ReadFull(r, f.MaskKey[:]); err != nil {
			return Frame{}, unexpected(err)
		}
	}
	f.Payload = make([]byte, n)
	if _, err := io.ReadFull(r, f.Payload); err != nil {
		return Frame{}, unexpected(err)
	}
	if f.Masked {
		maskBytes(f.MaskKey, f.Payload)
	}
	return f, nil
}

// unexpected turns a clean EOF in the middle of a frame into
// io.ErrUnexpectedEOF: only an EOF before the first header byte is clean.
func unexpected(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}
