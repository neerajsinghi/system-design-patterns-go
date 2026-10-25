package writeaheadlog

import (
	"errors"
	"sync"
)

// MemStorage simulates a file with an OS page cache: Write lands in a
// volatile buffer, Sync moves it to durable bytes, and Crash throws the
// volatile part away. That is exactly what a power cut does to un-fsynced data.
type MemStorage struct {
	mu       sync.Mutex
	durable  []byte
	volatile []byte
	syncs    int
	failSync bool
}

// NewMemStorage returns an empty simulated file.
func NewMemStorage() *MemStorage { return &MemStorage{} }

// ErrSyncFailed is returned by Sync when FailSync is set.
var ErrSyncFailed = errors.New("memstorage: fsync failed")

// Write implements Storage.
func (m *MemStorage) Write(p []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.volatile = append(m.volatile, p...)
	return nil
}

// Sync implements Storage.
func (m *MemStorage) Sync() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failSync {
		return ErrSyncFailed
	}
	m.durable = append(m.durable, m.volatile...)
	m.volatile = nil
	m.syncs++
	return nil
}

// ReadAll implements Storage. A running process sees its own unsynced writes.
func (m *MemStorage) ReadAll() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := append([]byte(nil), m.durable...)
	return append(out, m.volatile...), nil
}

// Truncate implements Storage. Truncation is applied to the durable bytes
// and discards anything volatile.
func (m *MemStorage) Truncate(size int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	all := append(m.durable, m.volatile...)
	if size < 0 || size > int64(len(all)) {
		return errors.New("memstorage: bad truncate size")
	}
	m.durable, m.volatile = all[:size:size], nil
	return nil
}

// Crash simulates power loss. Unsynced bytes are lost, except that the first
// tornBytes of them survive. That models a write that was half on disk when
// the power went.
func (m *MemStorage) Crash(tornBytes int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tornBytes = min(max(tornBytes, 0), len(m.volatile))
	m.durable = append(m.durable, m.volatile[:tornBytes]...)
	m.volatile = nil
}

// FlipByte corrupts the durable byte at off (bit rot). Out-of-range offsets are ignored.
func (m *MemStorage) FlipByte(off int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if off >= 0 && off < len(m.durable) {
		m.durable[off] ^= 0xFF
	}
}

// SetFailSync makes subsequent Syncs fail (or succeed again).
func (m *MemStorage) SetFailSync(fail bool) { m.mu.Lock(); m.failSync = fail; m.mu.Unlock() }

// Syncs returns how many successful Syncs happened.
func (m *MemStorage) Syncs() int { m.mu.Lock(); defer m.mu.Unlock(); return m.syncs }

// Size returns the durable plus volatile size.
func (m *MemStorage) Size() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.durable) + len(m.volatile)
}
