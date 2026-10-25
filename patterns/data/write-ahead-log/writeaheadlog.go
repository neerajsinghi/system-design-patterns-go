// Package writeaheadlog implements a write-ahead log: an append-only file of
// checksummed records that is forced to stable storage (fsync) before a
// change is acknowledged, then replayed on startup to rebuild state after a
// crash.
//
// Record layout (little-endian):
//
//	+---------+---------+---------+-----------------+
//	| len u32 | crc u32 | seq u64 | payload [len]   |
//	+---------+---------+---------+-----------------+
//
// crc is CRC-32C (Castagnoli) over seq+payload. On recovery, records are
// read until the first short, corrupt or out-of-sequence one. That is the
// torn tail of a write that was in progress when the process died, and the
// log is truncated there so new appends start from a clean boundary.
package writeaheadlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"
)

const (
	headerSize = 16
	// MaxRecordSize bounds a payload so a corrupt length can't make
	// recovery allocate gigabytes.
	MaxRecordSize = 16 << 20
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ErrTooLarge is returned for payloads above MaxRecordSize.
var ErrTooLarge = errors.New("writeaheadlog: record too large")

// Storage is the file the log lives in. Write appends to an OS buffer (not
// yet durable). Sync is the fsync hook: it makes everything written so far
// durable.
type Storage interface {
	Write(p []byte) error
	Sync() error
	ReadAll() ([]byte, error)
	Truncate(size int64) error
}

// Record is one log entry.
type Record struct {
	Seq     uint64
	Payload []byte
}

// Encode appends the wire form of r to dst.
func Encode(dst []byte, r Record) []byte {
	var h [headerSize]byte
	binary.LittleEndian.PutUint32(h[0:4], uint32(len(r.Payload)))
	binary.LittleEndian.PutUint64(h[8:16], r.Seq)
	crc := crc32.Update(0, castagnoli, h[8:16])
	crc = crc32.Update(crc, castagnoli, r.Payload)
	binary.LittleEndian.PutUint32(h[4:8], crc)
	dst = append(dst, h[:]...)
	return append(dst, r.Payload...)
}

// Decode parses records from data, stopping at the first invalid one. It
// returns the valid records and the byte length of the valid prefix.
// Sequence numbers must increase by exactly 1 from the first record.
func Decode(data []byte) (recs []Record, valid int64) {
	off := 0
	for len(data)-off >= headerSize {
		h := data[off : off+headerSize]
		n := int(binary.LittleEndian.Uint32(h[0:4]))
		if n > MaxRecordSize || len(data)-off-headerSize < n {
			break // torn write or garbage length
		}
		seq := binary.LittleEndian.Uint64(h[8:16])
		payload := data[off+headerSize : off+headerSize+n]
		crc := crc32.Update(0, castagnoli, h[8:16])
		crc = crc32.Update(crc, castagnoli, payload)
		if crc != binary.LittleEndian.Uint32(h[4:8]) {
			break // bit rot or partial overwrite
		}
		if len(recs) > 0 && seq != recs[len(recs)-1].Seq+1 {
			break
		}
		recs = append(recs, Record{Seq: seq, Payload: append([]byte(nil), payload...)})
		off += headerSize + n
	}
	return recs, int64(off)
}

// Recovery describes what Open found.
type Recovery struct {
	Records        int   // valid records replayed
	TruncatedBytes int64 // torn or corrupt tail bytes discarded
	LastSeq        uint64
}

// Log is a concurrency-safe write-ahead log.
type Log struct {
	mu        sync.Mutex
	st        Storage
	nextSeq   uint64
	syncEvery int
	unsynced  int
	durable   uint64 // highest seq known to be synced
	onSync    func(durableSeq uint64)
	buf       []byte
}

// Option configures a Log.
type Option func(*Log)

// WithSyncEvery syncs automatically after every n appends. 1 (the default)
// means every Append is durable when it returns. 0 means the caller decides
// when to Sync (group commit).
func WithSyncEvery(n int) Option {
	return func(l *Log) { l.syncEvery = max(n, 0) }
}

// WithOnSync registers a hook called after each successful Sync with the
// highest durable sequence number, for example to acknowledge waiting clients.
func WithOnSync(fn func(durableSeq uint64)) Option {
	return func(l *Log) { l.onSync = fn }
}

// Open recovers the log in st. It calls apply for every valid record in
// order, truncates any torn tail, and returns a Log ready for appends.
func Open(st Storage, apply func(Record) error, opts ...Option) (*Log, Recovery, error) {
	if st == nil {
		return nil, Recovery{}, errors.New("writeaheadlog: storage must not be nil")
	}
	data, err := st.ReadAll()
	if err != nil {
		return nil, Recovery{}, fmt.Errorf("writeaheadlog: read: %w", err)
	}
	recs, valid := Decode(data)
	rec := Recovery{Records: len(recs), TruncatedBytes: int64(len(data)) - valid}
	if rec.TruncatedBytes > 0 {
		if err := st.Truncate(valid); err != nil {
			return nil, rec, fmt.Errorf("writeaheadlog: truncate torn tail: %w", err)
		}
		if err := st.Sync(); err != nil {
			return nil, rec, fmt.Errorf("writeaheadlog: sync after truncate: %w", err)
		}
	}
	for _, r := range recs {
		if apply != nil {
			if err := apply(r); err != nil {
				return nil, rec, fmt.Errorf("writeaheadlog: apply seq %d: %w", r.Seq, err)
			}
		}
		rec.LastSeq = r.Seq
	}
	l := &Log{st: st, nextSeq: rec.LastSeq + 1, durable: rec.LastSeq, syncEvery: 1}
	for _, o := range opts {
		o(l)
	}
	return l, rec, nil
}

// Append writes payload as the next record and returns its sequence number.
// With the default WithSyncEvery(1), the record is durable when Append returns.
func (l *Log) Append(payload []byte) (uint64, error) {
	if len(payload) > MaxRecordSize {
		return 0, ErrTooLarge
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	seq := l.nextSeq
	l.buf = Encode(l.buf[:0], Record{Seq: seq, Payload: payload})
	if err := l.st.Write(l.buf); err != nil {
		return 0, fmt.Errorf("writeaheadlog: write: %w", err)
	}
	l.nextSeq++
	l.unsynced++
	if l.syncEvery > 0 && l.unsynced >= l.syncEvery {
		if err := l.syncLocked(); err != nil {
			return seq, err
		}
	}
	return seq, nil
}

// Sync forces every appended record to stable storage.
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.syncLocked()
}

func (l *Log) syncLocked() error {
	if l.unsynced == 0 {
		return nil
	}
	if err := l.st.Sync(); err != nil {
		// After a failed fsync the kernel may have dropped the dirty pages, so
		// the only safe response in production is to crash and recover.
		return fmt.Errorf("writeaheadlog: sync: %w", err)
	}
	l.unsynced = 0
	l.durable = l.nextSeq - 1
	if l.onSync != nil {
		l.onSync(l.durable)
	}
	return nil
}

// DurableSeq returns the highest sequence number known to be on stable storage.
func (l *Log) DurableSeq() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.durable
}
