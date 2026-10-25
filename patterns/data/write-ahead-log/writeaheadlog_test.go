package writeaheadlog

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func openT(t *testing.T, st Storage, opts ...Option) (*Log, []Record, Recovery) {
	t.Helper()
	var got []Record
	l, rec, err := Open(st, func(r Record) error { got = append(got, r); return nil }, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return l, got, rec
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	var buf []byte
	want := []Record{{1, []byte("a")}, {2, nil}, {3, bytes.Repeat([]byte("x"), 1000)}}
	for _, r := range want {
		buf = Encode(buf, r)
	}
	got, valid := Decode(buf)
	if valid != int64(len(buf)) || len(got) != len(want) {
		t.Fatalf("decoded %d records, valid=%d of %d", len(got), valid, len(buf))
	}
	for i := range want {
		if got[i].Seq != want[i].Seq || !bytes.Equal(got[i].Payload, want[i].Payload) {
			t.Fatalf("record %d mismatch", i)
		}
	}
}

func TestDecodeStopsAtBadData(t *testing.T) {
	good := Encode(Encode(nil, Record{1, []byte("one")}), Record{2, []byte("two")})
	first := int64(headerSize + 3)
	tests := []struct {
		name      string
		data      func() []byte
		wantRecs  int
		wantValid int64
	}{
		{"empty", func() []byte { return nil }, 0, 0},
		{"partial header", func() []byte { return good[:first+5] }, 1, first},
		{"partial payload", func() []byte { return good[:len(good)-1] }, 1, first},
		{"flipped payload byte", func() []byte {
			b := bytes.Clone(good)
			b[len(b)-1] ^= 1
			return b
		}, 1, first},
		{"huge length", func() []byte {
			b := bytes.Clone(good)
			b[first+3] = 0x7F // length field of record 2 becomes enormous
			return b
		}, 1, first},
		{"sequence gap", func() []byte {
			return Encode(Encode(nil, Record{1, []byte("one")}), Record{3, []byte("two")})
		}, 1, first},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recs, valid := Decode(tt.data())
			if len(recs) != tt.wantRecs || valid != tt.wantValid {
				t.Fatalf("got %d recs valid=%d, want %d valid=%d", len(recs), valid, tt.wantRecs, tt.wantValid)
			}
		})
	}
}

func TestOpenValidation(t *testing.T) {
	if _, _, err := Open(nil, nil); err == nil {
		t.Fatal("nil storage: expected error")
	}
	l, _, _ := openT(t, NewMemStorage())
	if _, err := l.Append(make([]byte, MaxRecordSize+1)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("want ErrTooLarge, got %v", err)
	}
}

func TestCrashRecovery(t *testing.T) {
	tests := []struct {
		name      string
		syncEvery int
		torn      int // bytes of the unsynced tail that reach disk
		appends   int
		wantRecs  int
	}{
		{"sync every append, clean crash", 1, 0, 5, 5},
		{"group commit: unsynced appends lost", 0, 0, 5, 3},
		{"group commit: torn record discarded", 0, headerSize + 2, 5, 3},
		{"group commit: one full + one torn record", 0, headerSize + 6 + headerSize/2, 5, 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := NewMemStorage()
			l, _, _ := openT(t, st, WithSyncEvery(tt.syncEvery))
			for i := range tt.appends {
				if _, err := l.Append([]byte(fmt.Sprintf("rec-%d", i))); err != nil {
					t.Fatal(err)
				}
				if tt.syncEvery == 0 && i == 2 {
					_ = l.Sync() // first 3 records made durable together
				}
			}
			st.Crash(tt.torn)

			l2, got, rec := openT(t, st)
			if len(got) != tt.wantRecs || rec.Records != tt.wantRecs {
				t.Fatalf("recovered %d records, want %d", len(got), tt.wantRecs)
			}
			for i, r := range got {
				if r.Seq != uint64(i+1) || string(r.Payload) != fmt.Sprintf("rec-%d", i) {
					t.Fatalf("record %d = %d %q", i, r.Seq, r.Payload)
				}
			}
			if tt.torn > 0 && rec.TruncatedBytes == 0 {
				t.Fatal("expected a torn tail to be truncated")
			}
			// Appending after recovery continues the sequence cleanly.
			seq, err := l2.Append([]byte("after"))
			if err != nil || seq != uint64(tt.wantRecs+1) {
				t.Fatalf("next seq %d err %v", seq, err)
			}
			_, again, _ := openT(t, st)
			if len(again) != tt.wantRecs+1 {
				t.Fatalf("after re-append: %d records", len(again))
			}
		})
	}
}

func TestBitRotTruncatesFromCorruption(t *testing.T) {
	st := NewMemStorage()
	l, _, _ := openT(t, st)
	for i := range 4 {
		_, _ = l.Append([]byte{byte(i)})
	}
	st.FlipByte(2*(headerSize+1) + headerSize) // payload of record 3
	_, got, rec := openT(t, st)
	if len(got) != 2 || rec.TruncatedBytes != 2*(headerSize+1) {
		t.Fatalf("got %d records, truncated %d", len(got), rec.TruncatedBytes)
	}
}

func TestOnSyncHookAndDurableSeq(t *testing.T) {
	st := NewMemStorage()
	var hooked []uint64
	l, _, _ := openT(t, st, WithSyncEvery(3), WithOnSync(func(s uint64) { hooked = append(hooked, s) }))
	for range 7 {
		_, _ = l.Append([]byte("x"))
	}
	if l.DurableSeq() != 6 || st.Syncs() != 2 {
		t.Fatalf("durable=%d syncs=%d", l.DurableSeq(), st.Syncs())
	}
	_ = l.Sync()
	_ = l.Sync() // nothing new: no extra fsync
	if fmt.Sprint(hooked) != "[3 6 7]" || st.Syncs() != 3 {
		t.Fatalf("hook calls %v, syncs %d", hooked, st.Syncs())
	}
}

func TestSyncFailureIsReported(t *testing.T) {
	st := NewMemStorage()
	l, _, _ := openT(t, st)
	st.SetFailSync(true)
	if _, err := l.Append([]byte("x")); !errors.Is(err, ErrSyncFailed) {
		t.Fatalf("want ErrSyncFailed, got %v", err)
	}
	if l.DurableSeq() != 0 {
		t.Fatal("durable seq must not advance on failed sync")
	}
}

func TestApplyErrorAbortsOpen(t *testing.T) {
	st := NewMemStorage()
	l, _, _ := openT(t, st)
	_, _ = l.Append([]byte("x"))
	boom := errors.New("boom")
	if _, _, err := Open(st, func(Record) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
}

func TestConcurrentAppends(t *testing.T) {
	st := NewMemStorage()
	l, _, _ := openT(t, st, WithSyncEvery(10))
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 100 {
				if _, err := l.Append([]byte(fmt.Sprintf("%d-%d", w, i))); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	_ = l.Sync()
	_, got, _ := openT(t, st)
	if len(got) != 800 {
		t.Fatalf("recovered %d, want 800", len(got))
	}
}
