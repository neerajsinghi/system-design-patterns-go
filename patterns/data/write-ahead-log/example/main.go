// Command example builds a tiny key-value store on a write-ahead log, pulls
// the power in the middle of a group commit, and recovers.
package main

import (
	"fmt"
	"log"
	"slices"
	"strings"

	"github.com/neerajsinghi/system-design-patterns-go/patterns/data/write-ahead-log"
)

// kv is the in-memory state rebuilt from the log. Payloads are "SET k v" or "DEL k".
type kv map[string]string

func (m kv) apply(r writeaheadlog.Record) error {
	parts := strings.SplitN(string(r.Payload), " ", 3)
	switch {
	case parts[0] == "SET" && len(parts) == 3:
		m[parts[1]] = parts[2]
	case parts[0] == "DEL" && len(parts) == 2:
		delete(m, parts[1])
	default:
		return fmt.Errorf("bad record %q", r.Payload)
	}
	return nil
}

func (m kv) String() string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%s=%s ", k, m[k])
	}
	return "{ " + b.String() + "}"
}

func main() {
	disk := writeaheadlog.NewMemStorage()

	// --- first process lifetime ---
	state := kv{}
	wal, _, err := writeaheadlog.Open(disk, state.apply,
		writeaheadlog.WithSyncEvery(0), // group commit: we call Sync ourselves
		writeaheadlog.WithOnSync(func(seq uint64) { fmt.Printf("  fsync: records <= %d are durable, ack clients\n", seq) }),
	)
	if err != nil {
		log.Fatal(err)
	}
	write := func(cmd string) {
		// Write-ahead rule: log first, then apply to in-memory state.
		if _, err := wal.Append([]byte(cmd)); err != nil {
			log.Fatal(err)
		}
		_ = state.apply(writeaheadlog.Record{Payload: []byte(cmd)})
		fmt.Printf("  append %-22q state %v\n", cmd, state)
	}

	fmt.Println("Batch 1:")
	write("SET balance:ada 100")
	write("SET balance:bob 50")
	write("DEL session:old")
	_ = wal.Sync()

	fmt.Println("Batch 2 (not yet fsynced):")
	write("SET balance:ada 70")
	write("SET balance:bob 80")

	fmt.Printf("\nPower cut! %d bytes written, but only batch 1 was fsynced; 25 bytes of batch 2 were half-written.\n", disk.Size())
	disk.Crash(25) // the first 25 unsynced bytes made it to disk: a torn record

	// --- second process lifetime ---
	recovered := kv{}
	_, rec, err := writeaheadlog.Open(disk, recovered.apply)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nRecovery replayed %d records (last seq %d), truncated %d torn bytes\n",
		rec.Records, rec.LastSeq, rec.TruncatedBytes)
	fmt.Printf("Recovered state: %v\n", recovered)
	fmt.Println("Batch 2 was never acknowledged, so losing it breaks no promise to clients.")

	disk.FlipByte(20)
	fmt.Println("\nNow a byte inside record 1 gets corrupted on disk (bit rot)...")
	rotted := kv{}
	_, rec, _ = writeaheadlog.Open(disk, rotted.apply)
	fmt.Printf("CRC check stops replay at record 1: replayed %d, truncated %d bytes, state %v\n",
		rec.Records, rec.TruncatedBytes, rotted)
}
