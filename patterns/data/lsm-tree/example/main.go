// Command example traces an LSM tree: writes filling the memtable, flushes
// creating immutable segments, tombstones, read amplification, and
// compaction cleaning it all up.
package main

import (
	"fmt"
	"log"

	"github.com/neerajsinghi/system-design-patterns-go/patterns/data/lsm-tree"
)

func show(t *lsmtree.Tree, label string) {
	s := t.Stats()
	fmt.Printf("  %-34s memtable=%d segments=%d segment entries=%d\n", label, s.MemtableKeys, s.Segments, s.SegmentEntries)
}

func main() {
	tree, err := lsmtree.New(lsmtree.Options{MemtableLimit: 4}) // tiny, to see flushes
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Writes (memtable flushes every 4 keys):")
	tree.Put("apple", "red")
	tree.Put("banana", "yellow")
	tree.Put("cherry", "red")
	show(tree, "3 puts")
	tree.Put("date", "brown")
	show(tree, "4th put triggers a flush")
	tree.Put("apple", "green") // newer version, older one still in segment 1
	tree.Delete("banana")      // tombstone
	tree.Put("elder", "purple")
	tree.Put("fig", "purple")
	show(tree, "overwrite + delete + 2 puts")
	tree.Put("grape", "green")
	tree.Put("apple", "golden")
	tree.Flush()
	show(tree, "2 more puts + manual flush")

	fmt.Println("\nReads walk memtable, then segments newest to oldest:")
	for _, k := range []string{"apple", "banana", "cherry", "zucchini"} {
		before := tree.Stats().SegmentProbes
		v, ok := tree.Get(k)
		probes := tree.Stats().SegmentProbes - before
		if ok {
			fmt.Printf("  Get %-10q -> %-7s probed %d segment(s)\n", k, v, probes)
		} else {
			fmt.Printf("  Get %-10q -> <none>  probed %d segment(s)\n", k, probes)
		}
	}
	fmt.Println("  (banana: a tombstone in a newer segment hides the older value)")

	fmt.Printf("\nScan(a..e): %v\n", tree.Scan("a", "e"))

	fmt.Println("\nCompaction merges all segments, keeps the newest versions, drops tombstones:")
	tree.Compact()
	show(tree, "after compaction")
	before := tree.Stats().SegmentProbes
	v, _ := tree.Get("cherry")
	fmt.Printf("  Get(cherry) = %s, probed %d segment(s)\n", v, tree.Stats().SegmentProbes-before)
}
