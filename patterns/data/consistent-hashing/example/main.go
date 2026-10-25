// Command example compares key movement under hash(key) % N with a
// consistent-hash ring when a cache cluster grows from 4 to 5 nodes.
package main

import (
	"fmt"
	"hash/fnv"
	"log"
	"slices"

	"github.com/neerajsinghi/system-design-patterns-go/patterns/data/consistent-hashing"
)

func modN(key string, n int) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return fmt.Sprintf("cache-%d", h.Sum32()%uint32(n)+1)
}

func main() {
	const total = 100_000
	ks := make([]string, total)
	for i := range ks {
		ks[i] = fmt.Sprintf("user:%d", i)
	}

	fmt.Println("Cluster grows from 4 to 5 cache nodes; 100,000 cached keys.")

	// Naive modulo placement.
	movedMod := 0
	for _, k := range ks {
		if modN(k, 4) != modN(k, 5) {
			movedMod++
		}
	}
	fmt.Printf("\nhash(key) %% N:   %6d keys move (%.1f%%), so most of the cache goes cold\n",
		movedMod, 100*float64(movedMod)/total)

	// Consistent hashing.
	ring, err := consistenthashing.New(200)
	if err != nil {
		log.Fatal(err)
	}
	for i := 1; i <= 4; i++ {
		ring.Add(fmt.Sprintf("cache-%d", i))
	}
	before := ring.Assign(ks)
	ring.Add("cache-5")
	after := ring.Assign(ks)
	moved := consistenthashing.Moved(before, after)
	fmt.Printf("consistent hash: %6d keys move (%.1f%%), ideal is 1/5 = 20%%\n", moved, 100*float64(moved)/total)

	toNew := 0
	for k := range before {
		if before[k] != after[k] && after[k] == "cache-5" {
			toNew++
		}
	}
	fmt.Printf("                 %6d of those moved to the new node (all of them)\n", toNew)

	fmt.Println("\nLoad per node with 200 virtual nodes each:")
	counts := map[string]int{}
	for _, n := range after {
		counts[n]++
	}
	for _, n := range ring.Nodes() {
		fmt.Printf("  %s  %6d keys  %5.1f%%\n", n, counts[n], 100*float64(counts[n])/total)
	}

	fmt.Println("\nVirtual nodes even out the load (5 nodes, max deviation from 20%):")
	for _, v := range []int{1, 10, 100, 1000} {
		r, _ := consistenthashing.New(v)
		for i := 1; i <= 5; i++ {
			r.Add(fmt.Sprintf("cache-%d", i))
		}
		c := map[string]int{}
		for _, n := range r.Assign(ks) {
			c[n]++
		}
		shares := make([]float64, 0, 5)
		for _, n := range r.Nodes() {
			shares = append(shares, 100*float64(c[n])/total)
		}
		lo, hi := slices.Min(shares), slices.Max(shares)
		fmt.Printf("  vnodes=%-5d  min %5.1f%%  max %5.1f%%\n", v, lo, hi)
	}

	fmt.Println("\nReplica placement for user:42 (owner + next 2 distinct nodes clockwise):")
	fmt.Printf("  %v\n", ring.GetN("user:42", 3))
}
