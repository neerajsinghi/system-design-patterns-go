// Command example contrasts hash and range sharding on the same data:
// load balance with sequential keys, range-scan fan-out, and the cost of
// adding a shard.
package main

import (
	"fmt"
	"log"

	"github.com/neerajsinghi/system-design-patterns-go/patterns/data/sharding"
)

func load(c *sharding.Cluster, n int) {
	for i := range n {
		c.Put(fmt.Sprintf("order:%06d", i), "...")
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		log.Fatal(err)
	}
	return v
}

func main() {
	const n = 40_000
	hashR := must(sharding.NewHashRouter(4))
	rangeR := must(sharding.NewRangeRouter("order:010000", "order:020000", "order:030000"))

	hashC := must(sharding.NewCluster(hashR))
	rangeC := must(sharding.NewCluster(rangeR))
	load(hashC, n)
	load(rangeC, n)

	fmt.Println("40,000 orders with sequential IDs on 4 shards")
	fmt.Printf("  hash  sharding, keys per shard: %v\n", hashC.Counts())
	fmt.Printf("  range sharding, keys per shard: %v\n", rangeC.Counts())

	fmt.Println("\nScan order:012000 .. order:012100 (100 keys)")
	k, q := hashC.Scan("order:012000", "order:012100")
	fmt.Printf("  hash : %d keys, queried %d shards (scatter-gather)\n", len(k), q)
	k, q = rangeC.Scan("order:012000", "order:012100")
	fmt.Printf("  range: %d keys, queried %d shard\n", len(k), q)

	fmt.Println("\nNew orders keep arriving: order:040000 .. order:049999")
	for i := 40_000; i < 50_000; i++ {
		hashC.Put(fmt.Sprintf("order:%06d", i), "...")
		rangeC.Put(fmt.Sprintf("order:%06d", i), "...")
	}
	fmt.Printf("  hash : %v (still balanced)\n", hashC.Counts())
	fmt.Printf("  range: %v (every new write hit the last shard: a hotspot)\n", rangeC.Counts())

	fmt.Println("\nAdd a 5th shard")
	moved := must(hashC.Reshard(must(sharding.NewHashRouter(5))))
	fmt.Printf("  hash  4 -> 5 (mod N): moved %5d of 50,000 keys (%.0f%%)\n", moved, 100*float64(moved)/50_000)
	moved = must(rangeC.Reshard(must(rangeR.Split("order:040000"))))
	fmt.Printf("  range split hot shard at order:040000: moved %5d keys (%.0f%%)\n", moved, 100*float64(moved)/50_000)
	fmt.Printf("  range shards now: %v\n", rangeC.Counts())
}
