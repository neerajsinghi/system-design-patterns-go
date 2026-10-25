// Command example puts a Bloom filter in front of a slow "username taken?"
// lookup, then shows the size / accuracy trade-off and what overfilling does.
package main

import (
	"fmt"
	"log"

	"github.com/neerajsinghi/system-design-patterns-go/patterns/data/bloom-filter"
)

func main() {
	const users = 100_000
	f, err := bloomfilter.New(users, 0.01)
	if err != nil {
		log.Fatal(err)
	}
	taken := make(map[string]bool, users) // stands in for the database
	for i := range users {
		name := fmt.Sprintf("user%d", i)
		taken[name] = true
		f.AddString(name)
	}
	fmt.Printf("Filter for %d usernames at 1%% FPR: %d bits (%.1f KiB), %d hash functions\n",
		users, f.M(), float64(f.M())/8/1024, f.K())
	fmt.Printf("A map[string]bool of the same names costs several MiB.\n\n")

	dbLookups, fps := 0, 0
	const signups = 50_000
	for i := range signups {
		name := fmt.Sprintf("newbie%d", i) // none of these exist
		if !f.TestString(name) {
			continue // definitely free: no database query needed
		}
		dbLookups++
		if !taken[name] {
			fps++
		}
	}
	fmt.Printf("%d sign-up checks for new names:\n", signups)
	fmt.Printf("  answered by the filter alone: %d\n", signups-dbLookups)
	fmt.Printf("  sent to the database:         %d (false positives: %d = %.2f%%)\n\n",
		dbLookups, fps, 100*float64(fps)/signups)

	fmt.Println("Tuning: memory per item vs false-positive rate (n = 100,000)")
	for _, p := range []float64{0.1, 0.01, 0.001, 0.0001} {
		m, k := bloomfilter.OptimalParams(users, p)
		fmt.Printf("  p=%-7g  %5.1f bits/item  k=%-2d  %6.1f KiB\n", p, float64(m)/users, k, float64(m)/8/1024)
	}

	fmt.Println("\nOverfilling a filter sized for 10,000 items at 1%:")
	small, _ := bloomfilter.New(10_000, 0.01)
	for i := range 40_000 {
		small.AddString(fmt.Sprint(i))
		if n := i + 1; n == 10_000 || n == 20_000 || n == 40_000 {
			fmt.Printf("  after %6d items: estimated FPR %.1f%%\n", n, 100*small.EstimatedFPR())
		}
	}
}
