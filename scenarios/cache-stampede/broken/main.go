// Command broken runs the broken version of the cache-stampede scenario.
package main

import (
	"os"

	cachestampede "github.com/neerajsinghi/system-design-patterns-go/scenarios/cache-stampede"
)

func main() { cachestampede.Broken().Print(os.Stdout) }
