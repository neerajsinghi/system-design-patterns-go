// Command fixed runs the fixed version of the cache-stampede scenario.
package main

import (
	"os"

	cachestampede "github.com/neerajsinghi/system-design-patterns-go/scenarios/cache-stampede"
)

func main() { cachestampede.Fixed().Print(os.Stdout) }
