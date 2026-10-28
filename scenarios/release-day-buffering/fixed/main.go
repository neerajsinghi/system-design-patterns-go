// Command fixed runs the fixed version of the release-day-buffering scenario.
package main

import (
	"os"

	releaseday "github.com/neerajsinghi/system-design-patterns-go/scenarios/release-day-buffering"
)

func main() { releaseday.Fixed().Print(os.Stdout) }
