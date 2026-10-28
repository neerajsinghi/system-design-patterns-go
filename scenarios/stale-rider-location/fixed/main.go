// Command fixed runs the fixed version of the stale-rider-location scenario.
package main

import (
	"os"

	staleriderlocation "github.com/neerajsinghi/system-design-patterns-go/scenarios/stale-rider-location"
)

func main() { staleriderlocation.Fixed().Print(os.Stdout) }
