// Command fixed runs the fixed version of the slow-vector-search scenario.
package main

import (
	"os"

	slowvectorsearch "github.com/neerajsinghi/system-design-patterns-go/scenarios/slow-vector-search"
)

func main() { slowvectorsearch.Fixed().Print(os.Stdout) }
