// Command broken runs the broken version of the slow-vector-search scenario.
package main

import (
	"os"

	slowvectorsearch "github.com/neerajsinghi/system-design-patterns-go/scenarios/slow-vector-search"
)

func main() { slowvectorsearch.Broken().Print(os.Stdout) }
