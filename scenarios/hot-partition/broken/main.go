// Command broken runs the broken version of the hot-partition scenario.
package main

import (
	"os"

	hotpartition "github.com/neerajsinghi/system-design-patterns-go/scenarios/hot-partition"
)

func main() { hotpartition.Broken().Print(os.Stdout) }
