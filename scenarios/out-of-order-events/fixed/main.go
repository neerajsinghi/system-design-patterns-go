// Command fixed runs the fixed version of the out-of-order-events scenario.
package main

import (
	"os"

	outoforder "github.com/neerajsinghi/system-design-patterns-go/scenarios/out-of-order-events"
)

func main() { outoforder.Fixed().Print(os.Stdout) }
