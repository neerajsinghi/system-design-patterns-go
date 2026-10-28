// Command fixed runs the fixed version of the charged-but-no-order scenario.
package main

import (
	"os"

	chargednoorder "github.com/neerajsinghi/system-design-patterns-go/scenarios/charged-but-no-order"
)

func main() { chargednoorder.Fixed().Print(os.Stdout) }
