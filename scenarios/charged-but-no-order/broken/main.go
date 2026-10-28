// Command broken runs the broken version of the charged-but-no-order scenario.
package main

import (
	"os"

	chargednoorder "github.com/neerajsinghi/system-design-patterns-go/scenarios/charged-but-no-order"
)

func main() { chargednoorder.Broken().Print(os.Stdout) }
