// Command broken runs the broken version of the gateway-bottleneck scenario.
package main

import (
	"os"

	gatewaybottleneck "github.com/neerajsinghi/system-design-patterns-go/scenarios/gateway-bottleneck"
)

func main() { gatewaybottleneck.Broken().Print(os.Stdout) }
