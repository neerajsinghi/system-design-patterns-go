// Command fixed runs the fixed version of the gateway-bottleneck scenario.
package main

import (
	"os"

	gatewaybottleneck "github.com/neerajsinghi/system-design-patterns-go/scenarios/gateway-bottleneck"
)

func main() { gatewaybottleneck.Fixed().Print(os.Stdout) }
