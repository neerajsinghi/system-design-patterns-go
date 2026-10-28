// Command fixed runs the fixed version of the consumer-double-processing scenario.
package main

import (
	"os"

	consumerdouble "github.com/neerajsinghi/system-design-patterns-go/scenarios/consumer-double-processing"
)

func main() { consumerdouble.Fixed().Print(os.Stdout) }
