// Command broken runs the broken version of the consumer-double-processing scenario.
package main

import (
	"os"

	consumerdouble "github.com/neerajsinghi/system-design-patterns-go/scenarios/consumer-double-processing"
)

func main() { consumerdouble.Broken().Print(os.Stdout) }
