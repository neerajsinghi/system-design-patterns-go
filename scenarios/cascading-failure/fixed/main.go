// Command fixed runs the fixed version of the cascading-failure scenario.
package main

import (
	"os"

	cascadingfailure "github.com/neerajsinghi/system-design-patterns-go/scenarios/cascading-failure"
)

func main() { cascadingfailure.Fixed().Print(os.Stdout) }
