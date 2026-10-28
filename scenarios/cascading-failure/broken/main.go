// Command broken runs the broken version of the cascading-failure scenario.
package main

import (
	"os"

	cascadingfailure "github.com/neerajsinghi/system-design-patterns-go/scenarios/cascading-failure"
)

func main() { cascadingfailure.Broken().Print(os.Stdout) }
