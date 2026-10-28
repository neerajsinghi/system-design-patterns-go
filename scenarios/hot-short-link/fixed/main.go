// Command fixed runs the fixed version of the hot-short-link scenario.
package main

import (
	"os"

	hotshortlink "github.com/neerajsinghi/system-design-patterns-go/scenarios/hot-short-link"
)

func main() { hotshortlink.Fixed().Print(os.Stdout) }
