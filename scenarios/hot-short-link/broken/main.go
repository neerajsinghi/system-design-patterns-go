// Command broken runs the broken version of the hot-short-link scenario.
package main

import (
	"os"

	hotshortlink "github.com/neerajsinghi/system-design-patterns-go/scenarios/hot-short-link"
)

func main() { hotshortlink.Broken().Print(os.Stdout) }
