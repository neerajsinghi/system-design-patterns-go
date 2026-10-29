// Command fixed runs the fixed version of the join-free-feed scenario.
package main

import (
	"os"

	joinfreefeed "github.com/neerajsinghi/system-design-patterns-go/scenarios/join-free-feed"
)

func main() { joinfreefeed.Fixed().Print(os.Stdout) }
