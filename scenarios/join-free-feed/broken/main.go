// Command broken runs the broken version of the join-free-feed scenario.
package main

import (
	"os"

	joinfreefeed "github.com/neerajsinghi/system-design-patterns-go/scenarios/join-free-feed"
)

func main() { joinfreefeed.Broken().Print(os.Stdout) }
