// Command broken runs the broken version of the otp-retry-storm scenario.
package main

import (
	"os"

	otpretrystorm "github.com/neerajsinghi/system-design-patterns-go/scenarios/otp-retry-storm"
)

func main() { otpretrystorm.Broken().Print(os.Stdout) }
