// Command broken runs the broken version of the surge-in-the-rain scenario.
package main

import (
	"os"

	surgerain "github.com/neerajsinghi/system-design-patterns-go/scenarios/surge-in-the-rain"
)

func main() {
	r := surgerain.Broken()
	r.Print(os.Stdout)
}
