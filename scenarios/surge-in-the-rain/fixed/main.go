// Command fixed runs the fixed version of the surge-in-the-rain scenario.
package main

import (
	"os"

	surgerain "github.com/neerajsinghi/system-design-patterns-go/scenarios/surge-in-the-rain"
)

func main() {
	r := surgerain.Fixed()
	r.Print(os.Stdout)
}
