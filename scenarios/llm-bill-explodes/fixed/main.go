// Command fixed runs the fixed version of the llm-bill-explodes scenario.
package main

import (
	"os"

	llmbill "github.com/neerajsinghi/system-design-patterns-go/scenarios/llm-bill-explodes"
)

func main() { llmbill.Fixed().Print(os.Stdout) }
