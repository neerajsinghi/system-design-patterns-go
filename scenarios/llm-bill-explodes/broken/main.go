// Command broken runs the broken version of the llm-bill-explodes scenario.
package main

import (
	"os"

	llmbill "github.com/neerajsinghi/system-design-patterns-go/scenarios/llm-bill-explodes"
)

func main() { llmbill.Broken().Print(os.Stdout) }
