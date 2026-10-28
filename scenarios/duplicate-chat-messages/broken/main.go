// Command broken runs the broken version of the duplicate-chat-messages scenario.
package main

import (
	"os"

	duplicatechat "github.com/neerajsinghi/system-design-patterns-go/scenarios/duplicate-chat-messages"
)

func main() { duplicatechat.Broken().Print(os.Stdout) }
