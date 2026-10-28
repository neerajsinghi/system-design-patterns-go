// Command fixed runs the fixed version of the duplicate-chat-messages scenario.
package main

import (
	"os"

	duplicatechat "github.com/neerajsinghi/system-design-patterns-go/scenarios/duplicate-chat-messages"
)

func main() { duplicatechat.Fixed().Print(os.Stdout) }
