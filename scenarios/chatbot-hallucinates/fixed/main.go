// Command fixed runs the fixed version of the chatbot-hallucinates scenario.
package main

import (
	"os"

	chatbothallucinates "github.com/neerajsinghi/system-design-patterns-go/scenarios/chatbot-hallucinates"
)

func main() { chatbothallucinates.Fixed().Print(os.Stdout) }
