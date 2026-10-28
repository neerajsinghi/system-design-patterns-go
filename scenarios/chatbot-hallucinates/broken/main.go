// Command broken runs the broken version of the chatbot-hallucinates scenario.
package main

import (
	"os"

	chatbothallucinates "github.com/neerajsinghi/system-design-patterns-go/scenarios/chatbot-hallucinates"
)

func main() { chatbothallucinates.Broken().Print(os.Stdout) }
