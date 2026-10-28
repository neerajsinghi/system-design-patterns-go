// Command fixed runs the fixed version of the notification-storm scenario.
package main

import (
	"os"

	notificationstorm "github.com/neerajsinghi/system-design-patterns-go/scenarios/notification-storm"
)

func main() { notificationstorm.Fixed().Print(os.Stdout) }
