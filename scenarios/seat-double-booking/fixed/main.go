// Command fixed runs the fixed version of the seat-double-booking scenario.
package main

import (
	"os"

	seatdoublebooking "github.com/neerajsinghi/system-design-patterns-go/scenarios/seat-double-booking"
)

func main() { seatdoublebooking.Fixed().Print(os.Stdout) }
