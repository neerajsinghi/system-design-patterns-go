// Command broken runs the broken version of the video-pipeline scenario.
package main

import (
	"os"

	videopipeline "github.com/neerajsinghi/system-design-patterns-go/scenarios/video-pipeline"
)

func main() { videopipeline.Broken().Print(os.Stdout) }
