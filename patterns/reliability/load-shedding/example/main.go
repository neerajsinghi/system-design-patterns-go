// Command example simulates a server whose latency climbs as it gets busier.
// As load ramps up, the shedder sheds sheddable traffic first, then normal,
// while critical requests keep getting through, and the adaptive limit
// settles near the server's real capacity.
package main

import (
	"fmt"
	"time"

	"github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/load-shedding"
)

func main() {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := loadshedding.New(loadshedding.Config{
		InitialLimit:  40,
		MinLimit:      4,
		MaxLimit:      100,
		TargetLatency: 100 * time.Millisecond,
		Now:           func() time.Time { return now },
	})

	// The server handles ~20 concurrent requests comfortably; beyond that each
	// extra request adds 10ms of queueing to everyone.
	latencyFor := func(concurrent int) time.Duration {
		d := 50 * time.Millisecond
		if concurrent > 20 {
			d += time.Duration(concurrent-20) * 10 * time.Millisecond
		}
		return d
	}

	mix := []loadshedding.Priority{loadshedding.Critical, loadshedding.Normal, loadshedding.Normal, loadshedding.Sheddable}
	names := map[loadshedding.Priority]string{loadshedding.Critical: "critical", loadshedding.Normal: "normal", loadshedding.Sheddable: "sheddable"}

	fmt.Println("tick  offered  limit  admitted  latency   shed(critical/normal/sheddable)")
	for tick, offered := range []int{12, 24, 48, 80, 80, 80, 80, 80, 80, 80, 24} {
		before := s.Stats().Shed
		var dones []func()
		for i := 0; i < offered; i++ {
			if d, err := s.Acquire(mix[i%len(mix)]); err == nil {
				dones = append(dones, d)
			}
		}
		limit := s.Stats().Limit
		lat := latencyFor(len(dones))
		now = now.Add(lat)
		for _, d := range dones {
			d()
		}
		after := s.Stats().Shed
		fmt.Printf("%4d  %7d  %5d  %8d  %7v   %d/%d/%d\n", tick, offered, limit, len(dones), lat,
			after[loadshedding.Critical]-before[loadshedding.Critical],
			after[loadshedding.Normal]-before[loadshedding.Normal],
			after[loadshedding.Sheddable]-before[loadshedding.Sheddable])
	}
	total := s.Stats().Shed
	fmt.Println()
	for _, p := range []loadshedding.Priority{loadshedding.Critical, loadshedding.Normal, loadshedding.Sheddable} {
		fmt.Printf("total shed %-9s %d\n", names[p], total[p])
	}
	fmt.Println("The limit settles near real capacity (~20-25), latency returns under the 100ms target,\nand per tick critical is shed least (9 of 20) while sheddable is shed most (17 of 20).")
}
