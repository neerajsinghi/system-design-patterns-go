// Command example serves product recommendations through a fallback chain:
// live service -> last known good (up to 5 minutes old) -> bestsellers.
package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/fallback"
)

func main() {
	start := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	now := start
	healthy := true

	recommend := func(_ context.Context, user string) ([]string, error) {
		if !healthy {
			return nil, errors.New("recs-service: 503")
		}
		return []string{"for-" + user + "-1", "for-" + user + "-2"}, nil
	}

	f := fallback.New(recommend, fallback.Options[string, []string]{
		MaxStale: 5 * time.Minute,
		Default: func(string) ([]string, bool) {
			return []string{"bestseller-1", "bestseller-2"}, true
		},
		Now: func() time.Time { return now },
	})

	show := func(user string) {
		res, err := f.Get(context.Background(), user)
		at := now.Sub(start)
		if err != nil {
			fmt.Printf("+%-5v %-6s error: %v\n", at, user, err)
			return
		}
		note := ""
		if res.Source == fallback.Stale {
			note = fmt.Sprintf(" (age %v)", res.Age)
		}
		if res.Cause != nil {
			note += " because " + res.Cause.Error()
		}
		fmt.Printf("+%-5v %-6s [%-7s] %s%s\n", at, user, res.Source, strings.Join(res.Value, ", "), note)
	}

	show("alice")
	fmt.Println("       -- recommendations service goes down --")
	healthy = false
	now = now.Add(2 * time.Minute)
	show("alice") // served from last known good
	show("bob")   // never seen: default
	now = now.Add(4 * time.Minute)
	show("alice") // 6 minutes old: too stale, default
	fmt.Println("       -- service recovers --")
	healthy = true
	show("alice")
	fmt.Println("\nThe page always renders; Source tells metrics how degraded it was.")
}
