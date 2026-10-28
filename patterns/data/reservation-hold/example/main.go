// Command example simulates a flash sale of 100 seats. Shoppers hold seats
// at checkout; some pay, some abandon the tab, and a few pay after their
// timer has run out. It compares holds that never expire with 10-minute TTL
// holds plus a once-a-minute reaper, using identical shoppers in both runs.
package main

import (
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"time"

	reservationhold "github.com/neerajsinghi/system-design-patterns-go/patterns/data/reservation-hold"
)

const (
	seats        = 100
	arrivalEvery = 6 * time.Second
	saleLength   = 30 * time.Minute // shoppers arrive during this window
	runUntil     = 45 * time.Minute // then pending payments and holds finish
	holdTTL      = 10 * time.Minute
	reapEvery    = time.Minute
)

type behaviour int

const (
	pays behaviour = iota
	abandons
	paysLate // pays after the 10-minute timer
)

type shopper struct {
	arrive time.Duration
	want   int
	does   behaviour
	payIn  time.Duration // after arrival
}

// shoppers builds the same crowd for every run.
func shoppers() []shopper {
	rng := rand.New(rand.NewPCG(42, 7))
	var out []shopper
	for t := time.Duration(0); t < saleLength; t += arrivalEvery {
		s := shopper{arrive: t, want: 1 + rng.IntN(3)}
		switch r := rng.Float64(); {
		case r < 0.35:
			s.does = abandons
		case r < 0.45:
			s.does = paysLate
			s.payIn = 11*time.Minute + time.Duration(rng.IntN(180))*time.Second
		default:
			s.does = pays
			s.payIn = time.Minute + time.Duration(rng.IntN(300))*time.Second
		}
		out = append(out, s)
	}
	return out
}

type result struct {
	turnedAway                      int
	lockedByAbandonedAtClose        int // seats in live abandoned holds when arrivals stop
	buyers                          int
	lateOnTime                      int // late payers whose hold never expired
	lateRejected, lateRescued       int
	lateRefunded, lateRefundedSeats int
	lateDoubleBooked                int // refunded seats that had no free seat to cover them
	totals                          reservationhold.Totals
	stats                           reservationhold.Stats
}

type payment struct {
	holdID, holder string
	seats          int
	late           bool
}

func seatList(n int) []string {
	items := make([]string, n)
	for i := range items {
		items[i] = "seat"
	}
	return items
}

func simulate(ttl time.Duration, reap bool) result {
	start := time.Unix(0, 0)
	now := start
	inv, err := reservationhold.New(map[string]int{"seat": seats},
		reservationhold.WithClock(func() time.Time { return now }))
	if err != nil {
		log.Fatal(err)
	}

	crowd := shoppers()
	payments := map[time.Duration][]payment{}
	var abandoned []reservationhold.Hold
	var res result

	next := 0
	for t := time.Duration(0); t <= runUntil; t += time.Second {
		now = start.Add(t)
		if reap && t%reapEvery == 0 {
			inv.Reap()
		}
		if t == saleLength {
			for _, a := range abandoned {
				if now.Before(a.ExpiresAt) {
					res.lockedByAbandonedAtClose += a.Units()
				}
			}
		}
		for next < len(crowd) && crowd[next].arrive == t {
			s := crowd[next]
			next++
			holder := fmt.Sprintf("shopper-%d", next)
			h, err := inv.Hold(seatList(s.want), holder, ttl)
			if errors.Is(err, reservationhold.ErrUnavailable) {
				res.turnedAway++
				continue
			} else if err != nil {
				log.Fatal(err)
			}
			if s.does == abandons {
				abandoned = append(abandoned, h)
				continue
			}
			at := t + s.payIn
			payments[at] = append(payments[at], payment{h.ID, holder, s.want, s.does == paysLate})
		}
		for _, p := range payments[t] {
			// Payment providers retry callbacks; the second Confirm must give
			// the same answer and must not sell twice.
			_, err1 := inv.Confirm(p.holdID)
			_, err2 := inv.Confirm(p.holdID)
			if (err1 == nil) != (err2 == nil) {
				log.Fatalf("confirm not idempotent: %v vs %v", err1, err2)
			}
			switch {
			case err1 == nil:
				res.buyers++
				if p.late {
					res.lateOnTime++
				}
			case errors.Is(err1, reservationhold.ErrHoldExpired):
				// The seats may belong to someone else now. Try to buy free
				// seats instead; if there are none, refund.
				res.lateRejected++
				free := inv.Available("seat")
				if h, err := inv.Hold(seatList(p.seats), p.holder, time.Minute); err == nil {
					if _, err := inv.Confirm(h.ID); err != nil {
						log.Fatal(err)
					}
					res.lateRescued++
					res.buyers++
				} else {
					res.lateRefunded++
					res.lateRefundedSeats += p.seats
					res.lateDoubleBooked += p.seats - free
				}
			default:
				log.Fatal(err1)
			}
		}
		delete(payments, t)
	}
	res.totals = inv.Totals()
	res.stats = inv.Stats()
	return res
}

func report(r result) {
	fmt.Printf("   buyers %d, shoppers turned away %d\n", r.buyers, r.turnedAway)
	fmt.Printf("   seats locked by abandoned carts when the last shopper arrived: %d\n", r.lockedByAbandonedAtClose)
	fmt.Printf("   seats sold %d / %d, unsold %d (of which still locked by holds at the end: %d)\n",
		r.totals.Sold, seats, r.totals.Available+r.totals.Held, r.totals.Held)
	fmt.Printf("   holds %d, expired lazily %d, expired by Reap %d\n\n",
		r.stats.Holds, r.stats.ExpiredLazy, r.stats.ExpiredReaped)
}

func main() {
	crowd := shoppers()
	fmt.Printf("Flash sale: %d seats, %d shoppers (one every %v for %v), 1-3 seats each.\n",
		seats, len(crowd), arrivalEvery, saleLength)
	fmt.Println("35% abandon checkout, 10% pay 11-14 minutes after holding, the rest within 6 minutes.")
	fmt.Println()

	a := simulate(24*time.Hour, false)
	fmt.Println("1) Holds with no expiry (modelled as a 24h TTL, longer than the whole run)")
	report(a)

	b := simulate(holdTTL, true)
	fmt.Printf("2) %v TTL holds, expired lazily on access and by Reap() every %v\n", holdTTL, reapEvery)
	report(b)

	fmt.Println("3) Late payments")
	fmt.Printf("   no expiry: %d late payers kept their seats, because nothing was ever handed back\n", a.lateOnTime)
	fmt.Printf("   with TTL:  %d late Confirm calls refused with ErrHoldExpired\n", b.lateRejected)
	fmt.Printf("              %d bought other free seats instead, %d (%d seats) were refunded\n",
		b.lateRescued, b.lateRefunded, b.lateRefundedSeats)
	fmt.Printf("              their old seats had gone to other shoppers and too few were free: accepting\n")
	fmt.Printf("              them would have double-booked %d seats (oversell)\n", b.lateDoubleBooked)
}
