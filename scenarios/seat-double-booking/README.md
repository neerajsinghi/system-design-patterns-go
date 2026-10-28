# Scenario: The Ticketing Site Sells the Same Seat Twice

## The incident

Tickets for a sold-out-in-minutes show go on sale. Within the first hour, the support queue fills with three kinds of complaint. Two people turn up with valid tickets for the same seat, and both have paid. Other people say the seat map showed only a handful of seats left, yet on the night there are empty seats in the stalls. And some customers have been charged twice for one ticket, and have two confirmation emails to prove it. No errors were logged. Every request returned success.

## Symptoms

- More tickets were issued than seats were sold. Each extra ticket points at a seat that already has another owner.
- Duplicate tickets cluster in the front rows and in the first minutes of the sale, when many people click the same seats at the same moment.
- Seats show as unavailable for the whole sale, but nobody ever paid for them.
- Customers whose payment webhook was delivered twice were charged twice and ticketed twice.

## Root cause

1. **Check-then-act booking.** Selecting a seat runs "read the seat; if it's free, mark it locked" as two separate statements. When several shoppers read the seat before any of them writes, they all see it free and all lock it. When their payments arrive, the service writes "booked" unconditionally, so every one of them gets a ticket and the last write decides who the database thinks owns the seat.
2. **Locks that never expire.** The lock is only cleared by a successful payment. A shopper who closes the tab at checkout keeps the seat off sale for the rest of the event.
3. **A non-idempotent payment webhook.** The gateway retries a webhook when it doesn't see a 200 in time. The handler captures the payment and issues a ticket on every delivery.

## The fix

| Change | Pattern | Effect |
| --- | --- | --- |
| Picking a seat takes an atomic hold with a 5-minute deadline; a reaper returns expired holds every minute | [Reservation hold](../../patterns/data/reservation-hold) | Only one shopper in a race gets the hold; the others are told "seat taken" before they pay. Abandoned seats go back on sale. A payment that finishes after the deadline is refunded rather than accepted |
| The final booking is a compare-and-set on the seat's version, read when the hold was taken | [Optimistic locking](../../patterns/data/optimistic-locking) | Even if the hold layer is wrong, a seat that has changed since the shopper chose it cannot be booked again; the loser is refunded |
| The payment webhook runs through an idempotency key (the payment ID) | [Idempotency key](../../patterns/reliability/idempotency-key) | A retried delivery replays the stored answer: no second capture, no second ticket, no second refund |

**Why both a hold and a compare-and-set?** They do different jobs. The hold is about the shopper and the inventory: it decides the race *before* anyone pays, so losers get a clean "seat taken" instead of a refund, and its deadline is what brings abandoned seats back. But holds usually live in a fast, separate store (Redis, an in-memory service), they expire on a timer, and they're the kind of thing that gets bypassed by a second sales channel or wiped by a failover. The compare-and-set lives on the seat record itself, at the moment of the write that matters, so it holds even when the hold layer doesn't. The simulation shows this: the hold store fails over at t=12m and loses six active holds, shoppers arriving afterwards can hold and pay for those same seats, and two such bookings are refused by the compare-and-set instead of becoming double bookings.

## Run it

```sh
go run ./scenarios/seat-double-booking/broken
go run ./scenarios/seat-double-booking/fixed
go test ./scenarios/seat-double-booking/
```

| | Broken | Fixed |
| --- | --- | --- |
| Seats double-booked | 5 | 0 |
| Tickets issued / seats sold | 46 / 34 | 40 / 40 |
| Customers charged twice | 7 (every duplicate webhook) | 0 (8 duplicates replayed) |
| Seats stuck at the end of the sale | 12 | 0 |
| Abandoned seats later sold to someone else | 0 of 13 | 7 of 15 |
| Shoppers told "seat taken" before paying | 0 | 6 |
| Refunds | 0 | 6 (4 paid after the hold expired, 2 caught by the compare-and-set) |

The tests check that the broken version sells a seat twice, charges a customer twice and leaves seats stuck, and that the fixed version does none of these, replays every duplicate webhook and returns abandoned seats to sale. A separate test checks that the hold-store failover leads to bookings the compare-and-set refuses: without that guard they would have been double bookings. Another test pins down the fix's cost. Broken never says no (it sells the seat twice instead), while Fixed turns some shoppers away with "seat taken" and refunds late payers. A last test checks both runs are deterministic.

The cost is real. Four customers who paid, but took longer than the 5-minute hold, are refunded even though some of their seats may still have been free. That is the price of a hard deadline, and in a real system it's softened by warning the shopper before the timer runs out, or by trying to re-hold the seat before refunding. The two compare-and-set refunds are worse, because those shoppers had a hold and played by the rules. Here they exist only because the hold store failed, and a refund is still far better than a second ticket for an occupied seat. Fixed doesn't sell every seat either. Shoppers only consider their 3 preferred seats, which lean heavily towards the front, and leave if none is free; they never fall back to "best available". So 8 seats (six of them in rows D-F) end the sale unsold and available, because no shopper who was still looking listed them. The per-run counts of abandoned checkouts and duplicate webhooks differ slightly between the runs for the same reason. Both runs replay the same shoppers, but the broken run turns more of them away before they reach checkout, because its seat map is full of stuck locks.

**How the simulation is built.** 48 seats (rows A-F, 8 per row) and 120 shoppers from a seeded generator, arriving 6 per simulated minute for 20 minutes. Each shopper wants 3 seats, chosen with a strong lean towards the front rows. Every round runs a read phase (all 6 look at the seat map) and then a write phase (each, in arrival order, claims the first seat that looked free). That makes the check-then-act race explicit and repeatable, instead of leaving it to goroutine scheduling. 20% of shoppers abandon after claiming a seat. Payers take 1-3 minutes, or 6-9 minutes for 10% of them. A quarter of webhooks are delivered a second time 30 seconds later. Seat records live in an `optimisticlocking.Store` in both designs: Broken writes with `Put`, Fixed with `CompareAndSet`. Fixed adds a `reservationhold.Inventory` (reaped every simulated minute) and an `idempotencykey.Store` keyed by payment. At t=12m, Fixed's hold store is replaced with an empty one rebuilt from the seat store (sold seats stay sold, active holds are lost). A shopper whose hold vanished goes straight to the compare-and-set. Everything runs on one simulated clock in a single goroutine, from an event queue.

**Simplifications.** "Refund" here means releasing the card authorisation. Payment is captured only after the booking succeeds, so a refund never moves money. The hold store, seat store and idempotency store are in memory and in-process. In production, the idempotency record and the booking should commit in the same database transaction, or the handler can crash between them. A booking made after the failover goes through the compare-and-set but isn't written back to the new hold store. The seat map stays correct because it checks the seat store too.

## Interview follow-ups

- *Why not lock the seat row with `SELECT ... FOR UPDATE` for the whole checkout?* The checkout includes a payment that takes minutes and can be abandoned. Holding a database lock that long blocks other readers and needs its own timeout anyway. A hold is that lock made explicit: it has a deadline, and it lives outside the transaction. A short row lock, or a conditional `UPDATE ... WHERE version = ?`, belongs only at the final write.
- *What TTL should the hold have?* Long enough for a normal payment, including 3-D Secure, so most payers finish in time. Short enough that abandoned seats come back while demand is still high. Show the timer to the shopper, and extend it once if they're actively on the payment page.
- *A payment succeeds after the hold expired. Refund, or try to book anyway?* Try to re-hold the same seat first. If it's still free, book it and nobody notices. If someone else holds it, refund. Never book over another shopper's active hold, or the honest shopper becomes the one who's refunded.
- *Would a distributed lock instead of the hold solve the double booking?* Only with fencing. A lock holder can pause past its lease and still write. The version check at the write is the fencing token, which is why the compare-and-set stays in the design ([distributed lock with fencing](../../patterns/distributed/distributed-lock-fencing)).
- *What if the gateway sends the webhook to two replicas at the same time?* The idempotency record has to be shared and claimed atomically (a unique key in the bookings database), so the second replica sees the claim in progress and backs off. The in-memory store here only shows the shape of it.
- *Hot on-sale with 100,000 people in the queue: does optimistic locking still work?* Conflicts on the final write stay rare, because the hold has already decided the race. The hot spot moves to the hold store, which is why big ticketing sites put a virtual waiting room in front of seat selection.

## Further reading

- Arvind Kumar, [BookMyShow Sells the Same Seat Twice](https://codefarm0.medium.com/bookmyshow-sells-the-same-seat-twice-system-design-interview-deep-dive-on-race-conditions-and-64e798f34d21), the interview scenario this walkthrough is based on
