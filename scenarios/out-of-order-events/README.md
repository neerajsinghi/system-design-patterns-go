# Scenario: Order Events Arrive Out of Sequence

## The incident

An order service publishes three events for every order: OrderCreated, PaymentCaptured, OrderShipped. A fulfilment and notification service consumes them from a topic with a pool of workers. Each order follows a strict state machine (created, then paid, then shipped), and handling OrderShipped sends the customer a "your order is on its way" email. Nobody chose a partition key, so the producer spreads events round-robin over the partitions. On a busy afternoon, support tickets come in from customers who got a "shipped" email for an order whose payment hadn't gone through yet. The fulfilment dashboard shows hundreds of rejected transitions, and more than 40% of the day's orders are stuck in "created" or "paid" even though the warehouse has dispatched them. The team pins down a second cause during the investigation. When a publish from the primary region times out, the producer in the second region retries it, and that copy reaches the broker tens of milliseconds after the events that came after it.

## Symptoms

- The state machine rejects transitions such as "OrderShipped while still created" or "PaymentCaptured for an order that doesn't exist yet".
- Customers are emailed "shipped" for orders that show as unpaid.
- Orders end the day in the wrong state, because a rejected event is dropped and never applied again.
- More workers make it worse, not better. Moving to one consumer would fix the races but can't keep up with the traffic.

## Root cause

1. **No partition key.** Round-robin puts an order's three events on different partitions, each with its own worker. Whichever worker finishes first wins, so a quick OrderShipped can overtake a slow PaymentCaptured.
2. **Upstream reordering.** A retry from the second region reaches the broker after the order's later events. Even a perfectly ordered consumer would see them in the wrong order, because the broker already has them in the wrong order.
3. **No sequence numbers.** The consumer has no way to tell that an event is early, so it applies or rejects whatever comes next.

## The fix

| Change | Pattern | Effect |
| --- | --- | --- |
| Partition by order ID, one sequential worker per partition | [Per-key ordering](../../patterns/distributed/per-key-ordering) (`PartitionFor`, `Dispatcher`) | An order's events are handled by one worker in broker order, while different orders still run in parallel |
| Producer stamps a per-order sequence number; a resequencer in front of each partition's worker releases events only in sequence | [Per-key ordering](../../patterns/distributed/per-key-ordering) (`Resequencer`) | The second-region retries are put back in order before the state machine sees them |
| Gap timeout, then repair from the order service | [Per-key ordering](../../patterns/distributed/per-key-ordering) (`Resequencer` gap policy) | A lost event holds back only its own order, for at most 100ms, then the consumer reads the missing state from the source of truth |

## Run it

```sh
go run ./scenarios/out-of-order-events/broken
go run ./scenarios/out-of-order-events/fixed
go test ./scenarios/out-of-order-events/
```

Both runs get 300 orders (900 events). 150 of the events are retried from the second region, and 93 of those reach the broker after a later event of their order.

| | Broken | Fixed |
| --- | --- | --- |
| Invalid state transitions | 208 | 0 |
| "Shipped" emails for unpaid orders | 133 | 0 |
| Orders not in their final state | 133 of 300 (58 stuck in paid, 75 in created) | 0 of 300 |
| Last event applied | 964ms | 970ms |
| Longest broker-to-applied lag | 13ms | 81ms |
| Events held by the resequencer | n/a | 126, mean 34.7ms, max 56.9ms, 0 gaps |

The fixed run also compares two alternatives on the same traffic:

| Alternative | Invalid transitions | Last event applied | Longest lag |
| --- | --- | --- | --- |
| Key partitioning without the resequencer | 131 (all from the retries) | n/a | n/a |
| Single global consumer + resequencer | 0 | 4.533s | 3.627s |

The single consumer is correct but far too slow. Events arrive about 5x faster than one worker can handle them, so it is seconds behind by the end. Key partitioning gives the same correctness while finishing within 6ms of the broken pool.

The cost run loses order-0150's PaymentCaptured. The resequencer holds that order's OrderShipped for 102ms (the 100ms gap timeout, plus up to one 5ms tick), skips the hole, and the consumer reads "paid" back from the order service. The order still ends shipped with no invalid transition, but its OrderShipped is applied 118ms after reaching the broker. Every other order's longest hold stays at 56.9ms, exactly as in the run with nothing lost.

The tests check that the broken run makes invalid transitions, sends shipped emails for unpaid orders and leaves orders in the wrong state, and that the fixed run does none of these, with no gaps and every hold under the gap timeout. They also check that both runs see identical traffic and that partitioning alone still breaks on the retries. Two more tests cover parallelism: the fixed design finishes within 10% of the broken pool, and the global consumer takes at least 3x as long. The cost test checks that the lost event's wait sits between the gap timeout and one tick past it, that the order is repaired, and that no other order is held longer. A last test checks both runs are deterministic.

The fixed design's 81ms longest lag, against 13ms for the broken run, is what correctness costs here. An event that arrives before its predecessor waits for it, up to the 60ms retry delay, instead of being applied (and rejected) at once.

**How the simulation is built.** A seeded generator starts an order every 3ms. Its events are published 2-8ms apart, each takes a worker 1-9ms, and 15% of them are retried from the second region, 20-60ms late. Time is simulated, so nothing sleeps. Every worker handles its own FIFO queue one item at a time, and an event's effect is applied when its worker finishes it. Broken hands the k-th arriving event to worker k mod 8. Fixed hands every event to `PartitionFor(orderID, 8)`, through a `perkeyordering.Resequencer` (100ms gap timeout, ticked every 5ms) running on the simulated clock. Released events join the worker queue at release time. A gap notice becomes a 10ms repair item on the same queue, which applies the skipped event's effect from the order service's record. Both designs run the same state machine and email code: an invalid transition is counted and the event dropped. The email goes out the moment OrderShipped is handled, whatever state the order is in.

**Simplifications.** The partitions and workers are modelled as queues on a simulated clock rather than the goroutine `Dispatcher`, so the numbers are exact and repeatable. The `Dispatcher` gives the same per-key guarantee with real goroutines, and the per-key-ordering example runs it. The order service is the source of truth for the repair and is assumed to answer. Duplicates aren't simulated; the resequencer would drop a second copy of an already-released sequence number, but a real consumer still needs an idempotent apply for redeliveries after a crash. Resequencer state lives in memory, whereas a real consumer has to persist each order's next expected sequence number with its offsets, or a restart replays events out of order.

## Interview follow-ups

- *Why not just use one partition?* It gives a total order, but one worker then has to handle every event. Here that falls 3.6s behind within a second of traffic. Customers only need each order's events in order, not all events, and that can run in parallel.
- *Partitioning by order ID fixed the worker race. Why do you still need sequence numbers?* Partitioning only keeps the order the broker received. A retry from another region, a producer with several in-flight requests, or a replicated topic can deliver events out of order before the consumer ever sees them. Only the producer knows the true order, so it has to write it down.
- *Where does the sequence number come from?* From the entity: the order's version column, incremented in the same transaction that changes the order and written to a [transactional outbox](../../patterns/distributed/transactional-outbox). A global counter or a timestamp doesn't work: clocks across regions disagree, and a global counter becomes a bottleneck.
- *What should the gap timeout be?* Longer than the normal retry delay (here the worst retry is 60ms, the timeout 100ms), short enough that a stuck order is noticed quickly. Too short and late events are skipped and need repair; too long and one lost event delays that order for the whole wait.
- *What happens when the missing event turns up after the gap was skipped?* The resequencer ignores it as a duplicate. The repair already read the order service's state, so the event has nothing left to change.
- *Could the consumer just reject out-of-order events and let the broker redeliver them?* That's a retry loop that holds a partition while it spins, and on Kafka it blocks every other order on the partition too. Buffering per order costs a little memory and holds back only the order that is waiting.
- *What about adding partitions later?* Changing the partition count remaps most order IDs, so for a moment an order's old events sit on one partition and its new events on another. Drain before resizing, or rely on the sequence numbers to catch the overlap.

## Further reading

- Arvind Kumar, [Order Events Arrive Out of Sequence](https://codefarm0.medium.com/order-events-arrive-out-of-sequence-system-design-deep-dive-on-event-ordering-and-kafka-41a0ad4e585a), the interview scenario this walkthrough is based on
