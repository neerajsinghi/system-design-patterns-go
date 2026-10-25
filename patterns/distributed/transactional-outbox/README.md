# Transactional Outbox

## Problem

A service must update its database *and* publish an event ("order placed").
These are two different systems, so a naive "write, then publish" (the
**dual-write problem**) can fail halfway:

- DB commit succeeds, publish fails or the process crashes: the order exists
  but nobody downstream ever hears about it.
- Publish first, then the DB commit fails: downstream acts on an order that
  does not exist.

Distributed transactions (two-phase commit) across the DB and the broker are
usually unavailable or too slow.

## Solution

Write the event into an **outbox table in the same database, in the same
local transaction** as the business change. Now the order and its event are
committed atomically. A separate **relay** reads unpublished outbox rows in
order, publishes them to the broker, and marks them as published.

If the relay crashes after publishing but before marking, it publishes the
row again on restart. Delivery is therefore **at-least-once**, and consumers
deduplicate using the outbox row ID.

## Diagram

```mermaid
sequenceDiagram
    participant S as Order service
    participant DB as Database (orders + outbox)
    participant R as Relay
    participant B as Broker
    participant C as Consumer (dedup by ID)
    S->>DB: BEGIN; INSERT order; INSERT outbox(#1); COMMIT
    loop every interval
        R->>DB: SELECT unpublished ORDER BY id
        R->>B: publish #1
        B->>C: #1 (processed)
        Note over R: crash before marking #1
        R->>DB: SELECT unpublished (still #1)
        R->>B: publish #1 again
        B->>C: #1 (duplicate, ignored)
        R->>DB: UPDATE outbox SET published WHERE id=1
    end
```

## How this implementation works

- `DB.WithTx(fn)` is an all-or-nothing transaction over an in-memory orders
  table and outbox table. If `fn` returns an error, the staged writes are
  discarded.
- `PlaceOrder` writes the `Order` and an `order.placed` outbox row in one
  transaction (`Tx.PutOrder` + `Tx.AddOutbox`).
- `Relay.RelayOnce` publishes pending rows in ID order and stops at the first
  failure, so a later event is never published before an earlier one.
  `Relay.Run` repeats it on a ticker until the context is cancelled.
- `Relay.AfterPublish` is a fault-injection hook used by the tests and the
  example to crash the relay between publish and mark.
- `DedupConsumer` remembers processed message IDs and counts duplicates.

## Trade-offs

- **At-least-once.** Duplicates are part of the contract; every consumer must
  be idempotent.
- **Latency** equals the polling interval. Change-data-capture (reading the
  DB's write-ahead log, e.g. Debezium) avoids polling and lowers latency.
- **Single relay assumed.** Two relays polling the same rows would double
  publish and could reorder. In production, run one active relay (leader
  election), use `SELECT ... FOR UPDATE SKIP LOCKED` with per-key ordering,
  or partition the outbox.
- **Table growth.** Published rows must be deleted or archived.
- Ordering here is global; most systems only need ordering per aggregate key.

## When to use / when not to

Use it whenever a state change and the event announcing it must not diverge:
order placement, payment captured, user signed up, and any saga step.

Avoid it if the database can be the event log itself (event sourcing), if the
event is best-effort (a metrics ping), or if you have no local transactional
store to put the outbox in.

## Real-world examples

- Debezium's outbox event router (CDC from the outbox table to Kafka).
- The pattern as described on microservices.io by Chris Richardson.
- Many payment and e-commerce systems use a polling publisher or CDC on
  PostgreSQL/MySQL for exactly this.

## Run it

```sh
go run ./patterns/distributed/transactional-outbox/example
go test ./patterns/distributed/transactional-outbox/
```

## Related patterns

- [Two-phase commit](../two-phase-commit): the heavyweight alternative.
- [Saga (orchestration)](../saga-orchestration) and [saga (choreography)](../saga-choreography): each step usually emits its event via an outbox.
- [Competing consumers](../competing-consumers) and [pub/sub](../pub-sub): where the events go.
- [Idempotency key](../../reliability/idempotency-key): consumer-side dedup.
- [Write-ahead log](../../data/write-ahead-log): what CDC-based relays read.
- [Event sourcing](../../data/event-sourcing)
- [Leader election](../leader-election): keep a single active relay.
