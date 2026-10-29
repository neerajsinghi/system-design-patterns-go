# Idempotent Consumer (Inbox)

## Problem

Message brokers and logs (Kafka, SQS, RabbitMQ, a chat server's delivery queue) promise **at-least-once** delivery, which is a polite way of saying "duplicates will happen". They come from three places:

- **The producer.** It sends a message, the broker stores it, and the ack is lost on the way back. The producer cannot tell that apart from "the broker never got it", so it resends, and the log now holds the message twice.
- **The consumer.** Handling a message is two writes to two systems: the side effect (credit an account, store a chat message, send an email) and the offset commit that tells the broker "I am done up to here". A crash between them leaves the two out of step.
- **The platform.** A consumer-group rebalance hands a partition to a new member, which starts from the last committed offset and replays whatever the old owner had processed but not committed. Offset resets and replays to rebuild a projection redeliver everything.

The order of those two consumer writes decides which way it fails:

| Order | Crash between the two writes | Result |
| --- | --- | --- |
| Commit offset, then apply effect | The log says "done", the effect never ran | **Lost message** (at-most-once) |
| Apply effect, then commit offset | The effect ran, the message is redelivered | **Duplicated effect** (at-least-once) |

Neither is acceptable for payments or chat: you either drop a deposit or credit it twice.

## Solution

Accept at-least-once delivery and make the consumer **idempotent**, so processing a message twice has the same effect as processing it once:

1. **The producer assigns each message a unique ID** before the first attempt and reuses it on every retry (producer ID + sequence number, a Snowflake ID, a UUID). The ID identifies the *message*, not the delivery.
2. **The consumer commits the offset after processing**, so nothing is ever lost.
3. **Processing is one local transaction** that applies the side effect *and* inserts the message ID into an **inbox** (a "processed messages" table with the ID as primary key). If the ID is already there, the transaction does nothing.

Because the effect and the inbox row commit together, there is no moment at which one exists without the other. A redelivery after any crash point either finds the ID (skip) or doesn't (the effect never happened, so apply it). That gives **effectively-once** processing on top of at-least-once delivery.

The inbox cannot grow forever, so IDs are **pruned after a dedup window**. A duplicate that arrives after its ID was pruned looks like a new message and is applied again. The window must be longer than the longest delay a duplicate can plausibly have: producer retry budget, consumer lag, the longest outage you would replay after.

## Diagram

```mermaid
sequenceDiagram
    participant P as Producer
    participant L as Log (partition 0)
    participant C as Consumer
    participant DB as Store (balances + inbox)
    P->>L: append id=p-7 (+50)
    L--xP: ack lost
    P->>L: append id=p-7 (+50) again
    L-->>P: ack
    Note over L: offsets 12 and 13 both hold p-7
    L->>C: offset 12, p-7
    C->>DB: BEGIN; p-7 in inbox? no; balance += 50; INSERT inbox(p-7); COMMIT
    Note over C: crash before committing offset 13
    C->>L: restart: committed offset is 12
    L->>C: offset 12, p-7 (redelivery)
    C->>DB: p-7 in inbox? yes -> skip
    C->>L: commit 13
    L->>C: offset 13, p-7 (the producer's retransmit)
    C->>DB: p-7 in inbox? yes -> skip
    C->>L: commit 14
```

## How this implementation works

- `Log` is an in-process partitioned log: `NewLog(partitions)`, `Append(m)` (partition by FNV hash of the key, offset assigned on append, **no** deduplication), `Read(partition, from, max)`, `End(partition)`, and per-group `Commit(group, partition, next)` / `Committed(group, partition)` using Kafka's convention that the committed offset is the next one to read. Committing a lower offset rewinds the group, which is how replays are simulated.
- `Store` is a transactional store holding balances and the inbox. `Apply(m, dedup, handler)` runs the handler against a `Tx` that stages writes (`Tx.Add`, `Tx.Balance`) and commits them in one step under the store's lock. With `dedup` set it first checks the inbox and, on commit, inserts the ID together with the writes. A handler error rolls everything back. `WithRetention(d)` sets the dedup window and `WithClock` the clock; expired IDs are pruned during `Apply` (and by `Prune()`) from an insertion-ordered queue, so pruning is O(1) amortised.
- `Consumer` reads its assigned partitions for a group and handles one message at a time with a `Strategy`: `CommitBefore`, `CommitAfter` or `Inbox`. It commits after every message.
- Crashes are injected deterministically with `WithCrashes(Crash{Partition, Offset, Point})` at `BeforeEffect`, `AfterEffect`, `BeforeCommit` or `AfterCommit`. Each fires once: `Poll` returns `ErrCrashed`, the consumer forgets its in-memory positions like a restarted process, and the next `Poll` resumes from the committed offsets. `Drain()` polls until caught up, restarting after each crash.
- `Producer` assigns `"<producerID>-<seq>"` IDs (or your own via `WithIDs`) and retries up to `WithMaxAttempts(n)` times. `WithFaults` decides per attempt whether the request is `Delivered`, lost (`LoseRequest`: nothing appended), or acked-but-lost (`LoseAck`: appended, producer resends). After the last attempt it returns `ErrNotAcked`, even though lost acks mean the message may be in the log several times.
- `Store.Audit(ids)` counts each ID as exact, lost or duplicated, from how many times its transaction committed. That counter is instrumentation for tests and the example, not part of the pattern.

**Simplification.** The store is one in-memory map behind one lock, standing in for a database transaction (`INSERT INTO inbox ... ON CONFLICT DO NOTHING` plus the business update, in one `BEGIN ... COMMIT`). The consumer commits after every message; real consumers commit in batches, which makes the duplicate window a whole batch wide but changes nothing else. There is no group coordinator: partition assignment is static (`WithPartitions`), and a rebalance is simulated by letting two members read the same partition at once. The log does no broker-side deduplication; Kafka's idempotent producer would have removed the retransmits before the consumer saw them (see below). The inbox protects one consumer's store; if the side effect is an external call (send an email, charge a card) the store cannot make it atomic with the inbox insert, and you need the downstream API's idempotency key instead.

## Trade-offs

| Pro | Con |
| --- | --- |
| Effectively-once effects on top of plain at-least-once delivery, with no distributed transaction | Only works when the side effect and the inbox live in the same transactional store |
| Covers every duplicate source at once: retransmits, crashes, rebalances, replays | An extra indexed write per message, and a table that has to be pruned |
| Commit-after means nothing is ever lost | A duplicate older than the dedup window is applied again; the window must exceed the worst-case redelivery delay |
| Needs only a stable message ID from the producer | Producers must assign IDs before the first attempt and reuse them; an ID generated per attempt defeats the whole scheme |
| Redeliveries are cheap (one primary-key lookup) and observable (skip count) | Message order is not fixed by dedup: a retransmit can land after later messages (see per-key ordering) |

## When to use / when not to

**Use it** for any consumer whose side effect is not naturally idempotent and lives in a database you control: ledgers and payments, order state machines, counters and aggregates, chat message storage, notification fan-out bookkeeping, projections built from an event stream.

**You may not need it** when the effect is already idempotent: an upsert of "user 42's email = x", setting a status to a final value, writing a file keyed by message ID. Version checks (optimistic locking) can also stand in when every message carries the version it applies to.

**It is not enough** when the side effect is a call to another system (payment gateway, email provider). Pass the message ID on as that system's idempotency key, or record "intent" in the inbox transaction and let an outbox relay make the call.

## Real-world examples

- **Kafka** consumers choose the commit timing: `enable.auto.commit` commits on a timer, independent of processing, and manual `commitSync` after processing gives at-least-once. The **idempotent producer** (`enable.idempotence=true`, the default since Kafka 3.0) gives each producer a producer ID and per-partition sequence numbers so the broker drops retransmits. **Transactions** (`isolation.level=read_committed`, `sendOffsetsToTransaction`) make "consume, produce, commit offset" atomic, exactly-once *within Kafka*; effects in an external database still need an inbox or offsets stored in that database.
- **Amazon SQS FIFO** queues drop messages with a repeated `MessageDeduplicationId` within a fixed **5-minute** dedup window, the same retention trade-off as here. Standard queues offer no dedup at all.
- **Stripe webhooks** may be delivered more than once and sent out of order; Stripe's guidance is to record processed event IDs (`evt_...`) and skip ones already handled.
- **Inbox pattern** in microservice frameworks (MassTransit, NServiceBus, Eventuate Tram) stores processed message IDs in the service's own database alongside the business change.
- **Chat systems** (WhatsApp-style) retry until the recipient's device acks, with a client-generated message ID so the server and the receiving client both drop repeats.

## Run it

```sh
go run ./patterns/distributed/idempotent-consumer/example
go test ./patterns/distributed/idempotent-consumer/
```

The example sends 1000 payments with a 3% chance of a lost ack and 2% of a lost request per attempt (1040 records in the log, 38 payments duplicated by the producer), injects the same 23 consumer crashes into each strategy, and audits the result:

| Strategy | Exact | Lost | Duplicated | Credited vs expected 51256 |
| --- | --- | --- | --- | --- |
| commit-before | 956 | 8 | 36 | +1388 |
| commit-after | 952 | 0 | 48 | +2365 |
| inbox | 1000 | 0 | 0 | +0 |

Then it replays 10 minutes of traffic after an offset reset: a 24h window skips all 600 duplicates, a 5-minute window lets 300 through, a 1-minute window lets 540 through.

## Related patterns

- [Idempotency key](../../reliability/idempotency-key): the request/response version, where the *client* sends a key and the server caches the response. Same idea (a stable ID plus a dedup table), but synchronous, and it replays the stored response instead of silently skipping.
- [Transactional outbox](../transactional-outbox): the producer-side mirror image. The outbox makes "change state + emit event" atomic; the inbox makes "consume event + change state" atomic. The outbox relay is at-least-once, so outbox and inbox are usually deployed together.
- [Competing consumers](../competing-consumers): many workers on one queue; redeliveries after a worker dies are exactly the duplicates the inbox absorbs.
- [Dead-letter queue](../dead-letter-queue): what to do with a message whose handler keeps failing, instead of redelivering it forever.
- [Per-key ordering](../per-key-ordering): dedup says nothing about order; a retransmit can arrive after newer messages for the same key.
- [Snowflake ID](../../data/snowflake-id): a way to generate unique, roughly time-ordered message IDs on the producer without coordination.

## Further reading

- Arvind Kumar, [Duplicate Messages in WhatsApp](https://codefarm0.medium.com/duplicate-messages-in-whatsapp-system-design-interview-deep-dive-on-at-least-once-delivery-and-cecdc091fc57), on where duplicate chat messages come from under at-least-once delivery and how client message IDs remove them
- Arvind Kumar, [Kafka Consumer Processes Same Event Twice](https://codefarm0.medium.com/kafka-consumer-processes-same-event-twice-system-design-deep-dive-on-offset-management-and-1134f0507792), on offset commit timing, rebalances and idempotent processing in Kafka consumers
- Khyati Jha, [High Level System Design Day-18: WebSockets (Part 6)](https://medium.com/high-level-system-desgin-interview-prep/high-level-system-design-day-178-websockets-part-6-2be2650b7b2f), on at-least-once delivery over WebSockets with acks and deduplication in chat
