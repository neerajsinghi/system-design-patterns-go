# Scenario: The Consumer Credits the Same Order Twice

## The incident

A loyalty service reads order events from a Kafka-style topic. For each order it credits the customer's points and sends a "you earned points" email. On a Tuesday afternoon the team rolls out a new build. Within the hour, support hears from customers who got two emails for one order and see double points in their account. Nobody touched the loyalty code in that release. While the team is still looking into it, a second alert fires: consumer lag on the topic is climbing and won't come down. One instance is in CrashLoopBackOff. Every time it restarts, it dies on the same event, and every restart produces another batch of duplicate emails, this time for customers who ordered minutes ago.

## Symptoms

- Duplicate credits and emails, clustered around each instance restart in the rolling deploy.
- Every duplicate is an event processed in the few seconds before an instance was killed.
- After the deploy, one partition's committed offset stops moving and its lag keeps growing. Lag on the other partitions owned by the same instance grows with it.
- The crashing instance's logs show the same offset and the same parse error on every restart. Some customers on that instance have now been credited three, four or five times for one order.

## Root cause

1. **The offset is committed after the side effect, in a separate write.** The consumer auto-commits its position every 5 seconds. A deploy kills an instance with up to 5 seconds of events already credited but not committed. The partitions are reassigned, the new owner starts from the last committed offset, and those events are credited again. Nothing remembers that they were already applied.
2. **A poison event crashes the consumer instead of failing the event.** One order event has no currency field, and the points calculation fails. The exception escapes the poll loop, the process exits, and Kubernetes restarts it. The restarted consumer resumes from the last commit, which is before the poison event. So it re-credits everything between that commit and the poison event, reaches the poison event, and crashes again. The partition can't get past it, and each lap of the crash loop adds another round of duplicates.
3. **Each crash is a rebalance.** A crash, and the restart that follows, both make the group stop and reassign partitions. The whole group pauses twice per lap, so lag builds on partitions that never saw the bad event.

## The fix

| Change | Pattern | Effect |
| --- | --- | --- |
| Credit the points and insert the event ID into an inbox table in one transaction; skip events already in the inbox | [Idempotent consumer](../../patterns/distributed/idempotent-consumer) (inbox) | A replayed event is a no-op, however it got replayed: deploy, rebalance or crash. Every event is credited exactly once |
| Catch handler errors and hand the event to a retry queue instead of crashing; commit past it | [Dead-letter queue](../../patterns/distributed/dead-letter-queue) | The partition keeps moving. A transient failure (an email 503) succeeds on a later attempt |
| After 3 failed attempts, park the event in a DLQ and alert on DLQ depth; redrive it once the parser fix ships | [Dead-letter queue](../../patterns/distributed/dead-letter-queue) (max attempts, redrive) | The poison event waits in the DLQ instead of blocking 200 events behind it. Redriven with its original ID, it is credited once |

The two fixes protect each other. The retry queue and the redrive are both at-least-once (the redrive may race a late retry, the retry queue may be fed twice by a replayed offset), so they only stay safe because the inbox makes a second application harmless.

## Run it

```sh
go run ./scenarios/consumer-double-processing/broken
go run ./scenarios/consumer-double-processing/fixed
go test ./scenarios/consumer-double-processing/
```

| | Broken | Fixed |
| --- | --- | --- |
| Events applied more than once | 115 (204 extra credits, 11,470 points; worst event credited 5x) | 0 |
| Events never applied | 0 | 0 |
| Redeliveries of already-applied events | 204 (91 after deploy kills, 113 after crashes) | 127 (all after deploy kills, all skipped by the inbox) |
| Consumer crashes / rebalances | 6 (4 on the poison event, 2 on the email 503) / 15 | 0 / 4 |
| Max lag / final lag | 637 / 0 | 190 / 0 |
| Peak events stuck behind the poison event | 209 | 0 |
| Poison event credited at | t=203s, after the fix restarts the crash-looping instance | t=200s, on redrive |
| DLQ peak / DLQ at end / redriven | n/a | 1 / 0 / 1 |
| Inbox rows at end | 0 | 3,585 (one per event) |

Broken loses nothing. Committing after the side effect is at-least-once, so its failure is duplication, not loss. It does catch up once the parser fix ships at t=200, so its final lag is 0 too, but by then the crash loop has credited some events five times and held 209 events behind the poison event for 80 seconds. Fixed's lag peaks at the parser-fix deploy, which restarts all three instances at once. In both runs, steady-state lag sits at a few dozen events because offsets are committed only every 5 seconds. The first rolling deploy replays the same 63 events in both runs. The difference in deploy replays (127 vs 91) comes from the parser-fix deploy at t=200: in Fixed it kills three instances holding 4 seconds of uncommitted work each (64 replays). In Broken the group had just rebalanced after the crash at t=199, which committed the survivors' positions, so there was less to lose (28 replays).

The tests check that Broken double-credits events, replays after both deploys and crashes, crash-loops on the poison event and stalls its partition. They check that Fixed credits every event exactly once, ends with zero lag, skips every redelivery in the inbox, never crashes, and dead-letters and redrives only the poison event. The transient event recovers within its retry budget. A cost test asserts that the inbox holds one row per event and that the poison event's points still wait for the fix. A last test runs both designs concurrently and checks they're deterministic.

**How the simulation is built.** One simulated clock ticks 300 seconds. A seeded generator produces 0-30 order events per second for 4 minutes (3,585 in all), keyed by customer, on an `idempotentconsumer.Log` with 6 partitions. Three instances each own two partitions and process up to 20 events per partition per second, then auto-commit every 5 seconds. The rolling deploy kills instances 0, 1 and 2 at t=64, 79 and 94, which is 4 seconds after a commit, so each kill lands after the side effect and before the commit (`idempotentconsumer.AfterEffect`). A rebalance stops the whole group for 3 seconds. Surviving instances commit their positions first, as Kafka's auto-commit does on revoke. Killed or crashed instances lose theirs. The poison event arrives at t=120 on partition 3 (instance 0). A second event, at t=150 on partition 5, fails twice with an email 503 and then succeeds. In Broken, a handler error crashes the instance, and Kubernetes restarts it after 10s, 20s, then 40s. In Fixed, the error sends the event to a `deadletterqueue.Queue` (3 attempts, 2-8s backoff), drained every second by a retry worker that applies events through the same inbox. At t=200 the parser fix deploys: every instance restarts and, in Fixed, the DLQ is redriven. Side effects run through `idempotentconsumer.Store.Apply`, with dedup off in Broken and on in Fixed. The store's per-ID application count is what "double-applied" and "lost" are measured from.

**Simplifications.** The pattern's own `Consumer` commits after every message, so a crash replays at most one. Real clients commit in batches, so the scenario drives the `Log` and `Store` directly with a 5-second commit interval. A crashed instance's partitions aren't reassigned to the survivors while it's down. In a real eager rebalance they would be, and the poison event would then crash each survivor in turn. Points and the email are a single store write here. A real email is outside the database transaction, so the inbox also has to cover it, for example via an outbox the mailer reads. Forwarding to the retry queue isn't transactional with the offset commit either. A local "already forwarded" set stands in for an idempotent producer.

## Interview follow-ups

- *Why not just commit the offset before processing?* Then a crash between the commit and the credit loses the event: at-most-once. Losing a customer's points is usually worse than crediting them twice. The way out is not reordering the two writes but making the second application harmless.
- *Can Kafka's exactly-once semantics fix this?* Only when the side effect is another Kafka write. Transactions commit the consumed offsets and the produced records atomically. A database credit and an email are outside that transaction, so you still need an inbox, or to store the offset in the same database transaction as the credit and seek to it on assignment.
- *How long do you keep inbox rows?* Longer than the longest plausible replay: an offset reset, a redrive from a DLQ that sat for a week, a consumer restored from an old commit. Prune by age, and alert if a redrive is older than the window. Here the window is a week. The inbox grows by one row per event until pruned, which is the main cost of the fix.
- *Doesn't moving the poison event to a retry queue break per-customer ordering?* Yes. Customer 42's later orders are credited before the one that failed. For points that's fine, because addition commutes. For something like order-state transitions it isn't. There you'd park the whole key (`WithParkKey` on the DLQ) or block only that key while the rest of the partition moves.
- *Should the poison event even get 3 attempts?* If the failure is clearly a decode or validation error, wrap it in `deadletterqueue.Permanent` and park it on the first failure. The retries are for errors that might clear, like the email 503. In this incident the error came from a bug deep in the points calculation, so the consumer couldn't tell it apart from a transient failure.
- *What should page someone?* DLQ depth above zero, and consumer lag growing for several minutes. Duplicates never show up as errors at all, and a crash loop is easy to read as "restarting, will recover". Lag and DLQ depth are what reveal both.

## Further reading

- Arvind Kumar, [Kafka Consumer Processes Same Event Twice](https://codefarm0.medium.com/kafka-consumer-processes-same-event-twice-system-design-deep-dive-on-offset-management-and-1134f0507792), the interview scenario this walkthrough is based on
