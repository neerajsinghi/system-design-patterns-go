# Scenario: Payment Deducted, but No Order

## The incident

An online shop's checkout runs four steps in one request handler: create the order, reserve the stock, charge the card, confirm the order. The payment call is synchronous. On a busy evening the payment provider slows down. Charges still go through, but some responses arrive after the order service's 3-second timeout. The order service treats a timeout as a failure and shows "Payment failed, please try again". The customer has been charged. Some customers give up and file a chargeback. Others press Pay again, get a second charge, and this time an order. A few deploys that week also killed order-service pods mid-checkout, with the same result. Meanwhile, fulfilment ships an order that support can't find in the database, and some orders that are in the database never ship, because fulfilment never heard about them.

## Symptoms

- "I was charged but have no order" tickets, clustered around payment-provider latency spikes and order-service restarts.
- Customers charged twice for one basket, roughly one per retried checkout that had already been charged.
- Orders stuck in `pending` forever, with their stock still reserved.
- Fulfilment and the order database disagree in both directions: shipments for orders that don't exist, and confirmed orders that never ship.

## Root cause

1. **A timeout is not a failure.** When the charge request times out, the charge may or may not have happened. Treating it as "failed", with nothing to reconcile or undo, leaves money taken and no order.
2. **No memory across a crash.** The checkout's progress lives only in the handler's stack. If the process dies after the charge, nothing knows a half-finished checkout exists, so nothing finishes it or undoes it.
3. **The payment API isn't idempotent.** Every press of Pay is a new request, a new order and a new charge. The provider has no way to tell a retry from a second purchase.
4. **Dual write.** The handler publishes "order confirmed" from inside the database transaction and then commits. If the commit aborts, the event is already out (an orphan). If the publish fails, the error is logged and the order commits anyway (a missing event).

## The fix

| Change | Pattern | Effect |
| --- | --- | --- |
| Run checkout as an orchestrated saga with a compensation per step (cancel order, release stock, refund) | [Saga (orchestration)](../../patterns/distributed/saga-orchestration) | A checkout that can't finish is undone, not abandoned: no order means no charge |
| Persist the saga's progress after every step, and resume unfinished sagas on retry or after a restart | [Saga (orchestration)](../../patterns/distributed/saga-orchestration) | A crash delays a checkout; it no longer loses it |
| Charge with an idempotency key derived from the checkout, and retry timeouts with the same key | [Idempotency key](../../patterns/reliability/idempotency-key) | Retries, client re-presses and resumes all get the original charge back |
| Write the order and its "confirmed" event in one transaction; a relay publishes it | [Transactional outbox](../../patterns/distributed/transactional-outbox) | An event exists if and only if the order committed, and it is delivered eventually even if the broker is down |

Two details matter. First, a timed-out charge might have succeeded, but the orchestrator only compensates steps that succeeded. So the refund is attached to a separate `assign-payment-key` step that runs just before the charge and always succeeds. If anything after it fails, the saga refunds whatever was charged under the key, and does nothing if nothing was. Second, once the card is charged, the saga goes forward: an aborted confirm transaction is retried, not compensated, because refunding a good payment over a transient database error would be worse than retrying.

## Run it

```sh
go run ./scenarios/charged-but-no-order/broken
go run ./scenarios/charged-but-no-order/fixed
go test ./scenarios/charged-but-no-order/
```

Both runs replay the same 500 checkouts with the same injected faults: 22 payment timeouts after charging (1-2 lost responses), 8 payment outages (4 lost responses in a row), 26 crashes (after reserving or after charging), 29 broker outages, 13 aborted confirm transactions, and 18 declined cards. The other 384 are clean.

| | Broken | Fixed |
| --- | --- | --- |
| Checkouts breaking the invariant | 87 | 0 |
| Charged, but no order | 30 | 0 |
| Charged more than once | 38 | 0 |
| Orphan events / missing events | 13 / 29 | 0 / 0 |
| Events published before their order committed | 434 | 0 |
| Orders stuck pending / stock units leaked | 97 / 97 | 0 / 0 |
| Customers who pressed Pay again | 47 | 20 |
| Orders confirmed | 450 | 474 |
| Sagas resumed after a crash | 0 | 26 (20 by the customer's retry, 6 by the recovery sweep) |
| Charges answered from the idempotency store | 0 | 62 |
| Confirm transactions retried | 0 | 13 |
| Sagas compensated / refunds issued | 0 / 0 | 26 / 8 |
| Clean checkout response time | 283ms | 296ms |
| Mean / max first response | 441ms / 3.02s | 648ms / 9.71s |
| Event lag after commit (mean / max) | none (published before commit) | 1.12s / 11.7s |

The tests check that the broken run breaks the invariant in every way listed above. They check that the fixed run holds it for every checkout, with no unfinished sagas or leaked stock, and that every checkout ends either confirmed or compensated. They also check that each mechanism did its job: one resume per crash, one confirm retry per aborted commit, replays from the idempotency store, relay retries during broker outages, and compensations equal to declines plus outages. A cost test pins down the price of the fix: one refund per payment outage, slower checkouts, and events that arrive a relay pass after commit. Two more tests check that both runs see identical faults, and that runs are deterministic and independent when run concurrently.

The cost is visible to customers. The 8 customers caught in a payment outage see a charge on their statement and then a refund, with no order. That's better than broken, where they have a charge and no order at all, but they still have to check out again. Checkout is slower: a clean checkout pays 13ms for persisting the saga row after each step. A timed-out charge is retried twice more instead of failing at the first timeout, so the slowest checkout (a payment outage) takes 9.7s against broken's 3s. Fulfilment hears about orders about a second later, because events wait for the relay. The 11.7s worst case is an event whose relay pass hit a down broker and then waited for the next pass, which in this single-threaded simulation runs only after the next customer's slow checkout. Fixed confirms 24 more orders than broken because it finishes crashed, timed-out and aborted checkouts that broken gave up on. Fewer customers press Pay again (20 against 47) because fixed only answers ambiguously when it crashes. Timeouts and aborted commits are handled inside the saga.

**How the simulation is built.** 500 checkouts with amounts between $10 and $199 come from a seeded generator, one every 2 simulated seconds. Each gets at most one fault, and 60% of customers press Pay again 5 seconds after an ambiguous error (a timeout, a 500 or a connection reset), but never after a definitive one like "declined". Every call advances a simulated clock: 4ms per database write, 20ms for inventory, 250ms for a charge, 3s for a payment timeout. The payment service always makes the charge before the response is lost, which is the dangerous case. A crash is a panic raised after a step's effect but before its completion is persisted, which unwinds out of `sagaorchestration.Orchestrator.Run` without compensating, just as a dead process would. Resuming calls `Run` again with the same steps. Each step first checks the persisted saga row and skips itself if already done, and the steps that might run twice (reserve, charge) are idempotent. A customer's retry finds the saga by checkout ID and resumes it. If they don't come back, a recovery sweep 30 seconds later does. Both designs use `transactionaloutbox.DB` for orders. Broken publishes from inside `WithTx`, while Fixed uses `Tx.AddOutbox` and a `Relay` that polls once a second. A `DedupConsumer` stands in for fulfilment. Charges go through an `idempotencykey.Store` keyed by `charge-<checkout>`, with a fingerprint of the checkout and amount. Everything runs in one goroutine.

**Simplifications.** The saga row, the idempotency store and the payment ledger are in-memory maps that survive the simulated crash. In production the saga state lives in the order database, and the payment provider holds the idempotency record. A pending order exists only in the saga row; the orders table holds confirmed orders, so "confirmed" means "has a row". Compensations never fail, and a crash never lands during a compensation. Stock is unlimited, so reservations never fail. The recovery sweep runs per checkout rather than on a timer, and the outbox relay runs after each customer's answer rather than on a wall-clock tick, so event lag depends on how slow the next checkout is. Payment outages are per customer, not provider-wide, and only one request is in flight at a time, so a customer can't press Pay while their first attempt is still running. With concurrency, the retry would find the saga in progress and should wait for it, or get a 409.

## Interview follow-ups

- *Why not just make the payment timeout longer?* It narrows the window but never closes it. Something still times out eventually, and a crash doesn't care about timeouts. The real fix is that an ambiguous outcome is resolved with a retry under the same key, or a lookup of the charge by key, never by guessing "failed".
- *Why is the refund attached to a step before the charge, not to the charge?* The orchestrator compensates steps that completed. A charge that timed out didn't complete as far as the orchestrator knows, but it may have taken the money. Hanging the refund on an always-successful step just before it means any failure from the charge onwards refunds whatever was taken under the key. The payment-intent APIs offered by card processors have the same shape: create the intent first, then confirm it, and cancel or refund the intent on failure.
- *Why retry the confirm instead of compensating?* The charge is the saga's pivot. Before it, undoing is cheap (release stock, cancel the row). After it, undoing means a refund the customer can see. Steps after the pivot should be retriable and retried until they succeed, with compensation kept for failures that will not go away.
- *What if the customer presses Pay while the first attempt is still running?* The checkout ID is the idempotency key for the whole checkout. The second request finds a saga in progress and either waits for its result or gets "still processing" (a 409). It must never start a second saga.
- *Could the downstream services just read the order database instead of events?* They could poll, but then every consumer couples to the order schema and polls it. The outbox is the order service polling its own table once, on everyone's behalf. Change data capture on the order table is the same idea without the extra table.
- *Choreography or orchestration here?* Orchestration. Checkout is a short, ordered flow with a pivot and money involved, and it's much easier to persist, resume and audit one saga row than to reconstruct state from events scattered across four services.

## Further reading

- Arvind Kumar, [Payment Deducted but Order Failed](https://codefarm0.medium.com/payment-deducted-but-order-failed-system-design-deep-dive-on-the-saga-pattern-and-distributed-025b27cb2e0a), the interview scenario this walkthrough is based on
