# Scenario: A Celebrity Post Floods the Push Pipeline

## The incident

A celebrity with millions of followers posts a photo. The fan-out job does what it always does: one "new post" push per follower, all appended to the notification queue. That queue also carries the pushes people are actually waiting for, such as login codes and "your order is out for delivery". The worker pool drains the queue as fast as it can, far faster than APNs and FCM accept from one sender, so the providers start answering 429. The workers retry every failure straight away, and the providers throttle them harder for it. Login codes reach phones a minute or two after they were requested, long after they have expired. Meanwhile about one device in twenty no longer has the app installed. The provider rejects those tokens as unregistered every time, the workers retry them like any other error, and they never leave the queue. Fans then pile onto the comments under the post, and each commenter's phone buzzes once for every like.

## Symptoms

- Login-code and delivery pushes arrive tens of seconds to minutes late, and support tickets about "the code never came" spike.
- Provider dashboards show mostly 429s. Push throughput drops *below* the provider's normal limit, because over-limit senders get a smaller allowance.
- Worker CPU is pegged while successful sends stay flat. Most calls go to tokens that were rejected as unregistered hours ago, or are 429 retries.
- Users complain about getting 40 separate "X liked your comment" pushes in a row.

## Root cause

1. **One queue for every kind of push.** A transactional push is enqueued behind the entire storm, so its delay equals the time to drain everything ahead of it.
2. **No rate limiting, and immediate retries.** The workers can make 2,000 calls/s, but each provider accepts 400/s. Every call past the limit is a 429, the retry in a tight loop is another 429, and the provider halves the sender's allowance for the next second because it kept going.
3. **Every error is treated as transient.** "Unregistered" is permanent, but the workers retry it forever. Pushes to dead tokens never leave the queue, so they make up a growing share of every pass and use up provider allowance that valid pushes needed.
4. **No coalescing.** Every like is its own push, which multiplies both the load and the annoyance.

## The fix

| Change | Pattern | Effect |
| --- | --- | --- |
| A queue per provider and priority lane; transactional is drained first, with a starvation limit so social still gets a share | [Fan-out feed](../../patterns/architecture/fan-out-feed) (`Dispatcher`, lanes, `WithStarvationLimit`) | Login codes skip the storm: p99 10 ms instead of 1m44s |
| A token bucket per provider, set below the provider's limit (360/s, burst 4, against 400/s) | [Fan-out feed](../../patterns/architecture/fan-out-feed) (`WithProvider`), built on [token bucket](../../patterns/reliability/rate-limiter-token-bucket) | Zero 429s, so no retries and no penalty; the storm drains in 35 s rather than 2.5 min |
| Coalesce likes per user and comment for 5 s | [Fan-out feed](../../patterns/architecture/fan-out-feed) (`WithCoalesceWindow`) | One push, "u14966 and 41 others liked your comment", instead of 42 |
| Hand pushes to workers through a queue that dead-letters permanent errors, and mark the token dead | [Dead-letter queue](../../patterns/distributed/dead-letter-queue) (`Permanent`) | Each dead token costs one call. 993 pushes are parked for the token-cleanup job, and later pushes to those tokens are dropped before they are sent |

## Run it

```sh
go run ./scenarios/notification-storm/broken
go run ./scenarios/notification-storm/fixed
go test ./scenarios/notification-storm/
```

| | Broken | Fixed |
| --- | --- | --- |
| Transactional delay p50 / p99 / max (600 pushes) | 38.3 s / 1m44s / 2m12s | 6 ms / 10 ms / 10 ms |
| 429s received | 383,580 | 0 |
| Busiest provider-second (limit 400) | 1,275 calls | 350 calls |
| Provider calls | 480,000 (the pool's full capacity for 4 min) | 20,887 |
| Pushes sent (for 33,200 notifications) | 31,661 | 19,894 (12,300 likes coalesced) |
| Most pushes to one phone / like pushes for one comment | 45 / 42 | 4 / 1 |
| 410 "unregistered" responses | 64,759 | 993 |
| Calls to tokens already known dead | 285,899 | 0 |
| DLQ depth | 0 (no DLQ) | 993 |
| Storm drain (last social push accepted) | 2m34s | 34.6 s |

The broken run keeps spinning until the 4-minute cap. By then only dead-token pushes are left, so its call counts and the known-dead-token row depend on where the cap is. The useful comparison is that it never stops. The fixed run drains slower than the providers' combined 800/s would allow: it sends at 90% of the limit, and every social push first waits out the 5 s coalescing window.

The tests assert the invariant for the fixed run (transactional p99 ≤ 1 s, no 429s and no provider-second over the limit, no call to a token already rejected as unregistered, every social push for a valid device delivered) and check that the broken run violates every part of it. They also check the fix's cost: no social push goes out before the coalescing window has passed, and every commenter gets exactly one like push, which reads "… and 41 others liked your comment". A last test runs both versions concurrently and checks that the reports are identical, so the simulation is deterministic and shares no state between runs.

**How the simulation is built.** Time advances in 10 ms ticks. Notifications are enqueued at their exact arrival times, and sends happen at the end of each tick. The seeded traffic is identical for both versions: 20,000 followers, each with one device on APNs or FCM (5% of the device tokens are stale), all getting the "new post" push at t=0; 300 of them each get 42 likes on their comment within the first 4 s; and 10 transactional pushes a second arrive for the first 60 s, sent to people currently using the app, whose tokens are therefore valid. Each provider accepts 400 calls per second from the sender. Every call counts against that allowance, including rejected ones, and a sender that drew a 429 in one second gets half the allowance in the next. Beyond the allowance a call gets 429; a call within it for a stale token gets 410. The worker pool can make 2,000 calls/s in total. Broken workers retry a failure twice on the spot, then put the push back at the tail of the single FIFO. In the fixed version the `Dispatcher` releases pushes within its token buckets, and each push then goes through a `deadletterqueue.Queue`, where the handler maps 410 to `Permanent` and anything else to a retryable error (which never happens here, because the buckets stay under the limit).

**Simplifications.** Real providers don't publish a fixed per-second quota or a "halve it next second" penalty. That rule stands in for the real behaviour: throttling gets worse the longer a sender ignores it. One device per user, no network latency and no provider-side errors other than 429 and 410. In production the token registry would be a database table. The DLQ would feed a job that deletes stale tokens rather than redriving them, and the rate limit would be shared across sender hosts (or split between them) rather than held in one process. The "new post" pushes also carry a collapse key, so they wait out the coalescing window too. That keeps every social entry's ready time in the same order as the queue, which the `Dispatcher`'s per-lane FIFO relies on.

## Interview follow-ups

- *Why not just make the storm wait in a lower-priority queue and keep retrying 429s?* Priority fixes the login codes but not the provider. A sender that keeps hitting 429 gets throttled harder, so the storm drains slower. Sending under the limit is what makes throughput go up.
- *What stops the social lane from starving if transactional traffic spikes?* The starvation limit: after 20 consecutive sends that passed over a sendable social push, the social lane gets the next one. Transactional traffic can never take more than about 20 of every 21 sends.
- *How do you pick the coalescing window?* From how stale a social push can be before it stops being worth sending, and how many pushes per hour a user tolerates. Seconds to a minute for likes, zero for messages and anything transactional. The window is a floor on delay, not just a cap. In this run no social push went out sooner than 5 s.
- *Why park dead tokens in a DLQ instead of just deleting them?* The DLQ records why and when each one failed, and its depth can be alerted on. A sudden jump in 410s usually means a bad token upload or a certificate problem rather than real uninstalls, and you want to be able to redrive the pushes once that is fixed.
- *The celebrity has 50 million followers, not 20,000. What changes?* Fan-out becomes a background job that pages through the follower list and enqueues in batches. The send rate per provider is the real ceiling, so you may switch the biggest accounts to fan-out-on-read (followers see the post in the feed, and only a sample gets a push) and spread the pushes over minutes on purpose.

## Further reading

- Arvind Kumar, [Instagram Notification Storm](https://codefarm0.medium.com/instagram-notification-storm-system-design-deep-dive-on-fanout-architecture-and-push-notification-0e8e3d86841c), the interview scenario this walkthrough is based on
