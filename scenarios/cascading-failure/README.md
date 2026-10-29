# Scenario: One Slow Service Takes Down Every Product Page

## The incident

An online shop's product page is assembled by a product service. For each page it calls catalog (title, images), pricing and inventory, which the page cannot do without, and a recommendations service for the "you might also like" strip, which it can. On a busy afternoon one of the four recommendations instances starts answering in about three seconds instead of forty milliseconds; a GC-bound JVM, nothing that a health check notices. Within seconds product pages start failing, and not a quarter of them: more than half. Pages that were never going to hit the bad instance fail too, because they cannot get a worker. The API gateway sees timeouts and retries each one twice, so the product service gets more traffic just when it can handle less. An hour later a bad recommendations deploy makes the whole service slow, and the shop is effectively offline for two minutes, even though catalog, pricing and inventory are perfectly healthy throughout.

## Symptoms

- Product service worker pool at 100% busy, with almost all of those workers waiting on recommendations.
- Error rate far above the slow instance's share of traffic: one bad instance out of four fails 55% of pages.
- Requests per page at the product service rise from 1.0 to 2.2, then 3.0: the gateway's retries.
- Catalog, pricing and inventory dashboards are green the whole time.
- The product service keeps working on requests the gateway abandoned long ago.

## Root cause

1. **No timeouts.** A call to a slow recommendations instance holds its worker for as long as the instance takes. At 400 pages/s, a quarter of them taking 3 s needs about 300 workers just for those calls, against a pool of 200.
2. **One shared pool.** Workers are not reserved for anything, so once they are all waiting on recommendations, the pages that only need catalog, pricing and inventory queue behind them and fail too. An optional dependency has become a hard one.
3. **Nothing stops the calls.** With no breaker, every page keeps calling a recommendations service that has been failing for minutes, and round robin keeps sending a quarter of traffic to the one bad instance.
4. **Retries amplify.** The gateway retries timeouts and 503s. The product service has no deadline, so it finishes the original request anyway (13,000 times in this run) as well as the retry.

## The fix

| Change | Pattern | Effect |
| --- | --- | --- |
| The gateway's 2 s budget is propagated; each hop gets a slice of what is left (300 ms cap for core dependencies, 150 ms for recommendations), and a queued request whose deadline has passed is dropped instead of started | [Timeout](../../patterns/reliability/timeout) (deadline propagation) | A worker can be held for at most ~1 s; nobody works on a request the gateway has given up on |
| At most 40 of the 200 workers may be inside a recommendations call at once | [Bulkhead](../../patterns/reliability/bulkhead) | Even a recommendations meltdown leaves 160 workers for catalog, pricing and inventory |
| A circuit breaker around recommendations opens after 20 consecutive failures, probes every 5 s | [Circuit breaker](../../patterns/reliability/circuit-breaker) | When the whole service is slow, pages stop waiting on it at all |
| Recommendations are balanced with passive outlier detection (5 consecutive errors ejects an instance for 30 s, doubling on repeat) | [Outlier detection](../../patterns/reliability/outlier-detection) | The one bad instance is taken out of rotation; the breaker stays closed because the service as a whole is fine |
| A failed or skipped recommendations call renders "popular items" | [Fallback](../../patterns/reliability/fallback) (static default) | The page is degraded, not broken |

## Run it

```sh
go run ./scenarios/cascading-failure/broken
go run ./scenarios/cascading-failure/fixed
go test ./scenarios/cascading-failure/
```

Page outcomes, per phase (pages counted by when they arrived at the gateway):

| Phase | Broken: ok / degraded / failed | Fixed: ok / degraded / failed | Peak workers, broken → fixed | p99, broken → fixed | Requests per page, broken → fixed |
| --- | --- | --- | --- | --- | --- |
| Baseline (0-30 s) | 99.98% / 0% / 0.02% | 98.90% / 1.10% / 0% | 34% → 32% | 290 ms → 215 ms | 1.00 → 1.00 |
| Phase 1: one instance slow (30-150 s) | 44.58% / 0% / **55.42%** | 98.87% / 1.13% / 0% | **100%** → 36% | 4 s → 214 ms | 2.17 → 1.00 |
| Phase 2: all of recommendations slow (150-270 s) | 0.17% / 0% / **99.83%** | 0% / **100%** / 0% | **100%** → 36% | 4 s → 152 ms | 3.00 → 1.00 |
| Recovery (270-300 s) | 98.84% / 0% / 1.16% | 87.07% / 12.93% / 0% | 100% → 34% | 398 ms → 215 ms | 1.02 → 1.00 |

Worst-phase availability is 0.17% broken and 100% fixed. The broken product service finishes 13,080 requests after the gateway has given up on them; the gateway's open connections peak at 287 against 73 when fixed. In the fixed run, recommendations are skipped because the breaker is open 49,422 times, time out 853 times, and are refused by the bulkhead 29 times; recs-2 is ejected 3 times and the breaker opens 24 times (once per 5 s probe interval in phase 2).

Three numbers need explaining. Fixed degrades 1.1% of pages even in the healthy baseline: 1% of healthy recommendation calls take 200-400 ms, which is past the 150 ms hop timeout. That is a deliberate trade, a slightly worse strip for a faster page; a 400 ms timeout would remove it and raise p99. Phase 1 costs only 0.03 points more than the baseline, because outlier detection ejects recs-2 after five timeouts and the only extra degraded pages are the few routed to it before each ejection and after each re-admission. And recovery is 13% degraded because the breaker stays open until its next probe, up to 5 s after recommendations recover.

The tests assert the invariant for the fixed version (every phase at least 99.9% available, degraded counted as available; the worker pool never fully busy; p99 under the gateway timeout; the gateway never needing to retry) and check that the broken version exhausts its pool, misses the target in phases 1 and 2, fails far more than the slow instance's 25% share, amplifies phase 2 traffic at least 2× through retries, and does wasted work. They check that phase 1 costs at most half a point of extra degraded pages over the baseline (the instance is ejected rather than the page degraded) and that the cost of the fix in phase 2 is real: no failed pages, but at least 99% of pages without personalised recommendations. A last test runs both designs concurrently and checks that they reproduce the shared results exactly and saw identical traffic.

**How the simulation is built.** It is a discrete-event simulation on a simulated clock, running five minutes of traffic in well under a second. Page requests arrive at the gateway as a Poisson stream at 400/s from a seeded generator. The gateway gives each attempt 2 s, and retries a timeout or a 503 up to twice, immediately. The product service has 200 workers and a backlog of 100; when both are full it answers 503. A worker is held from when it picks a request up until it writes the response: catalog, pricing and inventory are called one after another (10-30 ms each, a 0.5% tail of 80-150 ms), then recommendations (25-60 ms, a 1% tail of 200-400 ms; 2.5-3.5 s on recs-2 in phase 1; 3-5 s on every instance in phase 2), then 5 ms of rendering. Every latency is drawn from a PCG seeded by the page, attempt and dependency, so a given call takes the same time in both designs, whatever else the design did. Both designs route recommendations through the same [outlier-detection](../../patterns/reliability/outlier-detection) `Pool`: the broken one with both detectors switched off, which is plain round robin. All five patterns are the real packages: the breaker and the outlier pool take the simulated clock; the deadline maths is `timeout.HopPolicy.Budget`, which is a pure function of the deadline and "now", so it is fed simulated time; the bulkhead is acquired with no wait queue and released when the simulated call ends; and the fallback's primary returns the simulated call's result.

**Simplifications.** Calls are sequential; a real product service would fan out to the four dependencies in parallel, which shortens pages but does not change the arithmetic of workers held by a slow call. The deadline travels as a value on the request rather than in the `X-Request-Timeout-Ms` header (`timeout.WithHop` and `FromHeader` use real-time contexts). A timed-out recommendations call is treated as cancelled, so it frees its worker and its bulkhead slot at the timeout; in a real system the slow instance may keep working on it. The gateway is modelled only as attempts and open connections, not as a resource that can itself run out; in production its connection pool and threads are the next thing to fall over. The fallback is a static "popular items" list; no stale tier, since personalised recommendations are per shopper.

## Interview follow-ups

- *Why didn't a timeout alone fix it?* It does most of the work: a 150 ms recommendations timeout caps the worker time a slow call can take. But without the breaker, in phase 2 every page either waits the full 150 ms or is turned away by the bulkhead, and without outlier detection a quarter of pages lose their recommendations for the whole of phase 1. The bulkhead covers the case where the timeout is set too generously.
- *Why does the breaker not open in phase 1?* Three of four instances are fine, so failures are never consecutive; the breaker sees the service as healthy, which it mostly is. Tripping it would punish the three good instances. Per-instance problems need per-instance detection: outlier ejection, with a cap on how many instances can be ejected so a service-wide problem does not eject everything.
- *How do you choose the timeouts?* From the dependency's latency distribution and the caller's budget: start near the dependency's p99.9 and check that the sum of hop caps fits inside the deadline the caller propagates. The baseline here shows the cost of setting it near p99 instead.
- *Should the gateway stop retrying?* It should retry less, and only within a budget (see [retry budget](../../patterns/reliability/retry-budget)), and it should not retry a deadline it has already spent. In the fixed run its retries never fire, because the product service always answers in time; the dangerous combination was retries plus a service with no deadline.
- *How do you know the degraded mode is acceptable?* Ask the business, then measure it: count degraded pages separately (this simulation does), alert on them, and test the fallback path regularly so it works on the day it is needed.

## Further reading

- Arvind Kumar, [One Microservice Failure Takes Down Entire Platform](https://codefarm0.medium.com/one-microservice-failure-takes-down-entire-platform-system-design-deep-dive-on-circuit-breakers-71159bd0276a), the interview scenario this walkthrough is based on
