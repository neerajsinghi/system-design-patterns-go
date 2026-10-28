# Scenario: The Cache Takes Down the Database

## The incident

A product catalogue sits behind a cache with a 60-second TTL. A deploy goes out and a warm-up job fills all 1,000 product pages so the new servers start hot. Everything is fine for exactly one minute. Then the database's connection count spikes, queries start timing out, product pages return errors, and the on-call engineer sees that the cache hit ratio has dropped to almost zero. The incident repeats a minute later, and again after that, each time a little smaller.

## Symptoms

- Database load arrives in periodic spikes exactly one TTL apart, starting one TTL after the deploy.
- During a spike the cache hit ratio collapses, even though nothing was evicted.
- The same key is queried many times in the same second.
- Errors last several seconds after each spike, because failed loads leave keys uncached and they are retried.

## Root cause

Two problems compound:

1. **Synchronised expiry.** Every key was written in the same second with the same TTL, so every key expires in the same second. 1,000 keys need reloading at once against a database that can serve 300 queries/s.
2. **No request coalescing.** Each product is read 5 times a second. Under load a query takes about a second, so all 5 reads of an expired key miss and query the database, a herd per key. 1,000 keys × 5 reads = 5,000 queries in one second.

Failed queries don't populate the cache, so the next second repeats the herd for every key that failed, and the spike decays over several seconds instead of clearing.

## The fix

Use [refresh-ahead](../../patterns/data/refresh-ahead), which combines three defences:

| Defence | What it fixes |
| --- | --- |
| **TTL jitter** (up to 10% shorter) | Keys written together now expire across a 6-second window, and drift further apart every cycle |
| **Probabilistic early refresh** (XFetch) | A hot key is refreshed by one reader *before* it expires, so readers never wait on the database |
| **Request coalescing** | Concurrent misses for one key share a single query (also in [cache-aside](../../patterns/data/cache-aside)) |

## Run it

```sh
go run ./scenarios/cache-stampede/broken
go run ./scenarios/cache-stampede/fixed
go test ./scenarios/cache-stampede/
```

| | Broken | Fixed |
| --- | --- | --- |
| Busiest second after warm-up | 5,000 queries | ~150 queries |
| Seconds over the 300 q/s capacity | 16 | 0 |
| Reads that waited on the database | ~49,000 | 0 |
| Reads that failed | ~39,000 | 0 |

The test asserts the invariant (the database stays within capacity and no read fails) and checks that the broken version violates it.

**How the simulation is built.** Simulated time advances one second per tick; every product is read 5 times per tick. The database counts queries per second and rejects anything past 300. In the broken run, query results land at the end of the tick (modelling a one-second query under load), which is what lets every read in that second miss. The fixed run's reads are sequential, so coalescing has nothing to wait for; with real concurrency it produces the same one-query-per-key result, which the refresh-ahead package's own tests check.

## Interview follow-ups

- *Would a longer TTL fix it?* No, it only moves the spike. Any fixed TTL synchronises keys written together.
- *Why not warm the cache with staggered TTLs instead?* That helps the first cycle, but jitter at write time keeps keys apart on every later cycle too, including after restarts.
- *What if the database is already down when keys expire?* Then serve the stale value: see [serve-stale](../../patterns/reliability/serve-stale).
- *What if one key is so hot that even one query per TTL per server is too much?* Add an in-process L1 cache ([two-tier cache](../../patterns/data/two-tier-cache)), or see the [hot short link](../hot-short-link) scenario.
- *How would you detect this in production?* Alert on database QPS periodicity, on cache hit ratio dips, and on duplicate queries per key per second.

## Further reading

- Arvind Kumar, [Redis Cache Causes Production Outage](https://codefarm0.medium.com/redis-cache-causes-production-outage-system-design-deep-dive-on-cache-stampede-ttl-jitter-and-0e44d9366c82), the interview scenario this walkthrough is based on
- Vattani, Chierichetti and Lowenstein, *Optimal Probabilistic Cache Stampede Prevention* (VLDB 2015)
