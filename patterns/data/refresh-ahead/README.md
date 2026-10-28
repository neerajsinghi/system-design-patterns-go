# Refresh-Ahead (TTL Jitter and Probabilistic Early Refresh)

## Problem

TTL caches have a cliff: the moment an entry expires, the next reader pays the full database latency, and the database sees a burst of work. Two common situations turn that cliff into an outage:

- **Synchronised expiry.** A deploy, a restart or a warm-up script fills thousands of keys in the same second with the same TTL. Exactly one TTL later, they all expire together and the database takes thousands of loads in one second. It happens again every TTL.
- **Hot-key expiry.** A single very hot key expires and every in-flight request misses. [Cache-aside](../cache-aside) coalesces those misses into one load, but all of those readers still *wait* for it, so p99 latency spikes every TTL.

## Solution

Refresh before the cliff, and make sure keys don't share a cliff.

1. **TTL jitter.** Shorten each entry's TTL by a random fraction (here up to `f`, e.g. 10%). Keys written together now expire across a window, and the window keeps widening on later cycles. Jitter only ever shortens the TTL, so the configured TTL stays the hard bound on staleness.
2. **Probabilistic early refresh (XFetch).** On every cache hit, the reader rolls a die: refresh now if

   ```
   now − δ · β · ln(rand()) ≥ expiry
   ```

   where `δ` is how long the value took to recompute last time and `β` (default 1) tunes eagerness. `−ln(rand())` is exponentially distributed, so the refresh probability is tiny far from expiry and rises smoothly as expiry approaches, faster for expensive values. The lucky reader refreshes the entry; everyone else keeps getting the cached value. A hot key is therefore refreshed by one reader *before* it expires, and no reader ever waits on a miss.

The XFetch rule is proven optimal for stampede prevention in Vattani, Chierichetti and Lowenstein, *Optimal Probabilistic Cache Stampede Prevention* (VLDB 2015). It needs no locks, no background threads and no coordination between servers.

## Diagram

```mermaid
flowchart TD
    G[Get key] --> E{entry cached<br/>and not expired?}
    E -- no --> L{load already<br/>in flight?}
    L -- yes --> W[wait for that load]
    L -- no --> D[load from DB,<br/>store with TTL × (1 − jitter·rand)]
    E -- yes --> X{XFetch:<br/>now − δ·β·ln(rand) ≥ expiry?}
    X -- no --> H[return cached value]
    X -- "yes (one reader)" --> R[reload now, store,<br/>return new value]
    R -. "if the reload fails" .-> H
```

## How this implementation works

- `New(loader, ttl, opts...)` returns a `Cache[K, V]`. Options: `WithJitter(f)`, `WithEarlyRefresh(beta)`, `WithClock`, `WithRand` (both injectable so tests are deterministic) and `WithLoadCost`.
- Each entry stores its value, its expiry and `delta`, the recompute time of the load that produced it. `delta` is measured with the injected clock around the loader call; `WithLoadCost` overrides it when the clock is simulated or the cost is known from metrics.
- `Get` holds the mutex only to inspect state. On a fresh hit it runs the XFetch test, but only if no load for that key is already in flight, so a hot key gets one early refresh, not hundreds. The refreshing caller loads synchronously and returns the new value; if the refresh fails, it returns the still-valid cached value and the error is swallowed. The entry isn't stale yet.
- On a miss, concurrent readers share one load via an in-flight `call`, exactly as in [cache-aside](../cache-aside). Errors are returned to every waiter and never cached.
- `Invalidate` deletes the entry and marks any in-flight load as stale, so a load that started before a write can't cache the pre-write value.
- `Stats` separates `Misses` (reads that waited) from `EarlyRefreshes` (loads nobody waited for), which is the number that matters.

**Simplification.** The early refresh runs on the reader's goroutine, as in the paper. Caffeine's `refreshAfterWrite` instead serves the old value and reloads asynchronously, which hides even that one reader's latency at the cost of a background executor.

## Trade-offs

| Pro | Con |
| --- | --- |
| Jitter turns a once-per-TTL spike into a gentle spread (1,000 loads/s → ~170 in the example) | Jitter alone shortens average TTLs, so total misses go *up* (2,000 → ~3,000 in the example) |
| Early refresh means hot keys almost never miss (2,000 waiting reads → 4) | Values are refreshed before their TTL is used up, so total DB loads rise (~4,300 vs 3,000) |
| No locks, background workers or coordination; works across many servers | Cold keys still expire normally: early refresh only helps keys that are read near expiry |
| One extra `rand` and `log` per hit | Needs a decent estimate of recompute cost (`δ`) to pick a sensible refresh window |

## When to use / when not to

**Use jitter** almost always. It's a one-line change and synchronised expiry is the default outcome of every restart and bulk warm-up.

**Use early refresh** for keys that are hot and expensive to recompute: home pages, product pages, aggregated dashboards, configuration read on every request.

**Avoid early refresh** when database capacity, not latency, is the constraint and keys are mostly cold, since it spends extra loads to save waits. Avoid both when entries must be invalidated precisely on write; that's what invalidation (or [write-through](../write-through-cache)) is for, and TTL tricks only bound staleness.

## Real-world examples

- **XFetch** is from the VLDB 2015 paper above, which analyses its optimality; the same idea appears in many Redis and Memcached client libraries as "probabilistic early expiration".
- **Caffeine** (Java) offers `refreshAfterWrite` to reload hot entries in the background while serving the old value.
- **HTTP** has the same concept as `Cache-Control: stale-while-revalidate`, which lets a CDN or browser serve the cached response while it revalidates in the background.
- **Facebook's memcache** paper (NSDI 2013) uses leases to stop thundering herds; jittered expiries are standard advice in Redis's and AWS ElastiCache's caching guidance.

## Run it

```sh
go run ./patterns/data/refresh-ahead/example
go test ./patterns/data/refresh-ahead/
```

## Related patterns

- [Cache-aside](../cache-aside): the base pattern, including the request coalescing used here
- [Serve-stale](../../reliability/serve-stale): what to do when the refresh fails and the entry has expired
- [Tiered cache](../../architecture/tiered-cache): pre-warming for launches whose traffic you can predict
- [Load shedding](../../reliability/load-shedding): the last line of defence when a stampede happens anyway

## Further reading

- Arvind Kumar, [Redis Cache Causes Production Outage](https://codefarm0.medium.com/redis-cache-causes-production-outage-system-design-deep-dive-on-cache-stampede-ttl-jitter-and-0e44d9366c82), an interview scenario on stampedes and TTL jitter
- Khyati Jha, [High Level System Design Day-8: Caching (Part 3)](https://medium.com/high-level-system-desgin-interview-prep/high-level-system-design-day-8-caching-part-3-e3c7a27b7235), which covers stampede remedies
