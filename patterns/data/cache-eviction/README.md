# Cache Eviction Policies (FIFO, LRU, LFU, W-TinyLFU)

## Problem

A cache has a fixed amount of memory. Once it is full, every new entry forces an old one out, and the choice of victim decides the hit ratio. A bad choice quietly turns your cache into a pass-through.

The access patterns that break naive policies are common:

- **Scans.** A nightly export, a crawler or an analytics job reads thousands of keys exactly once. A recency-based cache treats each of them as the hottest item in the system and evicts genuinely hot data to make room.
- **Shifting popularity.** Yesterday's trending item still has a huge access count. A pure frequency-based cache keeps it long after nobody reads it.
- **Skew.** Real traffic is heavily skewed (roughly Zipfian): a small fraction of keys gets most of the reads. The policy has to find and keep that fraction.

## Solution

Four policies, from simplest to smartest:

| Policy | Evicts | Strength | Weakness |
| --- | --- | --- | --- |
| **FIFO** | The oldest insert | Trivial, no bookkeeping on reads | Ignores how hot an entry is |
| **LRU** | The least recently read entry | Adapts instantly to what's hot now | One scan flushes the whole hot set |
| **LFU** | The least frequently read entry (ties: least recent) | Resists scans; near-optimal for stable popularity | Old favourites never leave |
| **W-TinyLFU** | Whichever of two candidates the frequency sketch rates lower | Resists scans *and* adapts to change | More moving parts; approximate counts |

**W-TinyLFU** (the policy behind Java's Caffeine) splits the cache in two:

1. A tiny **window** LRU (1% of capacity) where every new key lands.
2. A **main** region for the rest.
3. A **count-min sketch** that approximately counts every read, including misses, in fixed memory.

When the window overflows, the key it pushes out competes with the main region's LRU victim. Whichever the sketch says is read more often stays. A one-off scan key has a count of 1, so it loses to any genuinely hot key and passes straight through. The sketch periodically halves all its counters, so stale popularity fades and a newly hot key can win admission.

## Diagram

```mermaid
flowchart LR
    New[new key] --> W[window LRU<br/>1% of capacity]
    W -- "evicted candidate" --> A{sketch:<br/>freq(candidate) ><br/>freq(victim)?}
    M[main LRU<br/>99% of capacity] -- "LRU victim" --> A
    A -- yes --> In[candidate joins main,<br/>victim is dropped]
    A -- no --> Out[candidate is dropped,<br/>victim stays]
    R[every Get] -. "increment" .-> S[(count-min sketch<br/>4 rows, halved every<br/>10 × capacity reads)]
    S -.-> A
```

## How this implementation works

- All four types implement `Policy[K, V]` (`Get`, `Put`, `Len`, `Name`), are safe for concurrent use behind one mutex, and do O(1) work per operation.
- `LRU` and `FIFO` are a `container/list` plus a map from key to list element. LRU moves an element to the front on `Get`; FIFO never does.
- `LFU` keeps one list per access count and a `minFreq` pointer to the lowest non-empty list. A read moves the entry to the next list up; eviction takes the back (least recent) of the `minFreq` list. This is the classic O(1) LFU layout.
- `TinyLFU` reuses the unexported LRU list for both its window and main regions. `Get` records an access in the sketch, then looks in both regions. `Put` adds new keys to the window and runs the admission contest on whatever the window evicts.
- `Sketch` is a count-min sketch: four rows of one-byte counters, 8 × capacity wide. One 64-bit `maphash` of the key is split into four row indexes by double hashing. Counters saturate at 15 (Caffeine uses 4-bit counters), and after 10 × capacity increments every counter is halved.
- `Replay` runs a trace through a policy as a read-through cache and returns hits and misses. The tests and the example use it to compare policies on the same workload.

**Simplifications.** Caffeine's main region is a segmented LRU (probation and protected), its window size adapts by hill climbing, and it adds a "doorkeeper" Bloom filter so one-off keys don't touch the sketch at all. Its counters are packed four bits each, so the sketch costs about 8 bytes per entry; this one uses 32. The admission idea is the same.

## Trade-offs

| Pro | Con |
| --- | --- |
| LRU is simple and good whenever recency predicts reuse | A single large scan evicts the entire working set |
| LFU holds on to the truly popular keys under skew | Never forgets: past popularity blocks new hot keys (example run 3) |
| W-TinyLFU is scan-resistant and adapts to change | Count-min estimates can over-count, so a few unlucky one-off keys still get admitted |
| W-TinyLFU's frequency history covers keys that aren't cached | The sketch costs memory and a hash per read, and has its own tuning (width, reset interval) |
| FIFO needs no work on reads, so it is very cheap under contention | Usually the lowest hit ratio of the four |

## When to use / when not to

- **LRU** is a fine default for small, local caches whose access pattern is mostly recency-driven, like sessions or recently viewed items.
- **LFU** suits stable, long-lived popularity, like static assets or reference data, and short-lived processes where "forever" is short.
- **W-TinyLFU** is the best general-purpose choice for large, shared caches facing mixed traffic: product catalogues, CDN metadata, database row caches.
- **FIFO** is for very high-throughput caches where lock contention on reads matters more than the last few percent of hit ratio, or as a building block (S3-FIFO, CLOCK).

Don't bother tuning eviction if the cache can hold the whole working set, or if a hit ratio around 99% is already reached: then TTLs and invalidation decide freshness, not eviction.

## Real-world examples

- **Caffeine** (Java) uses W-TinyLFU. The design comes from Einziger, Friedman and Manes, *TinyLFU: A Highly Efficient Cache Admission Policy* (ACM ToS, 2017).
- **Ristretto** (Go, by Dgraph) uses TinyLFU admission with a count-min sketch and a doorkeeper, paired with sampled-LFU eviction.
- **Redis** offers `allkeys-lru` and `allkeys-lfu` under `maxmemory-policy`. Its LFU uses a probabilistic, logarithmic 8-bit counter with time-based decay, a cousin of the sketch's halving.
- **Memcached** uses a segmented LRU per slab class.
- **Linux's page cache** uses a two-list (active/inactive) LRU so that a single read doesn't promote a page, which is the same scan-resistance idea.

## Run it

```sh
go run ./patterns/data/cache-eviction/example
go test ./patterns/data/cache-eviction/
```

## Related patterns

- [Cache-aside](../cache-aside): decides *when* to load and invalidate; eviction decides *what* to drop
- [Refresh-ahead](../refresh-ahead): keeps hot keys from expiring all at once
- [Two-tier cache](../two-tier-cache): a small, eviction-heavy L1 in front of a shared L2
- [Bloom filter](../bloom-filter): the same "approximate, fixed memory" trade-off, for membership instead of counts

## Further reading

- Khyati Jha, [High Level System Design Day-6: Caching (Part 1)](https://medium.com/@khyatijha777/high-level-system-design-day-6-caching-part-1-777c05b70e7c), which covers hit ratio and eviction policies
