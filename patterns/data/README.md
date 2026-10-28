# Data & Storage Patterns

How data is cached, partitioned, replicated, logged, and reshaped for reading. Each pattern is a small, dependency-free Go package with table-driven tests and a runnable in-process simulation. Databases, caches, disks, and nodes are simulated with maps, goroutines, and injectable clocks, so everything is deterministic and runs in milliseconds.

| Pattern | Problem it solves | Key trade-off |
| --- | --- | --- |
| [Cache-aside](cache-aside) | Repeated expensive reads of hot data, plus stampedes when a hot key expires | Simple and resilient, but stale until TTL or invalidation, and the first read always misses |
| [Cache eviction](cache-eviction) | A full cache throwing out the wrong entries, especially during scans | W-TinyLFU resists scans and adapts to change, but adds a sketch and approximate counts |
| [Refresh-ahead](refresh-ahead) | Keys loaded together expiring together, and hot keys missing at every TTL | Readers almost never wait, but entries refresh early, so total loads rise |
| [Two-tier cache](two-tier-cache) | One viral key saturating a single shared-cache node | L1 and replicas spread the load, but other servers stay stale for the L1 TTL |
| [Write-through cache](write-through-cache) | Reads right after writes miss or see stale data | Cache is always coherent, but every write pays store latency |
| [Write-behind cache](write-behind-cache) | The store can't absorb a high rate of writes to hot keys | Fast, coalesced, batched writes, but unflushed writes are lost on crash |
| [Consistent hashing](consistent-hashing) | `hash % N` remaps almost every key when nodes change | Only ~1/N of keys move, at the cost of `O(log n)` lookups and statistical balance |
| [Sharding](sharding) | Data or write volume exceeds one node | Hash gives even load but scatter-gather scans (hash slots make adding shards cheap). Range gives cheap scans and splits but hotspots |
| [Read replicas](read-replicas) | Read traffic saturates a single database | Scales reads, but replicas lag. Read-your-writes needs session versions |
| [CQRS](cqrs) | One model can't serve both validation-heavy writes and query-shaped reads | Each side is optimal, but two models and eventual consistency |
| [Event sourcing](event-sourcing) | Overwriting state loses history, auditability, and temporal queries | Full history and replay, but projections, snapshots, and event versioning |
| [Bloom filter](bloom-filter) | Membership checks against a huge set are too slow or too big | ~10 bits/item for 1% FPR, but false positives and no deletes |
| [Write-ahead log](write-ahead-log) | Crashes lose or half-apply in-memory and paged changes | Exact recovery via sequential appends, but fsync latency and a log to checkpoint |
| [LSM tree](lsm-tree) | Random in-place writes are slow for write-heavy workloads | Sequential, high-throughput writes, but read, write, and space amplification |
| [Materialized view](materialized-view) | Expensive aggregate queries run on every read | O(1) per change and instant reads, but needs a complete, ordered change stream |

## Run everything

```sh
go test -race ./patterns/data/...
for d in patterns/data/*/example; do go run ./$d; done
```
