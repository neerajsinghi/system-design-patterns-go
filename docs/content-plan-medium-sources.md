# Content plan: new patterns and scenarios from two Medium series

*Drafted 2026-10-28. Status: all six batches implemented, with their 20 scenarios (one PR per batch). The catalogue now has 78 patterns. The only planned item left out is `read-your-writes`, which was folded into `read-replicas` (see §4.2).*

This plan reviews two Medium sources and turns them into a backlog for this repo. It covers which new patterns to add, which existing patterns to extend, and a new `scenarios/` track of interview walkthroughs that combine patterns.

**Outcome in one line:** about 31 new patterns (48 → ~79), 12 extensions to existing patterns, one new `ai` category, and 20 scenario walkthroughs, shipped in 6 batches.

---

## 1. Sources and how they were used

| Source | Author | What it is | Access | How it's used here |
|---|---|---|---|---|
| [22 Scenarios for System Design Interview](https://codefarm0.medium.com/list/22-scenarios-for-system-design-interview-93dfe2489e13) (list) | Arvind Kumar (codefarm0) | 20 "System Design Real Scenarios" articles plus a series index. Each is a production failure framed as an interview question | Members-only | **Topic level only.** Built from the public title, subtitle and the free intro. The paywalled bodies were not read |
| [medium.com/@khyatijha777](https://medium.com/@khyatijha777) | Khyati Jha | An 18-day High-Level Design series (client-server → WebSockets), a 3-day Low-Level Design series, and unrelated DSA/Git/Linux posts | Free | Read in full and paraphrased into notes |

**Attribution rules for anything built from this plan:**

- Write every README, diagram and example from scratch. Do not copy text, tables or diagrams from either source.
- Where a pattern or scenario was inspired by an article, add a `## Further reading` line linking to it, next to the primary references (papers, vendor docs), which stay the main sources.
- Scenario titles below are paraphrased. Keep our own wording in the repo too.

The list says "22", but it holds 20 scenario articles plus the index post ("22 Scenario-Based System Design Questions…"). The two remaining scenarios are not public anywhere in the list, so this plan covers the 20 it can see.

---

## 2. Source inventory

### 2.1 Arvind Kumar: System Design Real Scenarios (20)

The "topics" column is the list of what each article tests, taken from its public subtitle.

| # | Scenario | Topics (from the public subtitle) | Link |
|---|---|---|---|
| S1 | URL shortener crashes during a cricket final (Ep 1) | CDN edge caching, Redis, local in-memory cache for hot keys | [link](https://codefarm0.medium.com/url-shortener-suddenly-crashes-during-ipl-finals-how-will-you-fix-3347c9911ba3) |
| S2 | Duplicate messages in a chat app | At-least-once delivery, retries, deduplication | [link](https://codefarm0.medium.com/duplicate-messages-in-whatsapp-system-design-interview-deep-dive-on-at-least-once-delivery-and-cecdc091fc57) |
| S3 | Delivery app shows the wrong rider location | Real-time streaming, WebSockets, GPS polling, geo-distribution, eventual consistency | [link](https://codefarm0.medium.com/swiggy-shows-wrong-rider-location-system-design-deep-dive-on-real-time-gps-tracking-and-eventual-b49e5f18fccd) |
| S4 | Ticketing site sells the same seat twice | Concurrency control, pessimistic and optimistic locking | [link](https://codefarm0.medium.com/bookmyshow-sells-the-same-seat-twice-system-design-interview-deep-dive-on-race-conditions-and-64e798f34d21) |
| S5 | Streaming service buffers after a big season release | CDN architecture, auto scaling, traffic spikes, cache pre-warming | [link](https://codefarm0.medium.com/netflix-buffers-after-a-new-season-release-system-design-deep-dive-on-cdn-architecture-and-cache-51ecc36a64e2) |
| S6 | Surge pricing during rain | Stream processing, real-time demand vs supply, dynamic pricing | [link](https://codefarm0.medium.com/uber-surge-pricing-during-rain-system-design-deep-dive-on-real-time-demand-supply-and-dynamic-f71fd58fdb03) |
| S7 | Notification storm | Fan-out on write, async queues, push-provider rate limits | [link](https://codefarm0.medium.com/instagram-notification-storm-system-design-deep-dive-on-fanout-architecture-and-push-notification-0e8e3d86841c) |
| S8 | Payment deducted but order failed | Saga, compensation, idempotency keys, retries | [link](https://codefarm0.medium.com/payment-deducted-but-order-failed-system-design-deep-dive-on-the-saga-pattern-and-distributed-025b27cb2e0a) |
| S9 | Video processing pipeline | Chunk-based parallel processing, distributed workers, encoding | [link](https://codefarm0.medium.com/youtube-video-processing-pipeline-system-design-deep-dive-on-distributed-encoding-and-async-a39a7afc4d72) |
| S10 | Kafka consumer processes the same event twice | Offset commits, at-least-once delivery, idempotent consumers | [link](https://codefarm0.medium.com/kafka-consumer-processes-same-event-twice-system-design-deep-dive-on-offset-management-and-1134f0507792) |
| S11 | Order events arrive out of sequence (Ep 12) | Ordering guarantees, Kafka partition keys | [link](https://codefarm0.medium.com/order-events-arrive-out-of-sequence-system-design-deep-dive-on-event-ordering-and-kafka-41a0ad4e585a) |
| S12 | Why a photo app avoids JOINs at scale | Denormalization, NoSQL vs SQL, read optimisation | [link](https://codefarm0.medium.com/why-instagram-avoids-joins-at-scale-system-design-deep-dive-on-denormalization-and-read-b66f4a9f2c91) |
| S13 | DynamoDB hot partition | Partition-key design, write sharding, read sharding, hotspots | [link](https://codefarm0.medium.com/dynamodb-hot-partition-problem-system-design-deep-dive-on-partition-keys-sharding-strategies-and-9b6758e494ef) |
| S14 | Redis cache causes an outage | Cache stampede, TTL jitter, request coalescing | [link](https://codefarm0.medium.com/redis-cache-causes-production-outage-system-design-deep-dive-on-cache-stampede-ttl-jitter-and-0e44d9366c82) |
| S15 | AI chatbot gives wrong answers | RAG, vector databases, chunking, prompt design | [link](https://codefarm0.medium.com/ai-chatbot-gives-wrong-answers-system-design-deep-dive-on-hallucinations-rag-architecture-and-6c9e7712f664) |
| S16 | AI platform becomes very expensive | Prompt optimisation, token reduction, model routing, semantic caching | [link](https://codefarm0.medium.com/ai-platform-suddenly-becomes-very-expensive-system-design-deep-dive-on-token-reduction-model-473a3a2920a2) |
| S17 | AI search feels slow | Vector index structures, ANN search, recall vs latency | [link](https://codefarm0.medium.com/ai-search-feels-slow-system-design-deep-dive-on-vector-indexing-ann-search-and-recall-vs-latency-cd72c6d1723d) |
| S18 | OTP service fails at peak | Rate-limiting strategies, retry-storm prevention, SMS provider failover | [link](https://codefarm0.medium.com/otp-service-fails-during-peak-traffic-system-design-deep-dive-on-rate-limiting-retry-storms-7c2677aded90) |
| S19 | One microservice failure takes down the platform (Ep 21) | Cascading failure, circuit breakers | [link](https://codefarm0.medium.com/one-microservice-failure-takes-down-entire-platform-system-design-deep-dive-on-circuit-breakers-71159bd0276a) |
| S20 | API gateway becomes a bottleneck | Gateway scaling, auth offloading, connection pooling, rate limiting | [link](https://codefarm0.medium.com/api-gateway-becomes-a-bottleneck-system-design-deep-dive-on-gateway-scaling-authentication-cfff15529317) |

Series index: [22 Scenario-Based System Design Questions…](https://codefarm0.medium.com/22-scenario-based-system-design-questions-that-reveal-how-real-systems-actually-break-7c75de3c5cd2)

### 2.2 Khyati Jha: High-Level Design series (18)

| Day | Topic | Ideas for this repo |
|---|---|---|
| 1 | [Client-server architecture](https://medium.com/high-level-system-desgin-interview-prep/system-design-interview-prep-high-level-dessign-day-1-302a3759e144) | Sync vs async, pub-sub, streaming, stateless vs sticky, event versioning |
| 2 | [HTTP/HTTPS](https://medium.com/high-level-system-desgin-interview-prep/system-design-interview-prep-high-level-dessign-day-2-b3fa231572b7) | TLS termination at the edge, mTLS between services |
| 3 | [REST APIs](https://medium.com/high-level-system-desgin-interview-prep/system-design-interview-prep-high-level-design-day-3-1c3ed3cd4190) | HTTP caching/ETag, versioning, async REST (202 + job status) |
| 4 | [Load balancers](https://medium.com/high-level-system-desgin-interview-prep/system-design-interview-prep-high-level-design-day-4-load-balancers-b9a35cae1991) | Least response time, IP hash, sticky sessions, L4 vs L7, LB HA, global LB, slow-but-alive backends |
| 5 | [DNS](https://medium.com/high-level-system-desgin-interview-prep/high-level-system-design-day-5-dns-12fdbdf0bcb5) | Resolver chain with layered TTL caches, DNS failover, weighted/geo answers |
| 6 | [Caching 1: fundamentals](https://medium.com/@khyatijha777/high-level-system-design-day-6-caching-part-1-777c05b70e7c) | Hit ratio, cache layers, TTL modes, eviction policies (LRU/LFU/FIFO/TinyLFU) |
| 7 | [Caching 2: patterns and consistency](https://medium.com/high-level-system-desgin-interview-prep/high-level-system-design-day-7-caching-part-2-9b61df0289db) | Read-through, write-around, refresh-ahead, read-your-writes, cache races, CDC invalidation, key versioning |
| 8 | [Caching 3: distributed caches and failure modes](https://medium.com/high-level-system-desgin-interview-prep/high-level-system-design-day-8-caching-part-3-e3c7a27b7235) | Redis hash slots, stampede (singleflight, XFetch, jitter), negative caching, L1/L2, hot-key scatter, warm-up |
| 9 | [Caching 4: real-world](https://medium.com/high-level-system-desgin-interview-prep/high-level-system-design-day-9-caching-part-4-ebec82ded648) | Case studies, what not to cache, geospatial grid, leaderboards |
| 10–12 | [CDN 1](https://medium.com/high-level-system-desgin-interview-prep/high-level-system-design-day-10-cdn-part-1-fd28b3363b0f) · [CDN 2](https://medium.com/high-level-system-desgin-interview-prep/high-level-system-design-day-11-cdn-part-2-b6de9b7886cd) · [CDN 3](https://medium.com/high-level-system-desgin-interview-prep/high-level-system-design-day-12-cdn-part-3-82c1c6eeac3f) | Pull vs push CDN, origin shield, multi-tier CDN, pre-warming, serve stale when the origin is down |
| 13–18 | [WS 1](https://medium.com/high-level-system-desgin-interview-prep/high-level-system-design-day-13-websockets-part-1-0e1a6b4b3cc4) · [WS 2](https://medium.com/high-level-system-desgin-interview-prep/high-level-system-design-day-14-websockets-part-2-6690efe31651) · [WS 3](https://medium.com/high-level-system-desgin-interview-prep/high-level-system-design-day-15-websockets-part-3-5e3156e7c356) · [WS 4](https://medium.com/high-level-system-desgin-interview-prep/high-level-system-design-day-16-websockets-part-4-fb945fae6297) · [WS 5](https://medium.com/high-level-system-desgin-interview-prep/addhigh-level-system-design-day-17-websockets-part-5-fea1631aaaa2) · [WS 6](https://medium.com/high-level-system-desgin-interview-prep/high-level-system-design-day-178-websockets-part-6-2be2650b7b2f) | Polling vs long polling vs WS, heartbeat, connection registry, presence, per-connection backpressure, cross-node fan-out, chat flow, per-conversation sequencing, at-least-once with ack and dedup, Snowflake IDs |

### 2.3 Khyati Jha: Low-Level Design series (3)

[Day 1: Why OOP exists](https://medium.com/low-level-system-design-interview-prep/lld-interview-preparation-day-1-before-classes-and-objects-understand-why-oop-exists-6ee77d0b32e3) · [Day 2: Class vs object](https://medium.com/low-level-system-design-interview-prep/low-level-system-design-day-2-class-vs-object-the-building-blocks-of-every-design-7015c8073bc2) · [Day 3: Encapsulation and abstraction](https://medium.com/low-level-system-design-interview-prep/low-level-system-design-day-3-designing-better-objects-with-encapsulation-and-abstraction-3312dfeb87b5)

These are OOP fundamentals, not distributed-systems patterns, so they don't belong in `patterns/`. See §6 for the one small piece worth keeping.

### 2.4 Out of scope

Her DSA (sorting, bit manipulation, Floyd's cycle, Horner's method), Git, Linux, Docker, IAM and JavaScript posts. Two posts are adjacent and worth a later look: *Deployment strategies* (canary, blue-green, shadow traffic, feature flags) and *Nginx vs Apache* (event loop vs thread-per-request).

---

## 3. Coverage map: the 20 scenarios against the current repo

✅ already in the repo · ➕ extend an existing pattern · 🆕 new pattern (see §4)

| Scenario | Already covered | Gaps this plan fills |
|---|---|---|
| S1 URL shortener hot keys | ✅ cache-aside, load-shedding, consistent-hashing | 🆕 two-tier-cache (L1 in-process + L2 with hot-key replicas), 🆕 tiered-cache (edge/shield/origin) |
| S2 Duplicate chat messages | ✅ idempotency-key, retry-backoff | 🆕 idempotent-consumer (ack + dedup store), 🆕 snowflake-id |
| S3 Wrong rider location | ✅ server-sent-events, pub-sub | 🆕 websocket-hub, 🆕 geospatial-index, 🆕 presence (latest-position with TTL) |
| S4 Seat sold twice | ✅ distributed-lock-fencing | 🆕 optimistic-locking, 🆕 reservation-hold |
| S5 Release-day buffering | ✅ load-balancing | 🆕 tiered-cache (pre-warming, origin shield), 🆕 serve-stale |
| S6 Surge pricing | ✅ pub-sub, sharded-counters | 🆕 windowed-stream-aggregation, 🆕 geospatial-index |
| S7 Notification storm | ✅ fan-out-feed, competing-consumers, token bucket | ➕ fan-out-feed: priority lanes + per-provider rate limit, 🆕 dead-letter-queue |
| S8 Payment ok, order failed | ✅ saga-orchestration, saga-choreography, idempotency-key, transactional-outbox | Scenario walkthrough only: all the pieces exist |
| S9 Video pipeline | ✅ competing-consumers, backpressure | 🆕 split-process-merge, 🆕 async-request-reply |
| S10 Consumer processes twice | ✅ idempotency-key | 🆕 idempotent-consumer (offset commit before/after side effect), 🆕 dead-letter-queue |
| S11 Out-of-order events | ✅ lamport-vector-clocks | 🆕 per-key-ordering (partition by key + resequencer) |
| S12 Avoiding JOINs | ✅ materialized-view, cqrs | ➕ materialized-view: denormalized fan-in example |
| S13 Hot partition | ✅ sharding, sharded-counters, consistent-hashing | 🆕 hot-key-salting (write sharding + scatter read), ➕ sharding: hash-slot partitioner |
| S14 Cache stampede | ✅ cache-aside (already coalesces) | 🆕 refresh-ahead (XFetch + TTL jitter), ➕ cache-aside: negative caching |
| S15 RAG hallucinations | none | 🆕 rag-retrieval |
| S16 AI cost | none | 🆕 semantic-cache, 🆕 model-router |
| S17 Slow vector search | none | 🆕 vector-ann-index |
| S18 OTP at peak | ✅ token bucket, sliding window, retry-backoff, circuit-breaker, fallback | 🆕 retry-budget, ➕ fallback: provider failover chain |
| S19 Cascading failure | ✅ circuit-breaker, bulkhead, timeout, fallback, load-shedding | 🆕 outlier-detection; scenario walkthrough |
| S20 Gateway bottleneck | ✅ api-gateway, rate limiters | 🆕 connection-pool, ➕ api-gateway: auth offloading (verify a signed token once at the edge) |

**Takeaway:** S8 and S19 are fully covered by today's patterns and only need scenarios. The biggest gaps are caching depth, consumer-side messaging semantics, real-time connections, and AI-serving patterns.

---

## 4. New patterns (31)

Every entry follows the repo's existing shape: `README.md` (problem, solution, mermaid diagram, how it works, trade-offs, when not to use, real-world examples, run it, related), `<pkg>.go`, `<pkg>_test.go` (table-driven, `-race`, injectable clock), and a narrated `example/main.go`. Standard library only.

Effort: **S** ≈ under 150 lines of implementation, **M** ≈ 150–350, **L** ≈ above 350 or tricky to make deterministic.

### 4.1 Reliability (`patterns/reliability`), 4 new

| Folder | Problem | What the example shows | Tests to write | Sources | Effort |
|---|---|---|---|---|---|
| `retry-budget` | Per-request retries multiply under an outage into a retry storm | 1,000 clients retrying against a failing SMS provider: with plain retries load is ~3×; with a 10% token-based budget, load stays ~1.1× | Budget refills with successes; retries are denied once it is spent; ratio holds under concurrency | S18 | S |
| `outlier-detection` | A slow-but-alive instance passes health checks while ruining p99 | 5 backends, one degraded to 800 ms; EWMA latency ejects it for a back-off period, then re-admits it | Ejection threshold, max-ejection-percent cap, re-admission timing with a fake clock | S19, Day 4 | M |
| `serve-stale` | The origin is down and the cache entry just expired, so users see errors | Origin fails; entries past TTL but within `stale-if-error` keep being served, flagged as stale | Stale window boundaries, a fresh value wins when the origin recovers, staleness metric | S5, Day 12 | S |
| `graceful-shutdown` | Deploys drop in-flight requests and live connections | `http.Server.Shutdown` plus a drain phase: readiness flips, new work is refused, in-flight work finishes or hits a deadline | No request lost inside the drain deadline; forced close after the deadline | Day 14 | S |

### 4.2 Data and storage (`patterns/data`), 11 new

| Folder | Problem | What the example shows | Tests to write | Sources | Effort |
|---|---|---|---|---|---|
| `cache-eviction` | A bounded cache must choose what to drop; a bad choice destroys the hit ratio | LRU, LFU (O(1) buckets), FIFO and W-TinyLFU (count-min sketch admission) replay the same Zipf trace and print hit ratios side by side | Each policy's eviction order; sketch decay; O(1) operation bounds | Days 6, 8 | L |
| `refresh-ahead` | Synchronised expiry causes stampedes and latency spikes | Three runs: fixed TTL (spikes), TTL jitter (spread out), XFetch probabilistic early refresh (smooth). DB QPS printed per tick | Jitter range; XFetch refresh probability grows near expiry (seeded rand); one refresh per key | S14, Days 7, 8 | M |
| `two-tier-cache` | One hot key saturates a single shared cache node | L1 in-process (short TTL) in front of L2 shared; a hot key is replicated as `key#0..N` in L2 with random reads; node load is printed | L1 hit avoids L2; replica fan-out on write; invalidation reaches both tiers | S1, Day 8 | M |
| `cdc-cache-invalidation` | Application-driven invalidation misses writes from other paths | A change feed (reuse the write-ahead-log idea) drives an invalidator; shows the delete-then-stale-set race and the versioned-set fix | Every committed change invalidates; an out-of-order set with an older version is rejected | Day 7 | M |
| `read-your-writes` *(built as an extension of `read-replicas`, which already had a read-your-writes session; adds portable signed tokens, monotonic reads and a leader-after-write window)* | A user updates a profile, refreshes, and sees the old value from a lagging replica | Session tokens carrying the last write LSN; reads go to a replica only once it has caught up, otherwise to the primary | Routing by LSN; fallback to the primary; no stale reads within a session | Day 7 | S |
| `optimistic-locking` | Two buyers read the same seat as free and both book it | Version-column compare-and-set: one wins, the other gets a conflict and retries or fails. Contrasted with a mutex (pessimistic) under high contention | Exactly one winner under N goroutines; retry loop with a bound; ABA note | S4 | S |
| `reservation-hold` | Seats or stock held during checkout are never released | Hold with TTL → confirm or expire; a background reaper returns expired holds; idempotent confirm | Expiry with a fake clock; confirm after expiry fails; no oversell under concurrency | S4 | M |
| `hot-key-salting` | One partition key (celebrity, flash sale) exceeds a partition's throughput limit | Writes spread across `key#0..K`, reads scatter-gather and merge; per-partition throughput before/after | Even spread; read merges all suffixes; adaptive K for only hot keys | S13, Day 8 | S |
| `geospatial-index` | "Find drivers near me" with a full scan is O(N) | Geohash grid: encode, neighbour cells, radius query with distance filtering; live updates as positions move | Encode/decode round trip; edge-of-cell neighbours; results match brute force | S3, S6, Day 9 | M |
| `leaderboard-top-k` | Rank queries over millions of scores under heavy writes | Skip-list sorted set with rank and range queries, plus a heap-based top-K for streams | Rank correctness after updates; ties; top-K matches sort | Day 9 | M |
| `snowflake-id` | Unique, roughly time-ordered IDs without a central counter | 41-bit time, 10-bit node, 12-bit sequence; shows clock-regression handling and sequence overflow | Uniqueness across goroutines; monotonic per node; clock-going-backwards path | S2, Day 18 | S |

### 4.3 Distributed coordination and messaging (`patterns/distributed`), 5 new

| Folder | Problem | What the example shows | Tests to write | Sources | Effort |
|---|---|---|---|---|---|
| `idempotent-consumer` | At-least-once delivery means duplicates; offset-commit timing decides between loss and duplication | In-process log + consumer; crashes are injected before and after the offset commit; an inbox table of processed IDs, committed with the side effect, makes it effectively-once. Covers sender ack/retransmit too | Commit-before loses, commit-after duplicates, the inbox makes both safe; dedup window expiry | S2, S10, Day 18 | M |
| `per-key-ordering` | Events for one order arrive out of sequence across partitions or consumers | Partition by key to keep per-key order; a resequencer buffers by per-key sequence number with a gap timeout | Per-key order holds with parallel partitions; gap handling; no cross-key head-of-line blocking | S11, Day 17 | M |
| `dead-letter-queue` | A poison message blocks the queue or is retried forever | Max-attempts with backoff, then park in the DLQ with an error reason; a redrive tool replays after a fix | Poison isolation; other messages keep flowing; redrive preserves IDs | S7, S10 | S |
| `windowed-stream-aggregation` | Real-time demand/supply per area per minute from an unbounded event stream | Tumbling and sliding windows keyed by geocell; watermark for late events; emits surge multipliers | Window boundaries; late-event handling; deterministic with a fake clock | S6, Day 1 | M |
| `split-process-merge` | One huge job (a video) takes hours serially and restarts from zero on failure | Split into chunks → parallel workers via competing consumers → ordered merge; per-chunk retry and checkpointing | Merge order; only the failed chunk reruns; worker-count speedup | S9 | M |

### 4.4 Architecture, APIs and scaling (`patterns/architecture`), 7 new

| Folder | Problem | What the example shows | Tests to write | Sources | Effort |
|---|---|---|---|---|---|
| `async-request-reply` | Long work behind a synchronous HTTP call times out | `POST` → `202 Accepted` + `Location: /jobs/{id}`; poll status; optional callback via webhook-delivery | State transitions; idempotent submit; expiry of finished jobs | S9, Day 3 | S |
| `long-polling` | Short polling wastes requests; the client can't use SSE or WS | Short polling vs long polling: requests per update and latency printed side by side | Waiter wakes on publish; timeout returns empty; no lost update between polls | Day 13 | S |
| `websocket-hub` | Real-time two-way delivery to millions of connected users across nodes | Hand-rolled RFC 6455 handshake and framing via `http.Hijacker`; hub with register/unregister/broadcast; ping/pong heartbeat; bounded per-connection send queue (drop or disconnect slow consumers); cross-node fan-out via pub-sub with a user→node registry | Handshake accept key; frame encode/decode; heartbeat timeout; slow consumer is evicted without stalling others | S3, Days 13–18 | L |
| `presence` | Knowing who is online, and on which server, without a write-heavy DB | Heartbeat-renewed TTL leases; online/away/offline transitions; last-seen; user→connection-server lookup | Lease expiry with a fake clock; flapping suppression; multi-device | Days 15, 17 | S |
| `conditional-requests` | Clients and caches re-download unchanged resources, and concurrent PUTs overwrite each other | `ETag` + `If-None-Match` → `304`; `If-Match` → `412` for optimistic concurrency on writes | 304 path; weak vs strong ETags; lost-update prevention | Days 3, 6 | S |
| `tiered-cache` | A traffic spike on new content hammers the origin from every edge | Edge caches → origin shield → origin. Shield coalesces misses (one origin fetch for N edges); pre-warming before a launch; per-tier hit-ratio report. Pull vs push mode | Shield collapses concurrent misses; pre-warmed keys never reach the origin; tier hit ratios | S5, S1, Days 10–12 | M |
| `connection-pool` | Opening a connection per request exhausts sockets and adds handshake latency | Bounded pool with idle timeout, max lifetime, health check on borrow and wait queue; with vs without pool latency | Never exceeds max; waiters are served FIFO; broken connections are discarded | S20 | M |

### 4.5 New category: AI serving (`patterns/ai`), 4 new

AI-serving failure modes are now common interview topics (S15–S17), and they fit the repo's "simulate it in-process" style. Use deterministic toy embeddings (hashed bag-of-words into a fixed-size float vector) so nothing needs a model or network. Link to the companion repos `go-rag-kit` and `llm-otel-go` for production-grade versions.

| Folder | Problem | What the example shows | Tests to write | Sources | Effort |
|---|---|---|---|---|---|
| `semantic-cache` | Paraphrased questions each pay for a full LLM call | Cache keyed by embedding; cosine-similarity threshold decides a hit; shows false hits when the threshold is too low | Threshold behaviour; TTL; per-tenant isolation | S16 | S |
| `model-router` | Every request goes to the most expensive model | Route by a cheap complexity score (length, keywords, tools needed) to small/medium/large fake models; prints cost and "quality" per strategy; escalate on low confidence | Routing table; escalation path; budget cap | S16 | S |
| `rag-retrieval` | The chatbot answers from memory instead of your documents | Chunking strategies (fixed, overlap, by heading) → index → top-k retrieval → grounded prompt assembly with citations; shows how chunk size changes recall on a small corpus | Chunk boundaries and overlap; retrieval recall on a fixture; "no context → refuse" path | S15 | M |
| `vector-ann-index` | Brute-force vector search is too slow at scale | Flat (exact) vs IVF (k-means coarse quantiser, probe `nprobe` lists); prints recall@10 vs latency as `nprobe` varies | Flat is exact; IVF recall rises with nprobe; deterministic k-means with a seed | S17 | L |

HNSW is the other common index. Mention it in the README as the production alternative rather than implementing it, because IVF shows the recall/latency dial more clearly in less code.

---

## 5. Extensions to existing patterns (12)

Smaller changes: a new section in the README, an option on the type, and a test or two each.

| Pattern | Extension | Source |
|---|---|---|
| `data/cache-aside` | Negative caching (cache "not found" with a short TTL); a `WithTTLJitter` option; a short read-through vs write-around comparison | S14, Days 7, 8 |
| `data/sharding` | Fixed hash-slot partitioner (CRC16 mod 16384, Redis-Cluster style) next to modulo and range | Day 8 |
| `data/materialized-view` | Denormalized feed-row example ("why no JOIN on the read path") | S12 |
| `architecture/load-balancing` | Least-response-time (EWMA) and IP-hash strategies; a note on sticky sessions vs shared session store | Day 4 |
| `architecture/api-gateway` | Auth offloading (verify a signed token once at the edge with `crypto/hmac`, pass claims downstream); TLS termination note | S20, Day 2 |
| `architecture/fan-out-feed` | Priority lanes (OTP/transactional vs marketing) and per-provider rate limits for push notifications | S7 |
| `architecture/service-discovery` | DNS-style TTL caching on the client, and the stale-endpoint problem it causes | Day 5 |
| `architecture/server-sent-events` | Comparison table: polling vs long polling vs SSE vs WebSocket, linking the new patterns | Day 13 |
| `reliability/fallback` | Ordered provider failover chain (primary SMS → secondary → voice) | S18 |
| `reliability/health-check` | Per-connection heartbeat variant (push-direction liveness) | Day 14 |
| `distributed/backpressure` | Bounded per-connection send queue with drop/disconnect policies | Day 16 |
| `distributed/pub-sub` | Event versioning / upcasting a v1 payload to v2 | Day 1 |

---

## 6. New track: `scenarios/`

Arvind Kumar's series is strongest as a *format*: start from a production incident, then reason to a fix. The repo can offer the same with something no article can, a runnable before-and-after.

**Layout**

```
scenarios/<slug>/
├── README.md        incident, symptoms, root cause, fix, which patterns, interview follow-ups
├── scenario.go      the simulation: Broken() and Fixed() each return a Report
├── scenario_test.go asserts the invariant: broken violates it, fixed holds it
├── broken/main.go   prints the broken run (the double booking, the stampede, …)
└── fixed/main.go    prints the fixed run, built from patterns in patterns/
```

Scenarios import the pattern packages, so they double as integration tests of the catalogue.

| Slug | Incident | Patterns combined |
|---|---|---|
| `hot-short-link` | A viral short link melts the cache node | two-tier-cache, tiered-cache, load-shedding |
| `duplicate-chat-messages` | Retries deliver a message twice | idempotent-consumer, snowflake-id, retry-backoff |
| `stale-rider-location` | Map shows an old position | websocket-hub, presence, geospatial-index |
| `seat-double-booking` | Two users get the same seat | optimistic-locking, reservation-hold, idempotency-key |
| `release-day-buffering` | New season overloads the origin | tiered-cache, serve-stale, load-shedding |
| `surge-in-the-rain` | Prices lag demand | windowed-stream-aggregation, geospatial-index |
| `notification-storm` | One post triggers millions of pushes | fan-out-feed, competing-consumers, token bucket, dead-letter-queue |
| `charged-but-no-order` | Payment succeeds, order fails | saga-orchestration, idempotency-key, transactional-outbox |
| `video-pipeline` | Encoding a long upload takes hours | split-process-merge, async-request-reply, competing-consumers |
| `consumer-double-processing` | Rebalance replays events | idempotent-consumer, dead-letter-queue |
| `out-of-order-events` | "Shipped" arrives before "paid" | per-key-ordering |
| `join-free-feed` | Feed query JOINs time out | materialized-view, cqrs, fan-out-feed |
| `hot-partition` | One key throttles the table | hot-key-salting, sharded-counters |
| `cache-stampede` | TTL expiry floods the DB | cache-aside, refresh-ahead |
| `chatbot-hallucinates` | Answers ignore the docs | rag-retrieval, vector-ann-index |
| `llm-bill-explodes` | Costs spike with traffic | semantic-cache, model-router |
| `slow-vector-search` | Search latency grows with corpus size | vector-ann-index |
| `otp-retry-storm` | SMS provider outage multiplies traffic | retry-budget, sliding-window limiter, fallback, circuit-breaker |
| `cascading-failure` | One slow service takes everything down | timeout, bulkhead, circuit-breaker, outlier-detection, fallback |
| `gateway-bottleneck` | Gateway CPU and sockets saturate | api-gateway (auth offloading), connection-pool, rate limiters |

**LLD content.** OOP fundamentals don't fit `patterns/`. The one useful piece is a short `docs/go-idioms.md` covering how the repo's code applies Go equivalents: unexported fields with invariant-enforcing methods, consumer-side interfaces, embedding over inheritance, functional options, state-machine methods (`booking.Cancel()` over `SetStatus`). Optional, and low priority.

---

## 7. README and catalogue updates

- Headline count: "48 system design patterns" → the new total after each batch. The CI badge and Quickstart stay the same.
- Add `### [AI serving](patterns/ai)` with a category README and table, matching the other four.
- Add a `## Scenarios` section after Patterns, linking `scenarios/` with a one-line incident per row.
- Add rows to **Which pattern do I need?**:

| If you're facing… | Start with |
|---|---|
| Two users booking the same thing | optimistic-locking, reservation-hold |
| One key hotter than a whole partition | hot-key-salting, two-tier-cache |
| Events processed twice or out of order | idempotent-consumer, per-key-ordering |
| Retries making an outage worse | retry-budget |
| Origin overwhelmed by a launch | tiered-cache, serve-stale |
| Real-time updates to many clients | websocket-hub, long-polling, server-sent-events |
| LLM costs or latency too high | semantic-cache, model-router, vector-ann-index |

- Update each category README table with the new rows (problem + key trade-off).
- Portfolio site (`neeraj-portfolio`): refresh the project card's pattern count and the agent knowledge entry once a batch merges.

---

## 8. Rollout (one PR per batch)

Ordered by interview value and by dependencies (scenarios need their patterns first).

| Batch | Contents | New patterns | Scenarios unlocked |
|---|---|---|---|
| 1. Caching depth | cache-eviction, refresh-ahead, two-tier-cache, serve-stale, tiered-cache + cache-aside/sharding extensions | 5 | cache-stampede, hot-short-link, release-day-buffering |
| 2. Concurrency and keys | optimistic-locking, reservation-hold, hot-key-salting, snowflake-id, read-your-writes, conditional-requests | 6 | seat-double-booking, hot-partition |
| 3. Messaging semantics | idempotent-consumer, per-key-ordering, dead-letter-queue, retry-budget, outlier-detection + fallback/fan-out extensions | 5 | duplicate-chat-messages, consumer-double-processing, out-of-order-events, otp-retry-storm, cascading-failure, notification-storm, charged-but-no-order |
| 4. Real-time | long-polling, presence, websocket-hub, geospatial-index, windowed-stream-aggregation, leaderboard-top-k + SSE/backpressure/health-check extensions | 6 | stale-rider-location, surge-in-the-rain |
| 5. Pipelines and edge | split-process-merge, async-request-reply, connection-pool, graceful-shutdown, cdc-cache-invalidation + api-gateway/LB/discovery/pub-sub/materialized-view extensions | 5 | video-pipeline, gateway-bottleneck, join-free-feed |
| 6. AI serving | semantic-cache, model-router, rag-retrieval, vector-ann-index + category README | 4 | chatbot-hallucinates, llm-bill-explodes, slow-vector-search |

Each batch: patterns first, then their scenarios, then the README and portfolio updates. Every PR must pass `gofmt`, `go vet` and `go test -race ./...`, and every new example must run in under a second.

---

## 9. Open questions

1. ~~**`scenarios/` at the top level or `patterns/scenarios/`?**~~ Decided: top level, since scenarios compose patterns rather than being one. The simulation lives in a `scenario.go` package so the test can compare both runs.
2. ~~**WebSocket from scratch vs a library.**~~ Decided: from scratch (RFC 6455 handshake, framing, close and ping/pong over `http.Hijacker`), keeping the standard-library rule; no extensions or subprotocols. A stdlib-only RFC 6455 implementation keeps the repo's rule and is a strong teaching piece, but it is the largest item here. An alternative is to show hub semantics over SSE + POST and leave framing out.
3. ~~**AI category scope.**~~ Decided: Keep it to the four patterns above, and point to `go-rag-kit` / `llm-otel-go` for anything production-shaped.
