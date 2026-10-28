# System Design Patterns in Go

[![CI](https://github.com/neerajsinghi/system-design-patterns-go/actions/workflows/ci.yml/badge.svg)](https://github.com/neerajsinghi/system-design-patterns-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/neerajsinghi/system-design-patterns-go.svg)](https://pkg.go.dev/github.com/neerajsinghi/system-design-patterns-go)

**69 system design patterns, each as a small, tested Go implementation you can run in under a second.**

Most system design material stops at boxes and arrows. This repo goes one step further: every pattern is real code with tests, a runnable simulation that prints what happens step by step, and a write-up covering the problem, a diagram, trade-offs, when *not* to use it, and real systems that rely on it.

- **Standard library only.** No Docker, brokers or databases needed. Nodes, networks, queues and disks are simulated in-process, so you can watch a partition, a crash or a retry storm play out.
- **Deterministic.** Time-dependent code takes an injectable clock, so tests are fast and don't flake.
- **Honest about simplifications.** Each README says where the teaching version differs from production systems (for example, leader election here is lease-based, not Raft).

## Quickstart

```bash
git clone https://github.com/neerajsinghi/system-design-patterns-go
cd system-design-patterns-go

go run ./patterns/reliability/circuit-breaker/example   # watch a breaker trip and recover
go run ./patterns/data/consistent-hashing/example       # see how few keys move when a node joins
go run ./patterns/distributed/saga-orchestration/example # watch a saga compensate after a failure

go test -race ./...                                      # run every pattern's tests
```

Requires Go 1.23 or later.

## Patterns

### [Reliability & resilience](patterns/reliability)

Keep a service up when its dependencies are slow, failing or overloaded.

[Retry with backoff](patterns/reliability/retry-backoff) ·
[Retry budget](patterns/reliability/retry-budget) ·
[Circuit breaker](patterns/reliability/circuit-breaker) ·
[Bulkhead](patterns/reliability/bulkhead) ·
[Timeout & deadline propagation](patterns/reliability/timeout) ·
[Token bucket](patterns/reliability/rate-limiter-token-bucket) ·
[Leaky bucket](patterns/reliability/rate-limiter-leaky-bucket) ·
[Sliding window rate limiter](patterns/reliability/sliding-window-rate-limiter) ·
[Load shedding](patterns/reliability/load-shedding) ·
[Fallback](patterns/reliability/fallback) ·
[Serve-stale](patterns/reliability/serve-stale) ·
[Health check](patterns/reliability/health-check) ·
[Outlier detection](patterns/reliability/outlier-detection) ·
[Hedged requests](patterns/reliability/hedged-requests) ·
[Idempotency key](patterns/reliability/idempotency-key)

### [Data & storage](patterns/data)

Cache, partition, replicate and persist data at scale.

[Cache-aside](patterns/data/cache-aside) ·
[Cache eviction](patterns/data/cache-eviction) ·
[Refresh-ahead](patterns/data/refresh-ahead) ·
[Two-tier cache](patterns/data/two-tier-cache) ·
[Write-through cache](patterns/data/write-through-cache) ·
[Write-behind cache](patterns/data/write-behind-cache) ·
[Consistent hashing](patterns/data/consistent-hashing) ·
[Sharding](patterns/data/sharding) ·
[Hot-key salting](patterns/data/hot-key-salting) ·
[Read replicas](patterns/data/read-replicas) ·
[CQRS](patterns/data/cqrs) ·
[Event sourcing](patterns/data/event-sourcing) ·
[Bloom filter](patterns/data/bloom-filter) ·
[Write-ahead log](patterns/data/write-ahead-log) ·
[LSM tree](patterns/data/lsm-tree) ·
[Materialized view](patterns/data/materialized-view) ·
[Optimistic locking](patterns/data/optimistic-locking) ·
[Reservation hold](patterns/data/reservation-hold) ·
[Snowflake IDs](patterns/data/snowflake-id) ·
[Geospatial index](patterns/data/geospatial-index) ·
[Leaderboard and top-K](patterns/data/leaderboard-top-k)

### [Distributed coordination & messaging](patterns/distributed)

Agree, order, communicate and stay consistent across machines.

[Pub/Sub](patterns/distributed/pub-sub) ·
[Competing consumers](patterns/distributed/competing-consumers) ·
[Idempotent consumer](patterns/distributed/idempotent-consumer) ·
[Per-key ordering](patterns/distributed/per-key-ordering) ·
[Dead-letter queue](patterns/distributed/dead-letter-queue) ·
[Transactional outbox](patterns/distributed/transactional-outbox) ·
[Saga (orchestration)](patterns/distributed/saga-orchestration) ·
[Saga (choreography)](patterns/distributed/saga-choreography) ·
[Two-phase commit](patterns/distributed/two-phase-commit) ·
[Leader election](patterns/distributed/leader-election) ·
[Distributed lock with fencing](patterns/distributed/distributed-lock-fencing) ·
[Lamport & vector clocks](patterns/distributed/lamport-vector-clocks) ·
[Gossip](patterns/distributed/gossip) ·
[Quorum (N/R/W)](patterns/distributed/quorum) ·
[Backpressure](patterns/distributed/backpressure) ·
[Windowed stream aggregation](patterns/distributed/windowed-stream-aggregation)

### [Architecture, APIs & scaling](patterns/architecture)

Shape services, APIs and traffic flow.

[API gateway](patterns/architecture/api-gateway) ·
[Backend for Frontend](patterns/architecture/backend-for-frontend) ·
[Service discovery](patterns/architecture/service-discovery) ·
[Load balancing](patterns/architecture/load-balancing) ·
[Sidecar](patterns/architecture/sidecar) ·
[Strangler fig](patterns/architecture/strangler-fig) ·
[Sharded counters](patterns/architecture/sharded-counters) ·
[Fan-out feed](patterns/architecture/fan-out-feed) ·
[Cursor pagination](patterns/architecture/cursor-pagination) ·
[Webhook delivery](patterns/architecture/webhook-delivery) ·
[Scatter-gather](patterns/architecture/scatter-gather) ·
[Server-sent events](patterns/architecture/server-sent-events) ·
[Long polling](patterns/architecture/long-polling) ·
[WebSocket hub](patterns/architecture/websocket-hub) ·
[Presence](patterns/architecture/presence) ·
[Tiered cache](patterns/architecture/tiered-cache) ·
[Conditional requests](patterns/architecture/conditional-requests)

Each category page has a table of the problem every pattern solves and its key trade-off.

## Scenarios

Production incidents reproduced as runnable before-and-afters: the broken design, the fixed one built from the patterns above, and a test that asserts the difference. See [`scenarios/`](scenarios).

| Scenario | Incident | Fixed with |
|---|---|---|
| [Cache stampede](scenarios/cache-stampede) | Every cached key expires at once and floods the database | Refresh-ahead |
| [Hot short link](scenarios/hot-short-link) | One viral link overloads the cache node that owns it | Two-tier cache |
| [Seat double-booking](scenarios/seat-double-booking) | Two shoppers get tickets for the same seat | Reservation hold, optimistic locking, idempotency key |
| [Hot partition](scenarios/hot-partition) | One poll key throttles its table partition | Hot-key salting |
| [Duplicate chat messages](scenarios/duplicate-chat-messages) | Lost acks make messages appear twice or never | Idempotent consumer, Snowflake IDs |
| [Consumer double-processing](scenarios/consumer-double-processing) | A deploy replays uncommitted events and credits customers twice | Idempotent consumer, dead-letter queue |
| [Out-of-order events](scenarios/out-of-order-events) | "Shipped" is processed before "paid" | Per-key ordering |
| [OTP retry storm](scenarios/otp-retry-storm) | Retries multiply SMS traffic 30x during a provider outage | Retry budget, fallback chain, rate limiter |
| [Cascading failure](scenarios/cascading-failure) | One slow dependency exhausts the worker pool and fails every page | Timeout, bulkhead, circuit breaker, outlier detection, fallback |
| [Notification storm](scenarios/notification-storm) | A celebrity post delays OTPs behind millions of pushes | Fan-out feed dispatcher, dead-letter queue |
| [Charged but no order](scenarios/charged-but-no-order) | Payment succeeds, the order doesn't, and the retry charges again | Saga, idempotency key, transactional outbox |
| [Stale rider location](scenarios/stale-rider-location) | The map shows the rider far behind or jumping backwards | Presence, WebSocket hub, backpressure |
| [Surge in the rain](scenarios/surge-in-the-rain) | Surge pricing lags the rain by 15 minutes | Windowed stream aggregation, geospatial index |
| [Release-day buffering](scenarios/release-day-buffering) | A launch makes every CDN edge miss and crash-loops the origin | Tiered cache, serve-stale |

```sh
go run ./scenarios/cache-stampede/broken   # then ./fixed
```

## Which pattern do I need?

| If you're facing… | Start with |
|---|---|
| A flaky dependency | [Retry with backoff](patterns/reliability/retry-backoff), then a [circuit breaker](patterns/reliability/circuit-breaker) |
| Retries causing double charges | [Idempotency key](patterns/reliability/idempotency-key) |
| Retries making an outage worse | [Retry budget](patterns/reliability/retry-budget) |
| A provider is down and you have a backup | [Fallback](patterns/reliability/fallback) (failover chain), [circuit breaker](patterns/reliability/circuit-breaker) |
| One instance slow while its health check stays green | [Outlier detection](patterns/reliability/outlier-detection) |
| One slow service taking everything down | [Bulkhead](patterns/reliability/bulkhead), [timeouts](patterns/reliability/timeout), [load shedding](patterns/reliability/load-shedding) |
| Bad p99 latency | [Hedged requests](patterns/reliability/hedged-requests), [scatter-gather](patterns/architecture/scatter-gather) with a deadline |
| A database that can't keep up with reads | [Cache-aside](patterns/data/cache-aside), [read replicas](patterns/data/read-replicas) |
| Load spikes every time cached keys expire | [Refresh-ahead](patterns/data/refresh-ahead) (TTL jitter, early refresh) |
| One viral key overloading a cache node | [Two-tier cache](patterns/data/two-tier-cache) (in-process L1, hot-key replicas) |
| A cache with a poor hit ratio, or wiped out by batch scans | [Cache eviction](patterns/data/cache-eviction) (W-TinyLFU) |
| Errors whenever the origin is down, even for cached content | [Serve-stale](patterns/reliability/serve-stale) |
| A launch stampeding the origin from every CDN edge | [Tiered cache](patterns/architecture/tiered-cache) (origin shield, pre-warming) |
| Users save a change, refresh, and see the old value | [Read replicas](patterns/data/read-replicas) (session tokens, monotonic reads) |
| A database that can't keep up with writes | [Sharding](patterns/data/sharding), [write-behind cache](patterns/data/write-behind-cache), [sharded counters](patterns/architecture/sharded-counters) |
| One key hotter than a whole partition | [Hot-key salting](patterns/data/hot-key-salting), [two-tier cache](patterns/data/two-tier-cache) |
| Two users booking the same seat | [Optimistic locking](patterns/data/optimistic-locking), [reservation hold](patterns/data/reservation-hold) |
| Two editors overwriting each other, or clients re-downloading unchanged data | [Conditional requests](patterns/architecture/conditional-requests) (ETag, `If-Match`) |
| Real-time updates to many clients | [WebSocket hub](patterns/architecture/websocket-hub), [long polling](patterns/architecture/long-polling), [server-sent events](patterns/architecture/server-sent-events) |
| One slow WebSocket or SSE client stalling broadcasts to everyone | [Backpressure](patterns/distributed/backpressure) (per-connection send queue) |
| Showing who is online, or routing to the server holding a user's connection | [Presence](patterns/architecture/presence) |
| Finding nearby drivers or stores among many moving points | [Geospatial index](patterns/data/geospatial-index) |
| Real-time counts per area per minute that are wrong because events arrive late | [Windowed stream aggregation](patterns/distributed/windowed-stream-aggregation) |
| "My rank" or top 100 too slow with ORDER BY and COUNT(*) | [Leaderboard and top-K](patterns/data/leaderboard-top-k) |
| Unique, sortable IDs from many servers without a central counter | [Snowflake IDs](patterns/data/snowflake-id) |
| Updating a DB and publishing an event atomically | [Transactional outbox](patterns/distributed/transactional-outbox) |
| Events processed twice or out of order | [Idempotent consumer](patterns/distributed/idempotent-consumer), [per-key ordering](patterns/distributed/per-key-ordering) |
| One bad message blocking or slowing the whole consumer | [Dead-letter queue](patterns/distributed/dead-letter-queue) |
| A viral post's notifications delaying OTPs or hitting push-provider limits | [Fan-out feed](patterns/architecture/fan-out-feed) (notification dispatcher) |
| A business transaction spanning several services | [Saga](patterns/distributed/saga-orchestration), rather than [two-phase commit](patterns/distributed/two-phase-commit) |
| Only one worker may do a job at a time | [Leader election](patterns/distributed/leader-election), [lock with fencing](patterns/distributed/distributed-lock-fencing) |
| Replacing a legacy system | [Strangler fig](patterns/architecture/strangler-fig) |

## Layout

Every pattern follows the same shape:

```
patterns/<category>/<pattern>/
├── README.md        problem, solution, diagram, trade-offs, when to use, real-world examples
├── <pkg>.go         the implementation
├── <pkg>_test.go    table-driven tests, race-safe
└── example/main.go  a runnable simulation that narrates what happens
```

Package names are the folder name without hyphens, e.g. `patterns/reliability/circuit-breaker` is package `circuitbreaker`.

## Using the code

These implementations are written to be read and understood first. They are correct and tested, but kept deliberately small. For production use, prefer battle-tested libraries and infrastructure (etcd, ZooKeeper, Kafka, Redis, Envoy and so on); each README points to the relevant ones.

## Contributing

Issues and pull requests are welcome, whether for a new pattern, a clearer explanation or a bug fix. Please keep the same layout, stick to the standard library, and make sure `gofmt`, `go vet` and `go test -race ./...` pass.

## License

[MIT](LICENSE) © 2026 Neeraj Singhi
