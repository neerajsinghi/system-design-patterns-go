# System Design Patterns in Go

[![CI](https://github.com/neerajsinghi/system-design-patterns-go/actions/workflows/ci.yml/badge.svg)](https://github.com/neerajsinghi/system-design-patterns-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/neerajsinghi/system-design-patterns-go.svg)](https://pkg.go.dev/github.com/neerajsinghi/system-design-patterns-go)

**53 system design patterns, each as a small, tested Go implementation you can run in under a second.**

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
[Read replicas](patterns/data/read-replicas) ·
[CQRS](patterns/data/cqrs) ·
[Event sourcing](patterns/data/event-sourcing) ·
[Bloom filter](patterns/data/bloom-filter) ·
[Write-ahead log](patterns/data/write-ahead-log) ·
[LSM tree](patterns/data/lsm-tree) ·
[Materialized view](patterns/data/materialized-view)

### [Distributed coordination & messaging](patterns/distributed)

Agree, order, communicate and stay consistent across machines.

[Pub/Sub](patterns/distributed/pub-sub) ·
[Competing consumers](patterns/distributed/competing-consumers) ·
[Transactional outbox](patterns/distributed/transactional-outbox) ·
[Saga (orchestration)](patterns/distributed/saga-orchestration) ·
[Saga (choreography)](patterns/distributed/saga-choreography) ·
[Two-phase commit](patterns/distributed/two-phase-commit) ·
[Leader election](patterns/distributed/leader-election) ·
[Distributed lock with fencing](patterns/distributed/distributed-lock-fencing) ·
[Lamport & vector clocks](patterns/distributed/lamport-vector-clocks) ·
[Gossip](patterns/distributed/gossip) ·
[Quorum (N/R/W)](patterns/distributed/quorum) ·
[Backpressure](patterns/distributed/backpressure)

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
[Tiered cache](patterns/architecture/tiered-cache)

Each category page has a table of the problem every pattern solves and its key trade-off.

## Scenarios

Production incidents reproduced as runnable before-and-afters: the broken design, the fixed one built from the patterns above, and a test that asserts the difference. See [`scenarios/`](scenarios).

| Scenario | Incident | Fixed with |
|---|---|---|
| [Cache stampede](scenarios/cache-stampede) | Every cached key expires at once and floods the database | Refresh-ahead |
| [Hot short link](scenarios/hot-short-link) | One viral link overloads the cache node that owns it | Two-tier cache |
| [Release-day buffering](scenarios/release-day-buffering) | A launch makes every CDN edge miss and crash-loops the origin | Tiered cache, serve-stale |

```sh
go run ./scenarios/cache-stampede/broken   # then ./fixed
```

## Which pattern do I need?

| If you're facing… | Start with |
|---|---|
| A flaky dependency | [Retry with backoff](patterns/reliability/retry-backoff), then a [circuit breaker](patterns/reliability/circuit-breaker) |
| Retries causing double charges | [Idempotency key](patterns/reliability/idempotency-key) |
| One slow service taking everything down | [Bulkhead](patterns/reliability/bulkhead), [timeouts](patterns/reliability/timeout), [load shedding](patterns/reliability/load-shedding) |
| Bad p99 latency | [Hedged requests](patterns/reliability/hedged-requests), [scatter-gather](patterns/architecture/scatter-gather) with a deadline |
| A database that can't keep up with reads | [Cache-aside](patterns/data/cache-aside), [read replicas](patterns/data/read-replicas) |
| Load spikes every time cached keys expire | [Refresh-ahead](patterns/data/refresh-ahead) (TTL jitter, early refresh) |
| One viral key overloading a cache node | [Two-tier cache](patterns/data/two-tier-cache) (in-process L1, hot-key replicas) |
| A cache with a poor hit ratio, or wiped out by batch scans | [Cache eviction](patterns/data/cache-eviction) (W-TinyLFU) |
| Errors whenever the origin is down, even for cached content | [Serve-stale](patterns/reliability/serve-stale) |
| A launch stampeding the origin from every CDN edge | [Tiered cache](patterns/architecture/tiered-cache) (origin shield, pre-warming) |
| A database that can't keep up with writes | [Sharding](patterns/data/sharding), [write-behind cache](patterns/data/write-behind-cache), [sharded counters](patterns/architecture/sharded-counters) |
| Updating a DB and publishing an event atomically | [Transactional outbox](patterns/distributed/transactional-outbox) |
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
