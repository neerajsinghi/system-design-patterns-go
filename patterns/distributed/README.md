# Distributed Coordination and Messaging

Patterns for getting independent processes to cooperate when the network
drops, delays and duplicates messages, clocks disagree, and any node can crash
at the worst moment. Each pattern is a small, dependency-free Go package
whose nodes, brokers and databases are simulated in-process, with tests that
inject crashes, partitions and duplicates, and a runnable example. They are
teaching implementations; every README says what a production system adds
(etcd, ZooKeeper, Raft, durable brokers).

| Pattern | Problem it solves | Key trade-off |
|---|---|---|
| [Pub/Sub](pub-sub) | Fan one event out to many decoupled subscribers, across schema versions | A slow subscriber must be dropped or slowed, and upcaster chains grow with every version |
| [Competing consumers](competing-consumers) | Scale out workers on a shared queue without losing jobs on crashes | At-least-once delivery: handlers must be idempotent |
| [Split-process-merge](split-process-merge) | One huge job takes hours serially and a late failure restarts it from zero | Near-linear speedup with per-chunk retry and resume, but the serial merge and stragglers cap it |
| [Idempotent consumer](idempotent-consumer) | At-least-once delivery applies side effects twice after retransmits, crashes or rebalances | Effectively-once, but the side effect and the processed-ID table must share one transaction, and duplicates older than the window slip through |
| [Per-key ordering](per-key-ordering) | Events for one entity arrive or are processed out of sequence | Order per key with parallelism across keys, but hot keys serialize and resequencing adds buffering latency |
| [Dead-letter queue](dead-letter-queue) | A poison message retried forever blocks an ordered queue or burns workers | Healthy messages keep flowing, but parked ones need an owner, an alert and a redrive |
| [Transactional outbox](transactional-outbox) | Update the database and publish an event without dual-write inconsistency | Duplicates and relay latency instead of lost events |
| [Saga (orchestration)](saga-orchestration) | Multi-service transaction with a central coordinator and compensations | No isolation; the orchestrator couples to every service |
| [Saga (choreography)](saga-choreography) | Multi-service transaction driven purely by events | Loose coupling, but the flow is implicit and hard to follow |
| [Two-phase commit](two-phase-commit) | Atomic commit across several resource managers | Blocks with locks held if the coordinator fails after prepare |
| [Leader election](leader-election) | Exactly one active node, with automatic failover | Lease safety depends on clocks; failover leaves a leaderless gap |
| [Distributed lock with fencing](distributed-lock-fencing) | Mutual exclusion that survives paused or slow lock holders | The protected resource must check tokens |
| [Lamport and vector clocks](lamport-vector-clocks) | Order events and detect concurrency without synchronised clocks | Lamport is cheap but blind to concurrency; vectors grow with nodes |
| [Gossip](gossip) | Spread membership and state across a large cluster without a centre | Eventual consistency; detection speed vs false suspicion |
| [Quorum (N/R/W)](quorum) | Tunable consistency for replicated data | Latency and availability vs staleness; LWW loses concurrent writes |
| [Backpressure](backpressure) | A producer, or a fan-out publisher, outruns its consumers | Block keeps every message but slows the producer; drop or disconnect keeps it fast but loses data or the connection |
| [Windowed stream aggregation](windowed-stream-aggregation) | Real-time per-key aggregates over a stream whose events arrive late and out of order | Watermark delay and allowed lateness trade latency against completeness, and late results arrive as revisions |

Run everything in this category:

```sh
go test -race ./patterns/distributed/...
go run ./patterns/distributed/<pattern>/example
```
