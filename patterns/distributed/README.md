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
| [Pub/Sub](pub-sub) | Fan one event out to many decoupled subscribers | A slow subscriber must be dropped, lose messages, or be disconnected |
| [Competing consumers](competing-consumers) | Scale out workers on a shared queue without losing jobs on crashes | At-least-once delivery: handlers must be idempotent |
| [Transactional outbox](transactional-outbox) | Update the database and publish an event without dual-write inconsistency | Duplicates and relay latency instead of lost events |
| [Saga (orchestration)](saga-orchestration) | Multi-service transaction with a central coordinator and compensations | No isolation; the orchestrator couples to every service |
| [Saga (choreography)](saga-choreography) | Multi-service transaction driven purely by events | Loose coupling, but the flow is implicit and hard to follow |
| [Two-phase commit](two-phase-commit) | Atomic commit across several resource managers | Blocks with locks held if the coordinator fails after prepare |
| [Leader election](leader-election) | Exactly one active node, with automatic failover | Lease safety depends on clocks; failover leaves a leaderless gap |
| [Distributed lock with fencing](distributed-lock-fencing) | Mutual exclusion that survives paused or slow lock holders | The protected resource must check tokens |
| [Lamport and vector clocks](lamport-vector-clocks) | Order events and detect concurrency without synchronised clocks | Lamport is cheap but blind to concurrency; vectors grow with nodes |
| [Gossip](gossip) | Spread membership and state across a large cluster without a centre | Eventual consistency; detection speed vs false suspicion |
| [Quorum (N/R/W)](quorum) | Tunable consistency for replicated data | Latency and availability vs staleness; LWW loses concurrent writes |
| [Backpressure](backpressure) | Keep a fast producer from overwhelming a slow consumer | Block (slow producers), drop (lose data) or signal (needs cooperation) |

Run everything in this category:

```sh
go test -race ./patterns/distributed/...
go run ./patterns/distributed/<pattern>/example
```
