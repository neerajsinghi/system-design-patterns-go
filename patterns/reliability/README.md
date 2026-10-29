# Reliability and Resilience Patterns

Distributed systems fail partially and constantly: dependencies slow down,
replicas pause, networks drop responses, and traffic spikes past capacity.
These patterns keep a service correct and responsive anyway. They bound how
long and how much work any request may consume, stop failures from cascading,
and degrade gracefully instead of falling over. Each one is a small,
dependency-free Go package with table-driven tests and a runnable simulation.

| Pattern | Problem it solves | Key trade-off |
|---|---|---|
| [Retry with backoff](retry-backoff) | Transient failures that would succeed a moment later | Adds latency and load; needs jitter, a budget and idempotency |
| [Retry budget](retry-budget) | Per-request retries multiply load on a failing dependency, and multiply again across layers | Caps outage load near (1+ratio)x, but refuses some retries that would have succeeded in a partial outage |
| [Circuit breaker](circuit-breaker) | Callers wasting resources on a dependency that is down | Rejects some calls that would have succeeded while open |
| [Bulkhead](bulkhead) | One slow dependency exhausting every goroutine and connection | Static partitions waste idle capacity and need tuning |
| [Timeout and deadline propagation](timeout) | Hung calls, and downstream work nobody is waiting for any more | Too tight fails valid slow requests; too loose prolongs overload |
| [Token bucket rate limiter](rate-limiter-token-bucket) | Capping a client's sustained rate while allowing bursts | Bursts still reach the backend; per-process unless state is shared |
| [Leaky bucket rate limiter](rate-limiter-leaky-bucket) | Downstreams that need perfectly smooth, constant-rate input | Queued requests wait; no bursting even when idle |
| [Sliding window rate limiter](sliding-window-rate-limiter) | "N per window" quotas without the fixed-window boundary burst | Exact log costs O(N) memory; the O(1) counter is approximate |
| [Load shedding](load-shedding) | Goodput collapse when offered load exceeds capacity | Overload becomes visible errors; priorities must be honest |
| [Fallback](fallback) | One failing dependency or provider turning into user-facing errors | Stale or default answers, or pricier backup providers in a failover chain (and possible duplicate delivery) |
| [Serve-stale](serve-stale) | Cached entries expiring into errors while the origin is down | Outages become invisible, and so does how old the data is |
| [Health check](health-check) | Restarting the wrong thing, routing to instances that cannot serve, or keeping dead client connections open | Faster detection means more false positives and more probe traffic |
| [Outlier detection](outlier-detection) | A slow-but-alive host passes health checks while ruining p99 for its share of traffic | Ejects bad hosts automatically, but needs a few bad requests first and can't fix a pool-wide slowdown |
| [Hedged requests](hedged-requests) | Tail latency (p99) dominated by occasional slow replicas | Extra load (~5%); only safe for idempotent operations |
| [Idempotency key](idempotency-key) | Retried writes executing twice (double charges) | Record and side effect must commit atomically; storage per key |

Run everything in this category:

```sh
go test -race ./patterns/reliability/...
go run ./patterns/reliability/circuit-breaker/example   # or any other pattern
```
