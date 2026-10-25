# Timeout and Deadline Propagation

## Problem

A call with no timeout can hang forever and hold a goroutine, a connection and
memory with it. Fixed timeouts per hop are not enough either. If the gateway
gives up after 1s but each of three downstream hops has its own 1s timeout,
deep services keep working on requests nobody is waiting for any more, and
under load that wasted work is what tips the system over.

## Solution

Set **one deadline at the edge** and propagate it. Each hop computes its
downstream timeout from the *remaining* budget, keeps a small reserve for its
own post-processing, caps it, and **fails fast** when the remainder is too
small to be useful. Across process boundaries the remaining time travels in a
header, as gRPC does with `grpc-timeout`.

## Diagram

```mermaid
sequenceDiagram
    participant C as Client
    participant G as Gateway
    participant O as Orders
    participant I as Inventory
    C->>G: request (deadline T+200ms)
    G->>O: X-Request-Timeout-Ms: 200
    Note over O: 30ms local work, keep 20ms reserve
    O->>I: X-Request-Timeout-Ms: 150
    Note over I: slow; ctx expires at 150ms
    I-->>O: context.DeadlineExceeded
    O-->>G: 504 (with 20ms to spare)
    G-->>C: 504 before the client's deadline
```

## How this implementation works

- `HopPolicy.Budget(deadline, hasDeadline, now)` is a pure function:
  `remaining - Reserve`, capped by `Max`, falling back to `Default` when there
  is no incoming deadline, and returning `ErrInsufficientBudget` below `Min`.
- `WithHop(ctx, policy)` turns that into a child context. The returned cancel
  func is always non-nil, so `defer cancel()` is safe even on error.
- `Run(ctx, fn)` returns as soon as `ctx` is done, even if `fn` ignores it. The
  result channel is buffered, so the goroutine running a misbehaving `fn` exits
  when `fn` finally returns instead of leaking.
- `FormatRemaining` and `FromHeader` encode and decode the remaining budget in
  milliseconds. Malformed, negative or absurd header values are ignored rather
  than trusted, and the parent's own deadline still applies.

## Trade-offs

- Clock skew does not matter, because only *durations* cross the wire. Network
  transit time is not subtracted, though, so downstream budgets are slightly
  generous.
- Picking the edge deadline is a product decision: too short and slow but valid
  requests fail, too long and overload lasts longer.
- `Run` stops *waiting*, not *working*. Only code that checks `ctx` actually
  stops consuming resources.
- Timeouts plus retries need care: retries must fit inside the same budget, not
  reset it.

## When to use / when not to

Always set timeouts on network I/O. Use deadline propagation in any
multi-hop request path (HTTP or gRPC microservices, fan-out queries). For
long-running background jobs, use cancellation and heartbeats instead of tight
deadlines.

## Real-world examples

- gRPC propagates deadlines automatically through the `grpc-timeout` header,
  and Go's gRPC library maps them onto `context.Context`.
- Go's `net/http` server and client accept contexts, and `http.Server` has
  `ReadTimeout`, `WriteTimeout` and `IdleTimeout` settings.
- Envoy supports per-route timeouts and can honour
  `x-envoy-expected-rq-timeout-ms` to tell upstreams the remaining budget.

## Run it

```sh
go run ./patterns/reliability/timeout/example
go test ./patterns/reliability/timeout/
```

## Related patterns

- [Retry with backoff](../retry-backoff): retries must fit inside the deadline.
- [Hedged requests](../hedged-requests): spend part of the budget on a backup request.
- [Circuit breaker](../circuit-breaker): timeouts are the failures that trip it.
- [Load shedding](../load-shedding): drop requests whose deadline is already hopeless.
- [Scatter-gather](../../architecture/scatter-gather): fan-out needs a shared deadline.
- [API gateway](../../architecture/api-gateway): where the end-to-end deadline is usually set.
