# Scenarios

Interview-style incidents, each reproduced as a runnable before-and-after. A scenario starts from a production failure, finds the root cause, and fixes it by combining patterns from [`patterns/`](../patterns).

| Scenario | Incident | Patterns combined |
| --- | --- | --- |
| [Cache stampede](cache-stampede) | A deploy warms the cache, and one TTL later every key expires at once and floods the database | [Refresh-ahead](../patterns/data/refresh-ahead), [cache-aside](../patterns/data/cache-aside) |
| [Hot short link](hot-short-link) | One viral link overloads the single cache node that owns it | [Two-tier cache](../patterns/data/two-tier-cache) |
| [Seat double-booking](seat-double-booking) | Two shoppers both see a seat as free and both get tickets; abandoned carts lock seats; a retried webhook charges twice | [Reservation hold](../patterns/data/reservation-hold), [optimistic locking](../patterns/data/optimistic-locking), [idempotency key](../patterns/reliability/idempotency-key) |
| [Hot partition](hot-partition) | One live-vote poll overloads its table partition and throttles unrelated polls; adding partitions doesn't help | [Hot-key salting](../patterns/data/hot-key-salting), [sharded counters](../patterns/architecture/sharded-counters) |
| [Release-day buffering](release-day-buffering) | Every CDN edge misses on a new episode at launch and crash-loops the origin | [Tiered cache](../patterns/architecture/tiered-cache), [serve-stale](../patterns/reliability/serve-stale) |

## Layout

```
scenarios/<slug>/
├── README.md         incident, symptoms, root cause, fix, results, interview follow-ups
├── scenario.go       the simulation: Broken() and Fixed() return a Report
├── scenario_test.go  asserts the invariant: Broken violates it, Fixed holds it
├── broken/main.go    go run it to watch the failure
└── fixed/main.go     go run it to watch the fix
```

Both versions get identical traffic and identical dependencies; only the design changes. Simulated time and seeded randomness keep every run deterministic, and each one finishes in about a second. Because scenarios import the pattern packages, their tests also check that the patterns work together.

## Run them

```sh
go run ./scenarios/cache-stampede/broken
go run ./scenarios/cache-stampede/fixed
go test ./scenarios/...
```
