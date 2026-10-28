# Scenarios

Interview-style incidents, each reproduced as a runnable before-and-after. A scenario starts from a production failure, finds the root cause, and fixes it by combining patterns from [`patterns/`](../patterns).

| Scenario | Incident | Patterns combined |
| --- | --- | --- |
| [Cache stampede](cache-stampede) | A deploy warms the cache, and one TTL later every key expires at once and floods the database | [Refresh-ahead](../patterns/data/refresh-ahead), [cache-aside](../patterns/data/cache-aside) |
| [Hot short link](hot-short-link) | One viral link overloads the single cache node that owns it | [Two-tier cache](../patterns/data/two-tier-cache) |
| [Seat double-booking](seat-double-booking) | Two shoppers both see a seat as free and both get tickets; abandoned carts lock seats; a retried webhook charges twice | [Reservation hold](../patterns/data/reservation-hold), [optimistic locking](../patterns/data/optimistic-locking), [idempotency key](../patterns/reliability/idempotency-key) |
| [Hot partition](hot-partition) | One live-vote poll overloads its table partition and throttles unrelated polls; adding partitions doesn't help | [Hot-key salting](../patterns/data/hot-key-salting), [sharded counters](../patterns/architecture/sharded-counters) |
| [Out-of-order events](out-of-order-events) | "Shipped" overtakes "paid", so orders get rejected transitions and shipped emails before payment | [Per-key ordering](../patterns/distributed/per-key-ordering) |
| [Duplicate chat messages](duplicate-chat-messages) | Lost acks on flaky mobile networks make the sender resend and the server re-push, so messages show up twice or never | [Idempotent consumer](../patterns/distributed/idempotent-consumer), [Snowflake IDs](../patterns/data/snowflake-id), [retry with backoff](../patterns/reliability/retry-backoff) |
| [Consumer double-processing](consumer-double-processing) | A deploy kills consumers before they commit, so replays credit customers twice; one malformed event crash-loops the partition | [Idempotent consumer](../patterns/distributed/idempotent-consumer), [dead-letter queue](../patterns/distributed/dead-letter-queue) |
| [OTP retry storm](otp-retry-storm) | The SMS provider degrades at peak; retries at the server, app and "resend" button multiply calls 30x and codes arrive late | [Retry budget](../patterns/reliability/retry-budget), [circuit breaker](../patterns/reliability/circuit-breaker), [fallback](../patterns/reliability/fallback) (failover chain), [sliding-window rate limiter](../patterns/reliability/sliding-window-rate-limiter) |
| [Cascading failure](cascading-failure) | One slow recommendations service ties up the shared worker pool, so every product page fails and gateway retries make it worse | [Timeout](../patterns/reliability/timeout), [bulkhead](../patterns/reliability/bulkhead), [circuit breaker](../patterns/reliability/circuit-breaker), [outlier detection](../patterns/reliability/outlier-detection), [fallback](../patterns/reliability/fallback) |
| [Notification storm](notification-storm) | A celebrity post floods the push queue shared with OTPs; providers return 429s, retries make it worse, dead tokens retry forever | [Fan-out feed](../patterns/architecture/fan-out-feed) (notification dispatcher), [dead-letter queue](../patterns/distributed/dead-letter-queue), [token bucket](../patterns/reliability/rate-limiter-token-bucket) |
| [Charged but no order](charged-but-no-order) | A payment succeeds but the response times out or the order service crashes; the retry charges again, and events disagree with the database | [Saga (orchestration)](../patterns/distributed/saga-orchestration), [idempotency key](../patterns/reliability/idempotency-key), [transactional outbox](../patterns/distributed/transactional-outbox) |
| [Stale rider location](stale-rider-location) | The delivery map shows the rider hundreds of metres behind, jumping backwards, or frozen with no signal | [Presence](../patterns/architecture/presence), [WebSocket hub](../patterns/architecture/websocket-hub), [backpressure](../patterns/distributed/backpressure) (drop-oldest send queue) |
| [Surge in the rain](surge-in-the-rain) | Rain triples downtown demand, but city-wide 15-minute batches price it late, under-price the rain and over-price the dry suburbs | [Windowed stream aggregation](../patterns/distributed/windowed-stream-aggregation), [geospatial index](../patterns/data/geospatial-index) |
| [Video pipeline](video-pipeline) | Uploads encode synchronously, so requests time out at the load balancer, retries start duplicate encodes, and a crash at hour 5 restarts a 6-hour encode | [Async request-reply](../patterns/architecture/async-request-reply), [split-process-merge](../patterns/distributed/split-process-merge) |
| [Gateway bottleneck](gateway-bottleneck) | During a sale the gateway pins its CPU: a remote auth check and a new TLS connection per request, plus one partner sending 10x traffic | [API gateway](../patterns/architecture/api-gateway) (auth offloading), [connection pool](../patterns/architecture/connection-pool), [token bucket](../patterns/reliability/rate-limiter-token-bucket) |
| [Join-free feed](join-free-feed) | The home feed JOIN reads thousands of rows per view; at the evening peak the database pins and p99 goes to seconds | [Materialized view](../patterns/data/materialized-view), [CQRS](../patterns/data/cqrs), [fan-out feed](../patterns/architecture/fan-out-feed) |
| [Chatbot hallucinates](chatbot-hallucinates) | The support bot quotes a stale 14-day return window and invents a discount code; a first RAG attempt with tiny chunks still answers wrong and never says "I don't know" | [RAG retrieval](../patterns/ai/rag-retrieval) |
| [LLM bill explodes](llm-bill-explodes) | Every request goes to the largest model with the full history, paraphrased FAQs each pay again, and one tenant's loop burns a month's budget in a day | [Semantic cache](../patterns/ai/semantic-cache), [model router](../patterns/ai/model-router) |
| [Slow vector search](slow-vector-search) | Flat semantic search slows linearly as the catalogue grows; the IVF index added later is never retrained, so new categories get wrong results | [Vector ANN index](../patterns/ai/vector-ann-index) |
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
