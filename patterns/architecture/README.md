# Architecture, APIs & Scaling

These patterns cover how requests enter a system, find the right service, and
spread across replicas, and how APIs stay correct while data and topology
keep changing underneath them. Each one is a small, standard-library Go
package with table-driven tests and a runnable example that tells the story
on loopback.

| Pattern | Problem it solves | Key trade-off |
| --- | --- | --- |
| [API gateway](api-gateway) | Clients must know every service, and every service re-implements auth and tracing | One place for edge concerns, but an extra hop and a potential monolith-in-the-middle |
| [Backend for Frontend](backend-for-frontend) | One general API over-fetches for mobile and under-serves web | Payloads and failure policy tailored per client, at the cost of more deployables and duplicated glue |
| [Service discovery](service-discovery) | Instance addresses change constantly, and crashed instances don't announce it | TTL leases drop dead instances automatically, but detection lags by up to TTL plus cache time |
| [Load balancing](load-balancing) | Blind request spreading lets one slow replica drown | Load-aware strategies (least connections, P2C) avoid hot spots but need in-flight tracking |
| [Sidecar](sidecar) | Every app re-implements retries and metrics, in every language | Cross-cutting features with zero app changes, but an extra process and hop per instance |
| [Strangler fig](strangler-fig) | Big-bang rewrites of legacy systems are risky | Incremental, reversible migration, but two systems run in parallel for a long time |
| [Sharded counters](sharded-counters) | One hot counter row caps write throughput | Writes scale with shard count. Reads cost N lookups and aren't point-in-time |
| [Fan-out feed](fan-out-feed) | Home timelines and push notifications must stay fast despite celebrity accounts | Push makes reads cheap and pull makes writes cheap. Priority lanes protect OTPs from notification storms but don't add provider capacity |
| [Cursor pagination](cursor-pagination) | Offset paging repeats or skips items under concurrent writes and slows with depth | Stable, O(limit) pages and tamper-proof cursors, but no "jump to page N" |
| [Webhook delivery](webhook-delivery) | Notifying unreliable third-party servers securely and exactly once | At-least-once retries plus signed, deduplicated receivers, but you operate a retry queue and secrets |
| [Scatter-gather](scatter-gather) | Waiting for every parallel source lets the slowest one set your latency | Latency bounded by a deadline, but callers must handle partial results |
| [Server-sent events](server-sent-events) | Live server-to-client streams lose events across reconnects | Plain HTTP with built-in resume via `Last-Event-ID`, but one-way and limited by history depth |
| [Long polling](long-polling) | Clients need near-real-time updates but can't use SSE or WebSockets, and short polling wastes requests | Plain HTTP with near-push latency and no lost updates, but one held request per waiting client and a re-poll spike after every broadcast |
| [WebSocket hub](websocket-hub) | Two-way real-time delivery to users whose connections are spread over many nodes, with slow and dead clients | Any node can reach any user via a registry and pub/sub, but connections are stateful and pinned, and a full queue means dropping or disconnecting |
| [Presence](presence) | Knowing who is online, on which gateway node, and when they were last seen, without a DB write per heartbeat | Cheap heartbeats and stable presence, but offline is announced late (TTL plus grace) |
| [Tiered cache](tiered-cache) | Every CDN edge missing at once and stampeding the origin at launch | The origin sees one fetch per object, but the shield is a new concentration point |
| [Conditional requests](conditional-requests) | Clients re-download unchanged resources, and concurrent PUTs silently overwrite each other | 304s cut transfer about 20x and 412s turn lost updates into visible conflicts, but clients must retry or merge |

Run everything in this category:

```sh
go test -race ./patterns/architecture/...
for d in patterns/architecture/*/example; do go run ./$d; done
```
