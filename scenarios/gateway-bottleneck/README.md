# Scenario: The API Gateway Melts During the Sale

## The incident

A retailer's flash sale starts at 12:00, and API traffic triples within a second. The API gateway's CPU goes to 99%, and p99 latency for every client goes from tens of milliseconds to the gateway's 2-second timeout. The backends' dashboards stay green: they are healthy, but most requests never reach them. The autoscaler adds a gateway. CPU drops to about 80%, but latency keeps rising, because the queue just moves somewhere else. About fifteen seconds into the sale the gateway is answering almost every request with a 504, while its CPU sits at 25% and the auth service runs flat out on token checks for requests the gateway has already given up on. One partner integration had a retry bug and was sending ten times its usual volume through it all.

## Symptoms

- Gateway CPU pinned at 99% in the first seconds of the sale, while backend CPU and error rate look normal.
- Auth service at exactly its capacity (1,111 checks/s) from the first second of the sale until the end, with a growing queue.
- About 1,000 new upstream TCP+TLS handshakes per second during the sale, and up to 9,300 sockets in TIME_WAIT on one gateway.
- The backend refuses connections (1,478 in the first five seconds) although it handles only ~1,100 requests/s: sockets are held open while the pinned gateway gets around to their TLS handshake.
- After scaling out, CPU goes *down* and latency goes *up*.
- One client ID is a third of all traffic.

## Root cause

1. **The auth service is on every request's path.** The gateway asks it to validate every bearer token. It was sized at 3.6× the pre-sale load and the sale brings 4.1×, so its queue grows without limit. Each request pays a round trip and the queue, and once the queue is longer than the gateway's timeout every check is wasted work: the gateway gives up, and the auth service, which never hears about it, still does the check. That is a metastable failure; it doesn't end when the traffic does.
2. **A new upstream connection per request.** Each request opens a TCP+TLS connection to the backend and closes it afterwards. At 1.5 ms of client-side handshake CPU, the handshake costs five times the actual proxying (0.3 ms). Every close leaves a socket in TIME_WAIT for 60 s, eating the gateway's ephemeral ports; and while the CPU is pinned, connections sit half open waiting for their handshake, which fills the backend's connection limit.
3. **No per-client limit.** The noisy partner sends 500 req/s against a normal 50, and every one of them costs an auth check, a handshake and a backend slot. Nothing separates its traffic from everybody else's.
4. **Scaling out the gateway scales the problem.** A new gateway adds CPU, so more requests per second get as far as the auth service and the backend, each with its own new connection. The gateway was never the only bottleneck.

## The fix

| Change | Pattern | Effect |
| --- | --- | --- |
| Verify the signed (HS256) token at the edge, with a per-gateway cache of verified tokens | [API gateway](../../patterns/architecture/api-gateway) (`TokenVerifier`, auth offloading) | Auth service calls drop from 1 per request to **0**; 99.94% of verifications are a cache hit at 5 µs |
| A bounded pool of upstream connections per gateway, sized so a full fleet fits the backend's limit (4 × 16 ≤ 70) | [Connection pool](../../patterns/architecture/connection-pool) | Handshakes drop from ~1,000/s to 0.2/s; no TIME_WAIT; backend connections peak at 32 |
| A token bucket per client ID (100 req/s, burst 100), checked right after verification | [Token bucket](../../patterns/reliability/rate-limiter-token-bucket) (`Keyed`) | The noisy partner gets 23,735 fast 429s and is admitted at its quota; nobody else sees a 429 |

## Run it

```sh
go run ./scenarios/gateway-bottleneck/broken
go run ./scenarios/gateway-bottleneck/fixed
go test ./scenarios/gateway-bottleneck/
```

During the sale (t=20-80 s):

| | Broken | Fixed |
| --- | --- | --- |
| Peak gateway CPU (1 s) | 99% (3 gateways after autoscaling) | 16% (2 gateways, no scale-out) |
| Well-behaved clients: p50 / p99 | 2 s / 2 s (timeout) | 31 ms / 70 ms |
| Well-behaved requests served | 9,912 of 45,222 (34,437 × 504, 873 × 502) | 45,222 of 45,222 |
| Noisy partner | 29,833 admitted, 6,528 served | 6,098 admitted (quota 6,100), 23,735 × 429 |
| Auth service checks | 1,256/s requested against 1,111/s capacity; 1.00 per request | 0 |
| Upstream handshakes | 281/s average, ~1,000/s while traffic still got through | 0.2/s |
| Peak TIME_WAIT on one gateway | 9,336 of 28,232 ephemeral ports | 0 |
| Peak backend connections (limit 70) | 70, 1,478 connects refused | 32 |
| Requests that waited for a pooled connection | – | 11,809 (p99 wait 50 ms) |
| Revoked token served after revocation | 0 | 168 (valid until its exp, 14 min 10 s later) |

The timeline in `broken` shows the shape of the incident: at t=20-23 s CPU is 91-99%, the autoscaler adds a third gateway at t=24 s, CPU settles at 80%, and p99 keeps climbing (660 ms to 2 s by t=34 s) because the auth queue is still growing. From t=37 s every auth reply arrives after the 2 s timeout, so no request reaches the backend, handshakes drop to zero and CPU falls to 25%. The autoscaler doesn't add a fourth gateway because CPU is no longer high: the symptom it watches has gone away while the outage continues. TIME_WAIT stays under the port range in this run. A gateway's steady state is its connection rate × 60 s: at the ~1,100 connections/s the auth service lets through, that is about 22,000 sockets each on three gateways, but 33,000 on the original two, more than the 28,232 ports, had the auth service not run out first. In `fixed`, the worst single second for well-behaved p99 is 103 ms, in the first ten seconds of the sale, from pool waits as traffic jumps. Over the sale as a whole p99 is 70 ms.

The tests assert the invariant for the fixed version (every second's gateway CPU ≤ 80%; well-behaved p99 ≤ 100 ms and every well-behaved request served; zero auth service calls; backend connections within the limit and none refused; the noisy partner admitted no more than its quota of 100/s × 60 s + one burst; no handshake churn) and check that the broken version violates it: pinned CPU, p99 above target, one auth call per request, the partner over quota, handshake churn, refused backend connections, and most well-behaved requests failing. `TestQuotasAreNeeded` runs the fixed design without the per-client limits: CPU and auth are fine, but the noisy partner takes the pooled connections and well-behaved p99 goes to 2 s, so all three changes are needed. The cost tests check that 429s go to the noisy partner and nobody else, that requests do wait for pooled connections under burst but for less than the latency target, and that a revoked token keeps working until its exp (the verifier is also checked on a fake clock: a cache TTL longer than the token's remaining life doesn't extend it). A last test runs both designs concurrently and checks they reproduce the same results and saw identical traffic.

**How the simulation is built.** A discrete-event simulation on a simulated clock covers 80 s: 20 s before the sale and 60 s of it, in about 0.2 s of CPU. Ten well-behaved clients send Poisson traffic at 25 req/s each (75 during the sale), the noisy partner 50 req/s (500 during the sale), and a twelfth client at 5 req/s has its token revoked at t=50 s; arrivals come from a PCG seeded per client, and each backend latency (20-40 ms, 1% at 60-90 ms) is a hash of the request's index, so both designs get the same requests with the same backend times. Gateways start at two, one core each, and requests are spread round robin. The autoscaler (same in both) adds a gateway when fleet CPU over the last 5 s is above 80%, at most every 15 s, up to four. The cost model is CPU per piece of work: proxying 0.3 ms, a remote auth call 0.3 ms, a client-side TCP+TLS handshake 1.5 ms, a local HS256 verification 30 µs (5 µs on a cache hit), writing a 429/401 20 µs. Each gateway's cores run jobs first come, first served. The auth service has 4 workers at 3.6 ms per check (1,111/s), 0.5 ms away; a handshake also costs 2 RTTs. The backend accepts 70 concurrent connections; each gateway has 28,232 ephemeral ports and closed sockets stay in TIME_WAIT for 60 s. The gateway answers 504 once a request is 2 s old. All three patterns are the real packages on the simulated clock: tokens are real HS256 tokens made by `SignHS256` and checked by a `TokenVerifier` per gateway (cache of 1,024 entries, 60 s TTL); the auth service in the broken design checks them with an uncached `TokenVerifier` plus a revocation list; each gateway borrows from a real `connectionpool.Pool` with `MaxOpen` 16; the quota is `tokenbucket.Keyed`.

**Simplifications.** A request's gateway CPU is charged as one job at arrival (plus a second job for the handshake), not interleaved with I/O. The pool is never asked to block: the simulation checks `Stats()` first and keeps its own FIFO queue of waiting requests, handing each one a connection the moment one is released, which is what `Pool.Get`'s FIFO waiting does in real time; the pool's own `Waits` counter therefore stays at zero and the waits are counted by the simulation. The rate limit runs after authentication because it is keyed by the verified client ID; a real gateway might also limit by IP before verifying, to keep unauthenticated floods cheap. HTTP/1.1-style one request per connection at a time; HTTP/2 multiplexing would need even fewer connections. The auth service doesn't do deadline propagation or load shedding, which is what makes it metastable; adding them would fix the collapse but not the capacity problem. Token rotation, key distribution and JWKS fetching aren't modelled: all gateways share one HMAC secret. Per-gateway TIME_WAIT is counted against one backend address, as if the backends sat behind one VIP.

## Interview follow-ups

- *If tokens are verified locally, how do you revoke one?* You accept a delay. Keep access tokens short-lived (5-15 minutes) and refresh them through the auth service, which can refuse. For urgent cases, push a small deny list of revoked token IDs (`jti`) or subjects to every gateway; it is checked locally, so it doesn't put the auth service back on the request path. The per-gateway cache must never extend a token past its exp, and its TTL bounds how long a deny-list update can take to apply if the cache is checked first.
- *Why not just cache the auth service's answers?* That helps, and it is the step most teams take first. But a cold cache on a new gateway, or a deploy that flushes caches, sends the full request rate to the auth service at once, exactly when you are scaling out for a spike. A signature check needs nothing from anyone, so a new gateway is ready immediately.
- *How do you size the pool?* From the backend's connection limit divided by the largest fleet you'll ever run (here 70 / 4 → 16), then check it against Little's law: throughput × backend latency per gateway (~425 req/s × 30 ms ≈ 13 connections), with headroom for bursts. If the two don't fit, the backend needs more capacity or the gateways need HTTP/2 multiplexing, not a bigger pool.
- *Why did scaling out not help?* It added the one resource that was not the only bottleneck. More gateways forwarded more requests to the same auth service and opened more connections to the same backend. The autoscaler watched gateway CPU, which went down while the outage got worse: scale on what users experience (latency, errors) as well as on CPU.
- *Should the noisy partner be limited or shed?* Limited, by contract, with 429 and `Retry-After`, so its client can back off; that is cheap and predictable. Load shedding (see [load shedding](../../patterns/reliability/load-shedding)) protects the gateway itself when the total is too high even with every client within quota, and it should drop the least important work first.
- *What about the auth service's metastable collapse?* Even with the fix, give it deadlines: the gateway should send its remaining budget, and the auth service should drop checks whose caller has already given up. Otherwise any future spike that reaches it will cause the same pile-up.

## Further reading

- Arvind Kumar, [API Gateway Becomes a Bottleneck](https://codefarm0.medium.com/api-gateway-becomes-a-bottleneck-system-design-deep-dive-on-gateway-scaling-authentication-cfff15529317), the interview scenario this walkthrough is based on
