# Scenario: The Map Shows the Rider in the Wrong Place

## The incident

A food-delivery app shows customers their rider on a live map. Complaints start arriving at dinner time. One customer's rider is "still a kilometre away" while ringing the doorbell. Another watches the dot move towards them, then jump back a few streets. A third sees the rider "moving" at the same spot for two minutes: the rider's phone had lost signal in a basement car park, and the app gave no sign that the dot had stopped updating. Support can't tell customers anything useful, because the map is wrong in a different way for each of them. During the investigation the team also finds that, after one of the customer-facing gateway nodes restarted, its customers' updates kept going to that node for a while before the routing caught up.

## Symptoms

- The dot lags the rider by hundreds of metres, sometimes close to a kilometre.
- The dot jumps backwards, to a position the customer already saw the rider pass.
- A rider with no signal keeps showing at their last position, with nothing to say how old it is.
- Opening the app more often doesn't help: the API returns the same position for up to a minute.

## Root cause

1. **Polling.** The customer app asks for the rider's position every 30s, so even a perfect answer is on average 15s old by the time it is replaced.
2. **A long cache TTL on something that changes every two seconds.** The location API caches each rider for 60s, so every other poll returns the previous answer and the shown position can be 90s old.
3. **Last write wins by arrival, not by measurement time.** Rider GPS fixes go through a queue. Some are delayed several seconds behind newer ones, and a few get stuck in a consumer retry and are redelivered more than a minute late. The store overwrites whatever it has with whatever arrives, so an old fix replaces a newer one. If a poll lands in that window, the dot goes backwards.
4. **No staleness signal.** The response carries no timestamp, and nothing tracks whether a rider is still sending. A silent rider looks exactly like a rider waiting at a red light.
5. **Routing that doesn't follow reconnects.** When a gateway restarts, its customers reconnect to other nodes. Anything that remembers "this customer is on node X" from before the restart sends updates into the void.

## The fix

| Change | Pattern | Effect |
| --- | --- | --- |
| Riders send fixes with their device timestamps; the store keeps a fix only if it is newer than the one it has | [Presence](../../patterns/architecture/presence) (`UpdatePosition`) | Late and redelivered fixes are rejected, so the dot never goes backwards |
| Each rider holds a heartbeat-renewed lease; when it expires the customer is told the dot is stale | [Presence](../../patterns/architecture/presence) (leases, `Watch`, `LatestPosition` with `maxAge`) | A rider who lost signal shows "last updated 40s ago" instead of a confident wrong dot |
| Customers hold a WebSocket; each accepted fix is pushed to whichever node the registry says holds the customer's socket | [WebSocket hub](../../patterns/architecture/websocket-hub) (`Registry`) | No polling delay, and pushes follow the customer to the new node after a gateway restart |
| One-slot, drop-oldest send queue per connection | [Backpressure](../../patterns/distributed/backpressure) (`SendQueue`, `DropOldest`) | A slow link gets the latest position, not a backlog of old ones |
| Display error measured with haversine distance | [Geospatial index](../../patterns/data/geospatial-index) (`Distance`) | The p95 error target is in metres on the ground |

## Run it

```sh
go run ./scenarios/stale-rider-location/broken
go run ./scenarios/stale-rider-location/fixed
go test ./scenarios/stale-rider-location/
```

| | Broken | Fixed |
| --- | --- | --- |
| Display error p50 / p95 / max | 167 m / 390 m / 929 m | 14 m / 43 m / 719 m |
| Largest error while the dot was not flagged stale | 929 m | 96 m |
| Age of the shown position p50 / p95 / max | 34 s / 61 s / 189 s | 2 s / 6 s / 127 s |
| Oldest position shown without a stale flag (limit 25 s) | 189 s | 18 s |
| Stale-but-unflagged customer-seconds | 14,336 of 22,600 | 0 |
| Customer-seconds flagged "last updated ... ago" | 0 | 468 |
| Backward jumps | 18 | 0 |
| Older fixes overwriting newer ones / rejected | 1,589 overwrote | 1,649 rejected |
| Customer-facing messages | 800 polls (399 from cache) | 10,048 pushes enqueued, 8,951 written |
| Persistent connections | 0 | 40 (one per customer) |
| Intermediate positions dropped by the one-slot queue | n/a | 1,092 |
| Gateway restart | not affected (stateless polls) | 10 customers reconnected; 9 pushes lost to the dead node, 772 routed to the new node |

The fixed run's 719 m maximum error is a rider who lost signal: the dot stays where the rider was last seen while the rider keeps riding for two minutes, but the lease expires and the UI flags it stale about 8s after the last fix arrived, which is why the unflagged maximum is 96 m. Of the 10 stale markers, 4 are the riders who lost signal. The other 6 are healthy riders whose next few fixes were all delayed past the 8s lease, a short false alarm that clears with the next accepted fix. The two "older fix" counts differ slightly because broken compares an arriving fix with whatever it last stored (which may itself be old), while fixed compares it with the newest fix it has.

The tests assert the invariant for the fixed version and check that the broken version violates it: no position older than 25s is shown without a stale flag, the dot never moves to a fix older than one already shown, and the p95 distance between dot and rider is at most 100 m. They also check that the silent riders were flagged by lease expiry, that after the gateway crash the registry routes pushes to the customer's new node and loses at most one push per customer per second of detection delay, that both runs see identical traffic, and that both are deterministic. The cost test checks one persistent connection per customer, a fan-out of one push per accepted fix (0.998, because pushes in the crash window have nowhere to go), more than five times as many messages as polling, and that the drop-oldest queue does drop intermediate positions while the invariant still holds. Every push is accounted for as written, dropped, or still queued when its node crashed or the run ended.

**How the simulation is built.** 40 riders move for 600 simulated seconds: every 20s each picks a new heading and either a speed of 14-32 km/h or a stop. Phones send a fix every 2s with a few metres of GPS noise and the device timestamp. 85% of fixes arrive within 0.2-1.5s, 12% are delayed 4-12s (so newer fixes overtake them) and 3% are redelivered 35-90s late. Riders 0, 10, 20 and 30 lose signal from t=180s to t=300s, and their fixes in that window are never sent. Each rider has one watching customer, and the error and age are sampled for every customer every second from t=35s. Broken and Fixed consume the same generated fixes in the same arrival order. In the fixed version, customers are spread over four gateway nodes. `gw-1` crashes at t=420s, losing its sockets and anything queued on them; the registry keeps pointing at it for 2s until the cluster notices, and its ten customers reconnect to the next node 3-7s after the crash and get a snapshot from `LatestPosition`. Every customer's socket writer sends once a second, except every fifth customer, who is on a slow link and can only take one message every 5s. **Simplification.** The fixed version uses the WebSocket hub's `Registry` and its one-queue-per-connection design, but models the sockets and the cluster bus in simulated time rather than running real `Hub`s on loopback sockets. Real sockets would make delivery timing depend on the scheduler, and the run would no longer be deterministic or finish in well under a second. Other simplifications: one customer device per order (so fan-out is 1), the rider's phone doesn't buffer fixes during signal loss, a crashed node's registry entries are cleaned up by a fixed detection delay rather than by heartbeats, and the client trusts the server's ordering instead of also checking timestamps itself.

## Interview follow-ups

- *Why not just poll every 2 seconds?* Each poll is a full HTTP request with headers, auth and a cache lookup, whether or not the rider moved, and it still adds up to half a poll interval of delay. With many customers watching it costs more than a push, and the cache in front of it, which is what made polling affordable, is what made the dot stale.
- *Why does the server need the device timestamp? Couldn't it use arrival time?* Arrival time is exactly what broke: a fix that arrives late is not new. The device clock can be off, but it's consistent for one phone, and the comparison is only ever between fixes from the same phone. Sequence numbers from the device work too.
- *Why is a queue with one slot fine?* A position replaces the one before it. A customer on a slow link only needs the newest one. Queuing intermediate positions would just make them watch the rider replay the last minute. Here the queue dropped 1,092 intermediate positions, and the invariant still held. Chat messages are different: every one matters, so they need a deeper queue or a disconnect policy.
- *How long should the lease be?* A few fix intervals: long enough that one or two delayed fixes don't flag a healthy rider (here 8s caused 6 brief false alarms over 10 minutes and 40 riders), short enough that a silent rider is flagged before the customer notices. Showing "last updated 12s ago" in grey is a soft signal, so erring short is cheap.
- *What happens when the customer's gateway restarts?* The client reconnects to any node, which registers it, and the next push looks the customer up in the registry, so it goes to the new node. The client also gets a snapshot of the latest position on connect, so it doesn't wait for the rider's next fix. Pushes in the window before the cluster notices the dead node are lost, which is fine for positions because the next one supersedes them.
- *How does this scale to a city?* Shard the presence store by rider ID, and the registry by customer ID. Only push to customers with the map open: the watch list is the active orders, not every customer. For "show riders near this restaurant", keep a geospatial index updated from the same accepted fixes.

## Further reading

- Arvind Kumar, [Swiggy Shows Wrong Rider Location](https://codefarm0.medium.com/swiggy-shows-wrong-rider-location-system-design-deep-dive-on-real-time-gps-tracking-and-eventual-b49e5f18fccd), the scenario this walkthrough is based on
