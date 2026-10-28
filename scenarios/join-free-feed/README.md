# Scenario: The Home Feed JOIN That Melts the Database

## The incident

A photo-sharing app builds every home feed with one SQL query: take the accounts the viewer follows, join their posts, join each post's author for the name and avatar, count each post's likes, sort by time and keep 20. At launch people follow a dozen accounts, posts have a handful of likes, and the query takes a few milliseconds. A year later the average user follows hundreds of accounts and popular posts collect thousands of likes. Nothing in the code changed, but each page view now reads thousands of rows. Every evening, when traffic peaks, database CPU sits at 100%, p99 feed latency climbs into seconds and the app shows spinners and time-outs. The team adds read replicas. That helps for a few weeks, until the graph grows again.

## Symptoms

- Database CPU is fine during the day and pinned during the evening peak, even though the query plan never changed.
- Rows read per feed request keep rising with follow counts and like counts, not with traffic.
- Feed latency is bimodal: fast off-peak, seconds at peak, and slow for a while after the peak too, while the backlog drains.
- Each new read replica moves the cliff a little later but doesn't remove it.

## Root cause

1. **The read does work proportional to the graph, not the page.** To return 20 posts, the JOIN reads every follow edge, every post by every followee (to sort them), the 20 authors, and every like row of the 20 posts for `COUNT(*)`. With 80 follows, 5 posts each and a few hundred likes on popular posts, that's about 2,800 work units for a 20-item page.
2. **The same work is repeated on every page view.** Nothing about the result is stored, so a million views of the same feed run the same join a million times.
3. **Queues make overload non-linear.** Once demand exceeds what the database can serve, the backlog grows every second. Requests that time out still run, because the database doesn't know the client gave up, so the backlog lasts after the peak.
4. **Replicas scale the wrong thing.** They multiply capacity by a constant, while per-view cost grows with the graph.

## The fix

| Change | Pattern | Effect |
| --- | --- | --- |
| Split the write model from the read model: commands write the normalized tables, and feeds are read from a separate store built for that query | [CQRS](../../patterns/data/cqrs) | Feed reads never touch the normalized tables |
| Keep one denormalized row per (viewer, post) with the author's name embedded, maintained from the tables' change stream | [Materialized view](../../patterns/data/materialized-view) (`FeedView`) | A feed read is one partition range scan: **183** work units at p50 instead of **2,790** |
| Store like counts in one counter row per post and look them up at read time, instead of copying them into every feed row | [Materialized view](../../patterns/data/materialized-view) (`ReferenceLikes`) | A like writes **2** rows, not one per follower of the author |
| Don't fan celebrity posts out on write: each celebrity gets one outbox partition, and readers merge the outboxes of the celebrities they follow | [Fan-out feed](../../patterns/architecture/fan-out-feed) (hybrid push/pull) | A post by an account with 325 followers writes **2** rows instead of 326 |

## Run it

```sh
go run ./scenarios/join-free-feed/broken
go run ./scenarios/join-free-feed/fixed
go test ./scenarios/join-free-feed/
```

| | Broken | Fixed |
| --- | --- | --- |
| Work per feed view (p50 / max) | 2,790 / 3,667 | 183 / 244 (invariant ≤ 300) |
| DB load off-peak / peak second | ~48% / 158% | ~4% / 12% |
| Feed latency p50 / p99 | 3.24s / 8.56s | 4ms / 8ms |
| Timed-out feed views (timeout 1s) | 680 of 1,000 | 0 |
| Rows written per post / celebrity post | 1 / 1 | 81 / 2 |
| Rows written per rename / celebrity rename | 1 / 1 | 391 / 21 |
| Rows written per like | 1 | 2 |
| Stored feed rows | 0 | 164,844 |
| Feeds that differ from the JOIN after catch-up | n/a | 0 of 400 |

Broken is fine off-peak: the database runs at about half its capacity and no view times out. That is why the query shipped. At the peak, demand is 140-150% of capacity, the backlog grows by about half a second of work every second, and from a few seconds into the peak every view times out. Views keep timing out for about 14 seconds after the peak ends, while the backlog drains.

Fixed reads cost 61 work units per partition: one lookup to find it, 20 rows, and 20 point lookups on like counters (a lookup and a row each). A viewer who follows two of the three celebrities reads three partitions (183); one who follows all three reads four (244), which is the worst case. The price is on the write path. A regular post is copied into each of its author's ~80 followers' partitions, and a rename rewrites every embedded copy of the name: 5 posts × ~78 followers. The view is also eventually consistent. The projector catches up once a simulated second (at most 41 changes per catch-up), so of the 1,000 views, 250 missed a post from the last second or showed an old name, and 748 more had the right posts but a like count up to a second behind. After the final catch-up, every one of the 400 feeds equals what the JOIN returns.

The tests assert the invariant (at most 300 work units per feed view, peak load within capacity, no time-outs, every feed equal to the JOIN after catch-up) for the fixed version, and check that the broken version violates it while staying healthy off-peak. A cost test checks write amplification: a post writes about one row per follower and a rename about posts × followers, while a like or a celebrity post writes at most 2. Another builds the naive view (every author fanned out, likes embedded) on the same graph: there, one celebrity post and one like on it each write 325 rows. A small hand-built graph checks eventual consistency: a new post is invisible until the projector catches up, then appears in both followers' feeds; a celebrity's post reaches followers through the read-time merge; a rename shows the old name until catch-up and rewrites exactly the two rows that embedded it.

**How the simulation is built.** A seeded graph of 400 regular users and 3 celebrities. Each regular user follows 80 random regular users and each celebrity with 80% probability, so celebrities have about 325 followers, above the celebrity threshold of 200. Regular users have 5 posts each with 0-40 likes; celebrities have 20 posts each with 200-350 likes. Both versions then get the same 60-second script: 10 feed views/s, 30 at peak (t=20-39s), 1 new post and 40 likes per second (the likes go to the 100 newest posts), a celebrity post at t=25, a regular user's rename at t=30 and a celebrity's rename at t=35. Cost comes from the `Cost` values (lookups + rows) that `SocialDB.HomeFeedJoin`, `FeedView.HomeFeed` and `FeedView.Apply` report. One work unit is one index lookup or one row read or written, and the database cluster serves 60,000 units per second, shared by feed reads, normalized writes (2 units each) and view maintenance. Latency comes from a FIFO fluid queue: a request's latency is the work queued ahead of it plus its own, divided by capacity. Feed views are spread evenly across each second, and writes arrive at the start of it. The projector is a small wrapper around `FeedView`. It reads the tables' change stream, keeps follow edges that point at a celebrity in a per-viewer pull list instead of forwarding them, and renumbers the changes it does forward so the view sees a gap-free stream. Each celebrity's posts land in one `outbox:<id>` partition. The view's initial build happens before the clock starts (a backfill done off-peak), so it isn't counted as peak load.

**Simplifications.** Celebrities are fixed at seed time. A real system has to promote accounts that cross the threshold, and that means backfilling an outbox and removing pushed rows. The feed store and the normalized tables share one capacity budget. In production they'd usually be separate clusters, and the fan-out writes would run on the feed store's own write budget, often asynchronously, through a queue. Real query cost isn't linear in rows (buffer-cache hits, sort memory, lock contention), and the fluid queue ignores parallelism inside the database. The `cqrs` package's inventory types aren't imported: here the write side is `SocialDB` and the read side is `FeedView`, which apply the same split to this domain. Feed partitions aren't trimmed, so they hold every post ever, while production keeps only the newest few hundred per viewer.

## Interview follow-ups

- *Why not add an index or more read replicas?* The follows and posts indexes already exist. The cost is the number of rows the query must touch, not finding them. Replicas multiply capacity by a constant, while per-view cost grows with follow and like counts, so they only postpone the next cliff. They are still useful for serving the denormalized store. See [read replicas](../../patterns/data/read-replicas).
- *Why reference like counts instead of embedding them?* A like is the most frequent write and changes the most volatile field. Embedding it means one like rewrites a row for every follower of the author: 325 rows for a celebrity post in this graph, millions in production. Referencing costs one extra point lookup per item on read, which is cheap and easy to cache. Author names change rarely, so they're embedded.
- *A user follows someone. What does their feed show?* The `Followed` change backfills the followee's posts into the new follower's partition on the next catch-up. For a celebrity, the projector just adds them to the pull list, with no backfill at all.
- *How stale can a feed be, and does it matter?* Up to one catch-up interval, here one second. A missing like or a post that appears a second late is acceptable for a feed. Your own post not showing up in your own feed is not, so real apps add it on the client or read it from the write side (read-your-writes).
- *What does a rename cost, and how do you keep it from hurting?* Posts × followers rows: 391 here, far more for a prolific account. Run it asynchronously and rate-limited, or store only an author ID plus a small, cached profile lookup when profile changes are frequent.
- *Where does the celebrity threshold come from?* It's a write-cost versus read-cost trade. Each pulled account adds one partition read to every follower's page view, and each pushed post costs one write per follower. Pick it where those balance for your traffic, and cap how many pulled accounts one read may merge.

## Further reading

- Arvind Kumar, [Why Instagram Avoids JOINs at Scale](https://codefarm0.medium.com/why-instagram-avoids-joins-at-scale-system-design-deep-dive-on-denormalization-and-read-b66f4a9f2c91), the scenario this walkthrough is based on: denormalized feed rows, read models and the write-amplification trade-off
