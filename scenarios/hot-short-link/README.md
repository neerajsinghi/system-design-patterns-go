# Scenario: One Short Link Takes Down the URL Shortener

## The incident

A URL shortener serves redirects from a three-node cache cluster that has run at 15% utilisation for months. During a cricket final, a broadcaster tweets a short link to the live scorecard. Within a minute that one link carries 90% of all redirects. One cache node hits its CPU limit and starts timing out, and redirects fail, including for thousands of unrelated links that happen to live on that node. Adding a fourth cache node during the incident changes nothing.

## Symptoms

- One cache node is saturated while the others sit nearly idle. Total cluster load is well within capacity.
- Errors are not limited to the viral link: every key that hashes to the hot node is affected.
- Scaling the cluster out doesn't help, because the viral key still maps to exactly one node.
- App servers spend most of their time on network round trips for a value that never changes.

## Root cause

A sharded cache spreads **keys** evenly, not **traffic**. Each key lives on exactly one node, so a single key's load can never be split by adding nodes. With 6,000 redirects/s and 90% for one link, that node receives about 5,600 ops/s against a capacity of 2,000, while the other two receive about 250 each.

## The fix

Use the [two-tier cache](../../patterns/data/two-tier-cache) pattern:

| Change | Effect |
| --- | --- |
| **In-process L1 cache** (1s TTL) on every app server | Each server fetches the viral link from the cluster about once a second, instead of thousands of times |
| **Hot-key replication**: the viral key is stored on all three nodes, each read picks one at random | What does reach the cluster is spread across nodes instead of piling onto one |

The cost is staleness: when the link's target is edited, servers that already hold it in L1 keep redirecting to the old target until their L1 entry expires (at most 1 second here). For a redirect, that's an easy trade.

## Run it

```sh
go run ./scenarios/hot-short-link/broken
go run ./scenarios/hot-short-link/fixed
go test ./scenarios/hot-short-link/
```

| | Broken | Fixed |
| --- | --- | --- |
| Load on the hot node | ~5,650 ops/s (2.8× capacity) | ~250 ops/s |
| Busiest node-second | 5,800 ops | ~440 ops |
| Redirects rejected | ~110,000 over 30s | 0 |
| Old target served after an edit | 0 | ~0.5s (bounded by the 1s L1 TTL) |

The tests assert that the fixed version keeps every node within capacity and every edit visible within the L1 TTL, that the broken version overloads a node, and that only the fixed version pays any staleness.

**How the simulation is built.** Each simulated second sends 6,000 redirects to random app servers; 10% go to a long tail of 5,000 other links. The cache cluster is `twotier.Shared`, which counts operations per node. Requests beyond a node's capacity in a given second are counted as rejected. The link is edited half-way through second 15 via one server, which invalidates the cluster and its own L1; the other servers' L1 copies expire on their own.

## Interview follow-ups

- *How do you know which key is hot before it hurts?* Sample requests at the app or proxy, use `redis-cli --hotkeys` (LFU-based), or count keys in a sliding window on each client and promote anything above a threshold.
- *Why not just put the link behind a CDN?* For a redirect service that's a good answer too: cache the 301/302 at the edge with a short TTL. It's the same idea one tier further out ([tiered cache](../../patterns/architecture/tiered-cache)).
- *What if the link must change instantly (e.g. it was hijacked)?* Push invalidations to every server's L1 over pub/sub, or use Redis client-side caching, which tracks and invalidates L1 copies.
- *What about a hot key that is written, not read (a like counter)?* L1 and replication don't help writes. Use [sharded counters](../../patterns/architecture/sharded-counters).

## Further reading

- Arvind Kumar, [URL Shortener Suddenly Crashes During IPL Finals](https://codefarm0.medium.com/url-shortener-suddenly-crashes-during-ipl-finals-how-will-you-fix-3347c9911ba3), the interview scenario this walkthrough is based on
- Khyati Jha, [High Level System Design Day-8: Caching (Part 3)](https://medium.com/high-level-system-desgin-interview-prep/high-level-system-design-day-8-caching-part-3-e3c7a27b7235), which covers local caches and hot-key scatter
