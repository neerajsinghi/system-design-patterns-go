# Scenario: Everyone Buffers When the New Season Drops

## The incident

A streaming service releases a new season at 12:00. At 12:00:01 every CDN edge in the country is asked for episode 1's manifest, none of them has it, and all of them fetch it from the origin at once. The origin, sized for steady-state refill traffic, falls over. Viewers see endless buffering. A minute later the origin restarts and immediately falls over again. Engineers watch it crash-loop for an hour, and the incident only ends when someone manually blocks edge traffic and lets it recover. Two days later, an unrelated bad deploy takes the origin down for 90 seconds, and every edge whose cache entry expires in that window starts showing errors too.

## Symptoms

- Origin traffic jumps from near zero to one request per edge in the very first second of the launch.
- The origin crashes, recovers, and crashes again at regular intervals: a crash loop.
- During an unrelated origin outage, errors rise in step with cache expiries, even for content that was cached a moment earlier.

## Root cause

1. **Every edge misses at once.** Edges each collapse their own viewers into one fetch, but 20 edges still means 20 origin fetches in the same second, against a capacity of 5.
2. **Synchronised retries make it metastable.** When the origin comes back, every edge that still lacks the episode retries in the same second. That's the same overload that crashed it, so it crashes again. The system has a stable bad state it can't leave on its own.
3. **Expired means gone.** With plain TTLs, an entry that expires during an origin outage turns into an error, even though the old manifest would have been perfectly fine.

## The fix

| Change | Pattern | Effect |
| --- | --- | --- |
| Push the episode into every tier before release | [Tiered cache](../../patterns/architecture/tiered-cache) (pre-warming) | The launch second makes **zero** origin fetches |
| Put an origin shield between the edges and the origin | [Tiered cache](../../patterns/architecture/tiered-cache) (origin shield) | Edge refreshes collapse into one fetch per shield TTL, however many edges there are |
| Have the shield serve stale content when the origin fails | [Serve-stale](../../patterns/reliability/serve-stale) | An origin outage shorter than the grace period is invisible to viewers |

## Run it

```sh
go run ./scenarios/release-day-buffering/broken
go run ./scenarios/release-day-buffering/fixed
go test ./scenarios/release-day-buffering/
```

| | Broken | Fixed |
| --- | --- | --- |
| Origin fetches in the launch second | 20 (capacity 5) | 0 |
| Origin crashes | 6 (never recovers) | 0 |
| Failed viewer requests | 75,000 of 84,000 | 0 |
| Viewers affected by the 90s origin outage | all of them (it was already down) | none, served from stale |

The tests assert the invariant (origin within capacity, no viewer request fails, including during the outage) for the fixed version, check that the broken version violates it, and check that the pre-warmed launch makes no origin fetch at all.

**How the simulation is built.** Twenty edges each get 10 manifest requests per simulated second for seven minutes. The origin serves 5 fetches/s; the sixth fetch in a second crashes it for 60 seconds. Separately, it is down from t=240s to t=330s. A second's viewers at an edge share that edge's in-flight fetch, so one failed fetch fails all 10 of them rather than each retrying. In the fixed version, the shield's origin access goes through a serve-stale cache (1h grace, 10s retry interval), and the pre-warm fetch happens off-peak, before the clock starts.

## Interview follow-ups

- *The origin is crash-looping right now. How do you stop it?* Break the synchronisation: shed load at the edges (serve an error page or a lower-quality fallback without contacting the origin), add jittered, exponential retries, then re-enable traffic gradually. Turning on a shield mid-incident also works, because it collapses 20 retries into 1.
- *Why not just scale the origin up for the launch?* Do that too, but the edge count grows with the CDN and the herd grows with it. The shield caps origin load at about one fetch per object per TTL, regardless of edge count.
- *How much do you pre-warm?* Only what you can predict: the first episodes, trailers, artwork. Pushing a whole catalogue evicts useful content from the edges.
- *When is serving stale wrong?* When the old content is harmful, not just old: a pulled episode, a takedown, an expired licence. Those need an explicit purge that also removes the stale copy.

## Further reading

- Arvind Kumar, [Netflix Buffers After a New Season Release](https://codefarm0.medium.com/netflix-buffers-after-a-new-season-release-system-design-deep-dive-on-cdn-architecture-and-cache-51ecc36a64e2), the interview scenario this walkthrough is based on
- Khyati Jha, [High Level System Design Day-12: CDN (Part 3)](https://medium.com/high-level-system-desgin-interview-prep/high-level-system-design-day-12-cdn-part-3-82c1c6eeac3f), which covers origin shields, pre-warming and serving content while the origin is down
- Bronson et al., *Metastable Failures in Distributed Systems* (HotOS 2021), on why systems like the broken one don't recover on their own
