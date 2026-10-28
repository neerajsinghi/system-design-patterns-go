# Scenario: One Poll Throttles the Votes Table

## The incident

A streaming app lets viewers vote during a live talent-show final. Votes go into the same DynamoDB-style table as every other poll in the app, keyed by poll ID, with ten partitions of 1,000 writes/s each. On a normal evening the table takes about 2,000 votes/s across hundreds of community polls and sits at 20% of its capacity. When the host opens voting, the final's poll starts taking another 4,000 votes/s. The table is still only at 60% of its total capacity, yet half of all votes start failing with throttling errors. Users of unrelated polls ("best goal of the week") also start seeing "vote not recorded". The on-call engineer doubles the provisioned capacity, the partition count goes from 10 to 20, and the error rate barely moves.

## Symptoms

- Throttling errors, even though consumed write capacity for the table as a whole is well below what's provisioned.
- One partition's write load is about 4x its limit; every other partition is nearly idle.
- Failures aren't limited to the final's poll. Votes for any poll whose key hashes to the same partition are throttled too.
- Adding capacity or partitions doesn't help.
- Clients that retry on throttling make the hot partition even busier (not modelled here, which makes the real incident worse than the numbers below).

## Root cause

A partitioned table spreads **keys** evenly, not **writes**. Each partition key hashes to exactly one partition, and each partition has its own write ceiling. A single key that receives 4,000 writes/s needs four partitions' worth of capacity, but it can only ever use one. Adding partitions changes which partition the key lands on, never how many it lands on: with 20 partitions the busiest one still receives about 4,100 writes/s. The cold polls that share the hot partition lose because they compete with the finale for the same 1,000 slots each second.

## The fix

| Change | Pattern | Effect |
| --- | --- | --- |
| Count writes per key over a 1-second window and promote any key above 500 writes/s | [Hot-key salting](../../patterns/data/hot-key-salting) (adaptive detector) | The final's poll is found within the second voting opens, with no one naming it in advance |
| Write a promoted key to one of K=20 physical keys, `poll:finale`, `poll:finale#1` … `#19` | [Hot-key salting](../../patterns/data/hot-key-salting) | The poll's 4,000 writes/s spread over every partition; the busiest one stays under 900/s |
| Read a salted key by fetching all K physical keys and summing them | [Scatter-gather](../../patterns/architecture/scatter-gather) | The vote total stays exact; only the salted poll pays 20 lookups, cold polls still pay 1 |

The cost is on reads: one read of the final's poll costs 20 lookups and 20 times the read capacity. For a vote tally that's shown on screen every few seconds that's cheap, and a cached rolled-up total would hide it completely. Because suffix 0 is the original key, the votes written before promotion are already part of what every read fetches, so nothing has to be migrated.

**Where sharded counters fit.** If you know the hot key before it gets hot, and here you do (the final is on the TV schedule), you don't need detection at all. Pre-salt the poll (`WithHotKeys`), or model it as a [sharded counter](../../patterns/architecture/sharded-counters) with a fixed number of shards from the start. The adaptive detector covers the keys you didn't predict: a community poll that goes viral, or a celebrity reposting one. Production systems usually do both.

## Run it

```sh
go run ./scenarios/hot-partition/broken
go run ./scenarios/hot-partition/fixed
go test ./scenarios/hot-partition/
```

| | Broken | Fixed |
| --- | --- | --- |
| Busiest partition, while voting is open | ~4,200 votes/s (4.2x capacity) | 1,250 in the detection second, then at most 896 |
| Votes throttled over 12s | 31,999 of 64,000 (50.0%) | 250 (0.4%), all in the second voting opened |
| Votes for unrelated polls throttled | 1,496 | 52 |
| Throttled after the first second of voting | 28,810 | 0 |
| Same design with 20 partitions | busiest partition still 4,114/s, 31,013 throttled | n/a |
| Lookups to read the final's poll / a cold poll | 1 / 1 | 20 / 1 |

The 250 throttled votes in the fixed run are detection lag. The detector needs 500 votes to promote the poll, and those 500 land on the unsalted key, on the same partition that also takes the suffixes hashed there. The tests assert that the broken version keeps throttling after detection would have happened (including for unrelated polls) and that doubling its partitions doesn't fix that. They assert the fixed version's invariant: the finale is salted in the second voting opens, from the next second on no partition exceeds capacity and no vote is throttled, every poll reads back exactly the votes the table accepted, and accepted plus throttled equals the traffic sent. They also check the cost (20 lookups for the salted poll, 1 for a cold one) and that pre-salting the poll removes the detection-second throttling entirely (0 throttled).

**Why K=20 and not 8.** The salted suffixes are hashed to partitions like any other key, so some partitions get two or three of them. With K=8 (500 writes/s per suffix), two suffixes plus that partition's cold polls add up to about 1,200 writes/s and the run still throttles 2,424 votes, 1,831 of them after detection. K needs to be well above the overload factor, which is 4x here. K=20 puts at most three suffixes (about 600 writes/s) on any partition. A suffix derived from the partition map, rather than hashed, would get an even spread with a smaller K.

**How the simulation is built.** The table is `hotkeysalting.Table`: 10 partitions with FNV-1a key hashing, each accepting 1,000 writes per simulated second and rejecting the rest. Each second sends 2,000 votes to 400 cold polls chosen at random, plus 4,000 votes for `poll:finale` from t=2s, shuffled and spaced evenly across the second on a fake clock. The traffic comes from its own seeded generator, so both runs send exactly the same votes in the same order. Broken uses a `Salter` in `Unsalted` mode, which writes the plain key. Fixed uses `Adaptive` mode with K=20 and a 500 writes/s detector. A throttled vote is counted and dropped, with no retry. Reads aren't throttled; in DynamoDB they draw on separate read capacity. Real DynamoDB also has adaptive capacity and split-for-heat, which soften this incident but can't split a single item: one item key is still capped at about 1,000 writes/s.

## Interview follow-ups

- *How do you find the hot key during the incident?* CloudWatch Contributor Insights for DynamoDB lists the most-accessed and most-throttled keys. In your own code, count writes per key in a sliding window at the app layer, which is what the detector here does.
- *Why not pick a better partition key?* Do that when the skew comes from the key design (a status flag, or today's date as the whole key). Here the skew is real: one poll really does get two-thirds of the votes, and no choice of key changes that.
- *What if the write isn't a plain increment, e.g. "one vote per user"?* Salting needs a merge that doesn't care which suffix a write landed on. A uniqueness check can't be made against one suffix, so keep the dedup elsewhere (a per-user item keyed by `poll#user`, written with a condition) and only salt the tally.
- *How do readers know a key is salted?* Every app server needs to agree, so the hot-key set is shared state: publish promotions through config or a small table, or always read K suffixes for keys that might be hot, since reading empty suffixes is cheap.
- *Would a write buffer help instead?* Yes, for counters: aggregate votes in memory or in a queue and flush one `+N` per second per poll. That trades freshness and durability-on-crash for writes; salting keeps each vote a separate, durable write.

## Further reading

- Arvind Kumar, [DynamoDB Hot Partition Problem](https://codefarm0.medium.com/dynamodb-hot-partition-problem-system-design-deep-dive-on-partition-keys-sharding-strategies-and-9b6758e494ef), the interview scenario this walkthrough is based on
