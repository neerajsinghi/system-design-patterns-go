# Scenario: Semantic Search Gets Slow, Then Gets Wrong

## The incident

An online store launches semantic product search: every product description is turned into an embedding, and a search phrase is embedded and matched against all of them. The first version uses a flat (exact) index, which scores the query against every product. At launch, with 200,000 products, a search takes under 80 ms. A year later the catalogue has passed a million products, p99 search latency is several hundred milliseconds, and the search page starts timing out at peak.

The team ships an IVF index: k-means centroids trained on that day's catalogue, nprobe tuned once, latency back under the SLO. Nobody looks at it again. Over the next quarters the store adds whole new categories, and support tickets start to arrive: searches in the new categories return products that are only vaguely related, while searches in the original categories still look fine. Dashboards show nothing wrong, because latency is green and no request fails.

## Symptoms

- Flat search latency grows in step with the catalogue: 2.5 times the products, 2.5 times the latency.
- After the IVF index ships, latency is fine and the old categories search well, but searches in categories launched later miss about half of their true top-10.
- Raising nprobe brings recall back, but only by scanning so much of the index that latency goes over the SLO again.

## Root cause

1. **Flat search is O(N) per query.** Every query scores every product, so latency is proportional to catalogue size. No cache or extra replica changes that, because each query still does all the work.
2. **The IVF centroids are a snapshot.** IVF files each new vector under the nearest *existing* centroid. Products from a category that did not exist at training time land in whichever old lists happen to be closest, spread over many of them along boundaries that mean nothing for the new category. A query probes only the few nearest lists, so it misses true neighbours that were filed elsewhere.
3. **Nothing measures recall or drift.** Latency and error-rate alerts can't see wrong answers. The index was tuned once, against the catalogue of one day.

## The fix

| Change | Pattern | Effect |
| --- | --- | --- |
| Serve from an IVF index instead of flat search | [Vector ANN index](../../patterns/ai/vector-ann-index) (IVF) | Each query scans about 3% of the vectors instead of all of them |
| At every growth stage, choose the lowest nprobe that reaches recall@10 ≥ 0.97 on held-out tuning queries, measured against exact search | [Vector ANN index](../../patterns/ai/vector-ann-index) (`Search` nprobe, `Recall`) | The least work that still meets the recall target, re-checked as the data changes |
| Retrain when the corpus has doubled since the last training, or when new vectors sit at least 1.3 times further from their list's centre than the training data did | [Vector ANN index](../../patterns/ai/vector-ann-index) (`Retrain`, `ListOf`, `NearestList`) | New categories get their own lists; recall on them goes from about 0.5 back to 0.98 |

## Run it

```sh
go run ./scenarios/slow-vector-search/broken
go run ./scenarios/slow-vector-search/fixed
go test ./scenarios/slow-vector-search/
```

Per growth stage (SLO: p99 ≤ 100 ms and recall@10 ≥ 0.95 for both query groups):

| Stage | Products | Broken index | Broken scanned/query | Broken p99 | Broken recall (launch / new) | Fixed nprobe | Fixed scanned/query | Fixed p99 | Fixed recall (launch / new) | Fixed retrains |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 200k | flat | 500 (100%) | 76.8 ms | 1.000 / - | 4 | 16 (3.2%) | 4.7 ms | 0.972 / - | 0 |
| 2 | 500k | flat | 1,250 (100%) | **192 ms** | 1.000 / - | 3 | 34 (2.7%) | 9.6 ms | 0.987 / - | 1 (growth) |
| 3 | 1.0M | flat | 2,500 (100%) | **384 ms** | 1.000 / - | 4 | 81 (3.2%) | 26.3 ms | 0.995 / - | 2 (growth) |
| 4 | 1.5M | IVF, nprobe=3 | 117 (3.1%) | 31.5 ms | 0.978 / **0.497** | 3 | 106 (2.8%) | 28.2 ms | 0.990 / 0.980 | 3 (drift 4.64) |
| 5 | 2.0M | IVF, nprobe=3 | 171 (3.4%) | 52.6 ms | 0.965 / **0.550** | 3 | 164 (3.3%) | 50.0 ms | 0.997 / 0.987 | 3 (drift 1.11, no retrain) |
| 6 | 2.5M | IVF, nprobe=3 | 190 (3.0%) | 62.1 ms | 0.967 / **0.573** | 3 | 158 (2.5%) | 52.0 ms | 0.998 / 0.977 | 4 (drift 2.95) |

| | Broken | Fixed |
| --- | --- | --- |
| Stages over the latency SLO | 2 of 6 (flat, stages 2-3) | 0 |
| Stages under the recall target | 3 of 6 (new categories, stages 4-6) | 0 |
| k-means trainings | 1 (1,303,040 distance computations) | 5 (5,989,120 distance computations) |
| Centroid table | 16 KB (2% of the 800 KB of stored vectors) | same |
| Winning recall back by raising nprobe instead of retraining (stage 6) | nprobe=32: recall on new categories 0.977, but p99 334.4 ms | not needed |

Some results need explaining:

- **Broken's IVF index fixes latency completely.** It scans about 3% of the corpus, the same as the fixed index. The incident's second half is not about speed: the stale index is fast and wrong, and only for some queries. Launch-category queries keep recall around 0.97, so an aggregate recall number would hide much of the problem.
- **Raising nprobe is the wrong lever.** On the stale centroids, a new-category query's neighbours are spread across many lists, so reaching recall 0.97 takes nprobe=32 (a quarter of the lists), and p99 rises to 334 ms. Retraining reaches the same recall at nprobe=3.
- **The fixed design retrains at stages 2 and 3 because of growth, not drift.** It was trained on 500 vectors for 128 lists (about 4 per list), so its first centroids are rough, and doubling the corpus gives k-means much more to learn from. Stage 5 adds more of the categories it had just been retrained on: the drift ratio is 1.11 and the corpus grew by a third, so neither trigger fires and nothing is retrained.
- **Fixed recall is below 1.0 everywhere.** IVF is approximate. The test requires that.
- **Training cost.** The five trainings cost 5,989,120 vector-to-centroid distance computations (k-means++ seeding, Lloyd iterations and re-filing every stored vector). At stage 6, a fixed query does 158 vector scans and 128 centroid comparisons where flat search would do 6,250, so the training work of the whole run is repaid after about 1,000 queries.

The tests assert the invariant (at every stage, modelled p99 within the SLO and recall@10 at least 0.95 for both query groups). The fixed run holds it at every stage, always on an IVF index probing a minority of lists. The broken run violates it in both ways: its flat stages go over the SLO, with latency exactly proportional to catalogue size, and its stale IVF stages stay under 0.95 recall on new categories while launch categories stay within 0.05 of the target. Raising nprobe on the stale index restores recall but breaks the SLO. The tests also check that a drift-triggered retrain happens at every stage that introduces new categories and not at stage 5. The cost test checks for more training work than the broken design, recall below 1, a centroid table under 5% of the vector storage, and that training pays for itself within 10,000 queries. One more test runs both designs concurrently under `-race` and checks that a second run gives an identical report.

**How the simulation is built.** Embeddings are 32-dimensional Gaussian blobs from the pattern's `Clustered` generator: 40 launch categories, then two waves of 12 new categories each, all with per-coordinate spread 0.6. The catalogue grows through six stages (500, 1,250, 2,500, 3,750, 5,000 and 6,250 simulated vectors). Queries are stored vectors plus 0.3 noise per coordinate, drawn per stage with a seed that depends only on the stage: 60 evaluation queries and 40 tuning queries for launch categories and the same for new categories (once any exist). The exact top-10 for every query comes from a flat index. Both designs get the same data, queries and ground truth. Both IVF indexes use cosine similarity, 128 lists, at most 8 Lloyd iterations and a training sample of at most 1,280 vectors. The broken design serves flat search for stages 1-3, then trains an IVF index on the stage-3 catalogue (launch categories only), tunes nprobe once, and keeps both for stages 4-6. The fixed design trains at stage 1. At each later stage it adds the new vectors, checks its two triggers, calls `Retrain` if either fires, re-tunes nprobe and then serves. The drift monitor uses only the index's public API: after each training it records the mean of each list's members (`ListOf`) and their average cosine distance to it. For new vectors it computes the same distance to the centre of the list they were filed in (`NearestList`), and divides by the training average.

**Latency is modelled, not timed.** One simulated vector stands for 400 products, and the model prices a 768-dimensional embedding at 0.5 ns per multiply-add: `(vectors scanned × 400 + centroids compared) × 768 × 0.5 ns`. Flat search over 200,000 products is therefore 76.8 ms. p99 is the nearest-rank 99th percentile over the stage's evaluation queries. Simplifications: centroid comparisons are not multiplied by 400 (a real index over 2.5 million products would use thousands of lists, not 128, and probe a much smaller fraction of them, so the scanned fractions here only show the shape). The model ignores memory bandwidth, heap work, re-ranking and the network. nlist is fixed for the life of the index. `Retrain` runs synchronously under the index's write lock, where a real system would build a new index in the background and swap it in. The list centres the drift monitor uses are member means, which approximate the k-means centroids. Synthetic blobs are much more cleanly separated than real product embeddings.

## Interview follow-ups

- *Why not just raise nprobe when recall drops?* Because the cause is the partition, not the probe count. In this run, restoring new-category recall on the stale index takes nprobe=32 and 334 ms p99. Retraining gets the same recall at nprobe=3 and 52 ms. Raising nprobe is a reasonable stopgap while the retrain runs.
- *How do you measure recall in production, where there is no ground truth?* Sample real queries, compute their exact top-k offline with a flat scan (expensive, but only for a sample), and compare with what the ANN index returned. Track it per segment (category, language, tenant), because a global average hides a segment that has drifted, as the launch-category numbers show here.
- *How do you retrain without downtime?* Build the new index from a snapshot in the background, apply the writes that arrived in the meantime, verify its recall on the evaluation set, then switch reads to it and keep the old one for rollback. Retraining on every write is too expensive: the five trainings here cost as much as about 1,000 flat queries.
- *How many lists should IVF have?* A common starting point is a few times √N, with at least 30-50 training vectors per centroid. More lists mean smaller scans per probe but more probes for the same recall and more training data needed. Re-evaluate as N grows, because the right nlist grows with the corpus.
- *Would HNSW avoid the drift problem?* Mostly. A graph index links each new vector to its actual neighbours as it is inserted, so new regions of the space stay searchable without retraining. It costs more memory per vector and more work per insert, and deletes are harder. IVF stays attractive when memory matters, especially with compressed (product-quantised) lists.
- *What else would you alert on?* Inverted-list size skew (new data piling into a few lists), the drift ratio used here, the share of the corpus added since the last training, and recall on a fixed golden query set run every hour.

## Further reading

- Arvind Kumar, [AI Search Feels Slow](https://codefarm0.medium.com/ai-search-feels-slow-system-design-deep-dive-on-vector-indexing-ann-search-and-recall-vs-latency-cd72c6d1723d), the interview scenario this walkthrough is based on: vector indexing, ANN search and the recall-versus-latency trade-off
