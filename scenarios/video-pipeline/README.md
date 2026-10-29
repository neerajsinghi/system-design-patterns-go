# Scenario: Uploads That Never Finish Processing

## The incident

A video platform's evening peak: creators upload everything from one-minute clips to two-hour streams. The upload API encodes the video inside the POST handler, on whichever encode server picks up the request, from the first frame to the last. Encoding takes about three times the video's length, so even a one-minute clip outlives the load balancer's 60-second timeout. The app shows "upload failed, retrying" and sends the upload again. The server never noticed the first client left, so the first encode keeps running and the retry starts a second one, then a third and a fourth. A creator's two-hour keynote needs six hours on one machine; at hour five that machine is recycled and the encode starts over from zero. By 21:00 the encode fleet is full of duplicate copies and long videos, and a two-minute clip uploaded at 20:30 is not playable until well after midnight.

## Symptoms

- 100% of upload requests end in a gateway timeout, yet the videos eventually appear. Creators re-upload "failed" videos by hand, which makes it worse.
- Encode servers are busy all evening, but most of the work is copies of videos that are already done or still being done elsewhere.
- Time-to-ready for long videos is measured in hours and grows with the video's length. A crash late in an encode costs almost the whole encode.
- Short clips wait behind long ones: every server they could use is busy for hours.

## Root cause

1. **Long work behind a synchronous request.** The handler holds the request for the whole encode, so the load balancer's timeout decides the outcome, not the encode.
2. **Non-idempotent retries.** A retry of the same upload is a new encode. The server cannot tell "same video again" from "new video", so every timeout multiplies the work by up to four.
3. **One job, one machine.** A serial encode's wall time equals its CPU time. Forty-seven other servers cannot help with the keynote, and a crash throws away everything because nothing is saved part-way.
4. **No isolation between short and long work.** A server that takes a six-hour encode is gone for six hours; with enough long videos and duplicates in the queue, everyone waits.

## The fix

| Change | Pattern | Effect |
| --- | --- | --- |
| `POST /jobs` stores the upload and answers `202 Accepted` with a job ID; the app polls `GET /jobs/{id}` or gets a signed callback | [Async request-reply](../../patterns/architecture/async-request-reply), [webhook delivery](../../patterns/architecture/webhook-delivery) | No request is held for the encode; zero load-balancer timeouts |
| The app sends an `Idempotency-Key` per upload; a resubmit returns the same job | [Idempotency key](../../patterns/reliability/idempotency-key) (inside async request-reply) | A lost `202` and the retry it causes start no second encode |
| Cut each video into 4-second chunks and let every worker take chunks from one queue | [Split-process-merge](../../patterns/distributed/split-process-merge), [competing consumers](../../patterns/distributed/competing-consumers) | The keynote's 6h of work runs on 48 workers at once |
| Requeue a dead worker's chunk when its lease expires; checkpoint every finished chunk | [Split-process-merge](../../patterns/distributed/split-process-merge) (per-chunk retry, checkpoint/resume) | A crash redoes only the chunks that were in flight |
| Run backup copies of the slowest chunks once the queue is empty; merge chunks in order as they complete | [Split-process-merge](../../patterns/distributed/split-process-merge) (speculation, streaming merge) | Stragglers stop setting each video's finish time; the merge adds well under a second |

## Run it

```sh
go run ./scenarios/video-pipeline/broken
go run ./scenarios/video-pipeline/fixed
go test ./scenarios/video-pipeline/
```

| | Broken | Fixed |
| --- | --- | --- |
| Upload requests / cut at the 60s LB timeout | 320 / 320 | 92 / 0 (12 resubmits after a lost 202) |
| Encodes started for 80 uploads | 320 (240 duplicates) | 80 (0 duplicates) |
| Time to ready, p50 / p95 / max | 1h06m / 6h01m / 8h00m | 9m / 23m / 24m |
| Clips of 10 min or less (57), p50 / max | 29m / 2h31m | 9m / 23m |
| Keynote (2h video): encode wall time vs ideal (work / 48 workers = 8m) | 6h00m | 12m (incl. a 2m coordinator restart) |
| Worst video of 1h+: encode wall time vs its ideal | 48.0x (a serial encode always is: one worker, not 48) | 1.5x |
| Work redone after the 6 worker crashes | 1h15m | at most 3m (3 chunks) |
| Keynote crash at 83% of its encode | 5h00m lost, restarts from zero | 6m lost: 1,498 of 1,800 chunks checkpointed, 48 in flight redone |
| Worker time spent / needed (82.7h) | 337.1h (25% useful), busy until t=10h16m | 105.2h (79% useful), done by t=3h08m |
| Client learns the video is ready | never (its requests all timed out) | callback: at once; polling: p50 47s, max 2m later, 782 polls |

Two numbers in the broken column need explaining. The keynote is ready 6h01m after upload, not 11h, because a *duplicate* encode started by the app's first retry (70 seconds in, on another server) is still running when the original's machine dies at hour five: the duplicates accidentally act as redundancy, at four times the cost. And the broken fleet is only 68% busy on average because the queue is empty for the last few hours, while the remaining long encodes run on single machines.

The fix has costs, and the run shows them. Chunking adds a 2-second overhead to every 12-second chunk and a 20ms-per-chunk merge; with jitter and stragglers that is why the fixed pipeline spends 105h of worker time on 83h of encoding, and why the keynote takes 12 minutes rather than the ideal 8. Speculation started 3,093 backup chunks and only 230 of them won. Without it (`fixed(false)` in the tests) the cluster spends 102.1h instead of 105.2h, about 3% less, but p95 time-to-ready rises from 23m to 31m and max from 24m to 33m: the extra copies run on workers that would otherwise sit idle at the end of each job. The 4h51m the report lists as "lost attempts and cancelled copies" overstates the net cost, because a cancelled loser is often the slow original, whose time would have been spent anyway. Polling clients pay up to one backoff interval (capped at 2 minutes) of latency and 782 status requests; callbacks pay neither.

The tests assert the invariant for the fixed version (exactly one encode per upload, no request held anywhere near the LB timeout, every video of an hour or more processed within 2x of its work / workers, at most one lost chunk per worker crash and at most one per worker for the coordinator crash, a non-empty checkpoint on resume, one callback per upload), check that the broken version violates it, check that fixed beats broken at p50, p95 and max, and measure the costs: polling latency within the backoff cap, per-chunk overhead and merge time, and speculation's extra worker time against its latency gain. A determinism test runs Fixed twice and compares every number.

**How the simulation is built.** 80 uploads arrive at seeded times over three hours: a 2-hour keynote at t=0, then 75% clips of 1-10 minutes, 20% videos of 15-45 minutes and 5% streams of 90-150 minutes. Encoding costs 3 seconds per second of video, and the cluster has 48 workers in both versions. Six worker crashes happen at the same times on the same worker indices in both; on top of that, the machine (broken) or job coordinator (fixed) running the keynote dies 5/6 of the way through its encode. A crashed machine or coordinator is back after 2 minutes. Broken is a small discrete-event model: each attempt is an encode job queued for the next free server, the app retries 70 seconds after each attempt (60s timeout, 10s pause) up to four attempts, and an encode interrupted by a crash restarts from zero at the head of the queue. Fixed runs `splitprocessmerge.Simulate` for each video (4-second chunks, 2s overhead per attempt, ±20% jitter, 2% of attempts 4x slower, a 30s lease before a dead worker's chunk is requeued, backups on, streaming merge at 20ms per chunk). The keynote runs to the crash point with `StopAt`, then again with `Resume` set to the checkpoint. Every upload then goes through the real `asyncrequestreply` server and client over loopback HTTP, with a simulated clock: 10% of the 202s are "lost" and the app resubmits with the same key, the job's work blocks until the clock reaches the time the cluster finished that video, the client polls with backoff (5s doubling to 2 minutes), and the server POSTs a signed callback through `webhookdelivery`.

**Simplifications.** The fixed cluster works on one upload at a time, in the order they were accepted, spreading it over all 48 workers; the next upload's chunks start as soon as the previous upload's last chunk is accepted. That is why short clips still wait a median of 9 minutes: they queue behind whole jobs, including the tail of each job where few chunks are left. A production scheduler interleaves chunks from many jobs (fair share or a separate pool for short videos), which would cut that wait and use fewer backups. A worker that crashes stays dead for the rest of that upload and is replaced before the next. The redo bound for worker crashes multiplies the chunks lost by the most one attempt can cost (about 60s), because the simulator does not report lost time separately from cancelled backups; the keynote's figure is exact, since nothing else is wasted before its crash point. The broken model encodes with no per-chunk overhead, stragglers or failures, which flatters it. Upload transfer time, storage, the CDN and per-resolution ladders are not modelled, and callback delivery takes no simulated time. Each upload's API conversation is replayed one after another on the simulated clock; they do not interact at the API, because the cluster schedule already accounts for the contention between them.

## Interview follow-ups

- *Why not just raise the load balancer timeout to 8 hours?* Every in-progress upload then holds a connection, a handler and often a thread for hours, a deploy kills all of them, and mobile networks drop long requests anyway. The client still has no way to resume or ask "is it done?", so it still retries.
- *The client lost the 202 and retried. How does the server know it's the same upload?* The app generates an idempotency key per upload (before the first attempt) and sends it on every retry. The server stores key → job ID with a fingerprint of the request, returns the existing job for a match, and rejects the same key with a different body with 422.
- *Polling or callbacks?* Callbacks for servers (signed, retried, deduplicated by job ID), polling with backoff and `Retry-After` for mobile apps that cannot receive one, and server-sent events or push notifications while the creator is watching the progress bar. Polling is the fallback that always works; callbacks fail when the receiver is down, so the status endpoint stays the source of truth.
- *How small should chunks be?* Small enough that a crash loses little and the work spreads over every worker; large enough that the per-chunk overhead (here 2s on 12s of encoding, about 17%) and encoder quality at boundaries stay acceptable. Cutting at keyframes keeps every chunk independently decodable.
- *Doesn't speculation waste money?* Some. Backups only run when a worker would otherwise be idle, and here they cost about 3% more worker time for a 26% lower p95. Cap them per job, start them only for chunks running well past the median, and turn them off when the queue is long, since then there is no idle capacity to use.
- *What if the coordinator itself crashes?* Keep the job state and the chunk checkpoint in a database, not in memory. A new coordinator loads the checkpoint, requeues the chunks that are not done, and the merge continues from the first missing index. Here that cost the keynote 48 in-flight chunks instead of five hours.

## Further reading

- Arvind Kumar, [YouTube Video Processing Pipeline](https://codefarm0.medium.com/youtube-video-processing-pipeline-system-design-deep-dive-on-distributed-encoding-and-async-a39a7afc4d72), the interview scenario this walkthrough is based on: synchronous upload timeouts, distributed chunked encoding and async job status
