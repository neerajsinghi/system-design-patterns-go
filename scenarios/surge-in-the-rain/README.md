# Scenario: Surge Pricing Misses the Rain

## The incident

At 18:00 a shower starts over downtown and the east side. Within three minutes ride requests there are three times normal. The pricing service doesn't notice. It computes one demand/supply ratio for the whole city from a batch job that runs every 15 minutes over the requests that reached the server in that period. The first batch that includes the rain finishes at 18:15, and it shows a modest city-wide 1.24x. Downtown, where riders outnumber drivers two to one, gets 1.24x. The dry suburbs, where drivers are waiting for work, also get 1.24x, and some of their riders give up on the price. Nothing pulls drivers toward the rain, so downtown riders queue for up to ten minutes and some give up. The rain stops at 18:30. The 18:30 batch, still full of rain-time requests, raises the surge to 1.29x for everyone until 18:45. On top of that, phones in the rain have a bad connection: their requests arrive seconds to minutes late and are counted in the minute they arrived, not the minute they were made.

## Symptoms

- Surge stays at 1.0x for the first 15 minutes of the rain, then shows the same value in every part of the city.
- Downtown riders wait a long time: hundreds are unmatched at any moment, and some give up.
- Suburban riders are offered surge prices while drivers there sit idle.
- Surge is highest after the rain has stopped.
- Demand per minute looks smeared: delayed requests from the rain show up in later minutes.

## Root cause

1. **Processing-time batches.** Grouping requests by when they arrived, in 15-minute blocks, means the surge can only change every 15 minutes and always describes the previous block. A 30-minute shower is mostly over by the time it is priced, and the last batch prices it after it has ended.
2. **One number for the whole city.** Demand tripled in 6 of 16 areas. Averaged over the city that's about 1.3x, which is too low where it's raining and too high where it's dry. A city-wide price also gives drivers no reason to move.
3. **Arrival time instead of event time.** A request delayed by two minutes on a weak connection is counted in the wrong minute, and sometimes in the wrong batch.

## The fix

| Change | Pattern | Effect |
| --- | --- | --- |
| Key demand by geohash cell (precision 5, about 4.9 km x 3.7 km here) | [Geospatial index](../../patterns/data/geospatial-index) (`Encode`) | Rain cells and dry cells are priced separately |
| Count requests in event-time windows of 5 minutes, sliding every minute | [Windowed stream aggregation](../../patterns/distributed/windowed-stream-aggregation) (`Sliding`) | A new surge value per cell every minute, covering the last 5 minutes |
| Watermark 30 s behind the newest event, 2 minutes of allowed lateness | [Windowed stream aggregation](../../patterns/distributed/windowed-stream-aggregation) (watermark, `WithAllowedLateness`) | Delayed requests revise the window they belong to; only very late ones (9+ minutes) are dropped and counted |
| Supply = drivers in the cell, from an index every driver's position is written to every minute; match riders to the nearest idle driver | [Geospatial index](../../patterns/data/geospatial-index) (`IDsInCell`, `Nearby`) | Surge = clamp(demand / supply) per cell; drivers next to the rain see the higher surge and move toward it |

## Run it

```sh
go run ./scenarios/surge-in-the-rain/broken
go run ./scenarios/surge-in-the-rain/fixed
go test ./scenarios/surge-in-the-rain/
```

| | Broken | Fixed |
| --- | --- | --- |
| Rain cells reach 1.5x (a perfect pricer: 4 min) | never (peaks at 1.29x) | after 5-6 min |
| Rain cells back to 1.0x after the rain stops | 15 min | 5 min |
| Mean \|applied - true\| per rain-cell minute, 18:00-18:37 | 0.84 | 0.09 |
| Dry cell-minutes priced more than 0.1 above their true multiplier | 300 of 900 (worst +0.29) | 0 (worst +0.08) |
| Dry-cell riders priced out | 363 | 194 |
| Rain-cell riders priced out | 434 | 1,074 |
| Rider wait proxy: unmatched rider-minutes (peak waiting) | 17,984 (685) | 2,106 (116) |
| Riders who gave up after 10 minutes | 59 | 0 |
| Drivers who repositioned toward the rain | 0 | 141 |
| Delayed requests | 1,401 counted in the wrong minute | 645 folded into their own windows late, 65 dropped; 0 of 1,424 windows miscounted |
| Surge values computed | 6 | 2,266 (842 of them revisions from late data) |
| State held | up to 2,485 buffered requests | at most 128 (cell, window) counters |

"True" is what an instant, perfectly informed pricer would show at the start of each minute: every request made in the previous 5 minutes, wherever and whenever it arrived, against the drivers in the cell at that moment. Some results need explaining:

- **The fix still lags by one to two minutes.** The window ending at 18:05 can only close once the watermark passes 18:05, which needs an event 30 s later. Surge is sampled at the start of each minute, so it is always at least one window behind the perfect pricer. That's the cost of the watermark delay and the 1-minute slide, and the test asserts it.
- **Dry cells go above 1.1x in the fixed run (179 cell-minutes, peak 1.35x).** That's not over-pricing. 141 drivers left the suburbs for the rain, so suburban supply really did get tighter and the true multiplier went up with it. The fixed surge never exceeds a dry cell's true multiplier by more than 0.08. Nothing in the simulation sends drivers back afterwards, so some suburbs stay around 1.1x until the end of the run.
- **The fix matches fewer rides (9,517 vs 9,930).** A 1.4-2x price downtown turns away 1,074 riders, which is how surge balances demand. The broken run matches more rides because it leaves riders waiting longer: 8.5 times as many rider-minutes unmatched.

The tests assert the invariant: every rain cell reaches 1.5x within 7 minutes of the demand spike and is back to 1.0x within 8 minutes of the rain stopping, and no dry cell is ever priced more than 0.1 above its own true multiplier. The fixed run holds it. The broken run violates all three parts (it never reaches 1.5x, takes 15 minutes to recover, and over-prices 300 dry cell-minutes). The tests also check that delayed requests are attributed by event time: every emitted window's final count equals the number of requests made in it that arrived within the allowed lateness, while fewer than 1% are dropped. Riders wait at least 4 times less with the fix, and only per-cell surge moves drivers. The cost test checks for more than 100 times the surge values, revisions caused by late data, at least one counter per cell per open window, and a time-to-surge strictly later than the perfect pricer's in every rain cell.

**How the simulation is built.** The city is a 4 x 4 block of geohash-5 cells with 48 drivers in each. Each cell gets about 6 ride requests per minute (the rate's integer part each minute, plus or minus one, at random seconds), so a 5-minute window normally holds 30 requests against the 40 trips its drivers can start (6-minute trips), and surge is 1.0x. From 18:00 to 18:30, the 6 rain cells ramp to 3 times that demand over three minutes and back down over three minutes after. While it rains, 30% of requests from rain cells arrive 20-150 s late and 2% arrive 9-15 minutes late. Elsewhere and at other times the delay is 1-5 s. Each minute: every driver writes a position to the all-drivers index; arriving requests go to the pricer; each rider accepts the current surge `s` if a pre-drawn tolerance in [0, 1) is below `1/s`; waiting riders, oldest first, get the nearest idle driver within 2.5 km from a second index that holds only idle drivers; riders unmatched for 10 minutes give up. An idle driver in an unsurged cell moves to a neighbouring cell (`Neighbours`) whose surge is at least 0.5 higher, with probability 0.15 per minute, and is busy for 3 minutes while driving there. Both designs share the seeded riders and this driver behaviour. Only the pricer differs. Supply is `drivers x 5 / 6` trips per window, and surge is `requests / supply` clamped to [1.0, 3.0]. The broken pricer applies one city-wide value at 17:45, 18:00, 18:15, 18:30 and 18:45. Simplifications: each driver works one cell, so trips end back in the driver's cell (a driver who crosses the border for a pickup drives back), and only repositioning moves supply. Trip length is fixed. Matching runs once a minute rather than per request. A heartbeat advances the watermark at the end of every minute so quiet cells still close their windows. Real systems also smooth surge over neighbouring cells, cap how fast it can change, and forecast demand from weather feeds, and none of that is modelled.

## Interview follow-ups

- *Why event time and not just a faster processing-time job?* A 1-minute processing-time job fixes most of the lag but still counts every delayed request in the wrong minute. In this scenario those delays are concentrated exactly where demand is spiking, because bad weather brings bad connectivity. Event time with a watermark gives the correct per-minute counts and is told when it got something wrong (revisions, drops).
- *How do you pick the watermark delay and the allowed lateness?* From the delay distribution. The watermark delay sets how long you wait before the first answer, so it should cover the common case (here most requests arrive within seconds). Allowed lateness sets how long state is kept for revisions, so it should cover the long tail you still care about. Requests later than that would change a price that no longer matters, so dropping them and counting them is fine.
- *Why sliding windows instead of tumbling 5-minute windows?* A tumbling window updates every 5 minutes, which brings back a smaller version of the batch problem. Sliding by 1 minute gives a fresh value every minute that is still smoothed over 5 minutes of data. The cost is 5 counters per event and 5 times the emissions.
- *How big should a cell be?* Big enough that a window holds enough requests for a stable ratio, and small enough that a rain cell isn't averaged with a dry one. Geohash-5 cells (a few kilometres across) hold about 30 requests per window here. Geohash-6 cells (about 1.2 km x 0.6 km) would hold about one request per window and the surge would be noise. Production systems often use hexagonal cells (H3) and smooth each cell with its neighbours.
- *Doesn't the surge pull so many drivers that the suburbs run short?* Yes, and in the fixed run it does: the suburbs drift up to about 1.1x while drivers leave. That's the market working, but it is why real systems limit how fast surge can change and add incentives to return to under-served areas, rather than relying on surge alone.
- *What happens to a price a rider has already accepted when a late event revises the window?* Nothing. The quote is locked when the rider confirms. Revisions change the price shown to the next rider and the data used for reports.

## Further reading

- Arvind Kumar, [Uber Surge Pricing During Rain](https://codefarm0.medium.com/uber-surge-pricing-during-rain-system-design-deep-dive-on-real-time-demand-supply-and-dynamic-f71fd58fdb03), the interview scenario this walkthrough is based on
