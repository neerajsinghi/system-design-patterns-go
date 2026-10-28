# Scenario: OTP Codes Stop Arriving at Peak Traffic

## The incident

Exam results go live at 10:00 and tens of thousands of students try to log in with a one-time code sent by SMS. Two minutes in, the SMS provider starts struggling: its submit API slows to several seconds and fails most calls. Our API servers try the provider three times per request. The mobile app retries a failed request three times. Students who see no code tap "resend". Within a minute the provider gets thirty times the normal number of calls, fails even more of them, and the few messages it accepts sit in its backlog for half a minute or longer. Codes then arrive late and several at once, and most of them have already been replaced by a newer resend, so students type codes that no longer work. Support fills up with "I never got my OTP". Meanwhile a handful of numbers controlled by a script request a code every few seconds, and every one of those requests gets the same triple retry treatment. A third of the SMS bill pays for messages nobody can use.

## Symptoms

- Calls to the SMS provider go from about 1.5 per login to more than 30 per login during the degradation.
- Time to a working code goes from a few seconds to minutes; one in five users gives up.
- Users receive codes in bunches, and many of the codes are stale ("invalid OTP").
- A few phone numbers receive dozens of messages in ten minutes.
- About a third of the messages sent (3,010 of 8,745) go to abusing numbers, carry stale codes, or reach users who already left.

## Root cause

1. **Retries at every layer multiply.** Three server attempts per request times three app attempts per tap is up to nine provider calls per tap, and users tap several times. Each layer's retry limit looks reasonable on its own; together they turn one login into dozens of calls against a provider that is already failing.
2. **Nothing notices the provider is down.** Every request still starts at the failing primary and waits through its slow, failing submits, so the load stays on the one component that cannot take it.
3. **One provider, no failover.** There is nowhere else to send a code, so the only response to failure is to try the same provider again.
4. **No per-number limit.** A user tapping resend and a script hammering one number are both served without question. Each resend also issues a new code and invalidates the old one, which is why delayed messages show up stale.

## The fix

| Change | Pattern | Effect |
| --- | --- | --- |
| Remove app-level retries; the server retries the primary only from a budget of 10% of requests | [Retry budget](../../patterns/reliability/retry-budget) | Retries cannot multiply load; 91 retries for 7,330 sends (1.2%) |
| Trip a breaker on the primary after 3 consecutive failures, probe it again after 15s | [Circuit breaker](../../patterns/reliability/circuit-breaker) (one per link of the chain) | During the outage the primary gets 0.03 calls per login instead of 32 |
| Fail over in order: primary SMS → secondary SMS → voice call | [Fallback](../../patterns/reliability/fallback) (`Chain`) | 99.92% of users get a working code within 30s; p95 is 7s |
| Limit sends to 3 per phone number per 10 minutes and reply "try again in N seconds" | [Sliding-window rate limiter](../../patterns/reliability/sliding-window-rate-limiter) (`Log`) | Abusing numbers get 120 messages instead of 2,086; no number gets more than 3 |

## Run it

```sh
go run ./scenarios/otp-retry-storm/broken
go run ./scenarios/otp-retry-storm/fixed
go test ./scenarios/otp-retry-storm/
```

| | Broken | Fixed |
| --- | --- | --- |
| Provider calls per login, before the outage | 1.52× (primary) | 1.06× (primary) |
| Provider calls per login, during the outage | 32.23× (all on the primary) | 1.06× in total, 0.03× on the primary |
| Provider calls per login, after the outage | 2.71× | 1.03× |
| Users with a working code within 30s | 4,534 of 7,200 (62.97%) | 7,194 of 7,200 (99.92%) |
| Logged in late / gave up / locked out | 1,201 / 1,464 / 1 | 6 / 0 / 0 |
| p95 time to a working code (users who logged in) | 113s | 7s |
| Stale codes delivered | 854 | 2 |
| Messages sent (primary / secondary / voice) | 8,745 (8,745 / 0 / 0) | 7,330 (4,334 / 2,907 / 89) |
| SMS cost | ₹1,049.40 | ₹1,145.98 |
| Cost per user who logged in | ₹0.183 | ₹0.159 |
| Messages to abusing numbers | 2,086 | 120 |
| Most messages to one number in 10 min | 76 | 3 |
| Resend taps refused | 0 | 3,480 (all from abusing numbers) |

Some numbers need a word of explanation. Broken's 1.52× before the outage is not retries: the provider is healthy then, and the extra 0.5× is the 20 abusing numbers, which nothing limits. Broken recovers once the provider does (2.71× afterwards is users from the outage still tapping, plus the abusers); in this model the overload ends with the outage rather than becoming self-sustaining. Fixed spends about 9% *more* in total, because secondary SMS (₹0.20) and voice (₹0.50) cost more than the primary (₹0.12), but it delivers to every user, so it is cheaper per successful login. The retry budget refused nothing: the breaker takes the primary out of the path after three failures, so the budget never runs dry. Its job is the case the breaker misses, a provider failing, say, 30% of calls, which rarely produces three failures in a row but would still get 30% extra traffic from unbudgeted retries.

The tests assert the invariant (in every phase, provider calls stay within 1.2× legitimate logins; at least 99% of users have a working code within 30s; no number gets more than 3 messages in any 10 minutes). The broken version must violate all three parts and multiply outage load at least 9×; the fixed version must hold all three, keep the primary under 0.1 calls per login during the outage, return traffic to the primary afterwards, and keep budgeted retries within 10% of sends plus the banked burst. A cost test checks that the fix fails over to the pricier secondary and voice providers (so the average price per message exceeds the primary's), that it refuses some taps with a wait time, and that it is still cheaper per successful login. A last test checks that runs are deterministic.

**How the simulation is built.** Ten users start a login every simulated second for 720 seconds (7,200 users), and 20 abusing numbers tap "resend" every 4 seconds over the same period. A user taps, waits 20 to 40 seconds (seeded, the same in both runs), and taps again if no working code has arrived, up to 5 taps, and leaves after 240 seconds. Every tap issues a new code that replaces the old one; a user types every code that arrives, and three stale codes lock the account. The primary is healthy outside t=120–419s: 50 calls/s capacity, 1% errors, 1s submits, delivery in 2–5s. During the degradation its capacity drops to 20 calls/s, it fails 60% of calls when lightly loaded and up to 90% at capacity, submits take 3–8s, and the messages it accepts take another 15–45s to arrive. The secondary (40 calls/s, 3% errors) and voice (20 calls/s, 1% errors) stay healthy. Calls past a provider's per-second capacity are rejected at once. Broken issues the tap as an API request that tries the primary 3 times back to back and is itself retried 3 times by the app, each app retry a new request with a new code. Fixed passes each tap through a per-number `slidingwindowratelimiter.Log` (3 per 10 minutes), then a `fallback.Chain` of the three providers (breaker trips after 3 failures, 15s cooldown). Inside the primary's call, a failed or timed-out submit (over 4s) is retried once only if the `retrybudget.Budget` (ratio 0.1) allows it. Everything runs on one simulated clock that is injected into all three patterns. Simplifications: time moves in whole seconds, and all of a tap's failover attempts are counted in the second it started; a submit abandoned at the timeout is assumed never sent (a real provider may still deliver it, which is one more source of duplicates); delivery always succeeds once a provider accepts a message; and users and abusers are the only traffic.

## Interview follow-ups

- *Why not just lower the retry count from 3 to 1?* It helps, but the count is still per request and per layer, so a full outage still doubles load at each layer that retries. A budget caps retries as a share of all traffic, however many requests fail, and having only one layer retry stops the multiplication.
- *Should a resend generate a new code?* Usually not. Reusing the same code while it is still valid (say 5 minutes) means a late message is still a working message, which removes the stale-code lockouts entirely. Issue a new code only after the old one expires or is used.
- *How do you tell a legitimate resend from abuse?* The per-number limit protects the number's owner and your bill whoever is tapping. Add limits per IP, per device and per account, a CAPTCHA after the first resend, and alerts on SMS spend per destination prefix, since SMS pumping fraud targets premium-rate ranges.
- *Why is a secondary provider not enough?* It may share the same carrier route, and it can be throttled by a sudden switch of all traffic to it. Voice or an in-app or email channel fails differently. Warm up the secondary with a small share of traffic all the time so you know it works and its rate limits are known.
- *What does the user see while the limit applies?* A countdown ("try again in 588 seconds") rather than a silent failure, so they do not keep tapping. The response should come from the server's limiter, so a modified app cannot skip it.

## Further reading

- Arvind Kumar, [OTP Service Fails During Peak Traffic](https://codefarm0.medium.com/otp-service-fails-during-peak-traffic-system-design-deep-dive-on-rate-limiting-retry-storms-7c2677aded90), the interview scenario this walkthrough is based on
