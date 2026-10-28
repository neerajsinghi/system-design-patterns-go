# Scenario: The LLM Bill Grows Faster Than Usage

## The incident

A company ships an AI support assistant that its business customers (tenants) embed in their own help centres. It's a hit: within a month usage is ten times higher. The LLM invoice is worse than ten times higher. Nobody had looked at how requests were built. Every one goes to the largest model. Every turn resends a long system prompt and the whole conversation so far, so the twentieth message of a chat costs several times as much as the first. Thousands of users ask the same twenty questions in their own words, and each phrasing pays for a fresh answer. Then one tenant's ticket integration gets stuck in a retry loop, re-sending a growing thread every three seconds, and spends a month's allowance before lunch. Finance asks engineering to cut LLM spend by 70% without making the answers worse.

## Symptoms

- Cost per request goes up as usage grows, when it should stay flat or fall.
- Input tokens per request are in the thousands while the user's question is a dozen words. Most of the bill is context, not questions.
- The top questions in the logs are the same few intents in endless variations ("reset my pwd", "how can i reset my password?"), and every one shows a full model call.
- One tenant's spend chart is a wall: a whole month's allowance spent in a single day, at a steady rate that no human produces.

## Root cause

1. **One model for everything.** "What are your opening hours" pays large-model prices and waits for large-model latency, though a model a thirtieth of the price answers it about as well.
2. **Context grows every turn.** The system prompt and the full history are resent on each call, so input tokens grow with conversation depth and the cost of a conversation grows faster than its length.
3. **No reuse.** Answers to common questions are thrown away, so every paraphrase pays again.
4. **No per-tenant limit.** Nothing stops one tenant's bug from spending without bound, and the only alternative anyone had was a global kill switch that would take every tenant down.

## The fix

| Change | Pattern | Effect |
| --- | --- | --- |
| Look common questions up by meaning before calling a model; pick the similarity threshold from labelled data so that at most 1% of cache answers are for the wrong question; never cache answers that read the user's account | [Semantic cache](../../patterns/ai/semantic-cache) (`HashEmbedder`, per-tenant `Cache`, `GetOrCompute` with the cacheable flag) | 534 requests answered for $0 in model calls and 20ms; 3 of them wrong |
| Route each cache miss to the cheapest tier its complexity score allows, and retry on the next tier when the model's confidence is below 0.8 | [Model router](../../patterns/ai/model-router) (`Router`, `WithEscalation`, `FakeModel` tiers) | Large model answers 403 requests instead of 4,508; 112 escalations pay for two calls |
| Send only the last 4 history turns plus a 60-token running summary, folded once by the small model when turns leave the window | [Model router](../../patterns/ai/model-router) (`TrimHistory`, `EstimateTokens`) | Input tokens per request fall from 2,760 to 888 |
| Per-tenant daily budget: past $1.50 downgrade to the small model with no escalation, past $1.75 reject at the gateway | [Model router](../../patterns/ai/model-router) (`Budget`) | The looping tenant stops at $1.75 instead of $53.29; no other tenant is touched |

## Run it

```sh
go run ./scenarios/llm-bill-explodes/broken
go run ./scenarios/llm-bill-explodes/fixed
go test ./scenarios/llm-bill-explodes/
```

Choosing the cache threshold (fixed run, 500 labelled calibration prompts drawn with a different seed from production traffic):

| Threshold | Answered from cache | Of those, for the wrong question |
| --- | --- | --- |
| 0.70 | 41.2% | 5.8% |
| 0.75 | 34.8% | 5.2% |
| 0.80 | 27.8% | 2.9% |
| **0.85 (chosen)** | **20.2%** | **0.0%** |
| 0.90 | 15.4% | 0.0% |
| 0.95 | 14.2% | 0.0% |

0.85 is the lowest threshold within the 1% target. Lower thresholds look attractive on hit rate, but the calibration set shows what they return: "upgrade subscription please" matched to the cached answer for "steps to downgrade subscription?" (similarity 0.81), and "turn on the 2fa" matched to "turn off the notifications" (0.77), because the toy embedder sees shared words, not meaning. One downgrade/upgrade pair sits just under 0.85, so confusable paraphrases exist close to the chosen threshold; the production run, with different phrasings, does get 3 false hits.

| | Broken | Fixed |
| --- | --- | --- |
| Total cost (6 tenants, one day) | $72.70 | $6.72 (9%) |
| Well-behaved tenants' cost (5 tenants, 2,476 requests) | $19.40 | $4.97 (26%) |
| Per tenant: acme / globex / initech / soylent / umbrella | $4.45 / $3.33 / $3.95 / $4.00 / $3.67 | $1.14 / $0.90 / $0.99 / $0.91 / $1.03 |
| Runaway tenant (vandelay, 2,032 requests) | $53.29 | $1.75: 697 downgraded, 586 rejected |
| Cost per request (all / well-behaved) | $0.01613 / $0.00784 | $0.00149 / $0.00201 |
| Input tokens sent | 12,442,899 (2,760 per request) | 4,006,602 (888 per request) |
| Answers by source: cache / small / medium / large | 0 / 0 / 0 / 4,508 | 534 / 1,802 / 1,183 / 403 |
| Cache hit rate, false hits | n/a | 13.6% of 3,922 lookups, 3 false hits (0.6% of cache answers) |
| Escalations, extra spend | n/a | 112 requests, $0.28; history summaries $0.17 |
| Answer quality | 94.7% | 93.3% (−1.4 points) |
| p50 latency (well-behaved tenants) | 3.3s | 578ms |

**Answer quality** is the percentage of the five well-behaved tenants' 2,476 requests that got a correct answer. A model answer is correct when the fake model says so. A cache answer is correct only when the cached entry was for the same question (same intent label) and the answer stored there was itself correct; a false hit counts as wrong. A rejected request would count as wrong too, but none of these tenants has a request rejected. The runaway tenant is left out of quality because its loop is a bug, not a user.

The cache hit rate looks low because the denominator includes the loop's requests and account-specific questions, which can never hit. Of the non-loop requests that reached the cache, about 20% were answered from it, which is what the calibration predicted. Quality drops 1.4 points, mostly because the small model is slightly worse than the large one on easy questions and a cached wrong answer is repeated to everyone who asks that question. The fixed numbers for the runaway tenant include its real users: once the tenant is over budget, the gateway can't tell its loop from its help-centre visitors, so both are downgraded and then refused.

The tests assert the invariant for the fixed version: total cost and well-behaved tenants' cost are each at most 30% of broken's, quality is at least broken's minus 2 points, the runaway tenant spends no more than its hard budget plus one request, and no other tenant has a request downgraded or rejected. They check that the broken version lets the runaway tenant spend past the budget and sends everything to the large model. A cost test checks what the fix gives up: there are cache false hits on production traffic (but within the 1% target the threshold was chosen for), escalations pay for a second call, and the runaway tenant is downgraded and then rejected. Another test checks that the chosen threshold is the lowest one in the sweep that meets the target.

**How the simulation is built.** Six tenants each have 110 conversations in one simulated UTC day, 1 to 20 user turns deep (exponentially distributed, mean about 4.6), 45 seconds apart. Each turn is a common question (60%: 22 intents with Zipf popularity, a different popularity order per tenant, and phrasings built from verb and noun synonyms, templates, greetings and punctuation), an account-specific question (25%: needs tools, not cacheable) or a hard troubleshooting question (15%; a fifth of them are short and keyword-free so the router under-routes them and has to escalate). From 09:00 the vandelay integration sends 1,500 triage requests, one every 3 seconds, appending each reply to the thread and restarting the thread every 40 iterations. Every call carries a 510-token system prompt. Models are the router pattern's deterministic `FakeModel`s: small ($0.00015/$0.0006 per 1K tokens in/out), medium ($0.001/$0.004) and large ($0.005/$0.015); a model's chance of being right falls with the request's hidden difficulty, and the outcome is a hash of the request ID, so both runs face the same coin flips. Embedding costs $0.00002 per 1K tokens and 20ms on every fixed request. Requests are processed one at a time in timestamp order against an injected clock.

**Simplifications.** The fake models ignore history, so trimming costs no quality here; a real model can lose context a bad summary dropped. The summary is a fixed-size placeholder whose cost (a small-model call over the dropped turns) is charged, not a real summary. Cached answers ignore the conversation they were asked in, which is fine for FAQ answers but not in general. Calibration compares each prompt with the most similar earlier common question instead of replaying a full cache, and uses a toy hashed bag-of-words embedder; a real embedding model gives different, usually better, numbers but needs the same sweep. The system prompt is sent in full in both versions: provider-side prompt caching or a shorter prompt would cut further, and isn't modelled. Budgets are per tenant, not per API key or per conversation.

## Interview follow-ups

- *Finance wants 70% now. Which lever do you pull first?* Routing, because it's a config change with a measurable quality cost, and the large model is about 33× the small model's input price. Then trim history, which is pure token reduction. The cache comes last because it needs a labelled set to tune and the most care to get right.
- *How would you know the cache is hurting quality in production?* Log a sample of cache hits with the matched prompt and similarity, have humans or a judge model label them, and track the false-hit rate per intent. Re-run the threshold sweep when the embedding model changes, because thresholds don't transfer between models.
- *Why per-tenant budgets rather than a global spend cap?* A global cap turns one tenant's bug into everyone's outage. Per-tenant limits contain it, and the soft limit buys time for a human to look before anything is refused.
- *The runaway tenant's real users were refused too. Can you do better?* Detect the loop itself: rate-limit per API key or per conversation, reject a request whose prompt and growing thread repeat the previous one, or cap requests per conversation. The budget stays as the backstop for the loops you didn't predict.
- *Why not cache account-specific answers with the user ID in the key?* You could, but the hit rate would be near zero and the risk is serving one user's order details to another. The cacheable flag makes "this answer read private data" an explicit decision at the call site.
- *What about the system prompt?* Most providers now cache a repeated prompt prefix at a discount. Put the static part first and the per-request parts last so the prefix stays identical, and move rarely needed policy text into retrieval.

## Further reading

- Arvind Kumar, [AI Platform Suddenly Becomes Very Expensive](https://codefarm0.medium.com/ai-platform-suddenly-becomes-very-expensive-system-design-deep-dive-on-token-reduction-model-473a3a2920a2), the interview scenario this walkthrough is based on, covering token reduction, model routing, caching and budget controls
