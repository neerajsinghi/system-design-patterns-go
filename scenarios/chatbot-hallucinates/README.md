# Scenario: The Support Chatbot Makes Things Up

## The incident

Pebblebrook Home, a fictional home-goods retailer, launches a support chatbot on a general-purpose language model. Within a week, support agents are cleaning up after it. It tells customers they have 14 days to return an item, but the policy has said 30 since March. It hands out a 20% discount code that nobody ever created. It quotes a free-shipping threshold of $35, the figure the model learned in training, when the help centre now says $50. Customers who act on these answers get refused at the warehouse or at checkout, then complain that "your own bot told me so".

The team's first fix is retrieval-augmented generation (RAG). They split the help-centre articles into fixed 12-word chunks, embed them, retrieve the three nearest to each question and paste them into the prompt. The bot still gets most questions wrong. Each 12-word chunk rarely holds both the words a customer uses and the answer, so the passages it retrieves are about the right topic but don't contain the fact. The model quotes whatever it was handed, now with a citation that makes the wrong answer look checked. And it still never says "I don't know": asked about a student discount, it quotes the loyalty-points policy.

## Symptoms

- Confident answers that contradict the help centre, on exactly the topics that changed recently (returns, shipping) or never existed (discount codes).
- After the RAG launch, answers come with citations, but the cited passage is about something adjacent: refund timing instead of the return window, a shipping surcharge instead of the free-shipping threshold.
- No question is ever refused, including questions the help centre has nothing to say about.
- Offline evaluation shows the answer passage is in the top 3 for only a quarter of questions.

## Root cause

1. **The model answers from memory.** With no retrieval, the model can only state what it absorbed in training: the old returns window, an old shipping threshold, and a plausible-sounding discount code. Nothing it says carries a citation, so nobody can check it.
2. **Chunks too small to hold an answer.** Fixed 12-word windows cut straight through sentences. The chunk with "within 30 days of delivery" is not the chunk with "return window", so a question about returning things retrieves neighbouring fragments that share its words but not its answer. Recall@3 is 0.25.
3. **Dense-only retrieval on short text.** A single scorer on fragments this short ranks by a few overlapping words, and it has nothing to catch the exact policy terms ("restocking", "instalments") that keyword search matches directly.
4. **No way to say "I don't know".** There is no relevance threshold and nothing checks the answer, so an out-of-scope question gets the nearest passage quoted back at it, and when nothing in the context matches, the model falls back to memory with no citation at all.

## The fix

| Change | Pattern | Effect |
| --- | --- | --- |
| Chunk by Markdown heading, keeping "Title > Section" with each chunk | [RAG retrieval](../../patterns/ai/rag-retrieval) (`HeadingChunker`) | Each chunk is one whole policy section, so the answer and the words that describe it stay together. Recall@3 goes from 0.25 to 0.96 |
| Hybrid retrieval: BM25 + dense, fused with reciprocal rank fusion | [RAG retrieval](../../patterns/ai/rag-retrieval) (`Hybrid`, `FuseRRF`) | Exact policy terms (BM25) and fuzzy matches (dense) both count, without having to calibrate two score scales against each other |
| Top 3 inside a 150-token prompt budget | [RAG retrieval](../../patterns/ai/rag-retrieval) (`BuildPrompt`) | Lower-ranked chunks that don't fit are dropped, so the model sees fewer distractors. With a 250-token budget, accuracy was 33/48 instead of 36/48 |
| Refuse when retrieval found nothing relevant: dense similarity below 0.25 (`WithMinRelevance`), or the best passage explains less than 40% of the question's keyword weight | [RAG retrieval](../../patterns/ai/rag-retrieval) (`Pipeline` refusal gate) + a coverage gate in this scenario | 6 of 8 out-of-scope questions now get "I don't know" instead of an invented answer |
| Number the passages, require citations, and reject any answer whose sentences are not copied from the passage they cite | [RAG retrieval](../../patterns/ai/rag-retrieval) (`Grounded`) | No uncited or ungrounded answer ships. A memory answer becomes a refusal |

## Run it

```sh
go run ./scenarios/chatbot-hallucinates/broken
go run ./scenarios/chatbot-hallucinates/fixed
go test ./scenarios/chatbot-hallucinates/
```

All three designs answer the same 56 questions: the 48 in-scope questions of the rag-retrieval `EvalSet` (customer phrasings whose answer is in the help centre) and 8 out-of-scope questions it can't answer (student discount, promo codes, electric cars, who founded the company, renting furniture, buy-back for cash, the weather, a Tokyo store).

| | Model memory | Naive RAG | Fixed |
| --- | --- | --- | --- |
| In-scope answered correctly | 0 / 48 | 9 / 48 (19%) | 36 / 48 (75%) |
| In-scope wrong but confident | 48 | 39 | 9 |
| In-scope refused (false refusals) | 0 | 0 | 3 |
| Out-of-scope refused | 0 / 8 | 0 / 8 | 6 / 8 (75%) |
| Out-of-scope answered anyway | 8 | 8 | 2 |
| Answers with no citation / ungrounded | 56 / 56 | 1 / 1 | 0 / 0 |
| Grounded, cited and still wrong | 0 | 46 | 11 |
| Retrieval recall@3 | n/a | 0.25 | 0.96 |
| Mean prompt size (tokens) | 8 | 91 | 111 |
| "How many days do I have to return an item?" | "14 days" | "30 days" [1] | "30 days" [1] |
| "Do you offer a student discount?" | invents code PEBBLE20 | quotes the loyalty-points policy | "I don't know" |

The naive RAG bot answers the literal return-window question correctly, because that phrasing shares "return" and "items" with the right 12-word fragment. Change the wording to "How many days do I have to send something back?" (the `EvalSet` version) and it quotes refund timing instead. Its single ungrounded answer is the weather question: no retrieved fragment shares a word with it, so the model falls back on its generic "Yes, that is covered by our standard customer policy."

**What it costs.** The fixed bot refuses 3 in-scope paraphrases: "send something back" (no shared word with the return-window section, and the toy embedding doesn't know that "send back" means "return"), "when will my refund show up on my card" and "when will I know the arrival time for my sofa delivery". Turning the coverage gate off brings that down to 1 false refusal but lets only 2 of 8 out-of-scope questions be refused: the gate buys 4 correct refusals with 2 wrong ones. Every prompt is now 111 tokens instead of the 8-token bare question, about 103 extra tokens per request (14x), and only 20 more than the naive RAG prompt, because the budget caps how many chunks go in. And 11 shipped answers are grounded, cited and wrong. The 9 in-scope ones quote a real sentence from a retrieved chunk, just not the one that answers the question. Asked how long the warranty on a sofa is, it quotes the 1-year warranty on lamps and electronics. Asked about the fee to assemble furniture, it quotes the restocking fee for returning assembled furniture. The 2 out-of-scope ones get past both gates: "Who founded Pebblebrook Home?" is answered with "Pebblebrook Home is a fictional home-goods retailer.", and "Can I rent furniture by the month?" with the restocking-fee sentence. Groundedness only shows that a sentence came from the passage it cites. It does not show the sentence answers the question.

The tests check that both broken designs violate the invariant (accuracy of at least 70%, no ungrounded or uncited answer, at least 75% of out-of-scope questions refused) and never refuse anything, that model memory states the 14-day window and the invented code, and that naive recall@3 is under 0.5. They check that the fixed design holds the invariant, quotes the 30-day and $50 policies, refuses the student-discount question and beats naive recall. A separate test checks that the groundedness check rejects the model's uncited memory fallback. The cost test asserts that there are false refusals, that the prompt grows by at least 50 tokens and that grounded-but-wrong answers remain. It also asserts that turning the coverage gate off lowers both kinds of refusal. A last test runs `Fixed` concurrently under `-race` and checks the results are identical.

**How the simulation is built.** All three designs share one deterministic fake model. Given a prompt, it quotes the context sentence that best matches the question (the pattern's extractive `AnswerFrom`) and cites it. When no context sentence shares a word with the question, it answers from a small "training memory" instead: a 14-day return window, free shipping over $35 and a PEBBLE20 discount code, or a generic confident yes. It never refuses on its own. Model memory gives it an empty context. Naive RAG gives it the top 3 dense hits over 12-word chunks, with no budget, gate or check. The fixed design runs the pattern's `Pipeline` (heading chunks of at most 120 tokens, hybrid retrieval, top 3, a 150-token budget, a 0.25 dense-similarity gate) and adds a coverage gate. The coverage gate refuses when the best hit's BM25 score is under 40% of the sum of IDF over the question's terms. The fixed design then applies `Grounded` to the model's answer and replaces anything ungrounded with the refusal text. In this run the two gates stop every question that would have sent the model to memory, so the groundedness check never fires (its test exercises it directly). It is the backstop for a real model, which can paraphrase or blend in memory even when the context does match. Retrieval uses the pattern's toy embedding (hashed words and character trigrams), which is lexical, so dense and BM25 overlap more than they would with a real embedding model. The accuracy and refusal thresholds are set at what this toy pipeline reaches, not at what a production bot should promise. Tokens are counted as words.

## Interview follow-ups

- *Why not just fine-tune the model on the help centre?* Fine-tuning bakes in today's policy, which goes stale on the next policy change, just as training did. It also gives no citation to check. RAG updates when the documents do, and every answer points at the passage it came from. Fine-tuning helps with tone and format, not with facts that change.
- *Recall@3 is 0.96. Why is accuracy only 75%?* Retrieval is an upper bound, not a guarantee. The right chunk is in the prompt, but the model quotes a sentence from a different one. Measure end-to-end answer accuracy, not just recall, and keep the prompt small enough that the right passage isn't buried. Here a smaller budget raised accuracy.
- *How do you set the refusal threshold?* From an evaluation set that includes questions you *want* refused, by plotting false refusals against wrong answers at each threshold. Similarity alone separates poorly (here "Who founded Pebblebrook Home?" scores 0.62, higher than most real questions), which is why the fix adds a second signal. Production systems add a classifier or ask the model itself whether the context answers the question.
- *Grounded but wrong: how do you catch it?* Sentence-level groundedness can't. You need an answer-relevance check (does this sentence answer *this* question?, usually a second model call or an NLI model), plus offline evaluation on labelled questions and a feedback loop from the "this didn't help" button.
- *What happens with a million documents?* The linear scan in `Search` stops being affordable. Put the dense side behind an approximate index such as [IVF](../../patterns/ai/vector-ann-index) and keep BM25 in an inverted index. Fusion with RRF stays the same, because it only needs ranks.
- *How do you stop the old 14-day policy coming back?* Re-index when documents change (a change feed from the CMS), delete superseded chunks rather than keeping both versions, and keep an effective date on each chunk so that stale ones can be filtered out.

## Further reading

- Arvind Kumar, [AI Chatbot Gives Wrong Answers](https://codefarm0.medium.com/ai-chatbot-gives-wrong-answers-system-design-deep-dive-on-hallucinations-rag-architecture-and-6c9e7712f664), the interview scenario this walkthrough is based on
- Lewis et al., [Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks](https://arxiv.org/abs/2005.11401) (2020), the paper that introduced RAG
- Cormack, Clarke & Büttcher, *Reciprocal Rank Fusion Outperforms Condorcet and Individual Rank Learning Methods* (SIGIR 2009), the source of the fusion step used here
