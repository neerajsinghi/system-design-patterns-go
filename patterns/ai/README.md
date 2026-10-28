# AI Serving Patterns

How LLM-backed features stay cheap, fast and grounded once real traffic arrives. Each pattern is a small, dependency-free Go package with tests and a runnable simulation. There are no models and no network calls: embeddings are deterministic hashed bag-of-words vectors, and "models" are fakes with set prices, latencies and accuracy curves. The numbers show how each design behaves, not how good a real model is.

| Pattern | Problem it solves | Key trade-off |
| --- | --- | --- |
| [Semantic cache](semantic-cache) | Paraphrased questions each pay for a full LLM call, and exact-match caches rarely hit | Lower thresholds raise the hit rate but serve answers to the wrong question; needs per-domain tuning |
| [Model router](model-router) | Every LLM request pays large-model price and latency, though most are easy | Large cost savings, but hard prompts get misrouted, escalation pays twice, and thresholds need ongoing evaluation |
| [RAG retrieval](rag-retrieval) | A chatbot answers from model memory and repeats outdated or invented policies | Grounded, cited answers from current documents, but chunking, ranking and refusal thresholds must be measured |
| [Vector ANN index](vector-ann-index) | Brute-force vector search costs O(N·d) per query and slows linearly as the corpus grows | nprobe trades recall for work, but IVF needs k-means training and retraining when data drifts |

For production-grade Go code, see the companion repositories [go-rag-kit](https://github.com/neerajsinghi/go-rag-kit) (retrieval-augmented generation toolkit) and [llm-otel-go](https://github.com/neerajsinghi/llm-otel-go) (OpenTelemetry tracing for LLM calls, including token cost).

## Run everything

```sh
go test -race ./patterns/ai/...
for d in patterns/ai/*/example; do go run ./$d; done
```
