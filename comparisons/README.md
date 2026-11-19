# Comparisons

Side-by-side guides to technologies that do the same job, for system design interviews and for real choices. Each page explains the mechanism behind the differences rather than quoting benchmarks, so the advice holds up when version numbers change.

Every page follows the same shape:

- **Explain it like I'm 5:** the difference as a short story.
- **The short answer:** when to pick each one, and when to use both.
- **At a glance:** a table of the dimensions that matter.
- **How each one works:** a diagram of each technology's internals.
- **Where each one sits:** a quadrant chart placing them, and close relatives, on the two axes that separate them best. The positions are qualitative judgments, not measurements.
- **Decision guide:** a flowchart from your requirements to a choice.
- **Trade-offs in depth, common misconceptions and interview follow-ups.**
- **In this repo:** links to the patterns and scenarios that implement the underlying ideas.

## Data stores

| Comparison | The question it answers |
| --- | --- |
| [SQL vs NoSQL](sql-vs-nosql) | Relational, or one of the document, key-value, wide-column and graph families? |
| [PostgreSQL vs MySQL](postgresql-vs-mysql) | Which general-purpose relational database, and why their MVCC designs differ |
| [MongoDB vs Cassandra vs DynamoDB](mongodb-vs-cassandra-vs-dynamodb) | Which NoSQL store once a workload outgrows one relational node |
| [Strong vs eventual consistency](strong-vs-eventual-consistency) | What a read is allowed to return, CAP and PACELC done properly, and where databases sit |
| [Redis vs Memcached (and Valkey)](redis-vs-memcached) | Which in-memory store for caching, sessions, counters and leaderboards |
| [Elasticsearch vs PostgreSQL full-text search](elasticsearch-vs-postgres-full-text) | A dedicated search engine, or the search built into your database? |
| [pgvector vs dedicated vector databases](pgvector-vs-dedicated-vector-databases) | Embeddings next to your relational data, or a purpose-built vector store? |
| [Object vs block vs file storage](object-vs-block-vs-file-storage) | Where each kind of data should live: S3, EBS or EFS, and their equivalents |

## Messaging and real time

| Comparison | The question it answers |
| --- | --- |
| [Kafka vs RabbitMQ](kafka-vs-rabbitmq) | A replayable log or a routing broker, and where SQS, NATS and Pulsar fit |
| [Batch vs stream processing](batch-vs-stream-processing) | Spark, Flink or Kafka Streams, and how fresh results need to be |
| [WebSocket vs SSE vs long polling](websocket-vs-sse-vs-long-polling) | How to push fresh data to browsers and apps |

## Services and infrastructure

| Comparison | The question it answers |
| --- | --- |
| [REST vs gRPC vs GraphQL](rest-vs-grpc-vs-graphql) | Which API style for public APIs, browser and mobile clients, and service-to-service calls |
| [NGINX vs HAProxy vs Envoy](nginx-vs-haproxy-vs-envoy) | Which proxy at the edge, in front of a service tier, or in a service mesh |
| [Monolith vs modular monolith vs microservices](monolith-vs-microservices) | How to structure a backend as the team grows |
| [etcd vs ZooKeeper vs Consul](etcd-vs-zookeeper-vs-consul) | Which coordination store for leader election, locks, config and service discovery |
