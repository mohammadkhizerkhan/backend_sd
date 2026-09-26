Since you already know the fundamentals conceptually, the fastest path to real expertise is CLI first (build intuition for how brokers, partitions, and offsets actually behave) then Go (build the muscle memory of producer/consumer patterns you'll use in production). Here's a sequence that ends in a real project.

## Phase 0 — Environment (Day 0, ~1-2 hrs)
Set up a local multi-broker cluster so you see real distributed behavior, not a toy single-node setup.
- Use `docker-compose` with **3 Kafka brokers** (KRaft mode, no Zookeeper — that's the modern setup and what you'll see in interviews/prod now) + Kafka UI (like `provectuslabs/kafka-ui` or Redpanda Console) for visual inspection.
- Exit criteria: `docker compose up` gives you 3 brokers, and you can see them in the UI.

## Phase 1 — CLI fundamentals (Days 1-4)
Goal: internalize partitions, offsets, consumer groups, replication — by touching them, not reading about them.

**Day 1: Topics & partitions**
- Create topics with different partition counts (`kafka-topics.sh --create`), describe them, delete them.
- Produce/consume with `kafka-console-producer` / `kafka-console-consumer`.
- Exercise: create a topic with 3 partitions, produce 20 keyed messages, observe which partition each key lands on (`--property print.partition`). Understand hashing-based partitioning.

**Day 2: Consumer groups & offsets**
- Run two consumers in the same group against a multi-partition topic — watch partition rebalancing happen live.
- Use `kafka-consumer-groups.sh --describe` to inspect lag, committed offsets, current assignment.
- Exercise: kill one consumer mid-stream, watch the other pick up its partitions. Manually reset offsets (`--reset-offsets --to-earliest`).

**Day 3: Replication & fault tolerance**
- Create a topic with `replication-factor=3`, kill a broker container, watch leader election happen (`kafka-topics.sh --describe`).
- Exercise: produce while a broker is down, bring it back, watch it catch up (ISR — in-sync replicas).

**Day 4: Retention, compaction, and configs**
- Play with `retention.ms`, `segment.bytes`, and topic-level `cleanup.policy=compact` on a small topic (e.g., simulate a KV changelog).
- Exercise: set a short retention, watch messages actually expire.

**Exit criteria for Phase 1:** you can explain, from having watched it happen, why partition count determines max parallel consumers, what a rebalance looks like, and what ISR/replication actually buys you.

## Phase 2 — Go producer/consumer basics (Days 5-9)
Use **`segmentio/kafka-go`** or **`confluent-kafka-go`** — I'd recommend starting with `kafka-go` (pure Go, easier to read/debug) and later trying `confluent-kafka-go` (librdkafka-based, what most production shops use) so you can speak to both in interviews.

**Day 5: Basic producer**
- Write a Go producer that sends keyed JSON messages to your 3-partition topic.
- Exercise: verify in the UI that your keys are landing on consistent partitions.

**Day 6: Basic consumer**
- Write a Go consumer (single instance, manual partition assignment) that reads and prints messages.
- Then convert it to use **consumer groups** and run 2-3 instances — watch Go handle the rebalance.

**Day 7: Delivery guarantees**
- Implement and compare: fire-and-forget, at-least-once (manual commit after processing), and sync produce with acks=all.
- Exercise: kill your consumer mid-processing (before commit) and restart — confirm you get duplicate delivery. This is where "at-least-once" stops being theory.

**Day 8: Error handling & retries**
- Add a dead-letter-topic pattern: failed messages get published to `orders.DLT` instead of blocking the consumer.
- Exercise: simulate a poison message and confirm your pipeline doesn't stall.

**Day 9: Schema & serialization**
- Move from raw JSON to Avro or Protobuf with a schema registry (Redpanda's or Confluent's, both dockerizable).
- Exercise: evolve a schema (add an optional field) and confirm old consumers don't break.

**Exit criteria for Phase 2:** you can write a producer/consumer from scratch without looking anything up, and explain trade-offs of each delivery guarantee from having broken things yourself.

## Phase 3 — Production patterns (Days 10-13)
This is the "sound like an expert in conversation" layer.
- **Idempotent producers & transactions** (`enable.idempotence`, transactional producer/consumer for exactly-once) — implement a simple exactly-once pipeline (read from topic A, transform, write to topic B, commit offset — all atomically).
- **Consumer lag monitoring** — hook up Prometheus + a Kafka exporter, watch lag graphs under load.
- **Backpressure & throughput tuning** — batch size, linger.ms, compression (snappy/lz4), fetch.min.bytes — benchmark before/after.
- **Multi-topic joins / stream processing** — try a small Kafka Streams-equivalent in Go (or just hand-roll a join using local state + changelog topic) to understand what tools like Flink/ksqlDB actually solve.

## Phase 4 — Capstone project (Days 14-20)
Build something with enough surface area to touch everything above. A solid meaningful option given your Go background:

**"Order processing pipeline"**
- `orders-api` (Go HTTP service) → produces to `orders` topic (keyed by user_id, 6 partitions)
- `inventory-service` (Go consumer group, 3 instances) → validates stock, produces to `orders.validated` or `orders.DLT`
- `notification-service` (Go consumer) → reads validated orders, simulates sending an email/SMS
- Add: exactly-once processing between validate→publish, Prometheus metrics + Grafana dashboard for lag/throughput, graceful shutdown handling in-flight messages, chaos testing (kill a broker/consumer under load and confirm no data loss)

That last project alone will cover partitioning strategy, consumer groups, delivery semantics, schema evolution, and observability — the actual things that come up in real Kafka conversations.

Want me to scaffold the docker-compose setup and the Go module structure for Phase 0/1 so you can start today?