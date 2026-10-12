# Phase 1 Manual: Kafka CLI Fundamentals & Core Mechanics

This hands-on manual guides you through the foundational mechanics of Apache Kafka using the official CLI tools inside the 3-broker KRaft cluster. You will master **partitions, key-based hashing, consumer groups, rebalances, offset management, replication & failover (ISR), and log retention/compaction**.

---

## Prerequisites

Ensure your Phase 0 cluster is running:
```bash
docker compose -f kafka/docker-compose.yml up -d
docker compose -f kafka/docker-compose.yml ps
```

> [!IMPORTANT]
> When executing CLI scripts via `docker exec`, use the internal container listener:  
> `--bootstrap-server kafka-1:29092`  
> Kafka binary scripts are located at `/opt/kafka/bin/` inside each broker container.

---

## Day 1: Topics, Partitions & Key Hashing

### 1. Architectural Concepts: How Partitions Work
- **Topic**: A logical stream/category of messages.
- **Partition**: A physical, ordered, immutable append-only commit log. Topics are divided into partitions for parallelism and horizontal scaling.
- **Partitioning Strategy**:
  - **No Key (`null` key)**: Messages are distributed using sticky partitioning (batching into partitions round-robin style).
  - **With Key**: Kafka computes `murmur2(key) % partition_count` to deterministically route all messages with the same key to the exact same partition. This guarantees strict per-key ordering.

```
Producer ──(Key: "user-101")──► [Murmur2 Hash % 3 = 1] ──► Partition 1 (Strict Ordering)
Producer ──(Key: "user-202")──► [Murmur2 Hash % 3 = 0] ──► Partition 0
Producer ──(Key: "user-303")──► [Murmur2 Hash % 3 = 2] ──► Partition 2
```

---

### 2. Hands-On Topic Operations

#### Create Topics
Create a topic `orders` with 3 partitions and a replication factor of 3:
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092 \
  --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 3
```

#### List and Describe Topics
Inspect partition placement, leader brokers, replicas, and ISR:
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092 \
  --describe \
  --topic orders
```

Expected output:
```text
Topic: orders	TopicId: ...	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=1
	Topic: orders	Partition: 0	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1
	Topic: orders	Partition: 1	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2
	Topic: orders	Partition: 2	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3
```

#### Alter Partition Count
Increase the number of partitions to 4:
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092 \
  --alter \
  --topic orders \
  --partitions 4
```

> [!WARNING]
> You can **increase** partitions, but you can **never decrease** partitions in Kafka. Furthermore, changing partition count alters key-to-partition hashing (`murmur2(key) % new_partition_count`), breaking ordering consistency for subsequent messages with existing keys.

#### Delete a Topic
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092 \
  --delete \
  --topic orders
```

---

### 3. Keyed Message Hashing Exercise

Re-create a clean 3-partition topic `keyed-orders`:
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092 \
  --create \
  --topic keyed-orders \
  --partitions 3 \
  --replication-factor 3
```

#### Step A: Open a Formatted Console Consumer
Open a terminal and start a consumer that prints metadata (Partition, Offset, Key, and Value):
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:29092 \
  --topic keyed-orders \
  --from-beginning \
  --property print.key=true \
  --property print.value=true \
  --property print.partition=true \
  --property print.offset=true \
  --property print.timestamp=true \
  --property key.separator=" | "
```

#### Step B: Produce Keyed Messages
In another terminal, start the console producer configured for key-value pairs (using `:` delimiter):
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-1:29092 \
  --topic keyed-orders \
  --property parse.key=true \
  --property key.separator=":"
```

Paste these 10 keyed messages:
```text
user_A:order_1001
user_B:order_1002
user_A:order_1003
user_C:order_1004
user_B:order_1005
user_A:order_1006
user_D:order_1007
user_C:order_1008
user_A:order_1009
user_E:order_1010
```

#### Observation & Insight
Look at your consumer output. Notice that:
- Every message with `user_A` landed on the **exact same partition** in strictly increasing offset order (e.g. `order_1001` -> `order_1003` -> `order_1006` -> `order_1009`).
- Different keys (`user_B`, `user_C`, etc.) map across partitions 0, 1, or 2 based on their hash value.

---

## Day 2: Consumer Groups, Rebalances & Offsets

### 1. Architectural Concepts: Consumer Groups & Scalability

```
Topic: user-tracking (3 Partitions)
┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│ Partition 0 │   │ Partition 1 │   │ Partition 2 │
└──────┬──────┘   └──────┬──────┘   └──────┬──────┘
       │                 │                 │
       ▼                 ▼                 ▼
┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│ Consumer C1 │   │ Consumer C2 │   │ Consumer C3 │
└─────────────┘   └─────────────┘   └─────────────┘
  Consumer Group: "analytics-group" (Max parallel consumers = partition count = 3)
```

- **Consumer Group**: A set of consumers cooperating to consume data from a topic.
- **Partition Assignment Rule**:
  - Each partition is assigned to **exactly one** consumer within a given group at any time.
  - If `Consumers == Partitions`: 1 partition per consumer.
  - If `Consumers < Partitions`: Some consumers read from multiple partitions.
  - If `Consumers > Partitions`: Idle/spare consumers stand by (max concurrency = number of partitions).
- **Consumer Offsets (`__consumer_offsets`)**:
  - Kafka tracks progress by storing the committed offset for each `(consumer_group, topic, partition)` tuple in an internal compacted topic.
  - **Lag**: `Log End Offset (LEO) - Current Committed Offset`. Represents unprocessed messages.

---

### 2. Live Rebalance Demonstration

Let's observe rebalancing live using 2 consumers in the same group on a 3-partition topic.

#### Step A: Create Topic & Seed Data
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092 \
  --create \
  --topic telemetry \
  --partitions 3 \
  --replication-factor 3
```

#### Step B: Start Consumer 1 (Terminal A)
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:29092 \
  --topic telemetry \
  --group telemetry-processors \
  --property print.partition=true \
  --property print.key=true \
  --property print.value=true \
  --property key.separator=" | "
```

#### Step C: Inspect Group Assignment (Terminal C)
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:29092 \
  --describe \
  --group telemetry-processors
```
Notice that Consumer 1 owns **all 3 partitions (0, 1, 2)**.

#### Step D: Start Consumer 2 (Terminal B)
In a second terminal, launch Consumer 2 joining the exact same group:
```bash
docker exec -it kafka-2 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-2:29092 \
  --topic telemetry \
  --group telemetry-processors \
  --property print.partition=true \
  --property print.key=true \
  --property print.value=true \
  --property key.separator=" | "
```

Re-run `--describe --group telemetry-processors` in Terminal C.
You will see that partitions have been **rebalanced** (e.g., Consumer 1 gets partitions 0 and 1, while Consumer 2 gets partition 2).

#### Step E: Produce Messages and Test Failover
Produce messages:
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-1:29092 \
  --topic telemetry
```
Send `msg1`, `msg2`, `msg3`, `msg4`, `msg5`. Notice the messages split across Terminal A and Terminal B.

Now **kill Consumer 1 (`Ctrl+C` in Terminal A)**:
- Produce more messages.
- Notice Consumer 2 immediately takes over partition assignments and consumes all messages.

---

### 3. Inspecting Lag & Managing Offsets

#### Describe Consumer Group Details
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:29092 \
  --describe \
  --group telemetry-processors
```

Sample output:
```text
GROUP                TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
telemetry-processors telemetry       0          12              12              0               consumer-...    /172.28.0.2     console-consumer
telemetry-processors telemetry       1          8               8               0               consumer-...    /172.28.0.2     console-consumer
telemetry-processors telemetry       2          15              15              0               consumer-...    /172.28.0.2     console-consumer
```

---

### 4. Resetting Offsets (Reprocessing Data)

> [!NOTE]
> To reset offsets, all consumers in the target consumer group must be **stopped (inactive)**.

#### Scenario 1: Dry Run Reset to Earliest
Simulate rewinding the offset to the beginning without applying changes:
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:29092 \
  --group telemetry-processors \
  --topic telemetry \
  --reset-offsets \
  --to-earliest \
  --dry-run
```

#### Scenario 2: Execute Offset Reset to Earliest
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:29092 \
  --group telemetry-processors \
  --topic telemetry \
  --reset-offsets \
  --to-earliest \
  --execute
```

Now start a consumer in `telemetry-processors` again; you will see it reprocess every message from offset 0!

#### Other Offset Reset Strategies
```bash
# Shift offset backwards by 5 messages (per partition)
docker exec -it kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:29092 \
  --group telemetry-processors \
  --topic telemetry \
  --reset-offsets \
  --shift-by -5 \
  --execute

# Reset offset to a specific timestamp
docker exec -it kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:29092 \
  --group telemetry-processors \
  --topic telemetry \
  --reset-offsets \
  --to-datetime 2026-10-01T00:00:00.000 \
  --execute
```

---

## Day 3: Replication, High Availability & Fault Tolerance

### 1. Architectural Concepts: ISR & Failure Modes

```
Partition 0 (Replication Factor = 3, min.insync.replicas = 2)
┌──────────────────────────┐    Replicates to    ┌──────────────────────────┐
│  Broker 1 (LEADER)       ├────────────────────►│  Broker 2 (Follower/ISR) │
│  Handles Reads & Writes  │                     └──────────────────────────┘
└────────────┬─────────────┘    Replicates to    ┌──────────────────────────┐
             └──────────────────────────────────►│  Broker 3 (Follower/ISR) │
                                                 └──────────────────────────┘
```

- **Leader**: The single broker responsible for handling all producer writes and consumer reads for that partition.
- **Followers**: Passive replicas that fetch messages from the leader to stay synchronized.
- **ISR (In-Sync Replicas)**: The subset of replicas that are actively caught up with the leader's High Watermark.
- **`acks` Setting (Producer)**:
  - `acks=0`: Producer does not wait for any acknowledgement. Fast, but high risk of data loss.
  - `acks=1`: Producer waits for Leader acknowledgment only.
  - `acks=all` (or `acks=-1`): Producer waits until **all in-sync replicas** have written the message to their log. Combined with `min.insync.replicas=2`, this guarantees zero data loss if a broker crashes.

---

### 2. Replication & Failover Exercise

#### Step A: Create a Highly Available Topic
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092 \
  --create \
  --topic resilient-topic \
  --partitions 3 \
  --replication-factor 3 \
  --config min.insync.replicas=2
```

Inspect topic metadata:
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092 \
  --describe \
  --topic resilient-topic
```
Identify which broker is the leader for Partition 0 (e.g., Broker `1`).

#### Step B: Simulate Broker Crash
Kill the leader broker container (e.g., `kafka-1`):
```bash
docker stop kafka-1
```

#### Step C: Observe Automatic Leader Election
Run describe from one of the remaining live brokers (`kafka-2` or `kafka-3`):
```bash
docker exec -it kafka-2 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-2:29092 \
  --describe \
  --topic resilient-topic
```

Expected output:
```text
Topic: resilient-topic	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2
	Topic: resilient-topic	Partition: 0	Leader: 2	Replicas: 1,2,3	Isr: 2,3
	Topic: resilient-topic	Partition: 1	Leader: 2	Replicas: 2,3,1	Isr: 2,3
	Topic: resilient-topic	Partition: 2	Leader: 3	Replicas: 3,1,2	Isr: 3,2
```
Key observations:
- Broker `1` was dropped from the **Isr** (`Isr: 2,3`).
- A new leader (e.g. Broker `2`) was elected instantly.

#### Step D: Produce Data While Node is Down
Send messages to the cluster while Broker 1 is down:
```bash
docker exec -it kafka-2 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-2:29092 \
  --topic resilient-topic \
  --producer-property acks=all
```
Type messages (`m1`, `m2`, `m3`) and verify they are successfully committed because 2 replicas (`kafka-2` and `kafka-3`) satisfy `min.insync.replicas=2`.

#### Step E: Heal Node & Observe Re-synchronization
Restart `kafka-1`:
```bash
docker start kafka-1
```
Wait 5-10 seconds and re-check topic metadata:
```bash
docker exec -it kafka-2 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-2:29092 \
  --describe \
  --topic resilient-topic
```
Notice `kafka-1` has caught up on all missed records and rejoined the **Isr** (`Isr: 2,3,1`).

---

## Day 4: Log Retention, Segments & Log Compaction

### 1. Architectural Concepts: Segments & Retention

```
Partition Log Directory: /var/lib/kafka/data/my-topic-0/
┌──────────────────────────────────────┐  ┌──────────────────────────────────────┐
│ Segment 1 (Closed / Inactive)        │  │ Segment 2 (Active Segment)           │
│ 00000000000000000000.log (.index)    │  │ 00000000000000001000.log (.index)    │
│ Eligible for deletion / compaction   │  │ Writes currently appended here       │
└──────────────────────────────────────┘  └──────────────────────────────────────┘
```

- **Segments**: Partitions are stored on disk as multiple segment files (e.g., `.log`, `.index`, `.timeindex`).
- **Active Segment**: The current file receiving writes. Active segments are **never deleted or compacted**, regardless of retention settings.
- **Log Cleanup Policies**:
  - `cleanup.policy=delete` (Default): Segments older than `retention.ms` or logs larger than `retention.bytes` are deleted.
  - `cleanup.policy=compact`: Kafka retains the **latest value for each key**. Older records with matching keys in closed segments are purged during background compaction (ideal for KV state stores, caching, changelogs).

---

### 2. Time-Based Retention Experiment

Let's configure a topic with short retention (`10 seconds`) and a tiny segment size (`1 KB`) to force segment roll and log deletion.

#### Step A: Create Topic with Aggressive Retention
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092 \
  --create \
  --topic short-retention \
  --partitions 1 \
  --replication-factor 3 \
  --config cleanup.policy=delete \
  --config retention.ms=10000 \
  --config segment.bytes=1024 \
  --config segment.ms=10000
```

#### Step B: Produce Large Payload to Force Segment Rolls
Produce multiple lines of text:
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-1:29092 \
  --topic short-retention
```
Paste 50-100 long lines of text to exceed 1 KB.

#### Step C: Observe Expiration
Wait 15 seconds. Then consume from the beginning:
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:29092 \
  --topic short-retention \
  --from-beginning \
  --property print.offset=true
```
Notice that earliest offsets (e.g., 0 through 40) have been expired and cleaned up; the consumer starts from a higher offset.

---

### 3. Log Compaction (Changelog / KV State)

Let's test `cleanup.policy=compact`.

```
Before Compaction:
Key: K1, Val: V1 (offset 0)
Key: K2, Val: V1 (offset 1)
Key: K1, Val: V2 (offset 2)
Key: K3, Val: V1 (offset 3)
Key: K2, Val: V2 (offset 4)

After Compaction (Old segment cleaned):
Key: K1, Val: V2 (offset 2)
Key: K3, Val: V1 (offset 3)
Key: K2, Val: V2 (offset 4)
```

#### Step A: Create Compacted Topic
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092 \
  --create \
  --topic user-profiles-compact \
  --partitions 1 \
  --replication-factor 3 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.01 \
  --config segment.ms=5000 \
  --config segment.bytes=1024
```

#### Step B: Produce Keyed Updates & Deletions (Tombstones)
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-1:29092 \
  --topic user-profiles-compact \
  --property parse.key=true \
  --property key.separator=":"
```

Send sequential state updates for keys:
```text
user_100:{"name":"Alice","city":"New York"}
user_200:{"name":"Bob","city":"London"}
user_100:{"name":"Alice","city":"San Francisco"}
user_300:{"name":"Charlie","city":"Paris"}
user_200:{"name":"Bob","city":"Berlin"}
```

To delete a key in a compacted topic, produce a **tombstone** (message with key and `null` value):
```text
user_300:
```
*(With `--property parse.key=true`, an empty value produces a tombstone)*

#### Step C: Dynamic Topic Configuration via CLI
Inspect and alter dynamic configs at runtime using `kafka-configs.sh`:
```bash
# Describe dynamic configs
docker exec -it kafka-1 /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka-1:29092 \
  --entity-type topics \
  --entity-name user-profiles-compact \
  --describe

# Alter configuration dynamically
docker exec -it kafka-1 /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka-1:29092 \
  --entity-type topics \
  --entity-name user-profiles-compact \
  --alter \
  --add-config min.cleanable.dirty.ratio=0.001
```

---

## 5. Phase 1 Mastery & Exit Criteria Checklist

Before moving to Go client development (Phase 2), verify you can comfortably explain and execute:

- [ ] **Partition Mechanics**: Explain why partition count limits maximum consumer concurrency.
- [ ] **Key Hashing**: Demonstrate how `murmur2(key)` ensures per-key ordering and predictable partition routing.
- [ ] **Consumer Groups & Rebalances**: Explain group coordinators, heartbeats, and rebalance behavior when consumers join or exit.
- [ ] **Offset Management**: Inspect lag with `kafka-consumer-groups.sh --describe` and rewind/reset offsets to reprocess streams.
- [ ] **High Availability & Fault Tolerance**: Explain `replication-factor`, Partition Leader vs Follower roles, and `ISR`.
- [ ] **Durability Guarantees**: Explain `min.insync.replicas` and producer `acks=all` under broker failure.
- [ ] **Log Retention & Segments**: Contrast `cleanup.policy=delete` vs `cleanup.policy=compact` and explain why active segments are never purged immediately.
