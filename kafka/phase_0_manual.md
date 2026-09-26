# Phase 0 Manual: Modern 3-Broker KRaft Kafka Cluster Setup

This guide walks you through the concepts, configuration, and hands-on operational commands for running a distributed **3-broker Apache Kafka cluster in KRaft mode** (without ZooKeeper) using Docker Compose.

---

## 1. Core Architecture & Concepts

### ZooKeeper vs. KRaft (Kafka Raft)
In older Kafka architectures, cluster metadata, topic configurations, and partition leader elections were coordinated through an external **ZooKeeper** cluster.

```
[Old ZooKeeper Architecture]
Producers / Consumers ──► Kafka Brokers ◄──► ZooKeeper Ensemble (Separate System)
```

**KRaft (KIP-500)** replaces ZooKeeper entirely by embedding a Raft-based consensus protocol directly into Kafka itself:

```
[Modern KRaft Architecture]
Producers / Consumers ──► Kafka Cluster (Brokers + KRaft Controller Quorum)
                               ▲
                      Internal @metadata log (Raft Protocol)
```

### Key Advantages of KRaft
1. **Unified Management**: Only one system to configure, scale, monitor, and secure.
2. **Instant Failover / Recovery**: Metadata changes are written as events to an internal metadata topic (`@metadata`), meaning new controllers can hydrate state instantly without scanning ZooKeeper trees.
3. **Partition Scalability**: Supports millions of partitions per cluster (ZooKeeper typically capped around 200,000).

---

## 2. Docker Compose Deep Dive

Here is the breakdown of the environment variables configured in [`docker-compose.yml`](file:///home/khizerkhan/backend_sd/docker-compose.yml):

| Configuration Variable | Value / Purpose | Explanation |
| :--- | :--- | :--- |
| `KAFKA_NODE_ID` | `1`, `2`, `3` | Unique numeric identifier for each node in the cluster. |
| `KAFKA_PROCESS_ROLES` | `'broker,controller'` | Combined role. The node serves client traffic (`broker`) and participates in consensus voting (`controller`). |
| `KAFKA_CONTROLLER_QUORUM_VOTERS` | `'1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093'` | Informs each controller of all voting members in the Raft quorum formatted as `<node_id>@<hostname>:<controller_port>`. |
| `KAFKA_LISTENERS` | `PLAINTEXT://:29092,CONTROLLER://:9093,PLAINTEXT_HOST://:9092` | Network interfaces and ports Kafka binds to locally inside the container. |
| `KAFKA_ADVERTISED_LISTENERS` | `PLAINTEXT://kafka-1:29092,PLAINTEXT_HOST://localhost:9092` | The hostnames/IPs that Kafka returns to clients when they discover where to connect. |
| `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` | `CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT` | Defines security protocols (plaintext/unauthenticated in this local dev environment) for each listener name. |
| `KAFKA_INTER_BROKER_LISTENER_NAME` | `'PLAINTEXT'` | Which listener brokers use to replicate partition data between each other. |
| `KAFKA_CONTROLLER_LISTENER_NAMES` | `'CONTROLLER'` | Which listener is dedicated strictly for KRaft voting consensus. |
| `CLUSTER_ID` | `'4L622nShTUiBenYhPXlu6Q'` | Base64-encoded 16-byte UUID representing the cluster ID used to format storage directories. |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | `3` | Ensures the internal `__consumer_offsets` topic is replicated across all 3 brokers for fault tolerance. |

---

## 3. Operations & Lifecycle Commands

### Starting the Cluster
Run from the directory containing `docker-compose.yml`:
```bash
docker compose up -d
```

### Checking Container Health and Status
```bash
docker compose ps
```

Expected output:
```text
NAME       IMAGE                           STATUS         PORTS
kafka-1    apache/kafka:3.8.0              Up             0.0.0.0:9092->9092/tcp
kafka-2    apache/kafka:3.8.0              Up             0.0.0.0:9093->9092/tcp
kafka-3    apache/kafka:3.8.0              Up             0.0.0.0:9094->9092/tcp
kafka-ui   provectuslabs/kafka-ui:latest   Up             0.0.0.0:8080->8080/tcp
```

### Viewing Real-Time Logs
```bash
# View all logs
docker compose logs -f

# View logs for a single broker
docker compose logs -f kafka-1

# View logs for Kafka UI
docker compose logs -f kafka-ui
```

### Stopping and Tearing Down
```bash
# Stop containers (preserves volume data if any)
docker compose stop

# Stop and remove containers and networks
docker compose down

# Stop, remove containers, and purge all storage volumes
docker compose down -v
```

---

## 4. Hands-On CLI Commands (Interactive Play)

You can execute Kafka CLI tools directly inside the `kafka-1` container using `docker exec`.

### 1. Check KRaft Metadata Quorum & Elected Leader
Inspect which node is currently elected as the KRaft Leader Controller and see the replication lag of followers:
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 \
  describe --status
```

Expected output highlights:
```text
ClusterId:              4L622nShTUiBenYhPXlu6Q
LeaderId:               1 (or 2, or 3)
LeaderEpoch:            1
HighWatermark:          ...
CurrentVoters:          [1, 2, 3]
```

### 2. Inspect Cluster Broker Metadata
List all live brokers known to the cluster:
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-cluster.sh \
  --bootstrap-server localhost:9092 \
  cluster-id
```

### 3. Create a Test Multi-Partition Replicated Topic
Create a topic `phase0-test` with 3 partitions and a replication factor of 3:
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 \
  --create \
  --topic phase0-test \
  --partitions 3 \
  --replication-factor 3
```
or 
```
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092 \
  --create --topic phase0-test-1 --partitions 3 --replication-factor 3
```

### 3a. list all the topics
```
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092 \
  --list
```

### 4. Describe Topic Partitions & In-Sync Replicas (ISR)
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic phase0-test
```
You will see which broker is the Leader for each partition, along with its Replicas and ISR list:
```text
Topic: phase0-test	TopicId: ...	PartitionCount: 3	ReplicationFactor: 3	Configs: 
	Topic: phase0-test	Partition: 0	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1
	Topic: phase0-test	Partition: 1	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2
	Topic: phase0-test	Partition: 2	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3
```

### 5. Produce and Consume a Test Message
Send a message using the Console Producer:
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic phase0-test
```
*(Type messages and press Enter. Press Ctrl+C to exit)*

Read messages from the beginning:
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic phase0-test \
  --from-beginning
```

---

## 5. Web UI Visual Inspection (Kafka UI)

Open your browser to:
👉 **`http://localhost:8080`**

### What to check in the UI:
1. **Brokers Page**: Confirm all **3 Brokers** (IDs: `1`, `2`, `3`) are listed as **Online**.
2. **Topics Page**: See the `phase0-test` topic, partition distribution across brokers, and replication factor.
3. **Consumers Page**: Monitor active consumer groups and lag.
4. **Cluster Statistics**: View total brokers, topics, partitions, and controller details.

---

## 6. Self-Study Exercises for Phase 0

1. **Simulate a Controller Failure**:
   - Run `kafka-metadata-quorum.sh describe --status` to identify which broker is currently the Leader.
   - Stop that container: `docker stop kafka-<ID>`.
   - Re-run the quorum command on one of the surviving brokers (e.g. `docker exec -it kafka-2 ...`).
   - Observe how KRaft immediately elects a new leader among the remaining voters.
   - Restart the stopped broker: `docker start kafka-<ID>` and verify it rejoins the quorum.

2. **Connect from Host Machine**:
   - Try connecting a tool on your host machine (or future Go script) to `localhost:9092`, `localhost:9093`, or `localhost:9094`.
