# Phase 0 Manual: Modern 3-Broker KRaft Kafka Cluster Setup

This guide walks you through the concepts, configuration, and hands-on operational commands for running a distributed **3-broker Apache Kafka cluster in KRaft mode** (without ZooKeeper) using Docker Compose.

---

## 1. Core Architecture & Concepts

### ZooKeeper vs. KRaft (Kafka Raft)
In older Kafka architectures, cluster metadata, topic configurations, and partition leader elections were coordinated through an external **ZooKeeper** ensemble.

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

## 2. Listener Architecture & Docker Compose Deep Dive

Kafka uses **listeners** to separate traffic types. In our 3-node cluster, each container exposes 3 dedicated listeners:

```
                      ┌────────────────────────────────────────────────────────┐
                      │                        kafka-1                         │
                      │                                                        │
Host (Go / CLI) ────► │ :9092   (PLAINTEXT_HOST) -> Advertised: localhost:9092  │
Docker Net (UI/Exec)► │ :29092  (PLAINTEXT)      -> Advertised: kafka-1:29092  │
KRaft Quorum ───────► │ :29093  (CONTROLLER)     -> Quorum: kafka-1:29093      │
                      └────────────────────────────────────────────────────────┘
```

### Why 3 Separate Listeners?
1. **`PLAINTEXT` (Port `29092` - Internal Docker Network)**:
   - Used for inter-broker data replication and communication within the Docker bridge network (`kafka-net`).
   - Used by **Kafka UI** and by CLI commands run via `docker exec`.
   - Advertised addresses: `kafka-1:29092`, `kafka-2:29092`, `kafka-3:29092`.
2. **`CONTROLLER` (Port `29093` - KRaft Quorum Consensus)**:
   - Dedicated exclusively to KRaft metadata replication and leader election among controllers.
   - Kept on port `29093` to prevent any collision with host-mapped broker ports (e.g., `9093`).
3. **`PLAINTEXT_HOST` (Ports `9092`, `9093`, `9094` on Host)**:
   - Used by client applications running directly on your **host machine** (outside Docker).
   - Advertised addresses: `localhost:9092` (broker 1), `localhost:9093` (broker 2), `localhost:9094` (broker 3).

### Environment Variables Breakdown

| Configuration Variable | Example Value (Broker 1) | Explanation |
| :--- | :--- | :--- |
| `KAFKA_NODE_ID` | `1` | Unique numeric identifier for each node in the cluster. |
| `KAFKA_PROCESS_ROLES` | `'broker,controller'` | Combined role: node handles client data (`broker`) and participates in consensus voting (`controller`). |
| `KAFKA_CONTROLLER_QUORUM_VOTERS` | `'1@kafka-1:29093,2@kafka-2:29093,3@kafka-3:29093'` | Informs controllers of all voting members formatted as `<id>@<host>:<controller_port>`. |
| `KAFKA_LISTENERS` | `PLAINTEXT://:29092,CONTROLLER://:29093,PLAINTEXT_HOST://:9092` | Network interfaces and ports Kafka binds to inside the container. |
| `KAFKA_ADVERTISED_LISTENERS` | `PLAINTEXT://kafka-1:29092,PLAINTEXT_HOST://localhost:9092` | Addresses returned to clients for establishing subsequent connections. |
| `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` | `CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT` | Maps listener names to transport security protocols. |
| `KAFKA_INTER_BROKER_LISTENER_NAME` | `'PLAINTEXT'` | Which listener brokers use to replicate partition data between each other. |
| `KAFKA_CONTROLLER_LISTENER_NAMES` | `'CONTROLLER'` | Which listener is dedicated strictly to KRaft quorum communication. |
| `CLUSTER_ID` | `'4L622nShTUiBenYhPXlu6Q'` | Base64-encoded 16-byte UUID representing the cluster ID. |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | `3` | Replicates the internal `__consumer_offsets` topic across all 3 brokers for fault tolerance. |

---

## 3. Operations & Lifecycle Commands

### Starting the Cluster
Run from the [`kafka/`](file:///home/khizerkhan/backend_sd/kafka) directory:
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

# View logs for a specific broker
docker compose logs -f kafka-1

# View logs for Kafka UI
docker compose logs -f kafka-ui
```

### Stopping and Tearing Down
```bash
# Stop containers (preserves state)
docker compose stop

# Stop and remove containers and networks
docker compose down

# Stop, remove containers, and purge all data
docker compose down -v
```

---

## 4. Hands-On CLI Commands

> [!IMPORTANT]
> When executing CLI commands **inside Docker (`docker exec`)**, always use the internal listener:  
> `--bootstrap-server kafka-1:29092`  
> Do not use `localhost:9092` inside `docker exec`, because `localhost` inside the container refers to the container's private network namespace rather than the host.

### 1. Check KRaft Metadata Quorum & Elected Leader
Inspect which controller is currently elected leader and verify voter synchronization:
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka-1:29092 \
  describe --status
```

Expected output:
```text
ClusterId:              4L622nShTUiBenYhPXlu6Q
LeaderId:               1
LeaderEpoch:            1
HighWatermark:          ...
MaxFollowerLag:         0
MaxFollowerLagTimeMs:   0
CurrentVoters:          [1,2,3]
CurrentObservers:       []
```

To see real-time log end offsets and replication lag across all voters:
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka-1:29092 \
  describe --replication
```

### 2. Inspect Cluster ID & Broker API Versions
```bash
# View Cluster ID
docker exec -it kafka-1 /opt/kafka/bin/kafka-cluster.sh cluster-id \
  --bootstrap-server kafka-1:29092

# View Broker API Versions and Connectivity
docker exec -it kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server kafka-1:29092
```

### 3. Create a Multi-Partition Replicated Topic
Create a topic `phase0-test` with 3 partitions and a replication factor of 3:
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092 \
  --create \
  --topic phase0-test \
  --partitions 3 \
  --replication-factor 3
```

### 4. List and Describe Topics
```bash
# List all topics
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092 \
  --list

# Describe topic partitions, leaders, and in-sync replicas (ISR)
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092 \
  --describe \
  --topic phase0-test
```

Sample output:
```text
Topic: phase0-test	TopicId: ...	PartitionCount: 3	ReplicationFactor: 3	Configs: 
	Topic: phase0-test	Partition: 0	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1
	Topic: phase0-test	Partition: 1	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2
	Topic: phase0-test	Partition: 2	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3
```

### 5. Produce and Consume Messages

**Produce messages:**
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-1:29092 \
  --topic phase0-test
```
*(Type messages and press Enter. Press `Ctrl+C` to exit)*

**Consume messages from the beginning:**
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:29092 \
  --topic phase0-test \
  --from-beginning
```

---

## 5. Connecting from the Host Machine (Outside Docker)

When connecting from code or tools running directly on your host machine (e.g., Go applications, Python scripts, `kcat`):

- **Bootstrap servers string**: `localhost:9092,localhost:9093,localhost:9094`

### Example with `kcat` / `kafkacat` (Host Machine):
```bash
# List cluster metadata from host
kcat -b localhost:9092 -L

# Produce to topic from host
kcat -b localhost:9092 -t phase0-test -P

# Consume from topic from host
kcat -b localhost:9092 -t phase0-test -C -o beginning
```

---

## 6. Web UI Visual Inspection (Kafka UI)

Open your browser to:
👉 **[http://localhost:8080](http://localhost:8080)**

### Key Areas to Inspect:
1. **Brokers Page**: Confirm all **3 Brokers** (`1`, `2`, `3`) are listed as **Online**.
2. **Topics Page**: Verify `phase0-test`, partition leaders, and replication health.
3. **Consumers Page**: Monitor active consumer groups and lag.
4. **Cluster Overview**: Verify KRaft controller role and active leader.

---

## 7. Operational & Fault Tolerance Exercises

1. **Simulate a Controller / Broker Failure**:
   - Check the current KRaft leader:
     ```bash
     docker exec -it kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:29092 describe --status
     ```
   - Stop that container (e.g. `docker stop kafka-1`).
   - Run the quorum command against another container (e.g. `docker exec -it kafka-2 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-2:29092 describe --status`).
   - Observe the new leader election.
   - Restart the broker (`docker start kafka-1`) and check `--replication` to watch it catch up.
