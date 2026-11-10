# Phase 0 Manual: Distributed Caching Environment & Topologies Setup

This guide walks you through the concepts, configuration, and hands-on operational commands for running a distributed caching stack: **Redis Master-Replica with a 3-Node Sentinel Quorum**, **Memcached (Multi-Threaded Slab Engine)**, **RedisInsight GUI**, and **Prometheus Redis Exporter** using Docker Compose.

---

## 1. Core Architecture & Mental Models

### The Hardware Memory Hierarchy
Caching exists because physical hardware memory tiers have vastly different latency and throughput characteristics:

```
┌───────────────────────────────────────────────┬────────────────────┬──────────────────────┐
│ Memory Tier                                   │ Typical Latency    │ Bandwidth            │
├───────────────────────────────────────────────┼────────────────────┼──────────────────────┤
│ CPU Registers / L1/L2/L3 Cache                │ 0.5 ns – 10 ns     │ ~1 TB/sec            │
│ Main Memory (DRAM - Redis / Memcached)        │ 50 ns – 100 ns     │ 50 GB – 100 GB/sec   │
│ Fast NVMe SSD (Flash Storage / RocksDB)       │ 10 µs – 100 µs     │ 3 GB – 7 GB/sec      │
│ Relational DB on Disk (PostgreSQL / MySQL)    │ 1 ms – 20 ms       │ 100 MB – 500 MB/sec  │
│ Network Roundtrip across Data Centers         │ 20 ms – 100 ms     │ Network-bounded      │
└───────────────────────────────────────────────┴────────────────────┴──────────────────────┘
```

> **Takeaway:** Accessing DRAM via Redis or Memcached is **10,000x to 100,000x faster** than fetching records from disk-bound databases. However, DRAM is volatile and limited by physical server capacity. Designing caching systems requires strict discipline around memory allocation, replication, and eviction.

---

### Caching Topologies Compared

```
[Topology 1: Standalone Cache]
Client ──► Single Redis Instance (Single point of failure; memory bounded by 1 machine)

[Topology 2: Master-Replica with Sentinel (Our Phase 0 Setup)]
Client ──► Sentinel Quorum (Discovers active Master)
                 │
                 ├──► Redis Master (Handles all Reads & Writes)
                 │         │
                 │         ├── PSYNC Replication ──► Redis Replica 1 (Read Replica)
                 │         └── PSYNC Replication ──► Redis Replica 2 (Read Replica)
                 │
           3 x Sentinels (Heartbeats, Quorum Voting, Automated Failover)

[Topology 3: Redis Cluster (Phase 3 Deep Dive)]
Client ──► Smart Client ──► 16,384 Hash Slots distributed across N Sharded Masters

[Topology 4: Memcached (Shared-Nothing)]
Client ──(Consistent Hash Ring)──► [Node A] [Node B] [Node C] (Independent, multi-threaded)
```

| Topology | Best Used For | Scalability Axis | High Availability Mechanism |
| :--- | :--- | :--- | :--- |
| **Standalone** | Development, non-critical caches, small session stores | None | None |
| **Master-Replica + Sentinel** | Production workloads up to ~50–100GB RAM requiring automated failover | Read scale (add replicas); Vertical RAM scale | **Redis Sentinel** quorum monitors and promotes replicas |
| **Redis Cluster** | High-throughput systems exceeding single-server RAM limits (>100GB) | Horizontal write & read scale (auto-sharded across 16,384 slots) | Internal master-replica gossip protocol |
| **Memcached** | Simple key-value string/blob caching, multi-core CPU saturation | Client-side consistent hashing across nodes | Node loss tolerated via cache miss backfill |

---

## 2. Docker Compose Infrastructure Deep Dive

Our Phase 0 environment spins up 7 synchronized services inside the `cache-net` bridge network:

```
                       ┌────────────────────────────────────────────────────────┐
                       │                     HOST MACHINE                       │
                       └───────────┬────────────────────────────────┬───────────┘
                                   │                                │
            Ports: 6379, 6380, 6381│            Ports: 26379, 26380, 26381  │ Port: 11211
                                   ▼                                ▼               ▼
┌──────────────────────────────────┴────────────────────────────────┴───────────────┴────────┐
│ Docker Network (`cache-net`)                                                               │
│                                                                                            │
│   ┌────────────────┐       Replication       ┌─────────────────┐    ┌─────────────────┐    │
│   │  redis-master  ├────────────────────────►│ redis-replica-1 │    │ redis-replica-2 │    │
│   │  (Port: 6379)  ├─────────────────────────┼─────────────────┼───►│  (Port: 6379)   │    │
│   └───────▲────────┘                         └─────────────────┘    └─────────────────┘    │
│           │                                                                                │
│           │ Heartbeats & Monitoring                                                        │
│   ┌───────┴───────────────┬───────────────────────────────┬────────────────────────────┐   │
│   │   redis-sentinel-1    │       redis-sentinel-2        │      redis-sentinel-3      │   │
│   │    (Port: 26379)      │        (Port: 26379)          │       (Port: 26379)        │   │
│   └───────────────────────┴───────────────────────────────┴────────────────────────────┘   │
│                                                                                            │
│   ┌────────────────┐      ┌─────────────────────────┐      ┌───────────────────────────┐   │
│   │   memcached    │      │   redisinsight (UI)     │      │   redis-exporter          │   │
│   │  (Port: 11211) │      │     (Port: 5540)        │      │    (Port: 9121)           │   │
│   └────────────────┘      └─────────────────────────┘      └───────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Port Mapping Reference

| Service | Internal Port | Host Port | Purpose |
| :--- | :--- | :--- | :--- |
| `redis-master` | `6379` | `6379` | Primary read/write Redis instance |
| `redis-replica-1`| `6379` | `6380` | Read-only replica 1 |
| `redis-replica-2`| `6379` | `6381` | Read-only replica 2 |
| `redis-sentinel-1`| `26379`| `26379`| Sentinel node 1 (Quorum voter) |
| `redis-sentinel-2`| `26379`| `26380`| Sentinel node 2 (Quorum voter) |
| `redis-sentinel-3`| `26379`| `26381`| Sentinel node 3 (Quorum voter) |
| `memcached` | `11211`| `11211`| Multi-threaded Memcached slab engine |
| `redisinsight` | `5540` | `5540` | Web UI Dashboard |
| `redis-exporter`| `9121` | `9121` | Prometheus metrics exporter |

---

### Key Configuration Flags Explained

#### 1. Redis Server Parameters
- `--maxmemory 256mb`: Caps total memory Redis will allocate. Prevents the OS Out-Of-Memory (OOM) killer from terminating the container.
- `--maxmemory-policy allkeys-lru`: When memory reaches 256MB, Redis evicts the least recently used keys across the entire keyspace.
- `--appendonly yes`: Enables the Append-Only File (AOF), logging every write command for strong persistence.
- `--save 900 1 --save 300 10 --save 60 10000`: Configures Redis Database (RDB) point-in-time snapshot intervals.
- `--replicaof redis-master 6379`: Directs replicas to connect to `redis-master` on startup and initiate synchronization.

#### 2. Redis Sentinel Parameters
- `sentinel monitor mymaster redis-master 6379 2`:
  - Monitors the master named `mymaster` at IP/hostname `redis-master:6379`.
  - The final number (`2`) is the **Quorum**: the minimum number of sentinels that must agree the master is down before triggering an automatic failover.
- `sentinel down-after-milliseconds mymaster 3000`: A master is considered down (`+sdown`) if it fails to answer PINGs for 3 seconds.
- `sentinel failover-timeout mymaster 6000`: Timeout for the failover procedure before another sentinel can re-attempt.
- `sentinel parallel-syncs mymaster 1`: Number of replicas that can re-sync simultaneously with the new master after failover (setting to 1 prevents all replicas from being unreadable at the same time).

---

## 3. Operations & Lifecycle Commands

### Starting the Cluster
Run from the root directory or the `cache/` directory:
```bash
docker compose -f cache/docker-compose.yml up -d
```

### Checking Container Health and Status
```bash
docker compose -f cache/docker-compose.yml ps
```

Expected output:
```text
NAME               IMAGE                             STATUS         PORTS
memcached          memcached:1.6-alpine              Up             0.0.0.0:11211->11211/tcp
redis-exporter     oliver006/redis_exporter:latest   Up             0.0.0.0:9121->9121/tcp
redis-master       redis:7.2-alpine                  Up             0.0.0.0:6379->6379/tcp
redis-replica-1    redis:7.2-alpine                  Up             0.0.0.0:6380->6379/tcp
redis-replica-2    redis:7.2-alpine                  Up             0.0.0.0:6381->6379/tcp
redis-sentinel-1   redis:7.2-alpine                  Up             0.0.0.0:26379->26379/tcp
redis-sentinel-2   redis:7.2-alpine                  Up             0.0.0.0:26380->26379/tcp
redis-sentinel-3   redis:7.2-alpine                  Up             0.0.0.0:26381->26379/tcp
redisinsight       redis/redisinsight:latest         Up             0.0.0.0:5540->5540/tcp
```

### Viewing Real-Time Logs
```bash
# Follow logs for the master
docker compose -f cache/docker-compose.yml logs -f redis-master

# Follow logs across sentinels to watch quorum activity
docker compose -f cache/docker-compose.yml logs -f redis-sentinel-1 redis-sentinel-2
```

### Stopping and Tearing Down
```bash
# Stop containers (preserves volume data)
docker compose -f cache/docker-compose.yml stop

# Stop, remove containers, networks, and purge volumes
docker compose -f cache/docker-compose.yml down -v
```

---

## 4. Hands-On CLI Verification Labs

### Lab 1: Verify Redis Replication Hierarchy
Connect to the master and inspect replication status:
```bash
docker exec -it redis-master redis-cli INFO replication
```

Expected output:
```text
# Replication
role:master
connected_slaves:2
slave0:ip=172.x.x.x,port=6379,state=online,offset=...,lag=0
slave1:ip=172.x.x.x,port=6379,state=online,offset=...,lag=0
master_replid:...
master_repl_offset:...
```

Write data to the master and verify instant replication on replica 1:
```bash
# Write to Master
docker exec -it redis-master redis-cli SET cluster_test "hello_distributed_world"

# Read from Replica 1 (Port 6380 on host, or via container)
docker exec -it redis-replica-1 redis-cli GET cluster_test

# Attempt writing to Replica 1 (Should be rejected)
docker exec -it redis-replica-1 redis-cli SET test "fail"
# (error) READONLY You can't write against a read only replica.
```

---

### Lab 2: Inspect Sentinel Quorum State
Connect to `redis-sentinel-1` on port `26379`:
```bash
# 1. Query the master known to Sentinel
docker exec -it redis-sentinel-1 redis-cli -p 26379 SENTINEL master mymaster

# 2. Query all registered replicas
docker exec -it redis-sentinel-1 redis-cli -p 26379 SENTINEL replicas mymaster

# 3. Query peer sentinels participating in the quorum
docker exec -it redis-sentinel-1 redis-cli -p 26379 SENTINEL sentinels mymaster
```

---

### Lab 3: Test Memcached Multi-Threaded Engine
Memcached communicates over a simple text protocol on port `11211`. Connect via `nc` (netcat):

```bash
# Store a key: set <key> <flags> <exptime_seconds> <bytes>\r\n<data>\r\n
printf "set user:101 0 900 13\r\nKhizer's Data\r\n" | nc localhost 11211
# Expected response: STORED

# Retrieve key:
printf "get user:101\r\n" | nc localhost 11211
# Expected response:
# VALUE user:101 0 13
# Khizer's Data
# END

# Inspect Memcached slab memory statistics:
printf "stats slabs\r\n" | nc localhost 11211
```

---

## 5. Web UI Visual Inspection (RedisInsight)

Open your browser to:
👉 **[http://localhost:5540](http://localhost:5540)**

### Setup Steps:
1. Accept the EULA and telemetry settings.
2. Click **"Add Redis Database"**.
3. Choose **"Add Database Manually"**:
   - **Host:** `redis-master` (if connecting within Docker network) or `localhost` (port `6379`).
   - **Port:** `6379`.
   - **Database Alias:** `Local-Master`.
4. Click **"Add Redis Database"**.

### Areas to Explore in RedisInsight:
- **Workbench:** Run commands (`INFO`, `MEMORY USAGE`, `MONITOR`).
- **Browser:** Visualize data types (Strings, Hashes, Sets, Sorted Sets, Streams).
- **Profiler:** Trace slow queries and inspect command throughput in real time.

---

## 6. Chaos Testing: Simulating Master Crash & Automated Failover

This hands-on exercise demonstrates how Sentinel guarantees **High Availability** by automatically promoting a replica to master when the active master crashes.

```
[Normal State]
redis-master (Active) ◄─── Heartbeats ───► Sentinel 1, 2, 3 (Quorum: 2)
     ▲
     │ Replication
redis-replica-1, redis-replica-2

[Crash Simulated: docker stop redis-master]
Sentinels detect missing PINGs for 3s -> Subjective Down (+sdown) -> Quorum reached -> Objective Down (+odown)
Sentinels elect a Leader Sentinel via Raft-like voting
Leader Sentinel executes FAILOVER: Promotes redis-replica-1 to Master (+switch-master)
Reconfigures redis-replica-2 to replicate from the new Master
```

### Step 1: Open Sentinel Logs in a Separate Terminal
```bash
docker compose -f cache/docker-compose.yml logs -f redis-sentinel-1
```

### Step 2: Kill the Master Container
```bash
docker stop redis-master
```

### Step 3: Watch the Live Failover Sequence in Sentinel Logs
Within 3 to 6 seconds, observe the following state machine transitions:
```text
+sdown master mymaster 172.x.x.x 6379
+odown master mymaster 172.x.x.x 6379 #quorum 2/2
+new-epoch 1
+try-failover master mymaster 172.x.x.x 6379
+vote-for-leader ...
+promoted-slave slave 172.x.x.x:6379
+failover-state-send-slaveof-noone
+switch-master mymaster 172.x.x.x 6379 -> new_master_ip 6379
```

### Step 4: Verify the New Master
Ask Sentinel who the new master is:
```bash
docker exec -it redis-sentinel-1 redis-cli -p 26379 SENTINEL get-master-addr-by-name mymaster
```
Notice it now returns the IP of `redis-replica-1` or `redis-replica-2`!

Connect to the promoted replica and verify it now accepts writes:
```bash
docker exec -it redis-replica-1 redis-cli INFO replication
# role: master!

docker exec -it redis-replica-1 redis-cli SET failover_verified "SUCCESS"
# OK!
```

### Step 5: Heal the Old Master
Restart the stopped container:
```bash
docker start redis-master
```

Wait 5 seconds and inspect `redis-master` replication info:
```bash
docker exec -it redis-master redis-cli INFO replication
```
**Observation:** Sentinel automatically sent a `SLAVEOF` command to the revived node. It has rejoined the cluster safely as a **read-only replica**, preventing split-brain data loss!

---

## 7. Phase 0 Readiness Checklist

Before proceeding to Phase 1, ensure you can answer and verify the following:
- [x] What is the purpose of Sentinel Quorum, and why are an odd number of Sentinels (e.g., 3 or 5) required to prevent split-brain?
- [x] How does `PSYNC` replication work between master and replica?
- [x] What happens when a client sends a write command (`SET`) to a read-only replica?
- [x] Why is Memcached's multi-threaded model fundamentally different from Redis's event loop?
- [x] All 9 containers are healthy and running cleanly under `docker compose ps`.

---

### Answers

#### Q1. Sentinel Quorum & why an odd number of Sentinels

Two thresholds are involved and they are often confused:

| Concept | Value in our setup | What it controls |
|---------|-------------------|------------------|
| **Quorum** (`sentinel monitor ... 2`) | `2` | How many Sentinels must agree the master is unreachable to flip it from **SDOWN** (Subjectively Down — one Sentinel's opinion) to **ODOWN** (Objectively Down — agreed fact). |
| **Majority** (implicit, `N/2 + 1`) | `2` of `3` | How many Sentinels must **vote for the same leader** before that leader is *authorized* to perform the failover. |

**Failover flow:**
1. A Sentinel misses PONGs for `down-after-milliseconds` (3s) → marks master `+sdown`.
2. It asks peers via `SENTINEL is-master-down-by-addr`. Once ≥ quorum agree → `+odown`.
3. Sentinels increment the **epoch** (a Raft-like term) and request votes. Each Sentinel votes **once per epoch** (first-come-first-served).
4. The candidate that receives a **majority** becomes leader, picks the best replica (lowest `replica-priority` → largest replication offset → lowest run ID), sends it `REPLICAOF NO ONE`, and repoints the other replicas.

**Why odd numbers?** Majority is what prevents split-brain, and odd counts maximize fault tolerance per node:

| Sentinels | Majority needed | Failures tolerated |
|-----------|-----------------|--------------------|
| 2 | 2 | **0** — losing one Sentinel makes failover impossible |
| 3 | 2 | 1 |
| 4 | 3 | 1 (no gain over 3, one more machine) |
| 5 | 3 | 2 |

During a network partition, only **one** side can hold a majority. So only one side can elect a leader and promote a new master. With an even count (e.g., 4 split 2/2) **neither** side gets a majority, so availability is lost for nothing.

> [!WARNING]
> Sentinel majority prevents **two failovers**. It does **not** stop the *old* master from accepting writes while it's isolated with some clients — those writes are lost when it rejoins as a replica. Mitigate with `min-replicas-to-write 1` + `min-replicas-max-lag 10` on the master so it refuses writes when it can't see its replicas.

Also: place Sentinels on **independent failure domains** (different hosts/AZs). 3 Sentinels on one VM give zero real fault tolerance.

---

#### Q2. How `PSYNC` replication works

Every master maintains:
- **Replication ID** — a random 40-char ID identifying a dataset history. A promoted replica gets a new ID (and remembers the old one as `master_replid2`, which lets the surviving replicas partially resync after failover).
- **Replication offset** — a byte counter incremented for every byte of write stream sent.
- **Replication backlog** — a fixed-size circular buffer (`repl-backlog-size`, default 1MB) holding the most recent write stream.

When a replica connects it sends `PSYNC <replid> <offset>`:

```text
Replica                                 Master
  │── PSYNC <replid> <offset> ─────────────►│
  │                                         │ replid matches AND offset still in backlog?
  │◄──────────── +CONTINUE ─────────────────│   YES → Partial resync: stream only missing bytes
  │                                         │
  │◄──────── +FULLRESYNC <replid> <off> ────│   NO  → Full resync:
  │◄──────── RDB snapshot (BGSAVE / fork) ──│        1. fork() child, write RDB snapshot
  │◄──────── buffered writes since fork ────│        2. buffer new writes in replica output buffer
  │◄──────── live command stream ───────────│        3. send RDB, then buffered writes, then stream live
```

- **First connect** uses `PSYNC ? -1` → always a full resync.
- **Brief disconnects** (network blip, replica restart) → partial resync if the gap still fits in the backlog. Size the backlog as `write_bytes_per_sec × max_expected_disconnect_secs`.
- Replication is **asynchronous**: the master acks the client *before* replicas receive the write. A crash in that window loses acknowledged writes. `WAIT numreplicas timeout` lets a client block until N replicas confirm, but this is still not strong consistency.
- Replicas send `REPLCONF ACK <offset>` every second. That's how the master computes `lag` in `INFO replication`.

---

#### Q3. Write command (`SET`) sent to a read-only replica

The replica rejects it immediately:

```text
(error) READONLY You can't write against a read only replica.
```

- Controlled by `replica-read-only yes` (the default). The data is **not** written locally and **not** forwarded to the master. Redis has no write proxying.
- The client is responsible for routing writes to the master. Sentinel-aware clients (e.g., go-redis `NewFailoverClient`) ask Sentinel via `SENTINEL get-master-addr-by-name mymaster` and reconnect after `+switch-master`.
- After a failover, a client still pinned to the old (now demoted) master hits `READONLY`. Good clients treat it as a signal to rediscover the master.
- If you set `replica-read-only no`, local writes succeed but **diverge** silently. They're never replicated and get wiped at the next full resync. Avoid it.

*Verified in Lab 1.*

---

#### Q4. Memcached multi-threading vs the Redis event loop

| Aspect | Memcached | Redis |
|--------|-----------|-------|
| **Execution model** | Multi-threaded: one listener thread accepts connections and hands them round-robin to N worker threads (`-t 4`), each running its own libevent loop. | Single-threaded event loop (epoll/kqueue) executes **all commands** serially. Redis 6+ `io-threads` parallelizes only socket read/write + protocol parsing, never command execution. |
| **Concurrency control** | Fine-grained locks: striped item locks on hash buckets, per-slab-class locks, LRU locks. | None needed. Every command is atomic because nothing else runs at the same time. |
| **Scaling** | Vertical: one process uses all cores. | Horizontal: one core per instance, so you scale by running more instances (Redis Cluster / sharding). |
| **Data model** | Opaque byte blobs (`get`/`set`/`incr`/`cas`). Operations are trivially O(1). | Rich structures (lists, hashes, sorted sets, streams), Lua scripts, transactions, which all rely on serial execution for atomicity. |
| **Failure mode** | Lock contention on hot keys; no single slow command blocks the whole server. | One slow command (`KEYS *`, `DEL` on a 1M-element set, heavy Lua) blocks **every** client. |
| **Memory** | Slab allocator: fixed-size chunk classes. No external fragmentation, but internal waste and possible slab calcification. | `jemalloc` with per-object allocation; monitor `mem_fragmentation_ratio`. |
| **Persistence / HA** | None. Pure cache, no replication. | RDB/AOF, replication, Sentinel, Cluster. |

**The fundamental difference:** Memcached spends cores to get throughput and pays for it with locking, which it can afford because its operations are simple. Redis gives up multi-core execution so that every command, script, and transaction is atomic with no locking. That keeps complex data structures fast and race-free, at the cost of needing sharding to use more cores.

---

#### Q5. Container health

There are **9** containers in the stack, not 7: master, 2 replicas, 3 sentinels, memcached, redisinsight, redis-exporter.

```bash
docker compose -f cache/docker-compose.yml ps
docker exec redis-sentinel-1 redis-cli -p 26379 SENTINEL ckquorum mymaster
# OK 3 usable Sentinels. Quorum and failover authorization can be reached
```

> [!NOTE]
> **Sentinel + hostnames gotcha:** Since Redis 6.2, Sentinel rejects hostnames like `redis-master` in `sentinel monitor` (*"Can't resolve instance hostname"*) unless `sentinel resolve-hostnames yes` appears **before** the monitor line. `wait-for-redis.sh` generates the config with this flag (plus `announce-hostnames yes`).

> [!TIP]
> After running the Section 6 chaos test, the master role moves to a replica. Always find the current master with `SENTINEL get-master-addr-by-name mymaster` rather than assuming `redis-master` is the master.
