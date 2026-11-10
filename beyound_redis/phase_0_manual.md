# Phase 0 Manual: Redis Beyond Caching — Event Engine, Persistence & Environment Setup

This guide walks you through the architecture, runtime flags, and verification labs for operating Redis 7.2+ not merely as a volatile cache, but as a **real-time event broker (Streams & Pub/Sub), probabilistic analytics engine (HyperLogLog & Bitmaps), and geospatial index**.

---

## 1. Core Architecture & Mental Models: Cache vs. Data Structure Engine

When you shift from using Redis as a **cache** to using it for **Pub/Sub, Streams, HyperLogLog, Bitmaps, and Geospatial indexing**, your operational constraints change fundamentally:

```
┌───────────────────────────┬─────────────────────────────────────┬────────────────────────────────────────────┐
│ Operational Dimension     │ Redis as a Volatile Cache           │ Redis Beyond Caching (Streams/PubSub/HLL)  │
├───────────────────────────┼─────────────────────────────────────┼────────────────────────────────────────────┤
│ Data Loss Tolerance       │ Acceptable (fallback to Primary DB) │ Unacceptable for Streams / Analytics       │
│ Eviction Policy           │ allkeys-lru / allkeys-lfu           │ volatile-lru or noeviction (Protect Streams│
│                           │                                     │ & HLL keys without TTLs from eviction!)    │
│ Persistence               │ Optional (or RDB snapshots only)    │ Mandatory AOF (appendonly yes) + RDB       │
│ Client Buffer Management  │ Standard normal client buffers      │ Strict Pub/Sub output buffer limits to     │
│                           │                                     │ prevent slow subscribers from OOMing Redis │
│ Event Notifications       │ Disabled (CPU overhead savings)     │ Keyspace Notifications (--notify-keyspace) │
└───────────────────────────┴─────────────────────────────────────┴────────────────────────────────────────────┘
```

> **Critical Staff-Level Takeaway:** If you run Redis Streams or daily HyperLogLog/Bitmap counters on a Redis instance configured with `maxmemory-policy allkeys-lru` and no TTL on your stream/analytics keys, **a memory spike will silently evict your entire Stream or HyperLogLog key!** When mixing caching and non-caching workloads on the same cluster, always use `volatile-lru` (so keys without TTL—like Streams and permanent HLL registers—are never evicted) and bound Stream memory explicitly using `XADD ... MAXLEN ~ N`.

---

## 2. Docker Compose Topology & Runtime Configuration

You can run all labs using the existing `cache/docker-compose.yml` cluster or with the dedicated runtime flags detailed below.

```
                        ┌────────────────────────────────────────────────────────┐
                        │                     HOST MACHINE                       │
                        └───────────┬────────────────────────────────┬───────────┘
                                    │                                │
             Port: 6379 (Master)    │         Ports: 6380, 6381      │ Port: 5540 (UI) / 9121 (Metrics)
                                    ▼                                ▼
┌───────────────────────────────────┴────────────────────────────────┴───────────────────────┐
│ Docker Network (`cache-net`)                                                               │
│                                                                                            │
│   ┌──────────────────────────────────────────┐         PSYNC2 / AOF Replication            │
│   │  redis-master (Redis 7.2)                ├─────────────────────┬───────────────────┐   │
│   │  • Streams (rax + listpack)              │                     ▼                   ▼   │
│   │  • Pub/Sub & Sharded Pub/Sub             │            ┌─────────────────┐ ┌──────────┐ │
│   │  • HyperLogLog (12KB Registers)          │            │ redis-replica-1 │ │replica-2 │ │
│   │  • Bitmaps / Bitfields / Geo ZSETs       │            └─────────────────┘ └──────────┘ │
│   └──────────────────────────────────────────┘                                             │
│                                                                                            │
│   ┌──────────────────────────────────────────┐         ┌─────────────────────────────────┐ │
│   │  redisinsight (Port: 5540)               │         │  redis-exporter (Port: 9121)    │ │
│   │  • Stream Consumer Group & PEL Inspector │         │  • Stream Lag & PubSub Channels │ │
│   │  • Pub/Sub Live Message Listener         │         │  • Memory & Buffer Metrics      │ │
│   └──────────────────────────────────────────┘         └─────────────────────────────────┘ │
└────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Critical Redis Server Flags for Non-Caching Workloads

When configuring Redis for Streams, Pub/Sub, and Analytics, four server configurations are essential:

#### 1. Keyspace Notifications (`notify-keyspace-events`)
By default, Redis does not broadcast events when keys are modified or expire (`notify-keyspace-events ""`) to save CPU cycles. To enable event-driven listeners:
```redis
CONFIG SET notify-keyspace-events ExKEA
```
- `K`: Keyspace events, published with `__keyspace@<db>__:<key>` prefix.
- `E`: Keyevent events, published with `__keyevent@<db>__:<event>` prefix.
- `x`: Expired events (emitted every time a key expires).
- `e`: Evicted events (emitted when a key is evicted for `maxmemory`).
- `A`: Alias for `"g$lshztxe"`, enabling all command events (strings, lists, sets, hashes, zsets, streams).

#### 2. Pub/Sub Client Output Buffer Limits (`client-output-buffer-limit pubsub`)
In Redis Pub/Sub, the server pushes messages into a per-client memory buffer before the OS kernel sends them over the TCP socket. If a subscriber is on a slow network or stalls while processing, its output buffer grows inside Redis RAM.
```redis
# Format: client-output-buffer-limit <class> <hard-limit> <soft-limit> <soft-seconds>
CONFIG GET client-output-buffer-limit
# Default pubsub: 32mb (hard limit), 8mb (soft limit) for 60 seconds
```
- **Hard Limit (`32mb`):** If a subscriber's buffer reaches 32MB, Redis **immediately disconnects** the subscriber to protect the server from an Out-Of-Memory (OOM) crash.
- **Soft Limit (`8mb 60`):** If the subscriber's buffer stays above 8MB continuously for 60 seconds, Redis disconnects it.

#### 3. Hybrid AOF + RDB Persistence (`appendonly yes` & `aof-use-rdb-preamble yes`)
Because Redis Streams store unacknowledged messages and consumer group offsets in memory (`rax` trees), restarting Redis without AOF persistence wipes all stream history and resets consumer groups, causing catastrophic message loss or full replay.
- `appendonly yes`: Logs every `XADD`, `XACK`, `XGROUP`, and `PFADD` mutation to disk.
- `aof-use-rdb-preamble yes` (Default in Redis 7): When rewriting the AOF (`BGREWRITEAOF`), Redis writes a compact binary RDB preamble (serializing Streams and HLLs in dense binary format) followed by an incremental AOF tail—achieving both fast restarts and 1-second durability.

#### 4. HyperLogLog Sparse Limit (`hll-sparse-max-bytes`)
```redis
CONFIG GET hll-sparse-max-bytes
# Default: 3000 bytes
```
Controls the memory threshold at which a HyperLogLog automatically converts from CPU-compact **Sparse RLE encoding** to fixed $12,288\text{-byte}$ **Dense register encoding**.

---

## 3. Starting & Configuring the Environment

### Step 1: Ensure the Cluster is Running
From `/Users/khizerkhan/Desktop/backend/backend_sd`:
```bash
docker compose -f cache/docker-compose.yml up -d
docker compose -f cache/docker-compose.yml ps
```

### Step 2: Apply "Beyond Caching" Runtime Settings on `redis-master`
Run the following command to enable Keyspace Notifications and protect non-volatile structures (`Streams`, `HLL`, `Geo`) from accidental LRU eviction:

```bash
docker exec -it redis-master redis-cli CONFIG SET notify-keyspace-events ExKEA
docker exec -it redis-master redis-cli CONFIG SET maxmemory-policy volatile-lru
```

Verify the configuration:
```bash
docker exec -it redis-master redis-cli CONFIG GET notify-keyspace-events
docker exec -it redis-master redis-cli CONFIG GET client-output-buffer-limit
docker exec -it redis-master redis-cli CONFIG GET hll-sparse-max-bytes
```

---

## 4. Hands-On Verification Labs

### Lab 1: Verify Real-Time Keyspace Expiration Notifications (2 Terminals)

**Terminal 1 (Subscribe to Expired Events on DB 0):**
```bash
docker exec -it redis-master redis-cli --csv SUBSCRIBE "__keyevent@0__:expired"
```
Output:
```text
"subscribe","__keyevent@0__:expired",1
```

**Terminal 2 (Create a Ephemeral Session Key with a 3-Second TTL):**
```bash
docker exec -it redis-master redis-cli SET session:order_lock:9001 "holding_inventory" EX 3
```

Wait 3 seconds and watch **Terminal 1** immediately emit:
```text
"message","__keyevent@0__:expired","session:order_lock:9001"
```

---

### Lab 2: Verify Redis Streams & Consumer Group Engine
Verify that Redis 7.2 stream commands (`XADD`, `XINFO STREAM`, `XGROUP`) work cleanly:

```bash
# 1. Append an event to a stream with automatic timestamp-sequence ID (*)
docker exec -it redis-master redis-cli XADD telemetry:events \* service "checkout" latency_ms "14" status "200"

# 2. Inspect the internal Radix Tree (rax) and Listpack encoding of the stream
docker exec -it redis-master redis-cli OBJECT ENCODING telemetry:events
# Expected: "stream"

docker exec -it redis-master redis-cli XINFO STREAM telemetry:events FULL
```

Expected output highlights:
```text
length: 1
radix-tree-keys: 1
radix-tree-nodes: 2
last-generated-id: "1730500000000-0"
```

---

### Lab 3: Verify Probabilistic & Geospatial Engines
```bash
# 1. HyperLogLog cardinality verification
docker exec -it redis-master redis-cli PFADD uv:verify "usr_1" "usr_2" "usr_3" "usr_1"
docker exec -it redis-master redis-cli PFCOUNT uv:verify
# Expected: (integer) 3

# 2. Bitmap bit-level operation verification
docker exec -it redis-master redis-cli SETBIT dau:verify 1001 1
docker exec -it redis-master redis-cli BITCOUNT dau:verify
# Expected: (integer) 1

# 3. Geospatial index verification
docker exec -it redis-master redis-cli GEOADD fleet:verify -122.4194 37.7749 "truck_sf_1"
docker exec -it redis-master redis-cli GEOPOS fleet:verify "truck_sf_1"
```

Clean up verification keys:
```bash
docker exec -it redis-master redis-cli DEL telemetry:events uv:verify dau:verify fleet:verify
```

---

## 5. Visualizing Streams & Pub/Sub in RedisInsight

Open **[http://localhost:5540](http://localhost:5540)** in your browser and select your `Local-Master` (`redis-master:6379`) database:
1. **Browser View (Streams):** Click any Stream key to inspect its **Entries**, **Consumer Groups**, **Active Consumers**, and **Pending Entries List (PEL)** with live idle timers.
2. **Pub/Sub Tool (Left Sidebar):** Click the **Pub/Sub** icon in the left navigation bar to subscribe to live channels and publish test messages visually without blocking a terminal.
3. **Workbench:** Run multi-line commands like `XINFO GROUPS <key>` or `GEOSEARCH` with visual table rendering.

---

## 6. Phase 0 Readiness Checklist

Before proceeding to Phase 1, ensure you can answer and verify the following:
- [ ] Why is `maxmemory-policy volatile-lru` safer than `allkeys-lru` when storing Redis Streams and HyperLogLog structures alongside cache keys?
- [ ] How does `client-output-buffer-limit pubsub 32mb 8mb 60` protect the single-threaded Redis server when a subscriber connection hangs?
- [ ] Why must `appendonly yes` be enabled when using Redis Streams with Consumer Groups (`XREADGROUP`)?
- [ ] What is the difference between `__keyspace@0__:<key>` and `__keyevent@0__:<event>` in Redis Keyspace Notifications?
