# Redis Beyond Caching Roadmap: Event Streaming, Probabilistic Data Structures, Geospatial & Real-Time Systems

Redis is far more than a key-value cache. At Staff and Principal Engineering levels, Redis is deployed as an **in-memory data structure server and real-time messaging/analytics engine** capable of powering event-driven microservices, real-time pub/sub fan-out, massive-scale cardinality estimation, bit-level cohort analytics, and sub-millisecond geospatial indexing.

This roadmap takes you from internal C data structures (`rax` Radix Trees, 6-bit HyperLogLog registers, 52-bit Geohash ZSETs, pub/sub client output buffers) to building resilient, high-throughput real-time Go systems.

---

## Phase 0 — Environment, Persistence & Event Engine Setup (Day 0, ~1-2 hrs)
Set up a production-grade Redis 7+ environment configured specifically for event streaming, keyspace notifications, pub/sub backpressure protection, and Redis Stack probabilistic modules.
- Configure and verify the Redis environment (`docker-compose`):
  - **Redis 7.2+ Master-Replica / Stack Engine** with **Keyspace Notifications** (`--notify-keyspace-events ExKEA`) enabled for real-time event hooks.
  - **Pub/Sub Client Output Buffer Tuning** (`--client-output-buffer-limit pubsub 32mb 8mb 60`) to protect the engine from OOM crashes caused by slow subscribers.
  - **AOF Persistence & RDB Hybrid Preambles** (`--appendonly yes --aof-use-rdb-preamble yes`) required when using Redis Streams as a durable log broker.
  - **RedisInsight Workbench & Profiler** (Visual inspection of Streams Consumer Groups, Pending Entries Lists, Pub/Sub channels, and HyperLogLog memory layouts).
- **Exit criteria:** `docker compose up` spins up healthy instances; you can trigger keyspace expiration notifications across terminals, append and inspect stream IDs in RedisInsight, and verify output buffer limits via `CONFIG GET`.

---

## Phase 1 — CLI Fundamentals & Internal Mechanics (Days 1-4)
**Goal:** Internalize the physical C memory layouts, algorithmic complexity, and failure modes of Pub/Sub, Streams, HyperLogLog, Bitmaps, and Geospatial indexes via hands-on CLI labs before writing application code.

### Day 1: Redis Pub/Sub Internals & Real-Time Fan-Out
- Deconstruct Redis Pub/Sub from first principles:
  - **Fire-and-Forget Delivery Semantics**: Why Pub/Sub is push-based, ephemeral (zero persistence), and drops messages for disconnected clients (`At-Most-Once`).
  - **Internal Data Structures**: `pubsub_channels` (hash table mapping channel $\rightarrow$ linked list of client pointers) vs `pubsub_patterns` (glob pattern list evaluated in $O(P)$ on every publish).
  - **Redis 7.0+ Sharded Pub/Sub (`SPUBLISH` / `SSUBSCRIBE`)**: Solving the Redis Cluster Pub/Sub broadcast storm where standard `PUBLISH` floods every node in the cluster ($O(N)$ network amplification) by pinning channels to specific CRC16 hash slots.
  - **Keyspace Notifications (`__keyspace@0__:*` & `__keyevent@0__:expired`)**: Building reactive expiration listeners and understanding why notifications fire on active/passive expiration rather than exact wall-clock timers.
  - **The Slow Subscriber Hazard**: How slow consumers bloat the `client-output-buffer-limit pubsub` and cause sudden connection drops.
- **Hands-on Exercise:** Open multiple `redis-cli` sessions; test `SUBSCRIBE`, `PSUBSCRIBE`, and `SSUBSCRIBE`; inspect live channel state with `PUBSUB CHANNELS`, `PUBSUB NUMSUB`, and `PUBSUB SHARDNUMSUB`; trigger and capture `__keyevent@0__:expired` events live.

### Day 2: Redis Streams Internals & Consumer Groups (`XADD` to `XAUTOCLAIM`)
- Demystify Redis Streams as an append-only log broker (the lightweight alternative to Apache Kafka):
  - **Internal Memory Layout**: The **Radix Tree (`rax`) + `listpack` Macro-Node** architecture. How Redis packs dozens of stream entries with delta-encoded timestamps and master field lists into a single contiguous `listpack` node to achieve Kafka-like sequential memory efficiency.
  - **Entry IDs (`<millisecondsTime>-<sequenceNumber>`)**: Monotonic ordering, clock-skew protection, and time-range slicing via `XRANGE` and `XREVRANGE`.
  - **Consumer Groups (`XGROUP CREATE`, `XREADGROUP`)**: Partitioning work across competing workers (`>` vs `0` ID semantics).
  - **The Pending Entries List (PEL)**: How Redis tracks unacknowledged in-flight messages (`XPENDING`), delivery counts, and idle times without blocking subsequent messages.
  - **Crash Recovery & Ownership Transfer**: Using `XACK` to clear the PEL, and `XCLAIM` / `XAUTOCLAIM` (Redis 6.2+) to rescue orphaned messages from crashed workers.
  - **Stream Trimming & Retention**: Exact (`MAXLEN = 10000`) vs Approximated (`MAXLEN ~ 10000`) radix node eviction and time-based trimming (`MINID ~ <timestamp>`).
- **Hands-on Exercise:** Create a stream and consumer group via CLI; simulate 2 consumers processing events; leave a message unacknowledged to simulate a crash; inspect it via `XPENDING`, and rescue it with `XAUTOCLAIM`.

### Day 3: Probabilistic Data Structures — HyperLogLog (`PFADD`, `PFCOUNT`, `PFMERGE`)
- Master massive-scale cardinality estimation in constant $12\text{ KB}$ of memory:
  - **The Exact Counting Bottleneck**: Why counting 100 million unique daily visitors using a Redis `SET` (`SADD`) or SQL `COUNT(DISTINCT)` consumes $3.2\text{ GB+}$ of RAM, whereas HyperLogLog uses **at most 12 KB** with a standard error of **$0.81\%$**.
  - **Mathematical Intuition**: Coin flips, leading zeros in 64-bit MurmurHash64A hashes ($2^{\rho_{\max}}$), stochastic averaging across $m = 16,384$ ($2^{14}$) 6-bit registers, and the **Harmonic Mean** formula that suppresses extreme outliers.
  - **Memory Representation Escalation**:
    - **Sparse Encoding**: Run-Length Encoding (`ZERO`, `XZERO`, `VAL` opcodes) consuming $< 1\text{ KB}$ for low cardinalities (`hll-sparse-max-bytes 3000`).
    - **Dense Encoding**: Fixed $12,288\text{ byte}$ ($16,384 \times 6\text{ bits}$) bit-packed register array + 16-byte HLL header (`HYLL`).
  - **Multi-Dimensional Aggregation (`PFMERGE`)**: Computing union cardinalities across hourly/daily/regional HLL keys by taking the register-wise maximum $M_{\text{union}}[j] = \max(M_A[j], M_B[j])$.
- **Hands-on Exercise:** Insert 10 entries vs 50,000 entries into an HLL; inspect `PFDEBUG ENCODING` and `MEMORY USAGE` to watch the exact transition from `sparse` to `dense` at 12,304 bytes; verify `PFMERGE` across multiple days.

### Day 4: Bitmaps, Bitfields & Geospatial Indexing
- **Bitmaps & Bit-Level Analytics**:
  - How Bitmaps operate directly on raw SDS strings up to 512MB ($2^{32}$ bits).
  - Storing daily active user (DAU) flags for 100M users in just **12.5 MB** (`SETBIT`, `GETBIT`).
  - Hardware-accelerated population counts (`BITCOUNT` via CPU `POPCNT` instructions) and bitwise cohort intersections (`BITOP AND/OR/XOR/NOT`) for retention analysis.
  - **Bitfields (`BITFIELD`)**: Packing arbitrary-width integer counters (e.g., `u4`, `i8`, `u16`) into contiguous bit arrays with configurable overflow policies (`WRAP`, `SAT`, `FAIL`).
- **Geospatial Indexes (`GEOADD`, `GEOSEARCH`)**:
  - **Under the Hood**: Why a Redis Geo index is physically a **Sorted Set (`ZSET`)** where the 64-bit `double` score holds a **52-bit interleaved Geohash** of latitude and longitude.
  - **Z-Order Space-Filling Curve**: How 2D coordinates map to 1D sorted integer ranges, and why Redis searches the target Geohash box plus its **8 surrounding neighbor cells** to eliminate edge boundary artifacts.
  - Modern `GEOSEARCH` & `GEOSEARCHSTORE` (replacing deprecated `GEORADIUS`): Querying `BYRADIUS` and `BYBOX` in $O(N + \log M)$ time.
- **Hands-on Exercise:** Build a 3-day user retention funnel using `SETBIT` and `BITOP AND`; index 10 city coordinates with `GEOADD`, verify `TYPE` is `zset`, inspect raw 52-bit Geohash scores with `ZRANGE ... WITHSCORES`, and run `GEOSEARCH` queries.

**Exit criteria for Phase 1:** You can explain from memory why Pub/Sub cannot replace a durable queue, how `rax` + `listpack` makes Streams memory-efficient, why HyperLogLog caps out at 12KB with 0.81% error, and how Geo queries translate 2D radiuses into ZSET score ranges.

---

## Phase 2 — Production Go Implementations: Real-Time Event & Analytics Clients (Days 5-9)
**Goal:** Build production-ready Go (`go-redis/v9`) implementations for every non-caching Redis paradigm, handling concurrency, backpressure, consumer crash recovery, and atomic scripting.

### Day 5: Resilient Pub/Sub & WebSocket Fan-Out Gateway in Go
- Build a real-time notification broadcaster in Go using `rdb.Subscribe` and `rdb.SSubscribe` (Sharded Pub/Sub).
- Handle channel backpressure (`redis.WithChannelSize`), automatic reconnection health checks (`Ping`), and graceful context cancellation so slow WebSocket/SSE clients never stall the Redis reader goroutine.

### Day 6: Durable Event Processing with Redis Streams & Consumer Groups in Go
- Build a multi-worker event consumer group using `XGroupCreateMkStream` and blocking `XReadGroup` (`Block: 2 * time.Second`).
- Implement **At-Least-Once processing**: parse payload $\rightarrow$ execute business logic $\rightarrow$ commit `XAck`.
- Build a **Background Janitor / Reaper Goroutine** using `XAutoClaim` to automatically detect, claim, and re-process messages left in the PEL by crashed workers, plus a **Dead-Letter Stream (`DLQ`)** router when `RetryCount > MaxRetries`.

### Day 7: High-Throughput Analytics Engine (HyperLogLog + Bitmaps + Bitfields) in Go
- Implement a real-time **Telemetry & Cohort Analytics Tracker** using Redis Pipelining:
  - Track unique hourly/daily/weekly active users (UV) and merge across time windows with `PFAdd`, `PFCount`, and `PFMerge`.
  - Track exact daily user login streaks and compute "3-Day Consecutive Retention Cohorts" in microseconds using `SetBit`, `BitOpAnd`, and `BitCount`.
  - Pack per-user feature usage counters into compact `BitField` arrays with `SAT` (saturating) overflow control.

### Day 8: Real-Time Location & Proximity Matching Engine in Go
- Build a ride-hailing / delivery driver proximity matcher using `GeoAdd`, `GeoSearchLocation` (`BYRADIUS` / `BYBOX`, `WITHDIST`, `WITHCOORD`, `ASC`), and `ZRem`.
- Combine Geospatial lookups with Redis Hashes/Pipelines to filter active drivers by status and vehicle tier in a single round-trip.

### Day 9: Atomic Multi-Structure Transactions (`WATCH`/`MULTI`/`EXEC`) & Lua Scripting
- Contrast optimistic locking (`WATCH` + `MULTI`/`EXEC` retry loops) against server-side atomic **Lua Scripts (`EVAL` / `EVALSHA`)** and **Redis 7 Functions (`FUNCTION LOAD`)**.
- Implement an atomic **"Geo-Fence + Stream Event + HLL Counter"** Lua script that validates driver availability, updates coordinates, and appends an audit event to a Redis Stream in a single indivisible operation.

**Exit criteria for Phase 2:** You have working, tested Go packages for Sharded Pub/Sub fan-out, fault-tolerant Redis Streams workers with `XAUTOCLAIM` + DLQ, HLL/Bitmap cohort analytics, and Geospatial proximity search.

---

## Phase 3 — Distributed Architecture, Comparative Trade-offs & Staff/Principal Patterns (Days 10-14)
**Goal:** Master architectural trade-offs (Redis Streams vs Kafka vs Pub/Sub), cluster slot constraints, persistence durability hazards, and capacity engineering for Staff/Principal system design interviews.

### Day 10: Architectural Showdown — Redis Pub/Sub vs Redis Streams vs Apache Kafka
- Deep comparative matrix across **Durability, Storage Engine (RAM vs Disk Page Cache), Consumer Fan-Out, Partitioning, Throughput, Retention, and Operational Complexity**.
- **Decision Framework**: When to choose Redis Streams (low-latency sub-millisecond queues, working set fits in RAM, lightweight infrastructure) vs Apache Kafka (multi-terabyte log retention, disk-backed sequential I/O, high-partition horizontal write scaling, replayable event sourcing).

### Day 11: Redis Cluster Constraints for Non-Caching Workloads
- Why standard `PUBLISH` causes an **all-to-all cluster broadcast bottleneck** in Redis Cluster and how **Sharded Pub/Sub (`SPUBLISH`)** isolates traffic to slot owners.
- Why `PFMERGE`, `BITOP`, and multi-stream `XREADGROUP` fail with `CROSSSLOT Keys in request don't hash to the same slot` in Redis Cluster, and how to design **Hash Tag Partitioning (`{metric:uv}:2026-11-08`)** without creating hot-slot imbalances.
- Scaling Redis Streams horizontally by sharding logical topics across $K$ stream keys (`orders:stream:{0..7}`).

### Day 12: Durability, Persistence & Replication Hazards for Streams
- Why Redis Streams are **NOT strictly durable by default**:
  - Asynchronous replication windows (`PSYNC`) and how a master crash before replica sync loses acknowledged stream entries.
  - `appendfsync everysec` vs `appendfsync always` vs `WAIT` / `WAITAOF` (Redis 7.2+) synchronous replication barriers.
- **Memory Exhaustion Disasters**: What happens when a Stream grows unbounded under `maxmemory-policy noeviction` vs `allkeys-lru` (accidental deletion of an entire Stream key + PEL!). Designing hybrid `volatile-lru` + mandatory `XADD MAXLEN ~ N` policies.

### Day 13: Advanced Probabilistic Structures (Bloom, Cuckoo, Count-Min Sketch, Top-K)
- Expanding beyond HyperLogLog with the probabilistic toolkit:
  - **HyperLogLog vs Bloom Filter vs Cuckoo Filter**: Set cardinality ($|\text{Set}|$) vs Set membership ($x \in \text{Set}$). Why Cuckoo Filters support deletions (`CF.DEL`) while standard Bloom Filters do not.
  - **Count-Min Sketch (`CMS`)**: Estimating the frequency of individual events in a stream using fixed 2D hash tables (solving the heavy-hitter frequency problem).
  - **Top-K (`TOPK`)**: Maintaining a real-time leaderboard of the $K$ most frequent items (e.g., top trending hashtags or DDoS IP detection) in constant space without sorting millions of elements in a ZSET.

### Day 14: Production Observability, Sizing & Failure Diagnosis
- Capacity planning formulas for Streams (`rax` + `listpack` compression ratios), Bitmaps ($\text{MaxOffset} / 8$), HLLs ($12\text{ KB} \times \text{Keys}$), and ZSET Geo indexes.
- Monitoring **Consumer Group Lag** (`XINFO GROUPS` `lag` & `pending` metrics), diagnosing stuck PEL entries, and alerting on `pubsub` output buffer overflows.

**Exit criteria for Phase 3:** You can defend architectural choices between Redis Streams and Kafka in a Staff system design interview, design cluster-safe hash-tagged analytics pipelines, and prevent stream data loss during failover.

---

## Phase 4 — Capstone Project: Real-Time Fleet Dispatch, Telemetry & Analytics Platform (Days 15-20)
Build an end-to-end, high-throughput Go distributed system integrating every non-caching Redis capability:
- **Geospatial Dispatch Engine (`GEOADD` / `GEOSEARCH`)**: Track 50,000 simulated delivery vehicles in real time and match incoming orders to the nearest available driver within a 3km radius.
- **Durable Order Event Pipeline (`Redis Streams` + `XREADGROUP` + `XAUTOCLAIM`)**: Process dispatch assignments with consumer groups, automatic crash recovery for stuck orders, and Dead-Letter Stream routing.
- **Live Customer Notification Bus (`Sharded Pub/Sub`)**: Stream real-time driver location updates and order state transitions to connected clients with zero polling.
- **Real-Time Executive Analytics (`HyperLogLog` + `Bitmaps` + `Bitfields`)**: Track unique active customers per city (`PFADD`/`PFMERGE`), daily driver shift attendance & retention cohorts (`SETBIT`/`BITOP`), and per-zone surge counters (`BITFIELD`).
- **Chaos & Load Verification**: Kill stream worker goroutines mid-flight, simulate slow Pub/Sub subscribers, and verify zero dropped orders (`XAUTOCLAIM` recovery) and sub-millisecond p99 analytics queries.
