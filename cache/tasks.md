# Caching Engineering Roadmap: From First Principles to Staff/Principal Level

Caching is not merely "putting Redis in front of a database." In modern distributed systems operating at scale, caching is a sophisticated discipline balancing **data freshness, memory mechanics, algorithmic eviction, consistency guarantees, concurrency hazards, and network topology**.

This roadmap takes you from raw memory fundamentals (Slab allocators, jemalloc, SDS, eviction sampling) to building resilient, distributed, high-throughput caching systems capable of handling millions of queries per second.

---

## Phase 0 — Environment & Cluster Topologies (Day 0, ~1-2 hrs)
Set up a production-grade local environment featuring multiple caching topologies to observe distributed failover, sharding, and memory allocation firsthand.
- Deploy a multi-node caching cluster via `docker-compose`:
  - **Redis Master-Replica with Sentinel Quorum** (3 nodes + 3 sentinels for automated failover & split-brain analysis).
  - **Memcached** (Multi-threaded shared-nothing memory cache for slab allocation comparisons).
  - **RedisInsight / Redis Commander** (Visual inspection of keyspaces, memory fragmentation, and data types).
  - **Prometheus & Redis Exporter** (Real-time observability into hit ratios, eviction spikes, and network latency).
- **Exit criteria:** `docker compose up` spins up healthy instances across all services; you can connect via CLI, verify replication status, and observe live cluster metrics in Prometheus and RedisInsight.

---

## Phase 1 — Memory Mechanics, Data Structures & Eviction Policies (Days 1-4)
**Goal:** Internalize how memory is physically structured, how keys expire, and how different eviction algorithms behave when memory reaches hardware limits.

### Day 1: Redis Data Structures & Memory Internals
- Deconstruct Redis core data structures from first principles:
  - **Simple Dynamic Strings (SDS)**: Binary safety, dynamic pre-allocation, constant time $O(1)$ length calculation vs C strings.
  - **Hashes, Lists & Sets**: `ziplist`, `listpack`, `quicklist`, and `hashtable` memory optimizations (`hash-max-listpack-entries`).
  - **Sorted Sets (ZSET)**: Dual data structure (`skiplist` + `dict`) enabling $O(\log N)$ range queries and lookups.
  - **Advanced Structures**: Bitmaps, HyperLogLog (cardinality estimation via stochastic count), and Geospatial indexes.
- **Hands-on Exercise:** Inspect memory footprints using `OBJECT ENCODING` and `MEMORY USAGE`. Watch Redis transition a list/hash from compact `listpack` to standard pointer-heavy structures as size thresholds cross.

### Day 2: Expiration Internals & TTL Lifecycle
- Demystify how TTLs are enforced:
  - **Passive (Lazy) Expiration**: Key is checked and removed only when accessed by a read/write command.
  - **Active Expiration**: Background cron cycle (10Hz) probabilistically sampling expired keys to reclaim memory without CPU starvation.
  - Unaccessed key memory leaks and why `EXPIRE` alone does not guarantee immediate RAM reclamation.
- **Hands-on Exercise:** Fill a cache with 100,000 keys with a 1-second TTL without reading them; inspect `MEMORY STATS` and monitor active expiration cycles reclaiming memory.

### Day 3: Eviction Algorithms & Algorithmic Trade-offs
- Deep dive into `maxmemory` eviction policies:
  - `noeviction`, `allkeys-lru`, `volatile-lru`, `allkeys-lfu`, `volatile-lfu`, `allkeys-random`, `volatile-ttl`.
  - **Exact LRU vs Approximated LRU**: Why Redis rejects linked-list LRU (saving 24 bytes of memory pointers per entry) in favor of a 24-bit timestamp clock and a 16-key eviction pool candidate sampling algorithm.
  - **LFU (Least Frequently Used) Internals**: 24-bit field split into a 16-bit decay time + 8-bit logarithmic counter (Morris counter). Tuning `lfu-log-factor` and `lfu-decay-time`.
- **Hands-on Exercise:** Cap Redis memory to 2MB. Produce access patterns (recency vs frequency), trigger evictions, and observe which keys survive under LRU vs LFU.

### Day 4: Memcached vs Redis Architecture
- **Thread Concurrency Models**: Memcached multi-threaded libevent epoll vs Redis single-threaded event loop (plus threaded I/O in Redis 6+).
- **Memory Allocation Mechanics**:
  - Memcached **Slab Allocator**: Slab classes, pages (1MB), chunks, internal fragmentation vs external fragmentation.
  - Redis dynamic memory allocation via **`jemalloc`**: Small, large, and huge allocations; tracking `mem_fragmentation_ratio`.
- **Hands-on Exercise:** Benchmark Memcached vs Redis under multi-core load. Trigger slab allocation limits in Memcached and compare latency profiles.

**Exit criteria for Phase 1:** You can articulate, based on memory benchmarks, why Redis chooses approximated LRU over exact LRU, how slab allocation eliminates external memory fragmentation, and how to tune LFU counters for skewed traffic.

---

## Phase 2 — Application Caching Patterns & Concurrency in Go (Days 5-9)
**Goal:** Build production Go client implementations of caching patterns while conquering concurrency hazards, race conditions, and distributed locks.

### Day 5: Core Caching Patterns
- Implement and compare caching topologies in Go:
  - **Cache-Aside (Lazy Loading)**: Read from cache; on miss, read DB and backfill cache. Analyze write latency vs stale read trade-offs.
  - **Read-Through & Write-Through**: Cache acts as the primary data store interface, synchronously delegating to persistence.
  - **Write-Behind (Write-Back)**: Asynchronous queue-buffered writes to DB for extreme write throughput. Risk analysis: crash recovery and Write-Ahead Logging (WAL).
  - **Refresh-Ahead**: Pre-emptive background cache reloading before TTL expiration based on access frequency.

### Day 6: Cache Stampede & Concurrency Hazards
- Solve the **Thundering Herd / Cache Stampede** problem (when a hot key expires and thousands of concurrent requests slam the database):
  - **In-Process Request Coalescing**: Utilizing Go's `golang.org/x/sync/singleflight` to collapse duplicate concurrent in-flight DB queries into a single execution.
  - **Distributed Mutex**: Locking key recalculation across horizontal microservice pods.
  - **Probabilistic Early Expiration (XFetch Algorithm)**: Recomputing cached values in the background before expiration using the formula:
    $$\Delta \cdot \beta \cdot (-\ln(\text{rand}())) > \text{remaining\_ttl}$$

### Day 7: Distributed Locks with Redis (Redlock & Lua)
- Why naive `SETNX` + `EXPIRE` creates deadlock vulnerabilities (split-brain on crash).
- The atomic `SET key uuid NX PX milliseconds` pattern.
- Atomic lock release and lock extension using **Lua scripts** to prevent releasing locks owned by other threads.
- Building a lock auto-renewal watchdog (heartbeat goroutine).
- **The Redlock Algorithm**: Quorum across $N$ independent Redis masters, network partition trade-offs, and fencing tokens (the Kleppmann vs Sanfilippo debate).

### Day 8: Serialization, Compression & Wire Protocols
- Benchmarking cache serialization formats: Raw JSON vs Protocol Buffers vs MessagePack vs Gob.
- Payload compression: When to apply Snappy or Zstandard (zstd) to reduce network I/O and Redis RAM consumption.
- Connection pooling, pipelining, and multiplexing in Go (`redis.Pipeliner` vs individual commands).

### Day 9: The Dual-Write Problem & Cache Invalidation
- The fundamental distributed systems challenge: **Keeping Database and Cache in sync**.
- Analyze race conditions:
  - Cache deletion before DB write vs Cache deletion after DB write.
  - The "Delayed Double Deletion" pattern.
  - Change Data Capture (CDC): Streaming DB binlogs (Debezium / Kafka) to invalidate caches asynchronously, achieving eventual consistency without application coupling.

**Exit criteria for Phase 2:** You have implemented a production-ready Go caching client featuring Cache-Aside, Singleflight stampede suppression, XFetch early expiration, and atomic Lua-backed distributed locks.

---

## Phase 3 — Distributed Caching, High Availability & Staff/Principal Patterns (Days 10-14)
**Goal:** Master the production failure modes, sharding strategies, multi-tier architectures, and disaster scenarios that define senior and staff engineering interviews.

### Day 10: Multi-Tier Caching (L1 Local + L2 Distributed) & Coherence
- **Architecture**: In-process memory cache (Go `sync.Map` / BigCache / Ristretto) as L1 + Remote Redis cluster as L2.
- **Cache Coherence**: Invalidation propagation across thousands of microservice instances using Redis Pub/Sub.
- **Client-Side Caching in Redis 6+**: Tracking mode, BCAST (broadcast invalidation messages), and RESP3 protocol.

### Day 11: Distributed Sharding & Consistent Hashing
- How to scale cache capacity beyond a single server's RAM:
  - **Client-Side Partitioning**: Modulo hashing vs **Consistent Hashing** (Hash ring, virtual nodes / Ketama algorithm). Analyzing key migration percentages when adding/removing nodes.
  - **Redis Cluster Topology**: 16,384 hash slots, CRC16 hashing, hash tags `{user_101}:orders`, `MOVED` and `ASK` redirection protocols, live cluster resharding without downtime.

### Day 12: High Availability, Sentinel & Replication Failover
- **Replication Internals**: Asynchronous replication, `PSYNC` protocol (replication ID + offset), partial resynchronization via replication backlog buffer.
- **Redis Sentinel**: Raft-like quorum voting, monitoring, automatic leader election, replica promotion, and split-brain prevention (`min-replicas-to-write`, `min-replicas-max-lag`).

### Day 13: Production Failure Modes & Disaster Mitigation
- **Cache Penetration**: Malicious/errant queries for non-existent IDs bypass cache and overwhelm the DB.
  - *Solutions:* Bloom Filters / Cuckoo Filters at the application gateway; caching null objects with short TTLs.
- **Cache Avalanche**: Hundreds of thousands of keys expiring at the exact same second, causing catastrophic DB spikes.
  - *Solution:* Randomizing expiration with **TTL Jitter** ($TTL = \text{base} + \text{rand}(0, \text{jitter})$).
- **Hot Key Problem**: A single key experiences millions of hits/sec, saturating a single Redis node's NIC or CPU.
  - *Solutions:* Hotkey detection (Redis 4+ `redis-cli --hotkeys` / LFU), dynamic local L1 caching, and key-splitting with salt prefixes (`item_101#1`, `item_101#2`).
- **Big Key Problem**: Massive collections (e.g., 500,000-element set or hash) blocking single-threaded Redis operations.
  - *Solutions:* Bigkey scanner (`--bigkeys`), non-blocking deletions via `UNLINK` (lazy background thread freeing) instead of blocking `DEL`, chunked scanning via `HSCAN`/`SSCAN`.

### Day 14: Observability, Sizing, Latency & Capacity Planning
- Memory sizing formulas, calculating working set size, and eviction forecasting.
- Diagnosing latency spikes: `SLOWLOG`, `LATENCY DOCTOR`, client output buffer limits, and active memory defragmentation (`activedefrag yes`).
- Core Prometheus alert thresholds for production caching tiers.

**Exit criteria for Phase 3:** You can architect resilient caching layers for 99.999% availability, handle petabyte-scale sharding, eliminate hotkey bottlenecks, and explain every distributed cache disaster mitigation pattern.

---

## Phase 4 — Capstone Project: High-Throughput E-Commerce Multi-Tier Engine (Days 15-20)
Build a production-grade, distributed microservice in Go integrating every technique mastered above:
- **L1 In-Memory + L2 Redis Cluster** multi-tier caching with client-side invalidations.
- **Stampede-proof Flash-Sale Inventory Engine** using atomic Lua scripts and XFetch early expiration.
- **Bloom-Filter Gateway** shielding against cache penetration attacks.
- **Distributed Rate Limiter** implementing Sliding Window Log / Token Bucket in Redis.
- **Chaos Testing**: Inject simulated network partitions, kill Redis masters, trigger flash-sale traffic spikes, and verify zero data corruption and sub-millisecond p99 latencies under load.
