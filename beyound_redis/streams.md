# Redis Streams Architecture, Internals & Hands-On Guide

This manual follows the natural architectural progression: **Problem Statement $\rightarrow$ Core Architecture & Internal Memory Layout (`rax` + `listpack`) $\rightarrow$ Stream Entry IDs & Consumer Groups $\rightarrow$ The Pending Entries List (PEL) & Crash Recovery (`XAUTOCLAIM`) $\rightarrow$ Trimming, Durability & Dead-Letter Queues $\rightarrow$ Production Golang Implementations**.

---

## 1. Problem Statement

1. **Pub/Sub Ephemerality vs. Durable Event Logs**: Redis Pub/Sub is a fire-and-forget broadcast bus (`At-Most-Once`). If a consumer restarts or disconnects for 50ms, all messages published during that window are permanently lost. Furthermore, Pub/Sub broadcasts every message to *every* subscriber rather than load-balancing work across a pool of competing workers.
2. **List Queue Limitations (`LPUSH` / `BRPOP`)**: Before Redis 5.0, engineers built work queues using Redis Lists. However, Lists lack multi-consumer fan-out (only one consumer group can pop a list), lack historical replay (once popped, the item is gone), and require brittle `RPOPLPUSH` backup lists to handle worker crashes.
3. **The Solution — Redis Streams**: Introduced in Redis 5.0 and expanded in Redis 6.2/7.0, **Redis Streams** is an in-memory, append-only commit log with native **Consumer Groups (`XREADGROUP`)**, **per-message out-of-order acknowledgments (`XACK`)**, **Pending Entries List (PEL) crash recovery (`XAUTOCLAIM`)**, and **$O(1)$ approximate log trimming**.

---

## 2. Architecture & Internal C Memory Layout (`rax` + `listpack`)

If Redis stored every stream event as an individual linked-list node or hash table entry, storing 10 million small log messages would waste gigabytes of RAM on 64-bit pointers.

Instead, Redis Streams combines two highly optimized C structures:
1. **A Compressed Radix Tree (`rax`)**: Indexes stream segments by their 128-bit Stream Entry ID.
2. **`listpack` Macro-Nodes**: Each leaf of the Radix Tree is a contiguous byte array (`listpack`) storing up to **100 entries or 4 KB** (`stream-node-max-entries 100`, `stream-node-max-bytes 4096`).

```
                     [ Stream Key: "orders:events" (OBJ_STREAM) ]
                                          │
                                          ▼
                   ┌────────────────────────────────────────────┐
                   │       Compressed Radix Tree (rax)          │
                   │  Keys = 128-bit Big-Endian Stream IDs      │
                   └───────────┬────────────────────┬───────────┘
                               │                    │
         Prefix: 1730500000... │                    │ Prefix: 1730500005...
                               ▼                    ▼
┌───────────────────────────────────────────┐  ┌───────────────────────────────────────────┐
│ Listpack Macro-Node A (Up to 100 Entries) │  │ Listpack Macro-Node B (Up to 100 Entries) │
├───────────────────────────────────────────┤  ├───────────────────────────────────────────┤
│ • Master ID: 1730500000100-0              │  │ • Master ID: 1730500005000-0              │
│ • Master Fields: ["order_id", "amount"]   │  │ • Master Fields: ["order_id", "amount"]   │
│ ───────────────────────────────────────── │  │ ───────────────────────────────────────── │
│ • Entry 1 (+0ms, +0seq): ["101", "49.99"] │  │ • Entry 1 (+0ms, +0seq): ["201", "15.00"] │
│ • Entry 2 (+3ms, +0seq): [SAME_FIELDS |   │  │ • Entry 2 (+8ms, +0seq): [SAME_FIELDS |   │
│                           "102", "89.00"] │  │                           "202", "99.00"] │
└───────────────────────────────────────────┘  └───────────────────────────────────────────┘
```

### Why `rax` + `listpack` Achieves Kafka-Like Memory Efficiency:
- **Master Field Deduplication (`SAME_FIELDS`):** When you append events with identical keys (`order_id`, `user_id`, `amount`), the `listpack` writes those field names **only once** in the macro-node header. The next 99 entries set a 1-bit `SAME_FIELDS` flag and store **only the values**, cutting memory usage by 50–70%!
- **Delta-Encoded IDs:** Instead of repeating 16 bytes for every 128-bit entry ID, subsequent entries inside the `listpack` store only the tiny varint delta (`+3ms, +0seq`) relative to the macro-node's Master ID.

---

## 3. Core Components & Consumer Group Mechanics

- **Stream Entry ID (`<millisecondsTime>-<sequenceNumber>`)**:
  - A 128-bit monotonic identifier (64-bit Unix timestamp in ms + 64-bit sequence counter within that millisecond), e.g., `1730500000125-0`.
  - Enables native time-range queries (`XRANGE stream 1730500000000-0 1730500060000-+`) without secondary indexes!
- **Consumer Group (`XGROUP CREATE`)**:
  - Multiple independent Consumer Groups can read the same Stream (e.g., `billing-group` and `analytics-group` each receive a full copy of the stream).
  - Within a single Consumer Group, messages are **load-balanced across competing consumers** so each message is delivered to only one worker.
- **Last Delivered ID (`last_delivered_id`)**:
  - The high-water mark cursor tracked per Consumer Group.
- **Pending Entries List (PEL)**:
  - Every Consumer Group maintains two Radix Trees (`pel` global to the group + per-consumer `pel`) tracking every message delivered via `XREADGROUP ... >` that has **not yet been acknowledged via `XACK`**.

```
                      [ Producer ]
                           │  XADD orders:stream MAXLEN ~ 10000 * order_id 101
                           ▼
             ┌────────────────────────────┐
             │   Stream: "orders:stream"  │
             │ [101-0]  [102-0]  [103-0]  │
             └──────┬──────────────┬──────┘
                    │              │
    ┌───────────────┘              └────────────────┐
    ▼                                               ▼
┌────────────────────────────────────────┐   ┌────────────────────────────────────────┐
│ Consumer Group: "billing-workers"      │   │ Consumer Group: "fraud-detector"       │
│ • Worker A ──► Reads [101-0], [103-0]  │   │ • FraudPod 1 ──► Reads [101-0..103-0]  │
│ • Worker B ──► Reads [102-0]           │   └────────────────────────────────────────┘
│ • PEL: Tracks un-ACKed IDs + IdleTime  │
└────────────────────────────────────────┘
```

---

## 4. The `>` vs `0` Cursor Semantics in `XREADGROUP`

When a consumer calls `XREADGROUP GROUP mygroup worker-1 STREAMS mystream <ID>`, the `<ID>` parameter controls two completely different modes:

1. **`>` (Greater-Than Symbol — New Undelivered Messages)**:
   - Tells Redis: *"Give me new messages that have never been delivered to any consumer in `mygroup`."*
   - Advances the group's `last_delivered_id` cursor and inserts the returned messages into `worker-1`'s **Pending Entries List (PEL)** with `delivery_count = 1`.
2. **`0` (or `0-0` — Own Pending History Recovery)**:
   - Tells Redis: *"Do NOT fetch new messages. Return the messages that were ALREADY delivered to `worker-1` earlier and are still sitting unacknowledged in `worker-1`'s PEL!"*
   - Used immediately on worker startup when a worker restarts with the **same `consumerName`** (`statefulset` pod `worker-1`) to finish processing its own interrupted in-flight work.

---

## 5. Fault Tolerance: Crash Recovery (`XAUTOCLAIM`) & Dead-Letter Queues (`DLQ`)

What if your workers run as ephemeral Kubernetes `Deployment` pods with random names (`worker-7f9b2`, `worker-8c1a4`), and `worker-7f9b2` gets `OOMKilled` while holding 5 messages in its PEL? That pod name will never come back!

### 1. Automatic Ownership Transfer with `XAUTOCLAIM` (Redis 6.2+)
Surviving workers run a periodic background loop calling `XAUTOCLAIM`:
```redis
XAUTOCLAIM orders:stream billing-workers worker-healthy 30000 0-0 COUNT 20
```
- Scans the Consumer Group's PEL starting at `0-0`.
- Finds any message across **any** consumer that has been idle (unacknowledged) for $\ge 30,000\text{ ms}$.
- Transfers ownership of those messages to `worker-healthy`, resets their idle timer to `0`, and **increments their `delivery_count` (`RetryCount`) by 1**!

### 2. Poison Message Protection via Dead-Letter Stream (`DLQ`)
If a message contains a corrupted payload that crashes every worker that touches it, `XAUTOCLAIM` will pass it from worker to worker forever unless you cap retries:
1. Check `delivery_count` in the PEL (`XPENDING` / `XPendingExt`).
2. When `delivery_count > MaxRetries` (e.g., `3`), atomically pipeline:
   - `XADD orders:dlq * original_id <id> error <err> payload <data>`
   - `XACK orders:stream billing-workers <id>` (removes the poison message from the primary PEL so the pipeline never stalls!).

---

## 6. Performance Tuning, Trimming & Durability

- **Approximate Trimming (`MAXLEN ~` / `MINID ~`)**:
  - ❌ `XADD mystream MAXLEN = 10000 * ...`: Forces Redis to inspect and splice individual elements inside a compressed `listpack` on every write.
  - ✅ `XADD mystream MAXLEN ~ 10000 * ...`: The `~` modifier tells Redis to only evict whole `listpack` macro-nodes from the Radix Tree when an entire node of ~100 entries falls outside the limit—running in **$O(1)$ amortized time**!
- **Time-Based Retention (`MINID ~`)**:
  - `XADD mystream MINID ~ 1730400000000 * ...`: Automatically evicts macro-nodes older than the specified Unix millisecond timestamp.
- **Synchronous Durability (`WAITAOF` / `WAIT`)**:
  - Because standard Redis replication is asynchronous, critical financial streams should follow `XADD` with `WAITAOF 1 1 1000` (Redis 7.2+) or `WAIT 1 1000` to guarantee the event is persisted to AOF and replicated to at least 1 replica before returning success.

---

## 7. Hands-On Golang Implementations (`github.com/redis/go-redis/v9`)

### 1. High-Throughput Producer with Approximate Trimming
```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	ctx := context.Background()

	// Pipeline 3 order events with approximate MAXLEN ~ 50,000 trimming
	pipe := rdb.Pipeline()
	for i := 1; i <= 3; i++ {
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: "orders:events",
			MaxLen: 50000,
			Approx: true, // Uses '~' for O(1) macro-node trimming
			ID:     "*",
			Values: map[string]interface{}{
				"order_id": fmt.Sprintf("ORD-%d", 100+i),
				"user_id":  fmt.Sprintf("usr_%d", i),
				"amount":   49.99 * float64(i),
				"ts":       time.Now().UnixMilli(),
			},
		})
	}
	cmds, err := pipe.Exec(ctx)
	if err != nil {
		log.Fatalf("Failed to publish stream batch: %v", err)
	}
	for _, cmd := range cmds {
		fmt.Printf("Published Stream Entry ID: %s\n", cmd.(*redis.StringCmd).Val())
	}
}
```

### 2. Competing Consumer Group Worker + `XAUTOCLAIM` Crash Recovery + DLQ Router
See the complete, runnable implementation in [`beyound_redis/phase_2_manual.md`](file:///Users/khizerkhan/Desktop/backend/backend_sd/beyound_redis/phase_2_manual.md) (`stream_worker.go`), which demonstrates:
- Idempotent `XGroupCreateMkStream` initialization.
- Blocking `XReadGroup` (`>`) loop with explicit `XAck`.
- Background `XAutoClaim` janitor scanning for stuck PEL entries idle longer than `MinIdle`.
- `XPendingExt` retry counter inspection and atomic `Pipelined` routing to `payments:dlq` + `XAck` when `RetryCount >= MaxDeliveries`.
