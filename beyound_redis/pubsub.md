# Redis Pub/Sub Architecture, Internals & Hands-On Guide

This manual follows the natural architectural progression: **Problem Statement $\rightarrow$ Core Architecture & Internal C Data Structures $\rightarrow$ Routing Mechanics (Exact, Pattern, Sharded) $\rightarrow$ Delivery Guarantees & Backpressure Hazards $\rightarrow$ Keyspace Notifications $\rightarrow$ Performance Tuning $\rightarrow$ Production Golang Implementations**.

---

## 1. Problem Statement

1. **Polling Overhead vs. Instant Push Fan-Out**: When thousands of microservice instances, WebSocket gateways, or live chat servers need to react immediately to state changes (e.g., L1 local cache invalidation, live sports scores, order status transitions, or typing indicators), polling a database or Redis key every 100ms creates massive CPU and network waste. **Redis Pub/Sub** solves this via an ultra-low-latency ($< 0.5\text{ ms}$) in-memory **Push Broadcast Bus** where publishers decouple completely from subscribers.
2. **Cluster Broadcast Amplification Bottleneck**: In traditional Redis Cluster, publishing a message to one node broadcasts that message to *every* node in the cluster ($O(N_{\text{nodes}})$ network amplification), saturating the cluster bus. **Redis 7.0+ Sharded Pub/Sub (`SPUBLISH` / `SSUBSCRIBE`)** solves this by hashing channel names to specific CRC16 hash slots, isolating traffic strictly to the shard owning that channel.

---

## 2. Architecture & Internal C Components

Unlike standard Redis keys (`STRING`, `HASH`, `STREAM`), **Pub/Sub channels do not live in the database keyspace (`db->dict`)**. They have no TTL, cannot be inspected via `TYPE` or `GET`, and consume **zero bytes of memory** when no clients are actively subscribed.

Inside the Redis C source code (`server.h` / `pubsub.c`), `struct redisServer` maintains three dedicated structures:

```c
struct redisServer {
    dict *pubsub_channels;        // 1. Exact Channels: Hash Table mapping Channel (SDS) -> Linked List of *client
    list *pubsub_patterns;        // 2. Glob Patterns: Doubly-linked list of pubsubPattern structs (or dict of lists)
    dict *pubsubshard_channels;   // 3. Sharded Channels (Redis 7+): Slot-pinned Hash Table for Cluster mode
};
```

```
                         [ Publisher Client ]
                                  │
                     PUBLISH "orders:us-east" "{...}"
                                  │
                                  ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                              REDIS SERVER EVENT LOOP                                   │
│                                                                                        │
│  Step 1: O(1) Dict Lookup in server.pubsub_channels["orders:us-east"]                  │
│  ┌────────────────────┐                                                                │
│  │ "orders:us-east"   │ ──► [ *client_1 ] ◄──► [ *client_2 ] ◄──► [ *client_3 ]        │
│  └────────────────────┘           │                  │                  │              │
│                                   ▼                  ▼                  ▼              │
│                           (TCP Output Buf)   (TCP Output Buf)   (TCP Output Buf)       │
│                                                                                        │
│  Step 2: O(P) Linear Glob Evaluation across server.pubsub_patterns                     │
│  ┌────────────────────┐                                                                │
│  │ Pattern: "orders:*"│ ──► Matches "orders:us-east"! ──► Writes to [ *client_4 ]      │
│  ├────────────────────┤                                                                │
│  │ Pattern: "users:*" │ ──► No Match                                                   │
│  └────────────────────┘                                                                │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

### Core Components:
- **Publisher**: Any standard Redis client executing `PUBLISH` or `SPUBLISH`. Does not enter a special state.
- **Subscriber Mode Client**: Once a client executes `SUBSCRIBE`, `PSUBSCRIBE`, or `SSUBSCRIBE`, its connection enters **Stateful Subscriber Mode**. In RESP2, this connection can no longer issue standard commands like `GET` or `SET`—it can only issue `[P|S]SUBSCRIBE`, `[P|S]UNSUBSCRIBE`, `PING`, `RESET`, or `QUIT` (in RESP3, multiplexed commands are supported via push frames).
- **Exact Channel (`SUBSCRIBE`)**: Routed via $O(1)$ hash table lookup in `server.pubsub_channels`.
- **Pattern Subscription (`PSUBSCRIBE`)**: Glob-style pattern (e.g., `orders:*`, `chat:room:[0-9]`). Evaluated against every published channel name in $O(P)$ time.
- **Sharded Channel (`SSUBSCRIBE` - Redis 7.0+)**: Hashed via `CRC16(channel) % 16384` to a specific cluster slot.

---

## 3. Message Routing & Subscription Modes

| Mode | Subscribe Command | Publish Command | Lookup Complexity | Cluster Behavior |
| :--- | :--- | :--- | :--- | :--- |
| **Exact Channel** | `SUBSCRIBE ch1 ch2` | `PUBLISH ch1 msg` | $O(1) + O(N_{\text{subs}})$ | Broadcasts to **all** cluster nodes |
| **Glob Pattern** | `PSUBSCRIBE news:*` | `PUBLISH news:tech msg`| $O(P_{\text{patterns}}) + O(N_{\text{matched}})$| Broadcasts to **all** cluster nodes |
| **Sharded Channel** | `SSUBSCRIBE {tag}:ch1`| `SPUBLISH {tag}:ch1 msg`| $O(1) + O(N_{\text{shard\_subs}})$ | Routed **only** to the slot owner shard |

### Why Excessive `PSUBSCRIBE` Patterns Are an Anti-Pattern
When you run `PUBLISH user:101:events "login"`, Redis first does an $O(1)$ hash lookup for `"user:101:events"`, and **then iterates through every pattern registered via `PSUBSCRIBE` on the server**, running `stringmatchlen()` on each.
- If 10,000 connected clients each register a unique pattern (`PSUBSCRIBE user:101:*`, `PSUBSCRIBE user:102:*`, ...), **every single `PUBLISH` command executes 10,000 glob string comparisons on the main Redis thread!**
- **Fix:** Always use exact channel names (`SUBSCRIBE user:101:events`) instead of per-entity wildcard patterns whenever the channel name is known in advance.

---

## 4. The Cluster Broadcast Bottleneck & Sharded Pub/Sub Solution

In a 50-node Redis Cluster:
```
[Standard PUBLISH in Redis Cluster — O(N_nodes) Amplification]
Publisher ──(PUBLISH ch "msg")──► Node 1
                                    ├──► Cluster Bus Broadcast ──► Node 2 (Processes on main thread)
                                    ├──► Cluster Bus Broadcast ──► Node 3 (Processes on main thread)
                                    └──► Cluster Bus Broadcast ──► Node 50 (Processes on main thread)
Result: Even if only 1 subscriber exists on Node 1, ALL 50 nodes spend CPU & network bandwidth processing the message!

[Redis 7.0+ Sharded Pub/Sub (SPUBLISH / SSUBSCRIBE) — O(1) Shard Isolation]
Publisher ──(SPUBLISH orders:{us} "msg")──► CRC16("orders:{us}") % 16384 = Slot 7120 (Node 12)
                                                  │
                                                  ├──► Delivered to local SSUBSCRIBE clients on Node 12
                                                  └──► Forwarded ONLY to Node 12's Read Replicas!
Result: Nodes 1..11 and 13..50 receive ZERO traffic. Pub/Sub throughput scales linearly with cluster size!
```

---

## 5. Delivery Guarantees, Fault Tolerance & The Slow Subscriber Hazard

### 1. `At-Most-Once` (Fire-and-Forget) Semantics
Redis Pub/Sub has **zero message persistence**:
- Messages are **never** written to RDB snapshots or AOF logs.
- If a subscriber disconnects for 100ms due to a network blip or pod restart, **all messages published during those 100ms are permanently lost** for that subscriber.
- When `PUBLISH` returns `(integer) 3`, it only means the message was copied into the in-memory TCP output buffers of 3 connected clients—it does **not** guarantee the clients' applications processed the message.
- *Rule of Thumb:* If losing a message causes financial, billing, or state corruption, use **Redis Streams (`XADD` / `XREADGROUP`)** or **Apache Kafka** instead.

---

### 2. The Slow Subscriber Hazard & Client Output Buffer Limits

What happens if a publisher emits 50,000 messages/sec (50 MB/sec), and one subscribed client is on a slow network or paused in garbage collection, reading only 1 MB/sec?

```
Publisher (50 MB/s) ──► Redis Server RAM: [ client->buf + client->reply (Growing by 49 MB/s!) ] ──► Slow Subscriber (1 MB/s)
```

Without protection, a single slow subscriber would consume all physical RAM on the Redis server and trigger the Linux OOM Killer!

#### How Redis Protects Itself (`client-output-buffer-limit pubsub`):
```redis
# Default Configuration in redis.conf:
client-output-buffer-limit pubsub 32mb 8mb 60
```
1. **Hard Limit (`32mb`):** The instant a subscriber's unread output buffer reaches **32 Megabytes**, Redis **forcibly closes and disconnects that subscriber connection** (`Client scheduled to be closed ASAP for overcoming of output buffer limits`).
2. **Soft Limit (`8mb 60`):** If a subscriber's unread output buffer stays continuously above **8 Megabytes for 60 seconds**, Redis forcibly disconnects it.

---

## 6. Redis Keyspace Notifications (`__keyspace@0__` & `__keyevent@0__`)

Redis can automatically publish Pub/Sub messages whenever keys are modified, evicted, or expired.

### 1. Configuration Flags (`notify-keyspace-events`)
Disabled by default (`""`). Enabled via `CONFIG SET notify-keyspace-events ExKEA`:
- **`K` (Keyspace channel):** Publishes the **event name** to channel `__keyspace@0__:<key_name>`.
  - *Use when watching a specific key:* `SUBSCRIBE __keyspace@0__:user:101` $\rightarrow$ receives `"set"`, `"del"`, `"expired"`.
- **`E` (Keyevent channel):** Publishes the **key name** to channel `__keyevent@0__:<event_name>`.
  - *Use when watching a specific event type across all keys:* `SUBSCRIBE __keyevent@0__:expired` $\rightarrow$ receives `"session:99"`, `"lock:order:42"`.
- **`x`:** Expired events.
- **`e`:** Evicted events (`maxmemory` reached).
- **`g$lshztx` or `A`:** Generic commands (`DEL`, `EXPIRE`), String (`$`), List (`l`), Set (`s`), Hash (`h`), ZSet (`z`), Stream (`t`).

### 2. Critical Caveat: Keyspace Expiration Timing Lag!
Redis does **not** emit `__keyevent@0__:expired` at the exact microsecond a key's TTL hits zero. It emits the event **when Redis actually deletes the key**:
1. Either when a client attempts to read the expired key (**Passive Expiration**), OR
2. When the background 10Hz cron job probabilistically samples and reclaims the key (**Active Expiration**).
> **Warning:** If you have 10 million expiring keys, active expiration sampling can lag by several seconds or minutes. Never rely on `__keyevent@0__:expired` as a precision real-time cron scheduler (use a Sorted Set `ZADD scheduled_jobs <unix_ts> <job_id>` polled by `ZRANGEBYSCORE` instead!).

---

## 7. Hands-On CLI Verification Labs

```bash
# Terminal 1: Subscribe to exact channel, pattern channel, and keyspace expiration channel
docker exec -it redis-master redis-cli
SUBSCRIBE chat:lobby __keyevent@0__:expired

# Terminal 2: Subscribe to a Redis 7 Sharded Channel
docker exec -it redis-master redis-cli
SSUBSCRIBE shard:{us-east}:alerts

# Terminal 3: Publish messages, trigger expiration, and inspect Pub/Sub state
docker exec -it redis-master redis-cli
PUBLISH chat:lobby "Welcome to the lobby!"
SPUBLISH shard:{us-east}:alerts "High CPU on node-4"
SET temp:otp:8812 "948102" EX 2

# Introspection Commands in Terminal 3:
PUBSUB CHANNELS *                     # List active normal channels
PUBSUB NUMSUB chat:lobby              # Count exact subscribers on chat:lobby
PUBSUB NUMPAT                         # Count active PSUBSCRIBE patterns
PUBSUB SHARDCHANNELS *                # List active Redis 7 sharded channels
PUBSUB SHARDNUMSUB shard:{us-east}:alerts
CLIENT LIST TYPE pubsub               # Inspect 'omem' (output buffer memory) of subscribers!
```

---

## 8. Hands-On Golang Implementations (`github.com/redis/go-redis/v9`)

Below are three production-grade Go patterns for Redis Pub/Sub:
1. **Multi-Pod L1 Cache Invalidation Bus** (solving Multi-Tier L1/L2 Cache Coherence).
2. **Sharded Pub/Sub High-Throughput Worker Pool** (preventing slow handlers from overflowing the `go-redis` channel buffer).
3. **Keyspace Expiration Listener with Distributed Lock Deduplication** (ensuring that if 5 microservice pods listen to `__keyevent@0__:expired`, only **one** pod executes the expiration callback!).

### 1. Multi-Pod L1 Cache Invalidation Bus (`pubsub_l1_invalidation.go`)

```go
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const InvalidationChannel = "cache:invalidation:bus"

// LocalL1Node represents an application pod holding an in-process L1 memory cache
// kept coherent across pods via Redis Pub/Sub.
type LocalL1Node struct {
	podID   string
	rdb     *redis.Client
	l1Store sync.Map
}

func NewLocalL1Node(podID string, rdb *redis.Client) *LocalL1Node {
	return &LocalL1Node{podID: podID, rdb: rdb}
}

// StartInvalidationSubscriber listens to the Redis Pub/Sub bus and evicts stale keys from local L1 RAM
func (n *LocalL1Node) StartInvalidationSubscriber(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	pubsub := n.rdb.Subscribe(ctx, InvalidationChannel)
	defer pubsub.Close()

	// Wait for subscription confirmation
	_, _ = pubsub.Receive(ctx)
	ch := pubsub.Channel(redis.WithChannelSize(2048))

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			evictedKey := msg.Payload
			n.l1Store.Delete(evictedKey)
			fmt.Printf("🧹 [%s] Evicted '%s' from local L1 memory via Pub/Sub broadcast\n", n.podID, evictedKey)
		}
	}
}

// UpdateKey updates L2 Redis and broadcasts an invalidation event to all sibling pods
func (n *LocalL1Node) UpdateKey(ctx context.Context, key, newVal string) error {
	pipe := n.rdb.Pipeline()
	pipe.Set(ctx, key, newVal, 10*time.Minute)
	pipe.Publish(ctx, InvalidationChannel, key)
	_, err := pipe.Exec(ctx)
	return err
}

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	ctx, cancel := context.WithCancel(context.Background())

	podA := NewLocalL1Node("Pod-A", rdb)
	podB := NewLocalL1Node("Pod-B", rdb)

	// Seed local L1 cache on both pods
	podA.l1Store.Store("product:99:price", "$199.00")
	podB.l1Store.Store("product:99:price", "$199.00")

	var wg sync.WaitGroup
	wg.Add(2)
	go podA.StartInvalidationSubscriber(ctx, &wg)
	go podB.StartInvalidationSubscriber(ctx, &wg)

	time.Sleep(150 * time.Millisecond)

	// Pod-A updates the price and publishes the invalidation event
	fmt.Println("📝 Pod-A updating 'product:99:price' to $149.00...")
	_ = podA.UpdateKey(ctx, "product:99:price", "$149.00")

	time.Sleep(200 * time.Millisecond)
	cancel()
	wg.Wait()
}
```

---

### 2. Sharded Pub/Sub with Non-Blocking Bounded Worker Pool (`pubsub_sharded_pool.go`)

If your subscriber executes slow I/O inside the `for msg := range pubsub.Channel()` loop, `go-redis`'s internal channel fills up and drops messages, or Redis's `32mb` output buffer fills up and drops the TCP connection. Always decouple the reader loop from processing using a bounded worker pool:

```go
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

func RunShardedConsumerWithPool(ctx context.Context, rdb *redis.Client, shardChannel string, numWorkers int) {
	pubsub := rdb.SSubscribe(ctx, shardChannel)
	defer pubsub.Close()

	_, _ = pubsub.Receive(ctx)

	// 1. Allocate a large internal Go channel buffer (8192 messages)
	ch := pubsub.Channel(
		redis.WithChannelSize(8192),
		redis.WithChannelHealthCheckInterval(3*time.Second),
	)

	// 2. Spawn N concurrent worker goroutines so slow handlers never block the socket reader!
	var workerWg sync.WaitGroup
	workQueue := make(chan *redis.Message, 8192)

	for i := 1; i <= numWorkers; i++ {
		workerWg.Add(1)
		go func(workerID int) {
			defer workerWg.Done()
			for msg := range workQueue {
				// Simulate downstream WebSocket push or HTTP webhook
				time.Sleep(10 * time.Millisecond)
				fmt.Printf("⚡ [Worker-%d] Processed Sharded Event on %s: %s\n",
					workerID, msg.Channel, msg.Payload)
			}
		}(i)
	}

	// 3. Fast non-blocking socket reader loop
	for {
		select {
		case <-ctx.Done():
			close(workQueue)
			workerWg.Wait()
			return
		case msg, ok := <-ch:
			if !ok {
				close(workQueue)
				workerWg.Wait()
				return
			}
			select {
			case workQueue <- msg:
			default:
				// Shed load gracefully or increment Prometheus counter if local queue is full
				fmt.Printf("⚠️ [BACKPRESSURE] Worker queue full, shedding message on %s\n", msg.Channel)
			}
		}
	}
}
```

---

### 3. Deduplicated Keyspace Expiration Handler Across Multiple Pods (`pubsub_keyspace_dedup.go`)

> **Classic Production Trap:** Because Redis Pub/Sub is a **broadcast** system, if you run 5 replicas of your Go service and all 5 subscribe to `__keyevent@0__:expired`, **all 5 pods receive the expiration event at the exact same millisecond!** If the handler sends an email ("Your cart expired!") or refunds inventory, the user gets 5 duplicate emails!
>
> **The Fix:** Combine the `__keyevent@0__:expired` Pub/Sub listener with a short-lived `SET NX EX` lock so exactly **one** pod claims the expiration event.

```go
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

func StartDeduplicatedExpirationWorker(ctx context.Context, rdb *redis.Client, podName string, wg *sync.WaitGroup) {
	defer wg.Done()

	pubsub := rdb.Subscribe(ctx, "__keyevent@0__:expired")
	defer pubsub.Close()
	_, _ = pubsub.Receive(ctx)

	ch := pubsub.Channel(redis.WithChannelSize(1024))

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			expiredKey := msg.Payload

			// Deduplicate across all listening pods using an atomic SETNX claim key!
			claimKey := fmt.Sprintf("exp_claim:%s", expiredKey)
			claimed, err := rdb.SetNX(ctx, claimKey, podName, 30*time.Second).Result()
			if err != nil || !claimed {
				// Another pod already claimed and processed this expiration event!
				fmt.Printf("⏭️  [%s] Skipping '%s' (already claimed by another pod)\n", podName, expiredKey)
				continue
			}

			fmt.Printf("🏆 [%s] WON CLAIM for expired key '%s'! Releasing reserved inventory...\n",
				podName, expiredKey)
		}
	}
}

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	ctx, cancel := context.WithCancel(context.Background())

	_ = rdb.ConfigSet(ctx, "notify-keyspace-events", "Ex").Err()

	// Simulate 3 concurrent microservice pods all listening to __keyevent@0__:expired
	var wg sync.WaitGroup
	for _, pod := range []string{"Pod-1", "Pod-2", "Pod-3"} {
		wg.Add(1)
		go StartDeduplicatedExpirationWorker(ctx, rdb, pod, &wg)
	}

	time.Sleep(150 * time.Millisecond)

	// Set a key that expires in 1 second
	_ = rdb.Set(ctx, "order:hold:ORD-5501", "2x_PS5_Console", 1*time.Second).Err()
	fmt.Println("⏳ Waiting 1.5s for 'order:hold:ORD-5501' to expire across 3 listening pods...")

	time.Sleep(1500 * time.Millisecond)
	cancel()
	wg.Wait()
}
```
