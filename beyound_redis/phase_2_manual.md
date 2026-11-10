# Phase 2 Manual: Production Go Implementations — Real-Time Event & Analytics Systems

This manual guides you through building production-grade Go (`github.com/redis/go-redis/v9`) implementations for Redis's advanced subsystems: **Resilient Pub/Sub & Sharded Pub/Sub Fan-Out, Fault-Tolerant Redis Streams Workers with `XAUTOCLAIM` Crash Recovery & Dead-Letter Routing, HyperLogLog + Bitmap Cohort Analytics Pipelines, Geospatial Proximity Matchers, and Atomic Multi-Structure Lua Scripts**.

---

## Prerequisites & Go Environment Setup

Ensure your Redis cluster is running:
```bash
docker compose -f cache/docker-compose.yml up -d
```

### Initialize Go Module for Beyond-Redis Clients
```bash
mkdir -p beyound_redis/go-clients
cd beyound_redis/go-clients
go mod init beyond-redis-clients
go get github.com/redis/go-redis/v9@latest
go get github.com/google/uuid@latest
```

---

## Day 5: Resilient Pub/Sub & Sharded Fan-Out Gateway in Go

### 1. Engineering Challenges with Go Pub/Sub Clients
When using `rdb.Subscribe()` or `rdb.SSubscribe()` in production Go services, three subtle bugs frequently cause outages:
1. **Go Channel Buffer Exhaustion:** By default, `go-redis` creates a Go channel of size `100` (`ch := pubsub.Channel()`). If your consumer goroutine blocks for 200ms (e.g., writing to a slow WebSocket client) while 500 messages arrive, **`go-redis` drops incoming messages and logs an overflow warning**!
2. **Stalled TCP Connections:** Need periodic `PING` health checks (handled automatically by `pubsub.Channel()`, which sends heartbeats and reconnects transparently if the network drops).
3. **Keyspace Notification Parsing:** Listening to `__keyevent@0__:expired` requires dedicated non-blocking worker dispatch so expiration spikes don't drop events.

---

### 2. Implementation: Sharded Pub/Sub & Keyspace Expiration Listener (`pubsub_fanout.go`)

Create `beyound_redis/go-clients/pubsub_fanout.go`:

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type OrderNotification struct {
	OrderID   string    `json:"order_id"`
	UserID    string    `json:"user_id"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

// StartShardedSubscriber listens on a Redis 7.0+ Sharded Pub/Sub channel (SSUBSCRIBE)
// with custom buffer sizing and non-blocking worker pool dispatch.
func StartShardedSubscriber(ctx context.Context, rdb *redis.Client, shardChannel string, wg *sync.WaitGroup) {
	defer wg.Done()

	// Use SSubscribe for Redis 7+ slot-pinned Sharded Pub/Sub (zero cluster broadcast overhead)
	pubsub := rdb.SSubscribe(ctx, shardChannel)
	defer pubsub.Close()

	// Wait for subscription confirmation before publishing
	if _, err := pubsub.Receive(ctx); err != nil {
		log.Fatalf("Failed to subscribe to %s: %v", shardChannel, err)
	}

	// Configure Go channel buffer size (default 100 -> 4096) and health check interval
	msgCh := pubsub.Channel(
		redis.WithChannelSize(4096),
		redis.WithChannelHealthCheckInterval(3*time.Second),
	)

	fmt.Printf("📡 [SSUBSCRIBE] Listening on sharded channel '%s'...\n", shardChannel)

	for {
		select {
		case <-ctx.Done():
			fmt.Println("🛑 [SSUBSCRIBE] Shutting down subscriber cleanly.")
			return
		case msg, ok := <-msgCh:
			if !ok {
				return
			}
			var notif OrderNotification
			if err := json.Unmarshal([]byte(msg.Payload), &notif); err != nil {
				log.Printf("⚠️ Malformed payload on %s: %v", msg.Channel, err)
				continue
			}
			fmt.Printf("🔔 [FAN-OUT RECEIVED] Channel: %s | Order: %s | Status: %s | Latency: %v\n",
				msg.Channel, notif.OrderID, notif.Status, time.Since(notif.Timestamp))
		}
	}
}

// StartExpirationWatcher listens to Redis Keyspace Notifications for expired session/cart locks
func StartExpirationWatcher(ctx context.Context, rdb *redis.Client, wg *sync.WaitGroup) {
	defer wg.Done()

	// Ensure server has keyspace notifications enabled for expired events ('Ex')
	_ = rdb.ConfigSet(ctx, "notify-keyspace-events", "ExKEA").Err()

	pubsub := rdb.Subscribe(ctx, "__keyevent@0__:expired")
	defer pubsub.Close()

	msgCh := pubsub.Channel(redis.WithChannelSize(1024))
	fmt.Println("⏱️  [KEYSPACE WATCHER] Listening for expired keys on DB 0...")

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-msgCh:
			fmt.Printf("⌛ [KEY EXPIRED] Key '%s' expired! Triggering cleanup hook...\n", msg.Payload)
		}
	}
}

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// Shard tag {region:us-east} pins this channel to a deterministic CRC16 slot
	channel := "notifications:{region:us-east}:orders"

	wg.Add(2)
	go StartShardedSubscriber(ctx, rdb, channel, &wg)
	go StartExpirationWatcher(ctx, rdb, &wg)

	// Allow subscribers to initialize
	time.Sleep(200 * time.Millisecond)

	// 1. Publish 3 Sharded Pub/Sub messages via SPUBLISH
	for i := 1; i <= 3; i++ {
		event := OrderNotification{
			OrderID:   fmt.Sprintf("ORD-900%d", i),
			UserID:    fmt.Sprintf("usr_%d", i*10),
			Status:    "OUT_FOR_DELIVERY",
			Timestamp: time.Now(),
		}
		payload, _ := json.Marshal(event)
		receivers, err := rdb.SPublish(ctx, channel, payload).Result()
		if err != nil {
			log.Printf("SPublish error: %v", err)
		} else {
			fmt.Printf("🚀 [SPUBLISH] Sent %s to %d active shard subscriber(s)\n", event.OrderID, receivers)
		}
	}

	// 2. Create a temporary reservation key with a 1-second TTL to trigger Keyspace Notification
	_ = rdb.Set(ctx, "checkout:reservation:item_77", "locked_by_usr_10", 1*time.Second).Err()
	fmt.Println("⏳ Waiting 1.5s for reservation key to expire...")
	time.Sleep(1500 * time.Millisecond)

	cancel()
	wg.Wait()
}
```

---

## Day 6: Durable Event Processing with Redis Streams & Consumer Groups in Go

### 1. Architecture of a Production Redis Streams Worker

Unlike simple Pub/Sub, a production Redis Streams consumer must implement a **3-Stage Fault-Tolerant Loop**:

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                       PRODUCTION REDIS STREAM WORKER ARCHITECTURE                      │
├────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                        │
│  1. Live Consumer Loop (XREADGROUP ... >)                                              │
│     • Blocks for up to 2s waiting for new arrivals                                     │
│     • Appends with Approximate Trimming (MAXLEN ~ 50,000)                              │
│     • Processes business logic ──► Calls XACK on success                               │
│                                                                                        │
│  2. Background PEL Reaper / Janitor Goroutine (XAUTOCLAIM)                             │
│     • Runs every 5s scanning for messages idle > MinIdleTime (e.g. 15s)                │
│     • Transfers ownership of crashed workers' messages to this healthy worker          │
│                                                                                        │
│  3. Poison Pill / Dead-Letter Stream Router (XPENDING DeliveryCount Check)             │
│     • Before re-processing a claimed message, checks its PEL delivery counter          │
│     • If DeliveryCount > MaxRetries (e.g. 3):                                          │
│       Atomically writes message + error metadata to 'orders:dlq' and XACKs original!   │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

### 2. Implementation: Stream Producer, Consumer Group, `XAUTOCLAIM` Janitor & DLQ (`stream_worker.go`)

Create `beyound_redis/go-clients/stream_worker.go`:

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	StreamKey     = "payments:stream"
	DLQStreamKey  = "payments:dlq"
	ConsumerGroup = "payment-processors"
	MaxStreamLen  = 10000
	MaxDeliveries = 3
)

type StreamProcessor struct {
	rdb          *redis.Client
	consumerName string
}

func NewStreamProcessor(rdb *redis.Client, consumerName string) *StreamProcessor {
	return &StreamProcessor{rdb: rdb, consumerName: consumerName}
}

// InitConsumerGroup idempotently creates the stream and consumer group
func (sp *StreamProcessor) InitConsumerGroup(ctx context.Context) error {
	err := sp.rdb.XGroupCreateMkStream(ctx, StreamKey, ConsumerGroup, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

// ProducePaymentEvent appends a new event with approximate MAXLEN ~ trimming
func ProducePaymentEvent(ctx context.Context, rdb *redis.Client, paymentID string, amount float64, simulatePoison bool) (string, error) {
	return rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: StreamKey,
		MaxLen: MaxStreamLen,
		Approx: true, // Always use '~' approximate trimming for O(1) macro-node eviction!
		ID:     "*",
		Values: map[string]interface{}{
			"payment_id": paymentID,
			"amount":     amount,
			"poison":     simulatePoison,
			"created_at": time.Now().UTC().Format(time.RFC3339Nano),
		},
	}).Result()
}

// processMessage executes business logic; returns error if message fails processing
func (sp *StreamProcessor) processMessage(msg redis.XMessage) error {
	paymentID, _ := msg.Values["payment_id"].(string)
	poison, _ := msg.Values["poison"].(string)

	if poison == "1" || poison == "true" {
		return fmt.Errorf("simulated gateway failure for poison payment %s", paymentID)
	}

	fmt.Printf("✅ [%s] Processed Payment %s (Stream ID: %s)\n", sp.consumerName, paymentID, msg.ID)
	return nil
}

// routeToDLQ moves a poison message that exceeded MaxDeliveries to the Dead-Letter Stream
func (sp *StreamProcessor) routeToDLQ(ctx context.Context, msg redis.XMessage, deliveryCount int64, reason string) {
	_, err := sp.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: DLQStreamKey,
			MaxLen: MaxStreamLen,
			Approx: true,
			ID:     "*",
			Values: map[string]interface{}{
				"original_id":    msg.ID,
				"payment_id":     msg.Values["payment_id"],
				"delivery_count": deliveryCount,
				"failure_reason": reason,
				"failed_at":      time.Now().UTC().Format(time.RFC3339Nano),
			},
		})
		// Acknowledge in primary stream so it leaves the PEL permanently
		pipe.XAck(ctx, StreamKey, ConsumerGroup, msg.ID)
		return nil
	})
	if err != nil {
		log.Printf("⚠️ Failed to move message %s to DLQ: %v", msg.ID, err)
		return
	}
	fmt.Printf("☠️  [DLQ ROUTED] Message %s (%v) moved to '%s' after %d failed deliveries!\n",
		msg.ID, msg.Values["payment_id"], DLQStreamKey, deliveryCount)
}

// RunLiveConsumer reads new messages ('>') and acknowledges them upon completion
func (sp *StreamProcessor) RunLiveConsumer(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			streams, err := sp.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    ConsumerGroup,
				Consumer: sp.consumerName,
				Streams:  []string{StreamKey, ">"},
				Count:    10,
				Block:    1 * time.Second,
			}).Result()

			if err != nil {
				if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) {
					continue
				}
				log.Printf("[%s] XReadGroup error: %v", sp.consumerName, err)
				continue
			}

			for _, stream := range streams {
				for _, msg := range stream.Messages {
					if err := sp.processMessage(msg); err != nil {
						// Leave unacknowledged in PEL so XAutoClaim janitor handles retries/DLQ
						fmt.Printf("⚠️ [%s] Failed ID %s: %v (Left in PEL for retry)\n", sp.consumerName, msg.ID, err)
						continue
					}
					_ = sp.rdb.XAck(ctx, StreamKey, ConsumerGroup, msg.ID).Err()
				}
			}
		}
	}
}

// RunAutoClaimJanitor periodically rescues stuck PEL messages via XAUTOCLAIM
func (sp *StreamProcessor) RunAutoClaimJanitor(ctx context.Context, minIdle time.Duration, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(minIdle / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cursor := "0-0"
			for {
				msgs, nextCursor, err := sp.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
					Stream:   StreamKey,
					Group:    ConsumerGroup,
					Consumer: sp.consumerName,
					MinIdle:  minIdle,
					Start:    cursor,
					Count:    20,
				}).Result()
				if err != nil || len(msgs) == 0 {
					break
				}

				for _, msg := range msgs {
					// Query XPENDING for this specific ID to inspect its delivery count
					pendingInfo, _ := sp.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
						Stream: StreamKey,
						Group:  ConsumerGroup,
						Start:  msg.ID,
						End:    msg.ID,
						Count:  1,
					}).Result()

					var deliveries int64 = 1
					if len(pendingInfo) > 0 {
						deliveries = pendingInfo[0].RetryCount
					}

					fmt.Printf("🚑 [XAUTOCLAIM] %s claimed stuck message %s (Deliveries: %d)\n",
						sp.consumerName, msg.ID, deliveries)

					if deliveries >= MaxDeliveries {
						sp.routeToDLQ(ctx, msg, deliveries, "Max retry attempts exceeded")
						continue
					}

					if err := sp.processMessage(msg); err == nil {
						_ = sp.rdb.XAck(ctx, StreamKey, ConsumerGroup, msg.ID).Err()
					}
				}

				if nextCursor == "0-0" {
					break
				}
				cursor = nextCursor
			}
		}
	}
}

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	ctx, cancel := context.WithCancel(context.Background())

	// Reset streams for clean demonstration
	_ = rdb.Del(ctx, StreamKey, DLQStreamKey).Err()

	worker1 := NewStreamProcessor(rdb, "worker-1")
	_ = worker1.InitConsumerGroup(ctx)

	var wg sync.WaitGroup
	wg.Add(2)
	go worker1.RunLiveConsumer(ctx, &wg)
	// Configure fast 400ms MinIdle so we can watch XAUTOCLAIM and DLQ routing live
	go worker1.RunAutoClaimJanitor(ctx, 400*time.Millisecond, &wg)

	// Produce 3 healthy payments and 1 poison payment
	_, _ = ProducePaymentEvent(ctx, rdb, "PAY-101", 25.00, false)
	_, _ = ProducePaymentEvent(ctx, rdb, "PAY-102", 99.50, false)
	_, _ = ProducePaymentEvent(ctx, rdb, "PAY-POISON-666", 5000.00, true)
	_, _ = ProducePaymentEvent(ctx, rdb, "PAY-103", 14.99, false)

	// Wait 2 seconds to observe 3 XAUTOCLAIM retries followed by automatic DLQ routing
	time.Sleep(2200 * time.Millisecond)
	cancel()
	wg.Wait()

	dlqLen, _ := rdb.XLen(ctx, DLQStreamKey).Result()
	fmt.Printf("\n📊 Final Verification: Dead-Letter Stream '%s' contains %d poison message(s)\n", DLQStreamKey, dlqLen)
}
```

---

## Day 7: High-Throughput Analytics Engine (HyperLogLog + Bitmaps + Bitfields) in Go

### 1. Combining HLL, Bitmaps, and Bitfields in a Pipelined Telemetry Engine
In production analytics services, each incoming user event should update multiple probabilistic/bit structures in a **single network round-trip (`rdb.Pipelined`)**:
- **HyperLogLog (`PFADD`):** Tracks unique visitors for arbitrary string identifiers (e.g., anonymous device UUIDs, IP addresses) in 12 KB.
- **Bitmaps (`SETBIT`):** Tracks exact boolean activity per dense integer `UserID` (e.g., logged-in users) to support bitwise `AND`/`OR` cohort retention funnels.
- **Bitfields (`BITFIELD`):** Packs small integer counters per `UserID` (e.g., daily API quota usage or feature engagements) with saturating overflow (`SAT`).

---

### 2. Implementation: Real-Time Cohort & Cardinality Engine (`analytics_engine.go`)

Create `beyound_redis/go-clients/analytics_engine.go`:

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type AnalyticsEngine struct {
	rdb *redis.Client
}

func NewAnalyticsEngine(rdb *redis.Client) *AnalyticsEngine {
	return &AnalyticsEngine{rdb: rdb}
}

// RecordUserActivity pipelines HLL UV tracking, Bitmap DAU bit-setting, and Bitfield counter increment
// in a SINGLE network round-trip!
func (a *AnalyticsEngine) RecordUserActivity(ctx context.Context, dateStr string, userID int64, deviceUUID string) error {
	hllKey := fmt.Sprintf("analytics:{cohort}:uv:%s", dateStr)
	dauBitmapKey := fmt.Sprintf("analytics:{cohort}:dau:%s", dateStr)
	quotaBitfieldKey := fmt.Sprintf("analytics:{cohort}:clicks:%s", dateStr)

	_, err := a.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		// 1. HyperLogLog: Estimate unique device UUIDs (12KB max memory)
		pipe.PFAdd(ctx, hllKey, deviceUUID)

		// 2. Bitmap: Flip exact bit for UserID (1 = active on dateStr)
		pipe.SetBit(ctx, dauBitmapKey, userID, 1)

		// 3. Bitfield: Increment 8-bit unsigned counter (0..255) for UserID with SATURATION overflow
		offset := fmt.Sprintf("#%d", userID)
		pipe.BitField(ctx, quotaBitfieldKey, "OVERFLOW", "SAT", "INCRBY", "u8", offset, 1)
		return nil
	})
	return err
}

// GetMultiDayUniqueVisitors merges daily HLL registers into a temporary union key and returns cardinality
func (a *AnalyticsEngine) GetMultiDayUniqueVisitors(ctx context.Context, dates ...string) (int64, error) {
	sourceKeys := make([]string, len(dates))
	for i, d := range dates {
		// Notice the {cohort} hash tag: ensures all keys map to the same Redis Cluster slot!
		sourceKeys[i] = fmt.Sprintf("analytics:{cohort}:uv:%s", d)
	}
	destKey := "analytics:{cohort}:uv:merged_temp"

	pipe := a.rdb.Pipeline()
	pipe.PFMerge(ctx, destKey, sourceKeys...)
	countCmd := pipe.PFCount(ctx, destKey)
	pipe.Expire(ctx, destKey, 60*time.Second)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return 0, err
	}
	return countCmd.Val(), nil
}

// GetConsecutiveRetainedUsers computes the exact count of users active on ALL specified dates via BITOP AND
func (a *AnalyticsEngine) GetConsecutiveRetainedUsers(ctx context.Context, dates ...string) (int64, error) {
	sourceKeys := make([]string, len(dates))
	for i, d := range dates {
		sourceKeys[i] = fmt.Sprintf("analytics:{cohort}:dau:%s", d)
	}
	destKey := "analytics:{cohort}:dau:retained_temp"

	pipe := a.rdb.Pipeline()
	pipe.BitOpAnd(ctx, destKey, sourceKeys...)
	bitCountCmd := pipe.BitCount(ctx, destKey, nil)
	pipe.Del(ctx, destKey)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return 0, err
	}
	return bitCountCmd.Val(), nil
}

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	ctx := context.Background()

	engine := NewAnalyticsEngine(rdb)
	days := []string{"2026-11-06", "2026-11-07", "2026-11-08"}

	// Clean previous keys
	for _, d := range days {
		_ = rdb.Del(ctx,
			fmt.Sprintf("analytics:{cohort}:uv:%s", d),
			fmt.Sprintf("analytics:{cohort}:dau:%s", d),
			fmt.Sprintf("analytics:{cohort}:clicks:%s", d),
		).Err()
	}

	// Simulate 1,000 users on Day 1 (IDs 1..1000)
	for id := int64(1); id <= 1000; id++ {
		_ = engine.RecordUserActivity(ctx, days[0], id, fmt.Sprintf("dev-%d", id))
	}
	// Simulate 800 users on Day 2 (IDs 401..1200) -> 600 overlap with Day 1
	for id := int64(401); id <= 1200; id++ {
		_ = engine.RecordUserActivity(ctx, days[1], id, fmt.Sprintf("dev-%d", id))
	}
	// Simulate 500 users on Day 3 (IDs 801..1300) -> Users 801..1000 (200 users) were active ALL 3 DAYS!
	for id := int64(801); id <= 1300; id++ {
		_ = engine.RecordUserActivity(ctx, days[2], id, fmt.Sprintf("dev-%d", id))
	}

	// 1. Query HLL 3-Day Deduplicated Unique Visitors (True union: IDs 1..1300 = 1,300 unique users)
	totalUV, _ := engine.GetMultiDayUniqueVisitors(ctx, days...)
	fmt.Printf("📈 [HyperLogLog PFMERGE] 3-Day Deduplicated Unique Visitors: %d (True: 1300)\n", totalUV)

	// 2. Query Bitmap 3-Day Consecutive Retention Cohort (Users 801..1000 = 200 users)
	retained, _ := engine.GetConsecutiveRetainedUsers(ctx, days...)
	fmt.Printf("🎯 [Bitmap BITOP AND]    3-Day Consecutive Retained Users:   %d (Exact: 200)\n", retained)
}
```

---

## Day 8: Real-Time Location & Proximity Matching Engine in Go

### Implementation: Geospatial Fleet Tracker & Atomic Dispatch (`geo_dispatch.go`)

Create `beyound_redis/go-clients/geo_dispatch.go`:

```go
package main

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

const FleetGeoKey = "fleet:{sf}:available_drivers"

type DriverMatch struct {
	DriverID   string
	DistanceKm float64
	Longitude  float64
	Latitude   float64
}

type FleetDispatcher struct {
	rdb *redis.Client
}

func NewFleetDispatcher(rdb *redis.Client) *FleetDispatcher {
	return &FleetDispatcher{rdb: rdb}
}

// UpdateDriverLocations ingests real-time GPS coordinates into the ZSET Geo index
func (f *FleetDispatcher) UpdateDriverLocations(ctx context.Context, locations ...*redis.GeoLocation) error {
	return f.rdb.GeoAdd(ctx, FleetGeoKey, locations...).Err()
}

// FindNearestDrivers queries the 52-bit Geohash ZSET using modern GEOSEARCH (BYRADIUS + ASC)
func (f *FleetDispatcher) FindNearestDrivers(ctx context.Context, riderLon, riderLat, radiusKm float64, maxCount int) ([]DriverMatch, error) {
	locations, err := f.rdb.GeoSearchLocation(ctx, FleetGeoKey, &redis.GeoSearchLocationQuery{
		GeoSearchQuery: redis.GeoSearchQuery{
			Longitude:  riderLon,
			Latitude:   riderLat,
			Radius:     radiusKm,
			RadiusUnit: "km",
			Sort:       "ASC", // Nearest drivers first
			Count:      maxCount,
		},
		WithCoord: true,
		WithDist:  true,
	}).Result()
	if err != nil {
		return nil, err
	}

	matches := make([]DriverMatch, 0, len(locations))
	for _, loc := range locations {
		matches = append(matches, DriverMatch{
			DriverID:   loc.Name,
			DistanceKm: loc.Dist,
			Longitude:  loc.Longitude,
			Latitude:   loc.Latitude,
		})
	}
	return matches, nil
}

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	ctx := context.Background()

	_ = rdb.Del(ctx, FleetGeoKey).Err()
	dispatcher := NewFleetDispatcher(rdb)

	// Ingest 4 San Francisco drivers
	_ = dispatcher.UpdateDriverLocations(ctx,
		&redis.GeoLocation{Name: "driver:101_sedan", Longitude: -122.4194, Latitude: 37.7749}, // Market St
		&redis.GeoLocation{Name: "driver:102_suv", Longitude: -122.4089, Latitude: 37.7833},   // Union Square
		&redis.GeoLocation{Name: "driver:103_ev", Longitude: -122.4313, Latitude: 37.7609},    // Castro
		&redis.GeoLocation{Name: "driver:104_far", Longitude: -122.2712, Latitude: 37.8044},   // Oakland (13km away)
	)

	// Rider requests pickup near Civic Center (-122.4167, 37.7793) within 3.0 km radius
	matches, _ := dispatcher.FindNearestDrivers(ctx, -122.4167, 37.7793, 3.0, 5)

	fmt.Println("🚕 Nearest Available Drivers within 3.0 km:")
	for i, m := range matches {
		fmt.Printf("   %d. %-18s | Distance: %.2f km | Coords: (%.4f, %.4f)\n",
			i+1, m.DriverID, m.DistanceKm, m.Longitude, m.Latitude)
	}
}
```

---

## Day 9: Atomic Multi-Structure Transactions & Lua Scripting

### 1. Why `MULTI/EXEC` Alone Cannot Conditionally Branch
In Redis, a `MULTI ... EXEC` block queues commands and runs them sequentially, **but you cannot read the result of command 1 inside `MULTI` to decide whether to run command 2**.
- **Option A (`WATCH` + `MULTI/EXEC`):** Optimistic concurrency control. If a watched key is modified by another client before `EXEC`, the entire transaction aborts and returns `redis.TxFailedErr`, requiring an application-side retry loop (high latency under contention).
- **Option B (Server-Side Lua Script via `redis.NewScript`):** Runs atomically inside the single-threaded Redis engine with **zero contention retries** and full conditional branching across Geo ZSETs, Streams, and HLLs!

---

### 2. Implementation: Atomic Geo-Claim + Stream Emit + HLL Counter in Lua (`atomic_dispatch_lua.go`)

Consider a ride-hailing dispatch: if two riders near Market Street request a ride at the same millisecond, a naive `"1. GEOSEARCH in Go -> 2. ZREM in Go"` sequence has a **TOCTOU (Time-of-Check to Time-of-Use) race condition** where both riders are assigned the exact same driver!

With a Lua script, finding the nearest driver, removing them from the available pool (`ZREM`), appending the assignment to the dispatch `Stream` (`XADD`), and recording the rider in the daily `HyperLogLog` (`PFADD`) happens as **one indivisible atomic operation**:

Create `beyound_redis/go-clients/atomic_dispatch_lua.go`:

```go
package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"
)

// Atomic Lua Script:
// KEYS[1] = Geo ZSET of available drivers   ("fleet:{sf}:available")
// KEYS[2] = Dispatch Audit Stream           ("fleet:{sf}:dispatches")
// KEYS[3] = Daily Unique Matched Riders HLL ("fleet:{sf}:hll:riders")
// ARGV[1] = Rider ID
// ARGV[2] = Rider Longitude
// ARGV[3] = Rider Latitude
// ARGV[4] = Search Radius (km)
var atomicDispatchScript = redis.NewScript(`
	local matches = redis.call("GEOSEARCH", KEYS[1],
		"FROMLONLAT", ARGV[2], ARGV[3],
		"BYRADIUS", ARGV[4], "km",
		"ASC", "COUNT", 1, "WITHDIST")

	if #matches == 0 then
		return nil
	end

	local driverID = matches[1][1]
	local distanceKm = matches[1][2]

	-- 1. Atomically remove driver from available pool so no concurrent rider can claim them
	redis.call("ZREM", KEYS[1], driverID)

	-- 2. Append durable dispatch event to Redis Stream
	local streamID = redis.call("XADD", KEYS[2], "MAXLEN", "~", 10000, "*",
		"rider_id", ARGV[1],
		"driver_id", driverID,
		"distance_km", distanceKm)

	-- 3. Record unique matched rider in HyperLogLog
	redis.call("PFADD", KEYS[3], ARGV[1])

	return {driverID, distanceKm, streamID}
`)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	ctx := context.Background()

	geoKey := "fleet:{sf}:available"
	streamKey := "fleet:{sf}:dispatches"
	hllKey := "fleet:{sf}:hll:riders"
	_ = rdb.Del(ctx, geoKey, streamKey, hllKey).Err()

	// Seed ONLY 2 drivers in San Francisco
	_ = rdb.GeoAdd(ctx, geoKey,
		&redis.GeoLocation{Name: "driver:alpha", Longitude: -122.4194, Latitude: 37.7749},
		&redis.GeoLocation{Name: "driver:bravo", Longitude: -122.4089, Latitude: 37.7833},
	).Err()

	// Compete 5 concurrent riders for those 2 available drivers!
	var wg sync.WaitGroup
	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go func(riderNum int) {
			defer wg.Done()
			riderID := fmt.Sprintf("rider_%d", riderNum)
			res, err := atomicDispatchScript.Run(ctx, rdb,
				[]string{geoKey, streamKey, hllKey},
				riderID, "-122.4180", "37.7760", "5.0",
			).Slice()

			if err == redis.Nil || len(res) == 0 {
				fmt.Printf("❌ [%s] No available drivers left in radius!\n", riderID)
				return
			}
			fmt.Printf("🎉 [%s] Atomically claimed %v (%v km away) | StreamID: %v\n",
				riderID, res[0], res[1], res[2])
		}(i)
	}
	wg.Wait()
}
```

---

## Phase 2 Mastery Checklist

- [ ] **Q: Why can `go-redis` Pub/Sub drop messages even if the network connection to Redis is healthy?**
  - *A:* If the Go application's consumer loop is slow and the internal Go channel buffer (default size 100) fills up, `go-redis` drops incoming messages to prevent blocking the socket reader. Always use `redis.WithChannelSize(N)` and dispatch slow work asynchronously.

- [ ] **Q: How does a production Redis Streams consumer recover from a worker pod crash (`SIGKILL` / OOM)?**
  - *A:* Unacknowledged messages remain in the consumer group's Pending Entries List (PEL). Surviving workers run a periodic `XAUTOCLAIM` background loop that scans for messages idle longer than `MinIdle` and transfers ownership to themselves.

- [ ] **Q: How do you prevent a poison message from causing an infinite `XAUTOCLAIM` loop across workers?**
  - *A:* Every time a message is delivered or claimed via `XAUTOCLAIM`, Redis increments its PEL `delivery_count` (`RetryCount`). When `RetryCount >= MaxDeliveries`, the worker writes the message to a Dead-Letter Stream (`DLQ`) and calls `XACK` on the primary stream.

- [ ] **Q: Why must keys passed to `PFMERGE`, `BITOP`, or a multi-key Lua script share a `{hash_tag}`?**
  - *A:* In Redis Cluster, multi-key commands require all target keys to reside on the same hash slot (`CRC16(tag) % 16384`), otherwise Redis rejects the command with a `CROSSSLOT` error.
