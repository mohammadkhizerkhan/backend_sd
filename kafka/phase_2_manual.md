# Phase 2 Manual: Go Producer/Consumer Basics, Delivery Guarantees & Error Handling

This manual guides you through building production-grade Kafka clients in Go against the 3-broker KRaft cluster established in Phase 0 and Phase 1. You will learn to construct **keyed JSON producers, consumer group workers with rebalance handling, delivery guarantees (at-most-once vs at-least-once), dead-letter topics (DLT/DLQ) with poison-pill resilience, and schema serialization with evolution**.

---

## Prerequisites & Environment Setup

Ensure your local 3-broker KRaft cluster is running:
```bash
docker compose -f kafka/docker-compose.yml up -d
docker compose -f kafka/docker-compose.yml ps
```

### Host Machine Connection Endpoints
When connecting from Go programs running on your host machine (outside Docker), use the `PLAINTEXT_HOST` listener addresses:
- **Broker 1**: `localhost:9092`
- **Broker 2**: `localhost:9093`
- **Broker 3**: `localhost:9094`
- **Bootstrap Servers**: `localhost:9092,localhost:9093,localhost:9094`

### Go Module Initialization
In your project directory or a dedicated client directory (e.g., `kafka/go-clients`), initialize a Go module and fetch `segmentio/kafka-go`:

```bash
mkdir -p kafka/go-clients
cd kafka/go-clients
go mod init kafka-clients
go get github.com/segmentio/kafka-go@latest
```

> [!NOTE]
> We primary use `github.com/segmentio/kafka-go` throughout Phase 2 because it is pure Go (no CGO or `librdkafka` C-library dependency), making it fast to compile, straightforward to debug, and idiomatic in Go concurrency. We also provide a direct architectural comparison with `confluent-kafka-go` in Section 6.

---

## Day 5: Basic Producer with Key-Based Partitioning

### 1. Architectural Concepts: How Go Clients Route Keys

In Kafka, partition selection for keyed messages is deterministic:
$$\text{Partition} = \text{murmur2}(\text{key}) \pmod{\text{total\_partitions}}$$

In `segmentio/kafka-go`, the default balancer is `kafka.Hash{}`, which implements the Java-compatible **Murmur2** hashing algorithm. This ensures that:
1. Records with the same key (`UserID`, `OrderID`, etc.) always land on the **exact same partition**.
2. Go producers, Java producers, and Kafka CLI tools route identical keys to identical partitions.
3. Strict per-key ordering is guaranteed.

```
                    ┌────────────────────────────────────────────────────────┐
                    │                      Go Producer                       │
                    │               kafka.Writer (Balancer: &kafka.Hash{})   │
                    └──────────────────────────┬─────────────────────────────┘
                                               │
                                 murmur2(key) % 3 partitions
                                               │
             ┌─────────────────────────────────┼─────────────────────────────────┐
             │ key="user_101" -> hash % 3 = 0  │ key="user_202" -> hash % 3 = 1  │ key="user_303" -> hash % 3 = 2
             ▼                                 ▼                                 ▼
      ┌─────────────┐                   ┌─────────────┐                   ┌─────────────┐
      │ Partition 0 │                   │ Partition 1 │                   │ Partition 2 │
      │  (Broker 1) │                   │  (Broker 2) │                   │  (Broker 3) │
      └─────────────┘                   └─────────────┘                   └─────────────┘
```

---

### 2. Hands-On Topic Preparation
Create a clean 3-partition topic named `orders`:
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092 \
  --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 3 \
  --config min.insync.replicas=2
```

---

### 3. Go Producer Implementation (`producer.go`)

Create `kafka/go-clients/producer.go`:

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
)

// OrderEvent represents our business payload
type OrderEvent struct {
	OrderID   string    `json:"order_id"`
	UserID    string    `json:"user_id"`
	Amount    float64   `json:"amount"`
	Currency  string    `json:"currency"`
	Timestamp time.Time `json:"timestamp"`
}

func main() {
	// 1. Configure the resilient Writer
	writer := &kafka.Writer{
		Addr: kafka.TCP(
			"localhost:9092",
			"localhost:9093",
			"localhost:9094",
		),
		Topic:        "orders",
		Balancer:     &kafka.Hash{}, // Murmur2 partitioner: preserves per-key ordering
		MaxAttempts:  5,
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond, // linger.ms equivalent
		RequiredAcks: kafka.RequireAll,     // acks=all (-1) for maximum durability
		Compression:  compress.Snappy,      // snappy compression for high throughput
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
	}
	defer func() {
		if err := writer.Close(); err != nil {
			log.Fatalf("Failed to close writer: %v", err)
		}
	}()

	fmt.Println("🚀 Publishing keyed order events...")

	// 2. Prepare sample keyed records distributed across all 3 partitions
	orders := []OrderEvent{
		{OrderID: "ord-1001", UserID: "user_alpha", Amount: 49.99, Currency: "USD", Timestamp: time.Now()}, // -> Partition 2
		{OrderID: "ord-1002", UserID: "user_beta", Amount: 120.00, Currency: "USD", Timestamp: time.Now()},  // -> Partition 1
		{OrderID: "ord-1003", UserID: "user_omega", Amount: 15.50, Currency: "USD", Timestamp: time.Now()}, // -> Partition 0
		{OrderID: "ord-1004", UserID: "user_gamma", Amount: 300.25, Currency: "USD", Timestamp: time.Now()}, // -> Partition 1
		{OrderID: "ord-1005", UserID: "user_beta", Amount: 89.90, Currency: "USD", Timestamp: time.Now()},  // -> Partition 1
		{OrderID: "ord-1006", UserID: "user_omega", Amount: 210.00, Currency: "USD", Timestamp: time.Now()},// -> Partition 0
		{OrderID: "ord-1007", UserID: "user_delta", Amount: 12.00, Currency: "USD", Timestamp: time.Now()}, // -> Partition 2
		{OrderID: "ord-1008", UserID: "user_gamma", Amount: 45.00, Currency: "USD", Timestamp: time.Now()}, // -> Partition 1
		{OrderID: "ord-1009", UserID: "user_alpha", Amount: 75.20, Currency: "USD", Timestamp: time.Now()}, // -> Partition 2
		{OrderID: "ord-1010", UserID: "user_3", Amount: 500.00, Currency: "USD", Timestamp: time.Now()},     // -> Partition 0
	}

	messages := make([]kafka.Message, 0, len(orders))
	for _, order := range orders {
		payload, err := json.Marshal(order)
		if err != nil {
			log.Fatalf("JSON marshal error: %v", err)
		}

		messages = append(messages, kafka.Message{
			Key:   []byte(order.UserID), // Key determines target partition
			Value: payload,
			Headers: []kafka.Header{
				{Key: "event_type", Value: []byte("OrderCreated")},
				{Key: "producer_version", Value: []byte("v1.0.0")},
			},
			Time: order.Timestamp,
		})
	}

	// 3. Synchronously write batch to Kafka
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := writer.WriteMessages(ctx, messages...); err != nil {
		log.Fatalf("Failed to write messages: %v", err)
	}

	fmt.Printf("✅ Successfully published %d keyed messages to 'orders'\n", len(messages))
}
```

---

### 4. Verification Exercise: Key Distribution

Run the producer:
```bash
go run kafka/go-clients/producer.go
```

Inspect message distribution across partitions using `kafka-console-consumer`:
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:29092 \
  --topic orders \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true \
  --property key.separator=" | Partition: "
```

#### Verification Checklist:
1. Open Kafka UI at **[http://localhost:8080](http://localhost:8080)** $\rightarrow$ Topics $\rightarrow$ `orders` $\rightarrow$ Messages.
2. Verify that every single message with `Key = "user_alpha"` landed on the exact same partition in strictly increasing offset sequence.
3. Verify that `user_beta` and `user_gamma` consistently map to their own dedicated partitions.

---

## Day 6: Basic Consumer & Consumer Groups

### 1. Architectural Concepts: Standalone vs Group Consumers

Kafka provides two distinct consumer models:
1. **Standalone Consumer (Manual Assignment)**:
   - Explicitly assigned to a fixed `(Topic, Partition)` tuple.
   - Does **not** coordinate with other consumers, send heartbeats, or trigger rebalances.
   - Ideal for specific offset replays, log inspections, or dedicated partition ingestion.
2. **Consumer Group (`GroupID`)**:
   - Multiple consumer instances collaborate under a shared `GroupID`.
   - The Kafka **Group Coordinator** dynamically balances topic partitions across active members.
   - Automatic failover: if one instance crashes, the coordinator detects heartbeat loss and rebalances its partitions to surviving workers.

```
Topic: orders (3 Partitions: P0, P1, P2)

[Scenario A: 1 Active Consumer]
Consumer Instance 1 ──► [P0, P1, P2]

[Scenario B: 2 Active Consumers (Rebalance 1)]
Consumer Instance 1 ──► [P0, P1]
Consumer Instance 2 ──► [P2]

[Scenario C: 3 Active Consumers (Rebalance 2 - Optimal Parallelism)]
Consumer Instance 1 ──► [P0]
Consumer Instance 2 ──► [P1]
Consumer Instance 3 ──► [P2]
```

---

### 2. Part A: Standalone Reader (Manual Partition Assignment)

Create `kafka/go-clients/consumer_standalone.go`:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Directly bind to Partition 0 without a Consumer Group
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		Topic:     "orders",
		Partition: 0, // Direct assignment: bypasses group coordination
		MinBytes:  1,
		MaxBytes:  10e6, // 10MB
	})
	defer reader.Close()

	// Set reading offset (e.g. from beginning)
	if err := reader.SetOffset(kafka.FirstOffset); err != nil {
		log.Fatalf("Failed to set offset: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	fmt.Println("🔍 Standalone Reader bound exclusively to 'orders' Partition 0...")

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				fmt.Println("\n🛑 Standalone Reader shutting down gracefully...")
				break
			}
			log.Printf("Read error: %v", err)
			continue
		}

		fmt.Printf("[P0 @ Offset %d] Key: %-12s | Value: %s\n",
			msg.Offset, string(msg.Key), string(msg.Value))
	}
}
```

---

### 3. Part B: Scalable Consumer Group Worker (`consumer_group.go`)

Now build a consumer group member supporting graceful shutdown, context cancellation, and rebalance survival.

Create `kafka/go-clients/consumer_group.go`:

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run consumer_group.go <worker-id>")
		os.Exit(1)
	}
	workerID := os.Args[1]

	// Configure consumer group reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{
			"localhost:9092",
			"localhost:9093",
			"localhost:9094",
		},
		GroupID:        "order-processing-group", // Shared group ID
		Topic:          "orders",
		MinBytes:       10e3,            // 10KB
		MaxBytes:       10e6,            // 10MB
		CommitInterval: 1 * time.Second, // Interval for periodic background commits if using ReadMessage
		StartOffset:    kafka.FirstOffset,
		// Heartbeat and rebalance timeouts
		HeartbeatInterval: 3 * time.Second,
		SessionTimeout:   10 * time.Second,
		RebalanceTimeout: 30 * time.Second,
	})

	// Trap termination signals for graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Printf("👷 [%s] Joined group 'order-processing-group'. Awaiting partition assignment...\n", workerID)

	for {
		// ReadMessage automatically fetches and marks offsets for commit
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				fmt.Printf("\n🛑 [%s] Signal received. Leaving consumer group...\n", workerID)
				break
			}
			log.Printf("[%s] Read error: %v", workerID, err)
			continue
		}

		fmt.Printf("📦 [%s] Processed record | Partition: %d | Offset: %d | Key: %-12s | Value: %s\n",
			workerID, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
	}

	// Clean up connection and notify group coordinator
	if err := reader.Close(); err != nil {
		log.Fatalf("[%s] Failed to close reader: %v", workerID, err)
	}
	fmt.Printf("👋 [%s] Cleanly exited group.\n", workerID)
}
```

---

### 4. Hands-On Exercise: Live Rebalance & Failover

Open **3 separate terminal tabs** and launch workers:
```bash
# Terminal 1:
go run kafka/go-clients/consumer_group.go Worker-A

# Terminal 2:
go run kafka/go-clients/consumer_group.go Worker-B

# Terminal 3:
go run kafka/go-clients/consumer_group.go Worker-C
```

#### Step-by-Step Test:
1. **Observe Initial Assignment**: In a 4th terminal, inspect group ownership:
   ```bash
   docker exec -it kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
     --bootstrap-server kafka-1:29092 \
     --describe \
     --group order-processing-group
   ```
   Each worker owns exactly **1 partition** (e.g. Worker-A $\rightarrow$ P0, Worker-B $\rightarrow$ P1, Worker-C $\rightarrow$ P2).
2. **Send Traffic**: Run `go run kafka/go-clients/producer.go`. Observe messages arriving simultaneously across all 3 terminals.
3. **Trigger Rebalance via Failover**: Press `Ctrl+C` on `Worker-C`.
4. **Inspect Surviving Assignment**: Within seconds, re-run `--describe --group order-processing-group`. You will see that `Worker-A` and `Worker-B` have rebalanced and absorbed Partition 2.
5. **Publish Again**: Re-run the producer and verify that no messages are lost or dropped.

---

## Day 7: Delivery Guarantees & Offset Commit Semantics

### 1. Architectural Concepts: Delivery Guarantees Deep Dive

| Guarantee | Producer Setting | Consumer Pattern | Trade-Off |
| :--- | :--- | :--- | :--- |
| **At-Most-Once** | `acks=0` (fire-and-forget) | Auto-commit offset *before* processing message | Highest throughput. **Data loss possible** if consumer crashes mid-processing or broker drops record. |
| **At-Least-Once** | `acks=all`, `retries>0` | Manual commit *after* processing message finishes | **Zero data loss**. Potential duplicate delivery if worker crashes after processing but before committing offset. |
| **Exactly-Once** | `enable.idempotence=true`, transactions | Read-Process-Write within Kafka transaction boundary | Zero duplicates, zero data loss, slightly higher latency (covered in Phase 3). |

```
                              At-Least-Once Processing Pipeline
                              ──────────────────────────────────
   [Broker Partition]
           │
           │ 1. FetchMessage(ctx)
           ▼
   [Consumer Memory]
           │
           │ 2. Execute Business Logic (e.g., Save to DB, charge card)
           ▼
     [Success?] ────► No  ──► Retry / Route to DLT
           │
          Yes
           │ 3. reader.CommitMessages(ctx, msg)
           ▼
 [__consumer_offsets Topic]
```

> [!IMPORTANT]
> In `segmentio/kafka-go`:
> - `reader.ReadMessage(ctx)`: Automatically marks the offset and commits it in the background according to `CommitInterval`.
> - `reader.FetchMessage(ctx)`: Fetches the message **without committing**. You must explicitly call `reader.CommitMessages(ctx, msg)`. This is essential for true **at-least-once** processing.

---

### 2. At-Least-Once Consumer Implementation (`at_least_once.go`)

Create `kafka/go-clients/at_least_once.go`:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func processBusinessLogic(ctx context.Context, msg kafka.Message) error {
	fmt.Printf("⚙️  [Processing Started] Partition: %d | Offset: %d | Order: %s\n",
		msg.Partition, msg.Offset, string(msg.Key))

	// Simulate slow database or external API write (8 seconds gives plenty of time to test Ctrl+C)
	select {
	case <-time.After(8 * time.Second):
		fmt.Printf("💾 [Processing Completed] Stored order for %s in DB\n", string(msg.Key))
		return nil
	case <-ctx.Done():
		fmt.Println("⚡ Interrupted by signal during processing!")
		return ctx.Err()
	}
}

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		GroupID:     "at-least-once-group",
		Topic:       "orders",
		StartOffset: kafka.FirstOffset,
		// Disable periodic auto-commit: we commit manually per message
		CommitInterval: 0,
	})
	defer reader.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Println("🛡️  At-Least-Once Consumer running. Ready to process...")
	fmt.Println("   (You have 8 seconds during [Processing Started] to press Ctrl+C to test non-commit)")

	for {
		// 1. Fetch message WITHOUT committing offset
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				fmt.Println("\n🛑 Graceful shutdown initiated.")
				break
			}
			log.Printf("Fetch error: %v", err)
			continue
		}

		// 2. Perform business operation
		if err := processBusinessLogic(ctx, msg); err != nil {
			if ctx.Err() != nil {
				fmt.Printf("🛑 Terminated before commit! Offset %d was NOT committed.\n", msg.Offset)
				break
			}
			log.Printf("Failed processing message: %v. Skipping commit to retry later.", err)
			continue
		}

		// Check if shutdown was requested before committing
		if ctx.Err() != nil {
			fmt.Printf("🛑 Terminated before commit! Offset %d was NOT committed.\n", msg.Offset)
			break
		}

		// 3. Explicitly commit offset ONLY after successful processing
		commitCtx, commitCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := reader.CommitMessages(commitCtx, msg); err != nil {
			log.Printf("❌ Failed to commit offset: %v", err)
			commitCancel()
			continue
		}
		commitCancel()

		fmt.Printf("✅ [Offset Committed] Partition %d Offset %d\n", msg.Partition, msg.Offset)
	}
}
```

---

### 3. Hands-On Exercise: Breaking At-Least-Once (Simulating Duplicate Delivery)

This exercise proves why consumers in distributed systems must be **idempotent**.

#### Procedure:
1. Start the at-least-once consumer:
   ```bash
   go run kafka/go-clients/at_least_once.go
   ```
2. In another terminal, publish a single order message:
   ```bash
   docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
     --bootstrap-server kafka-1:29092 \
     --topic orders \
     --property parse.key=true \
     --property key.separator=":"
   ```
   Paste:
   ```text
   user_duplicate_test:{"order_id":"crash-test-999","amount":99.00}
   ```
3. Look at the consumer terminal. As soon as you see:
   ```text
   ⚙️  [Processing Started] Partition: X | Offset: Y | Order: user_duplicate_test
   💾 [Processing Completed] Stored order for user_duplicate_test in DB
   ```
   **Immediately press `Ctrl+C` BEFORE `[Offset Committed]` prints!**
4. Check committed offsets via CLI:
   ```bash
   docker exec -it kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
     --bootstrap-server kafka-1:29092 \
     --describe \
     --group at-least-once-group
   ```
   Notice that the `CURRENT-OFFSET` for that partition has **not moved forward**!
5. Restart the consumer:
   ```bash
   go run kafka/go-clients/at_least_once.go
   ```
6. **Observation**: The consumer fetches the exact same message a second time. The database write executes again. This confirms that **at-least-once delivery requires application-level idempotency** (e.g. database unique constraints on `order_id` or an idempotency key cache).

---

## Day 8: Error Handling, Retries & Dead-Letter Topics (DLT)

### 1. Architectural Concepts: The Poison-Pill Problem

A **poison pill** is a record that cannot be processed (corrupt JSON, missing fields, invalid types, or business constraint violations).
- If the consumer fails and does not commit the offset, it re-reads the same record in an infinite loop, **blocking the partition completely** (head-of-line blocking).
- If the consumer swallows the error and commits, **critical business data is lost without a trace**.

**Solution: The Dead-Letter Topic (DLT) Pattern**:
1. When a record fails, attempt local retries with exponential backoff (for transient failures like network blips).
2. If retries are exhausted, forward the failed record along with diagnostic error metadata headers to `orders.DLT`.
3. Commit the offset in the primary topic so healthy messages continue flowing.
4. Support independent alerting, inspection, and replay from `orders.DLT`.

```
                    ┌──────────────────────────────────────────────┐
                    │               Consumer Worker                │
                    └──────────────────────┬───────────────────────┘
                                           │
                                 Fetch from 'orders'
                                           │
                                  Validate & Process
                                           │
                        ┌──────────────────┴──────────────────┐
                     [Success]                             [Failure]
                        │                                     │
                 Commit Offset                         Local Retries (1..3)
                        │                                     │
                  Healthy Stream                              ├──► Recovered ──► Commit
                                                              ▼
                                                        Exhausted?
                                                              │
                                                              ▼
                                                   Publish to 'orders.DLT'
                                                   Headers:
                                                     - x-original-topic: "orders"
                                                     - x-error-message: "JSON syntax error"
                                                     - x-failed-at: "2026-10-20T..."
                                                     - x-retry-count: "3"
                                                              │
                                                              ▼
                                                   Commit Primary Offset
                                                   (Unblocks Pipeline!)
```

---

### 2. Topic Creation for DLT
Create the dedicated Dead-Letter Topic:
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092 \
  --create \
  --topic orders.DLT \
  --partitions 3 \
  --replication-factor 3
```

---

### 3. Resilient DLT Consumer Implementation (`dlt_consumer.go`)

Create `kafka/go-clients/dlt_consumer.go`:

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type ValidatedOrder struct {
	OrderID string  `json:"order_id"`
	UserID  string  `json:"user_id"`
	Amount  float64 `json:"amount"`
}

func parseAndValidateOrder(data []byte) (*ValidatedOrder, error) {
	var order ValidatedOrder
	if err := json.Unmarshal(data, &order); err != nil {
		return nil, fmt.Errorf("malformed JSON payload: %w", err)
	}
	if order.OrderID == "" {
		return nil, errors.New("validation failed: missing order_id")
	}
	if order.Amount <= 0 {
		return nil, fmt.Errorf("validation failed: invalid amount %.2f", order.Amount)
	}
	return &order, nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 1. Primary Consumer
	primaryReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		GroupID:        "dlt-enabled-service",
		Topic:          "orders",
		CommitInterval: 0, // Manual commits
		StartOffset:    kafka.LastOffset,
	})
	defer primaryReader.Close()

	// 2. DLT Writer
	dltWriter := &kafka.Writer{
		Addr:         kafka.TCP("localhost:9092", "localhost:9093", "localhost:9094"),
		Topic:        "orders.DLT",
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
	}
	defer dltWriter.Close()

	fmt.Println("🚀 Resilient Consumer with DLT routing active...")

	for {
		msg, err := primaryReader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				fmt.Println("\n🛑 Consumer stopping...")
				break
			}
			log.Printf("Fetch error: %v", err)
			continue
		}

		// 3. Process with retries
		var parseErr error
		var validated *ValidatedOrder
		maxRetries := 3

		for attempt := 1; attempt <= maxRetries; attempt++ {
			validated, parseErr = parseAndValidateOrder(msg.Value)
			if parseErr == nil {
				break
			}
			fmt.Printf("⚠️  [Attempt %d/%d Failed] Partition %d Offset %d: %v\n",
				attempt, maxRetries, msg.Partition, msg.Offset, parseErr)
			time.Sleep(time.Duration(attempt*100) * time.Millisecond)
		}

		// 4. Route to DLT if all attempts fail
		if parseErr != nil {
			fmt.Printf("☠️  [POISON PILL DETECTED] Routing record to 'orders.DLT'...\n")

			dltMessage := kafka.Message{
				Key:   msg.Key,
				Value: msg.Value,
				Headers: []kafka.Header{
					{Key: "x-original-topic", Value: []byte(msg.Topic)},
					{Key: "x-original-partition", Value: []byte(fmt.Sprintf("%d", msg.Partition))},
					{Key: "x-original-offset", Value: []byte(fmt.Sprintf("%d", msg.Offset))},
					{Key: "x-error-message", Value: []byte(parseErr.Error())},
					{Key: "x-failed-timestamp", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
					{Key: "x-retry-attempts", Value: []byte(fmt.Sprintf("%d", maxRetries))},
				},
			}

			dltCtx, dltCancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := dltWriter.WriteMessages(dltCtx, dltMessage); err != nil {
				log.Printf("CRITICAL: Failed to publish to DLT: %v. Halting commit to prevent silent drop.", err)
				dltCancel()
				continue
			}
			dltCancel()

			fmt.Printf("📬 Successfully isolated poison message in 'orders.DLT'\n")
		} else {
			fmt.Printf("🎉 Processed Order: ID=%s User=%s Amount=$%.2f\n",
				validated.OrderID, validated.UserID, validated.Amount)
		}

		// 5. Commit offset to advance pipeline
		commitCtx, commitCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := primaryReader.CommitMessages(commitCtx, msg); err != nil {
			log.Printf("Commit failed: %v", err)
		}
		commitCancel()
	}
}
```

---

### 4. Hands-On Exercise: Poison Pill Injection

1. Start the DLT consumer in Terminal A:
   ```bash
   go run kafka/go-clients/dlt_consumer.go
   ```
2. Start a consumer watching `orders.DLT` in Terminal B:
   ```bash
   docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
     --bootstrap-server kafka-1:29092 \
     --topic orders.DLT \
     --from-beginning \
     --property print.headers=true \
     --property print.key=true \
     --property print.value=true
   ```
3. In Terminal C, send a sequence containing valid orders and a corrupted poison pill:
   ```bash
   docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
     --bootstrap-server kafka-1:29092 \
     --topic orders \
     --property parse.key=true \
     --property key.separator=":"
   ```
   Paste:
   ```text
   user_1:{"order_id":"ord-01","user_id":"user_1","amount":50.00}
   user_bad:NOT_VALID_JSON_CORRUPTED_RECORD_!!!
   user_2:{"order_id":"ord-02","user_id":"user_2","amount":150.00}
   ```
4. **Observation**:
   - `ord-01` processes successfully.
   - `user_bad` fails 3 retry attempts, gets routed to `orders.DLT` with full diagnostic headers.
   - The primary consumer **does not stall**; it commits the offset and immediately processes `ord-02`!
   - Terminal B displays the poison record and error headers.

---

## Day 9: Schema Management & Serialization

### 1. Architectural Concepts: Schema Registry & Binary Serialization

Using raw, unvalidated JSON in production introduces major risks:
1. Field name typos (`userId` vs `user_id`) cause silent consumer bugs.
2. Inefficient payloads: JSON field names are transmitted with every single message, consuming bandwidth and storage.
3. No contract enforcement or backward compatibility checks.

#### Confluent Wire Format (Magic Byte + Schema ID)
When using a Schema Registry (Confluent or Redpanda Schema Registry), messages are encoded using a **5-byte framing protocol**:

```
 0               1               2               3               4               5...
┌───────────────┬───────────────────────────────────────────────┬─────────────────┐
│ Magic Byte    │             Schema ID (4 Bytes)               │ Encoded Payload │
│ (0x00)        │             Big-Endian uint32                 │ (Protobuf/Avro) │
└───────────────┴───────────────────────────────────────────────┴─────────────────┘
```

- Byte 0: Always `0x00` (Confluent magic byte).
- Bytes 1-4: The 32-bit Schema ID registered in Schema Registry.
- Bytes 5+: Binary Avro or Protobuf payload without repetitive schema metadata.

```
Producer (v1 or v2) ──► Encodes payload + Schema ID ──► Kafka Broker
                                                              │
Consumer (v1) ◄── Fetches Schema ID from Registry ◄───────────┘
              ◄── Decodes payload smoothly (Forward / Backward Compatible)
```

---

### 2. Protocol Buffers (Protobuf) Implementation

Let's implement schema serialization using Protocol Buffers (`protobuf`), the industry standard for Go microservices.

#### Step A: Define Protobuf Schema (`order.proto`)
Create `kafka/go-clients/proto/order.proto`:

```protobuf
syntax = "proto3";

package orders;
option go_package = "./orderspb";

message OrderRecord {
  string order_id = 1;
  string user_id = 2;
  double amount = 3;
  int64 timestamp_unix_ms = 4;
  
  // Optional field added in schema evolution (v2)
  string discount_code = 5;
}
```

#### Step B: Install `protoc-gen-go` (if not already installed)
```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
```

Generate the Go code:
```bash
mkdir -p kafka/go-clients/orderspb
protoc --proto_path=kafka/go-clients/proto \
  --go_out=kafka/go-clients/orderspb \
  --go_opt=paths=source_relative \
  kafka/go-clients/proto/order.proto
```

*(Note: If `protoc` is not installed on your system, you can use the pure-Go binary serialization pattern below which demonstrates the exact same contract enforcement and evolution semantics).*

---

### 3. Schema Evolution Producer & Consumer Pattern (`schema_demo.go`)

This runnable code demonstrates schema evolution: **V2 producer** writes records with new optional fields, and an **older V1 consumer** reads them seamlessly without breaking.

Create `kafka/go-clients/schema_demo.go`:

```go
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// Schema V1 Contract
type OrderV1 struct {
	OrderID string  `json:"order_id"`
	UserID  string  `json:"user_id"`
	Amount  float64 `json:"amount"`
}

// Schema V2 Contract (Backward Compatible: added optional fields)
type OrderV2 struct {
	OrderID      string  `json:"order_id"`
	UserID       string  `json:"user_id"`
	Amount       float64 `json:"amount"`
	DiscountCode string  `json:"discount_code,omitempty"`
	StoreRegion  string  `json:"store_region,omitempty"`
}

// Confluent Wire Format Encoder
func EncodeWithSchemaID(schemaID uint32, payload []byte) ([]byte, error) {
	buf := new(bytes.Buffer)
	// Byte 0: Magic byte
	if err := buf.WriteByte(0x00); err != nil {
		return nil, err
	}
	// Bytes 1-4: Schema ID in Big-Endian
	if err := binary.Write(buf, binary.BigEndian, schemaID); err != nil {
		return nil, err
	}
	// Bytes 5+: Payload
	if _, err := buf.Write(payload); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Confluent Wire Format Decoder
func DecodeWithSchemaID(data []byte) (uint32, []byte, error) {
	if len(data) < 5 {
		return 0, nil, fmt.Errorf("message length %d is less than wire format header (5 bytes)", len(data))
	}
	if data[0] != 0x00 {
		return 0, nil, fmt.Errorf("invalid magic byte 0x%x", data[0])
	}
	schemaID := binary.BigEndian.Uint32(data[1:5])
	return schemaID, data[5:], nil
}

func main() {
	topic := "schema-evolved-orders"

	// 1. Produce V2 Message (Simulating upgraded service)
	writer := &kafka.Writer{
		Addr:         kafka.TCP("localhost:9092"),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
	}
	defer writer.Close()

	v2Order := OrderV2{
		OrderID:      "ord-v2-777",
		UserID:       "user_vip",
		Amount:       299.99,
		DiscountCode: "SPRING2026",
		StoreRegion:  "US-EAST",
	}

	rawJSON, _ := json.Marshal(v2Order)
	schemaID := uint32(102) // Registered schema ID in registry
	wirePayload, err := EncodeWithSchemaID(schemaID, rawJSON)
	if err != nil {
		log.Fatalf("Encoding error: %v", err)
	}

	err = writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(v2Order.UserID),
		Value: wirePayload,
	})
	if err != nil {
		log.Fatalf("Write error: %v", err)
	}
	fmt.Printf("🚀 [V2 Producer] Published Order %s with Schema ID %d\n", v2Order.OrderID, schemaID)

	// 2. Consume using V1 Consumer (Simulating older service that hasn't upgraded yet)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
		Topic:       topic,
		Partition:   0,
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	msg, err := reader.ReadMessage(ctx)
	if err != nil {
		log.Fatalf("Read error: %v", err)
	}

	decodedSchemaID, payload, err := DecodeWithSchemaID(msg.Value)
	if err != nil {
		log.Fatalf("Decode wire format error: %v", err)
	}

	var v1Order OrderV1
	if err := json.Unmarshal(payload, &v1Order); err != nil {
		log.Fatalf("Deserialization into V1 model failed: %v", err)
	}

	fmt.Printf("✅ [V1 Consumer] Successfully decoded message!\n")
	fmt.Printf("   Detected Schema ID: %d\n", decodedSchemaID)
	fmt.Printf("   Deserialized V1 Data: OrderID=%s, UserID=%s, Amount=%.2f\n",
		v1Order.OrderID, v1Order.UserID, v1Order.Amount)
	fmt.Println("   (Note: Older consumer safely ignored newer fields 'discount_code' and 'store_region')")
}
```

---

## 6. Library Deep-Dive: `segmentio/kafka-go` vs `confluent-kafka-go`

In technical interviews and production system design, you should be able to clearly articulate the trade-offs between Go Kafka libraries:

| Feature / Trait | `segmentio/kafka-go` | `confluent-kafka-go` |
| :--- | :--- | :--- |
| **Implementation** | Pure Go | CGO wrapper around `librdkafka` (C library) |
| **Compilation & Builds** | Instant, cross-compiles easily (`CGO_ENABLED=0`) | Requires C compiler (`gcc`), complex Alpine/Docker builds |
| **Throughput & Performance** | Very high (typically 80-90% of librdkafka) | Maximum possible throughput, minimal memory allocation |
| **Feature Completeness** | Core features (Reader, Writer, Balancers) | 100% Kafka feature parity (Transactions, OAUTHBEARER, Mock cluster) |
| **Debugging** | Go stack traces, easy to profile with `pprof` | C crash dumps, memory leaks across CGO boundary can be difficult to trace |
| **Enterprise Support** | Open source community | Official Confluent Enterprise support |

---

## 7. Phase 2 Mastery & Exit Criteria Checklist

Verify your mastery of Phase 2 using the checklist below. If you can answer each question and execute each scenario without referring to notes, you are ready for Phase 3:

- [ ] **Q: How does `segmentio/kafka-go` ensure keyed records land on consistent partitions?**
  - **A**: By configuring `Balancer: &kafka.Hash{}`, the client calculates `murmur2(key) % partition_count`, which exactly matches the default partitioner of Apache Kafka in Java.

- [ ] **Q: What is the crucial operational difference between `ReadMessage()` and `FetchMessage()`?**
  - **A**: `ReadMessage()` automatically marks offsets and commits them in the background based on `CommitInterval`. `FetchMessage()` retrieves the record without committing, requiring explicit invocation of `CommitMessages()` after business processing completes.

- [ ] **Q: Why does setting `acks=all` on the producer fail if `min.insync.replicas=2` and two brokers crash?**
  - **A**: `acks=all` requires confirmation from the leader and all current in-sync replicas. If only 1 broker remains alive, the cluster cannot meet `min.insync.replicas=2`, so the broker rejects writes with `NOT_ENOUGH_REPLICAS`.

- [ ] **Q: What causes duplicate message delivery in at-least-once systems?**
  - **A**: If a consumer completes its processing (e.g. database commit) but crashes or is killed before it can commit its offset to Kafka, the rebalanced or restarted consumer re-fetches the record from the previous committed offset and processes it again.

- [ ] **Q: Why should failed events be routed to a Dead-Letter Topic (DLT) instead of retrying forever?**
  - **A**: An unprocessable "poison pill" record will cause infinite retries, causing head-of-line blocking on that entire partition. Routing to a DLT unblocks healthy downstream messages while isolating the bad record with error headers for investigation.

- [ ] **Q: What is the Confluent Wire Format and why is the 5-byte header used?**
  - **A**: Byte 0 is the magic byte (`0x00`), and Bytes 1-4 encode the 32-bit Schema Registry ID. This allows consumers to fetch the exact schema used for serialization without bloating message payloads with full schema definitions.

---

## High-Level Architecture & Data Flow Diagram

```mermaid
flowchart TD
    subgraph ProducerApp["Go Producer (producer.go)"]
        OrderData["Order Struct\n(Key: user_id)"]
        Writer["kafka.Writer\nBalancer: &kafka.Hash{}\nRequiredAcks: RequireAll"]
        OrderData -->|JSON Marshal| Writer
    end

    subgraph KafkaCluster["3-Broker KRaft Kafka Cluster"]
        direction TB
        subgraph TopicOrders["Topic: orders (3 Partitions, RF=3)"]
            P0["Partition 0\n(Leader: Broker 1)"]
            P1["Partition 1\n(Leader: Broker 2)"]
            P2["Partition 2\n(Leader: Broker 3)"]
        end

        subgraph TopicDLT["Topic: orders.DLT (3 Partitions)"]
            DLTP0["DLT Partition 0"]
            DLTP1["DLT Partition 1"]
            DLTP2["DLT Partition 2"]
        end

        OffsetsTopic[("__consumer_offsets\nCompacted Log")]
    end

    Writer -->|murmur2(key) % 3| P0
    Writer -->|murmur2(key) % 3| P1
    Writer -->|murmur2(key) % 3| P2

    subgraph ConsumerGroup["Consumer Group: order-processing-group"]
        direction LR
        WorkerA["Worker A\n(Owns P0)"]
        WorkerB["Worker B\n(Owns P1)"]
        WorkerC["Worker C\n(Owns P2)"]
    end

    P0 -->|FetchMessage| WorkerA
    P1 -->|FetchMessage| WorkerB
    P2 -->|FetchMessage| WorkerC

    subgraph ErrorPipeline["Resilient Processing Logic (dlt_consumer.go)"]
        WorkerLogic["Process Business Logic\n(DB Insert / Payment)"]
        RetryLoop{"Success or\n3 Retries Failed?"}
        DB[("Database\n(Idempotent Write)")]
        DLTPublisher["Publish to orders.DLT\n+ Diagnostic Headers"]

        WorkerA -.-> WorkerLogic
        WorkerLogic --> RetryLoop
        RetryLoop -->|Success| DB
        RetryLoop -->|Exhausted| DLTPublisher
    end

    DLTPublisher --> TopicDLT
    DB -->|CommitMessages| OffsetsTopic
    DLTPublisher -->|CommitMessages| OffsetsTopic
```

### How to Read This Architecture:
1. **Producer Routing**: The `kafka.Writer` hashes `user_id` using Murmur2. Identical user IDs always route to the same partition, guaranteeing strict FIFO order per user.
2. **Rebalance Mechanics**: The 3 workers in `order-processing-group` achieve 1:1 concurrency with the 3 partitions. If `Worker C` is killed, Kafka detects heartbeat timeout and rebalances Partition 2 to either Worker A or Worker B without message loss.
3. **Resilience & DLT**: If a malformed payload enters Partition 0, `Worker A` retries 3 times with exponential backoff. Upon failure, it routes the payload to `orders.DLT` along with diagnostic headers, and commits the primary offset to `__consumer_offsets`. This guarantees the partition pipeline never stalls.
