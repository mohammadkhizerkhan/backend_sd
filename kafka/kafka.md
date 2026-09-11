The original logical order was solid, but reordering it slightly helps concepts build on top of each other more naturally: **Problem Statement $\rightarrow$ Architecture & Components $\rightarrow$ Routing Mechanics $\rightarrow$ Fault Tolerance $\rightarrow$ Errors & Retries $\rightarrow$ Performance $\rightarrow$ Hands-On Go Code**.

Here is the updated, detailed document with added concepts like **Offsets**, **In-Sync Replicas (ISR)**, **Key Salting**, and full **Golang code examples** using `segmentio/kafka-go`.

---

# Apache Kafka Architecture & Hands-On Guide

## Problem Statement

1. **Ordering vs. Scalability**: Standard message queues face a trade-off between strict ordering and horizontal scalability. Single-partition queues guarantee ordering but create processing bottlenecks. Multi-partition queues scale throughput but mix up event sequences. Kafka solves this using **Partition Keys**: events sharing a key always land in the same partition and preserve strict sequential order.
2. **Consumer Bottlenecks & Load Balancing**: A single consumer processing high-volume streams creates a single point of failure and a throughput ceiling. Kafka introduces **Consumer Groups**, enabling multiple instances to read from separate partitions concurrently. Kafka automatically assigns each partition to exactly one consumer per group and triggers **rebalancing** if an instance drops out.

---

## Architecture & Components

- **Topic**: A logical stream/category to which producers publish and consumers subscribe. Topics are split into one or more partitions.
- **Partition**: The physical commit log file on disk. Messages are appended sequentially and assigned an immutable ID called an **Offset**.
- **Offset**: A monotonic integer representing a message's exact position within a partition. Consumers track their progress by committing offsets.
- **Broker**: A single Kafka server. Brokers hold partitions, serve read/write requests, and replicate data across the cluster.
- **Producer**: Client application that serializes and publishes messages to Kafka topics.
- **Consumer & Consumer Group**:
- **Consumer**: Client instance reading messages from partitions.
- **Consumer Group**: A collection of consumers sharing the workload of a topic. Each partition in a topic is consumed by **only one** consumer instance within a group at a time.

- **Message (Record)**: The unit of data stored in Kafka.
- **Key**: Optional payload used for partition routing.
- **Value**: The actual message payload (JSON, Protobuf, Avro, Byte array).
- **Headers**: Key-value metadata pairs.
- **Timestamp**: Appended by the producer or broker.

```
          [ Producer ]
               │
               ▼
     ┌──────────────────┐
     │   Kafka Topic    │
     ├──────────────────┤
     │ Partition 0 ─────┼────► [ Consumer 1 ] ┐
     │ Partition 1 ─────┼────► [ Consumer 2 ] ├─ Consumer Group A
     │ Partition 2 ─────┼────► [ Consumer 3 ] ┘
     └──────────────────┘

```

---

## Message Routing & Partitioning Strategies

- **Without Key (Round-Robin / Sticky)**: Messages are distributed across partitions evenly. Modern Kafka drivers use "Sticky Partitioning" to batch records heading to the same partition before switching to the next, reducing network overhead.
- **With Key (Key-Based Hashing)**: Kafka hashes the key (`murmur2(key) % total_partitions`) to determine the target partition. All events with the same key go to the exact same partition, guaranteeing **in-order processing per key**.

---

## The Hot Partition Problem & Solutions

When key distribution is skewed (e.g., a massive merchant generating 80% of e-commerce transactions), a single partition receives disproportionate traffic, overloading its dedicated consumer.

**Mitigation Strategies:**

- **Compound Keys**: Instead of keying purely by `MerchantID`, key by `MerchantID_RegionID` or `MerchantID_Date`.
- **Key Salting**: Append a random integer suffix (`MerchantID_1`, `MerchantID_2`) to distribute heavy keys across $N$ partitions. The consumer side must aggregate these sub-streams if ordering across the entire entity isn't strictly required.

---

## Fault Tolerance, Durability & Availability

- **Replication Factor (RF)**: Defines how many broker nodes hold a copy of each partition. (Standard production RF is 3).
- **Leader & Followers**:
- **Leader Partition**: Handles all reads and writes.
- **Follower Partitions**: Sync data from the Leader.

- **In-Sync Replicas (ISR)**: The set of follower replicas actively keeping up with the leader partition.
- **Producer Acknowledgments (`acks`)**:
- `acks=0`: Fire-and-forget. Highest performance, potential data loss.
- `acks=1`: Producer succeeds once the **Leader** writes the message.
- `acks=all` (or `-1`): Producer succeeds only after the Leader **and all ISRs** write the message. Guaranteed zero data loss when paired with `min.insync.replicas`.

---

## Error Handling & Retry Strategies

### Producer-Side Retries

Producers can automatically retry transient network errors.

- `retries`: Number of retry attempts.
- `retry.backoff.ms`: Delay between retries.
- `enable.idempotence=true`: Ensures retries do not introduce duplicate records or out-of-order writes at the partition level.

### Consumer-Side Error Handling

Kafka brokers do not re-drive failed consumer events. Consumers must implement processing resiliency:

1. **Immediate Retry with Backoff**: Loop processing attempts locally with delays.
2. **Retry Topics**: Forward failed events to dedicated non-blocking retry topics (e.g., `orders-retry-5m`) to avoid stalling the primary partition pipeline.
3. **Dead Letter Queue (DLQ)**: If retries exhaust, route unprocessable messages to a DLQ topic (e.g., `orders-dlq`) for manual inspection or dead-letter processing.

---

## Performance Tuning Parameters

- **Batching & Latency Trade-Off**:
- `batch.size`: Max bytes to buffer before sending to a broker (e.g., `32KB` or `64KB`).
- `linger.ms`: Time to wait for `batch.size` to fill before sending anyway. Balancing `linger.ms=10` to `20ms` drastically increases throughput with minimal latency impact.

- **Compression**: Enable message payload compression (`lz4`, `zstd`, or `snappy`) at the producer to minimize network bandwith and broker storage demands.

---

## Hands-On Golang Implementation

Below are working patterns using the `[github.com/segmentio/kafka-go](https://github.com/segmentio/kafka-go)` library.

### 1. Producer with Keying & Compression

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
)

func main() {
	writer := &kafka.Writer{
		Addr:         kafka.TCP("localhost:9092"),
		Topic:        "user-events",
		Balancer:     &kafka.Hash{}, // Key-based partitioning
		Compression:  compress.Snappy,
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond, // linger.ms equivalent
		RequiredAcks: kafka.RequireAll,     // acks=all
	}
	defer writer.Close()

	// Events with the same key ("user_102") land on the same partition
	messages := []kafka.Message{
		{Key: []byte("user_102"), Value: []byte(`{"action": "login"}`)},
		{Key: []byte("user_102"), Value: []byte(`{"action": "add_to_cart"}`)},
		{Key: []byte("user_305"), Value: []byte(`{"action": "login"}`)},
	}

	err := writer.WriteMessages(context.Background(), messages...)
	if err != nil {
		log.Fatalf("Failed to write messages: %v", err)
	}
	fmt.Println("Messages successfully published!")
}

```

### 2. Parallel Consumer Group Worker

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		GroupID:  "analytics-group", // Consumer Group ID
		Topic:    "user-events",
		MinBytes: 10KB,
		MaxBytes: 1MB,
	})
	defer reader.Close()

	fmt.Println("Consumer active. Waiting for events...")

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v", err)
			break
		}

		fmt.Printf("[Partition: %d | Offset: %d | Key: %s]: %s\n",
			msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
	}
}

```

### 3. Consumer Processing with DLQ Routing

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func processMessage(msg kafka.Message) error {
	// Simulate business logic processing failure
	if string(msg.Value) == "bad_data" {
		return errors.New("corrupted payload")
	}
	return nil
}

func main() {
	ctx := context.Background()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		GroupID: "payment-group",
		Topic:   "payments",
	})
	defer reader.Close()

	dlqWriter := &kafka.Writer{
		Addr:  kafka.TCP("localhost:9092"),
		Topic: "payments-dlq",
	}
	defer dlqWriter.Close()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			break
		}

		// Attempt processing with retries
		var procErr error
		maxRetries := 3
		for attempt := 1; attempt <= maxRetries; attempt++ {
			procErr = processMessage(msg)
			if procErr == nil {
				break
			}
			time.Sleep(time.Duration(attempt*100) * time.Millisecond)
		}

		// Route to DLQ if attempts fail
		if procErr != nil {
			log.Printf("Failed processing message (Offset %d). Routing to DLQ...", msg.Offset)

			dlqMsg := kafka.Message{
				Key:   msg.Key,
				Value: msg.Value,
				Headers: []kafka.Header{
					{Key: "error", Value: []byte(procErr.Error())},
				},
			}

			if err := dlqWriter.WriteMessages(ctx, dlqMsg); err != nil {
				log.Printf("Failed writing to DLQ: %v", err)
				continue
			}
		}

		// Commit offset only after successful processing OR routing to DLQ
		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("Failed to commit offset: %v", err)
		}
	}
}

```
