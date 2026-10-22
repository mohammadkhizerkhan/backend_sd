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
		RequiredAcks: kafka.RequireAll,      // acks=all (-1) for maximum durability
		Compression:  compress.Snappy,       // snappy compression for high throughput
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
		{OrderID: "ord-1003", UserID: "user_omega", Amount: 15.50, Currency: "USD", Timestamp: time.Now()}, // -> Partition 0!
		{OrderID: "ord-1004", UserID: "user_gamma", Amount: 300.25, Currency: "USD", Timestamp: time.Now()}, // -> Partition 1
		{OrderID: "ord-1005", UserID: "user_beta", Amount: 89.90, Currency: "USD", Timestamp: time.Now()},  // -> Partition 1
		{OrderID: "ord-1006", UserID: "user_omega", Amount: 210.00, Currency: "USD", Timestamp: time.Now()},// -> Partition 0!
		{OrderID: "ord-1007", UserID: "user_delta", Amount: 12.00, Currency: "USD", Timestamp: time.Now()}, // -> Partition 2
		{OrderID: "ord-1008", UserID: "user_gamma", Amount: 45.00, Currency: "USD", Timestamp: time.Now()}, // -> Partition 1
		{OrderID: "ord-1009", UserID: "user_alpha", Amount: 75.20, Currency: "USD", Timestamp: time.Now()}, // -> Partition 2
		{OrderID: "ord-1010", UserID: "user_3", Amount: 500.00, Currency: "USD", Timestamp: time.Now()},     // -> Partition 0!
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
