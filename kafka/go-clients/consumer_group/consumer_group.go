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
		SessionTimeout:    10 * time.Second,
		RebalanceTimeout:  30 * time.Second,
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
