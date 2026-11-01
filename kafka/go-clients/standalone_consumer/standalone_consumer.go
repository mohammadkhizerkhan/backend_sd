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
		Partition: 2, // Direct assignment: bypasses group coordination
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

	fmt.Println("🔍 Standalone Reader bound exclusively to 'orders' Partition 2...")

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
