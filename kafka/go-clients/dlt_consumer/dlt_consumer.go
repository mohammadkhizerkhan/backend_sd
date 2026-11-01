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
