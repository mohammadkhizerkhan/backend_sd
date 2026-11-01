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

		// 3. Explicitly commit offset ONLY after successful processing and if not shutting down
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
