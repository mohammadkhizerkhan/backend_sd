package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	mode := "read_committed"
	if len(os.Args) > 1 {
		mode = strings.ToLower(os.Args[1])
	}

	var isolation kafka.IsolationLevel
	var modeLabel string
	if mode == "read_uncommitted" || mode == "uncommitted" {
		isolation = kafka.ReadUncommitted
		modeLabel = "READ_UNCOMMITTED (Dirty Reads - sees uncommitted and aborted records)"
	} else {
		isolation = kafka.ReadCommitted
		modeLabel = "READ_COMMITTED (Standard EOS - automatically hides aborted records)"
	}

	topic := "cleared-transactions"

	fmt.Println("==========================================================================")
	fmt.Printf("👓 CONSUMER ISOLATION LEVEL DEMO: %s\n", modeLabel)
	fmt.Println("==========================================================================")
	fmt.Printf("Reading from '%s' (Partition 0 from beginning)...\n\n", topic)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		Topic:          topic,
		Partition:      0,
		StartOffset:    kafka.FirstOffset,
		IsolationLevel: isolation,
	})
	defer reader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	count := 0
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			break
		}
		count++
		fmt.Printf("  [%d] Offset %d | Key: %-12s | Value: %s\n",
			count, msg.Offset, string(msg.Key), string(msg.Value))
	}

	fmt.Printf("\nDone! Read a total of %d records under %s.\n", count, mode)
	fmt.Println("==========================================================================")
}
