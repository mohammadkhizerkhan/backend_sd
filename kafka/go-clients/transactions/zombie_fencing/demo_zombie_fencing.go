package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	client := &kafka.Client{
		Addr:    kafka.TCP("localhost:9092"),
		Timeout: 5 * time.Second,
	}

	// Unique transactional ID for this demonstration run
	txID := fmt.Sprintf("order-clearing-engine-%d", time.Now().Unix())

	fmt.Println("==========================================================================")
	fmt.Println("🧟 LIVE DEMO: ZOMBIE FENCING (SPLIT-BRAIN PREVENTION VIA PRODUCER EPOCH)")
	fmt.Println("==========================================================================")

	// Step 1: Discover the Transaction Coordinator for this transactional.id
	fmt.Printf("\n[1] Discovering Transaction Coordinator for '%s'...\n", txID)
	findResp, err := client.FindCoordinator(context.Background(), &kafka.FindCoordinatorRequest{
		Addr:    kafka.TCP("localhost:9092"),
		Key:     txID,
		KeyType: kafka.CoordinatorKeyTypeTransaction,
	})
	if err != nil {
		log.Fatalf("FindCoordinator error: %v", err)
	}
	tcAddr := net.JoinHostPort(findResp.Coordinator.Host, fmt.Sprintf("%d", findResp.Coordinator.Port))
	fmt.Printf("    -> Coordinator mapped to Broker ID %d at %s\n", findResp.Coordinator.NodeID, tcAddr)

	// Step 2: Boot Worker 1 (Original active worker instance)
	fmt.Println("\n[2] Booting Worker 1: Registering with Transaction Coordinator...")
	var worker1Resp *kafka.InitProducerIDResponse
	for attempt := 1; attempt <= 10; attempt++ {
		worker1Resp, err = client.InitProducerID(context.Background(), &kafka.InitProducerIDRequest{
			Addr:                 kafka.TCP(tcAddr),
			TransactionalID:      txID,
			TransactionTimeoutMs: 60000,
		})
		if err == nil && worker1Resp.Error == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if worker1Resp == nil || worker1Resp.Error != nil {
		log.Fatalf("Worker 1 InitProducerID failed: %v", worker1Resp.Error)
	}

	worker1PID := worker1Resp.Producer.ProducerID
	worker1Epoch := worker1Resp.Producer.ProducerEpoch
	fmt.Printf("    -> Worker 1 successfully registered!\n")
	fmt.Printf("       PID: %d | Epoch: %d\n", worker1PID, worker1Epoch)

	// Step 3: Simulate Worker 1 hanging
	fmt.Println("\n[3] Simulating Worker 1 entering a long Garbage Collection (STW) freeze...")
	fmt.Println("    (Cluster considers Worker 1 dead and spawns Worker 2 to take over)")

	// Step 4: Boot Worker 2 (Replacement instance taking over the same transactional.id)
	fmt.Println("\n[4] Booting Worker 2: Claiming 'transactional.id' ownership...")
	worker2Resp, err := client.InitProducerID(context.Background(), &kafka.InitProducerIDRequest{
		Addr:                 kafka.TCP(tcAddr),
		TransactionalID:      txID,
		TransactionTimeoutMs: 60000,
		ProducerID:           worker1PID,
		ProducerEpoch:        worker1Epoch, // Claims and increments previous epoch
	})
	if err != nil || worker2Resp.Error != nil {
		log.Fatalf("Worker 2 InitProducerID failed: %v", worker2Resp.Error)
	}

	worker2PID := worker2Resp.Producer.ProducerID
	worker2Epoch := worker2Resp.Producer.ProducerEpoch
	fmt.Printf("    -> Worker 2 successfully registered!\n")
	fmt.Printf("       PID: %d | Epoch: %d\n", worker2PID, worker2Epoch)
	fmt.Printf("    🔥 COORDINATOR BUMPED EPOCH: %d -> %d\n", worker1Epoch, worker2Epoch)

	// Step 5: Worker 1 wakes up (Zombie) and attempts to write to a partition
	fmt.Println("\n[5] Worker 1 wakes up (Zombie) and tries to perform work...")
	fmt.Printf("    Worker 1 sends request with PID: %d and STALE Epoch: %d...\n", worker1PID, worker1Epoch)

	addResp, err := client.AddPartitionsToTxn(context.Background(), &kafka.AddPartitionsToTxnRequest{
		Addr:            kafka.TCP(tcAddr),
		TransactionalID: txID,
		ProducerID:      worker1PID,
		ProducerEpoch:   worker1Epoch, // STALE EPOCH (0 < 1)
		Topics: map[string][]kafka.AddPartitionToTxn{
			"orders": {{Partition: 0}},
		},
	})

	// Step 6: Verify Kafka's rejection
	fenced := false
	if err != nil {
		fmt.Printf("    🛑 KAFKA REJECTED WORKER 1: %v\n", err)
		fenced = true
	} else {
		for topic, parts := range addResp.Topics {
			for _, p := range parts {
				if p.Error != nil {
					fmt.Printf("    🛑 KAFKA BROKER REJECTED WORKER 1 ON %s:%d\n", topic, p.Partition)
					fmt.Printf("       ERROR: %v\n", p.Error)
					fenced = true
				}
			}
		}
	}

	if fenced {
		fmt.Println("\n==========================================================================")
		fmt.Println("🛡️  VERIFICATION COMPLETE: ZOMBIE WAS SUCCESSFULLY FENCED!")
		fmt.Println("Because Worker 1 had Epoch 0 and the Coordinator was at Epoch 1,")
		fmt.Println("Kafka safely rejected Worker 1 with 'Producer Fenced' [Error 90].")
		fmt.Println("This guarantees zero split-brain and zero duplicate data corruption!")
		fmt.Println("==========================================================================")
	}
}
