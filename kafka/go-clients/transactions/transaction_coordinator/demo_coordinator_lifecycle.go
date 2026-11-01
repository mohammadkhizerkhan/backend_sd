package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
)

func main() {
	mode := "commit"
	if len(os.Args) > 1 {
		mode = strings.ToLower(os.Args[1])
	}

	if mode != "commit" && mode != "abort" {
		fmt.Println("Usage: go run demo_coordinator_lifecycle.go [commit|abort]")
		os.Exit(1)
	}

	client := &kafka.Client{
		Addr:    kafka.TCP("localhost:9092"),
		Timeout: 5 * time.Second,
	}

	txID := fmt.Sprintf("tx-lifecycle-%d", time.Now().Unix())
	topic := "cleared-transactions"

	fmt.Println("==========================================================================")
	fmt.Printf("🔄 2-PHASE COMMIT TRANSACTION LIFECYCLE DEMO (MODE: %s)\n", strings.ToUpper(mode))
	fmt.Println("==========================================================================")

	// Step 1: Find Coordinator
	fmt.Printf("\n[Step 1] Finding Transaction Coordinator for '%s'...\n", txID)
	findResp, err := client.FindCoordinator(context.Background(), &kafka.FindCoordinatorRequest{
		Addr:    kafka.TCP("localhost:9092"),
		Key:     txID,
		KeyType: kafka.CoordinatorKeyTypeTransaction,
	})
	if err != nil {
		log.Fatalf("FindCoordinator error: %v", err)
	}
	tcAddr := net.JoinHostPort(findResp.Coordinator.Host, fmt.Sprintf("%d", findResp.Coordinator.Port))
	fmt.Printf("         -> Mapped to Broker ID %d (%s)\n", findResp.Coordinator.NodeID, tcAddr)

	// Step 2: InitProducerID
	fmt.Println("\n[Step 2] Sending InitProducerIDRequest to Transaction Coordinator...")
	var initResp *kafka.InitProducerIDResponse
	for attempt := 1; attempt <= 10; attempt++ {
		initResp, err = client.InitProducerID(context.Background(), &kafka.InitProducerIDRequest{
			Addr:                 kafka.TCP(tcAddr),
			TransactionalID:      txID,
			TransactionTimeoutMs: 60000,
		})
		if err == nil && initResp.Error == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if initResp == nil || initResp.Error != nil {
		log.Fatalf("InitProducerID failed: %v", initResp.Error)
	}
	pid := initResp.Producer.ProducerID
	epoch := initResp.Producer.ProducerEpoch
	fmt.Printf("         -> Allocated PID: %d | Epoch: %d\n", pid, epoch)

	// Step 3: AddPartitionsToTxn (Transition from Empty -> Ongoing)
	fmt.Printf("\n[Step 3] Adding topic '%s' (Partition 0) to active transaction...\n", topic)
	addResp, err := client.AddPartitionsToTxn(context.Background(), &kafka.AddPartitionsToTxnRequest{
		Addr:            kafka.TCP(tcAddr),
		TransactionalID: txID,
		ProducerID:      pid,
		ProducerEpoch:   epoch,
		Topics: map[string][]kafka.AddPartitionToTxn{
			topic: {{Partition: 0}},
		},
	})
	if err != nil {
		log.Fatalf("AddPartitionsToTxn error: %v", err)
	}
	for t, parts := range addResp.Topics {
		for _, p := range parts {
			if p.Error != nil {
				log.Fatalf("AddPartitionToTxn failed for %s:%d: %v", t, p.Partition, p.Error)
			}
		}
	}
	fmt.Println("         -> Coordinator logged state: ONGOING in __transaction_state.")

	// Step 4: Produce record with TransactionalID
	var payload string
	if mode == "commit" {
		payload = fmt.Sprintf(`{"order_id": "tx-%d", "amount": 149.50, "status": "VERIFIED_COMMITTED"}`, time.Now().Unix())
	} else {
		payload = fmt.Sprintf(`{"order_id": "tx-fraud-%d", "amount": 99999.0, "status": "FRAUD_DETECTED_ROLLBACK"}`, time.Now().Unix())
	}

	fmt.Printf("\n[Step 4] Producing transactional record to '%s:0'...\n", topic)
	fmt.Printf("         Payload: %s\n", payload)
	prodResp, err := client.Produce(context.Background(), &kafka.ProduceRequest{
		Addr:            kafka.TCP("localhost:9092"),
		Topic:           topic,
		Partition:       0,
		TransactionalID: txID,
		RequiredAcks:    kafka.RequireAll,
		Records: kafka.NewRecordReader(kafka.Record{
			Key:   protocol.NewBytes([]byte("tx-account")),
			Value: protocol.NewBytes([]byte(payload)),
			Time:  time.Now(),
		}),
	})
	if err != nil || prodResp.Error != nil {
		log.Fatalf("Produce failed: %v", prodResp.Error)
	}
	fmt.Println("         -> Record appended to broker log. (Pending commit control marker)")

	// Step 5: EndTxn (Phase 1 Prepare + Phase 2 Commit/Abort Marker)
	isCommit := (mode == "commit")
	actionStr := "COMMIT"
	if !isCommit {
		actionStr = "ABORT"
	}

	fmt.Printf("\n[Step 5] Sending EndTxnRequest(Committed = %t) -> Instructing coordinator to %s...\n", isCommit, actionStr)
	endResp, err := client.EndTxn(context.Background(), &kafka.EndTxnRequest{
		Addr:            kafka.TCP(tcAddr),
		TransactionalID: txID,
		ProducerID:      pid,
		ProducerEpoch:   epoch,
		Committed:       isCommit,
	})
	if err != nil || endResp.Error != nil {
		log.Fatalf("EndTxn failed: %v", endResp.Error)
	}

	fmt.Println("\n==========================================================================")
	if isCommit {
		fmt.Println("✅ TRANSACTION SUCCESSFULLY COMMITTED!")
		fmt.Println("   1. Coordinator logged PREPARE_COMMIT to __transaction_state")
		fmt.Println("   2. Coordinator appended COMMIT Control Marker to partition log")
		fmt.Println("   3. Coordinator logged COMPLETE_COMMIT to __transaction_state")
		fmt.Println("   Downstream consumers (read_committed) can now read this record.")
	} else {
		fmt.Println("🚨 TRANSACTION SUCCESSFULLY ABORTED (ROLLED BACK)!")
		fmt.Println("   1. Coordinator logged PREPARE_ABORT to __transaction_state")
		fmt.Println("   2. Coordinator appended ABORT Control Marker to partition log")
		fmt.Println("   3. Coordinator logged COMPLETE_ABORT to __transaction_state")
		fmt.Println("   Downstream consumers (read_committed) will automatically skip this record!")
	}
	fmt.Println("==========================================================================")
}
