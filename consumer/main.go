package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	brokerAddress = "localhost:9092"
	topic         = "pizza-orders"
	groupID       = "kitchen-group"
	numWorkers    = 3
)

func runWorker(ctx context.Context, workerID int, wg *sync.WaitGroup) {
	defer wg.Done()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{brokerAddress},
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	log.Printf("[Worker-%d] Started, waiting for orders...", workerID)

	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Printf("[Worker-%d] Shutting down gracefully.", workerID)
				return
			}
			log.Printf("[Worker-%d] Error reading message: %v", workerID, err)
			return
		}

		fmt.Printf("[Worker-%d] Received order (partition=%d, offset=%d): %s\n",
			workerID, message.Partition, message.Offset, string(message.Value))
		fmt.Printf("[Worker-%d] --- Processing & Cooking... ---\n", workerID)

		time.Sleep(3 * time.Second)

		fmt.Printf("[Worker-%d] Done! Order %s is ready for delivery!\n", workerID, string(message.Key))
		fmt.Printf("[Worker-%d] -------------------------------------------\n", workerID)

		if err := reader.CommitMessages(ctx, message); err != nil {
			log.Printf("[Worker-%d] Failed to commit message: %v", workerID, err)
		}
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup

	fmt.Printf("Kitchen Service starting %d workers in consumer group '%s'...\n", numWorkers, groupID)

	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go runWorker(ctx, i, &wg)
	}

	sig := <-sigChan
	fmt.Printf("\nReceived signal: %v. Shutting down consumer group...\n", sig)
	cancel()

	wg.Wait()
	fmt.Println("All workers stopped. Goodbye!")
}
