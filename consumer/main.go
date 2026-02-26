package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"go-kafka/producer/models"

	"github.com/segmentio/kafka-go"
)

const (
	brokerAddress = "localhost:9092"
	topic         = "pizza-orders"
	groupID       = "kitchen-group"
	numWorkers    = 3
)

func runWorker(ctx context.Context, workerID int, deliveryChan chan<- models.Order, wg *sync.WaitGroup) {
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

		var order models.Order
		if err := json.Unmarshal(message.Value, &order); err != nil {
			log.Printf("[Worker-%d] Error parsing message: %v", workerID, err)
			return
		}
		fmt.Printf("[Worker-%d] 👨‍🍳 Cooking: %s for %s\n", workerID, order.PizzaType, order.CustomerName)
		time.Sleep(2 * time.Second)

		deliveryChan <- order

		fmt.Printf("[Worker-%d] Done! Order %s is ready for delivery!\n", workerID, string(message.Key))
		fmt.Printf("[Worker-%d] -------------------------------------------\n", workerID)

		if err := reader.CommitMessages(ctx, message); err != nil {
			log.Printf("[Worker-%d] Failed to commit message: %v", workerID, err)
		}
	}
}

func runDelivery(ctx context.Context, deliveryChan <-chan models.Order, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case order := <-deliveryChan:
			fmt.Printf("[Shipper] 🛵 Delivering Order %s: %s to %s!\n",
				order.OrderID, order.PizzaType, order.CustomerName)
			time.Sleep(1 * time.Second)
		}
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	deliveryChan := make(chan models.Order, 10)

	wg.Add(1)
	go runDelivery(ctx, deliveryChan, &wg)

	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go runWorker(ctx, i, deliveryChan, &wg)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	fmt.Printf("\nReceived signal: %v. Shutting down consumer group...\n", sig)
	cancel()

	wg.Wait()
	fmt.Println("All workers stopped. Goodbye!")
}
