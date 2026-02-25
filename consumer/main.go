package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "pizza-orders",
		GroupID:  "kitchen-group",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	fmt.Println("Kitchen Service is ready, waiting for orders...")

	for {
		message, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v", err)
			break
		}

		fmt.Printf("Kitchen received order: %s\n", string(message.Value))
		fmt.Println("--- Processing & Cooking... ---")

		// Simulate cooking time
		time.Sleep(3 * time.Second)

		fmt.Printf("Done! Order %s is ready for delivery!\n", string(message.Key))
		fmt.Println("-------------------------------------------")
	}
}
