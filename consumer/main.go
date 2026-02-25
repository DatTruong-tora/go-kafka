package main

import (
	"context"
	"fmt"
	"log"

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

	fmt.Println("Kitchen is ready to serve!")

	for {
		message, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatal("failed to read message: ", err)
		}

		fmt.Printf("KITCHEN: cooking: %s (at Partition %d Offset %d)\n", string(message.Value), message.Partition, message.Offset)
	}
}
