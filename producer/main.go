package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"go-kafka/producer/models"

	"github.com/segmentio/kafka-go"
)

func main() {
	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "pizza-orders",
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	http.HandleFunc("/order", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}

		var order models.Order
		if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		order.OrderID = fmt.Sprintf("ORD-%d", time.Now().Unix())
		payload, _ := json.Marshal(order)
		err := writer.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(order.OrderID),
				Value: payload,
			},
		)

		if err != nil {
			log.Printf("Failed to write message: %v", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}

		fmt.Fprint(w, "Order placed successfully")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"status":   "Order sent to kitchen",
			"order_id": order.OrderID,
		})
	})

	fmt.Println("Producer is running on port 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
