package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

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
		pizzaName := r.URL.Query().Get("name")
		if pizzaName == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		err := writer.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte("Order"),
				Value: []byte(pizzaName),
			},
		)

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "Order placed successfully")
	})

	fmt.Println("Producer is running on port 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
