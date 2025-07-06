package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/Shopify/sarama"
)

type Event struct {
	Type      string    `json:"type"`
	Data      string    `json:"data"`
	Timestamp time.Time `json:"timestamp"`
}

type EventResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

var (
	producer sarama.SyncProducer
)

func main() {
	// Configuration
	port := getEnv("PORT", "8082")
	brokers := getEnv("KAFKA_BROKERS", "kafka:9092")
	topicPrefix := ""

	// Create Kafka producer
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	var err error
	producer, err = sarama.NewSyncProducer([]string{brokers}, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Start Kafka consumer
	go startConsumer(brokers, topicPrefix)

	// HTTP Handlers
	http.HandleFunc("/api/events/health", healthHandler)
	http.HandleFunc("/api/events/movie", eventHandler("movie"))
	http.HandleFunc("/api/events/user", eventHandler("user"))
	http.HandleFunc("/api/events/payment", eventHandler("payment"))

	// Start server
	log.Printf("Starting events service on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func eventHandler(eventType string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var payload map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, `{"error":"Invalid request"}`, http.StatusBadRequest)
			return
		}

		// Create event
		event := Event{
			Type:      eventType,
			Timestamp: time.Now().UTC(),
		}

		if data, err := json.Marshal(payload); err == nil {
			event.Data = string(data)
		}

		// Send to Kafka
		msg := &sarama.ProducerMessage{
			Topic: fmt.Sprintf("%s-events", eventType),
			Value: sarama.StringEncoder(event.Data),
		}

		if _, _, err := producer.SendMessage(msg); err != nil {
			http.Error(w, `{"error":"Failed to send event"}`, http.StatusInternalServerError)
			return
		}

		// Return response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(EventResponse{
			Status:  "success",
			Message: "Event created",
		})
	}
}

func startConsumer(brokers, topicPrefix string) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Create consumer
	consumer, err := sarama.NewConsumer([]string{brokers}, config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Topics to consume
	topics := []string{"movie-events", "user-events", "payment-events"}

	// Consume each topic
	for _, topic := range topics {
		partitions, err := consumer.Partitions(topic)
		if err != nil {
			log.Printf("Error getting partitions for %s: %v", topic, err)
			continue
		}

		for _, partition := range partitions {
			pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
			if err != nil {
				log.Printf("Error consuming partition %d: %v", partition, err)
				continue
			}

			go func(pc sarama.PartitionConsumer) {
				for msg := range pc.Messages() {
					log.Printf("[%s] Received event: %s", msg.Topic, string(msg.Value))
				}
			}(pc)
		}
	}

	// Keep consumer running
	select {}
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
