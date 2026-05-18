package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Configuration
	kafkaBroker := getEnv("KAFKA_BROKER", "localhost:9092")
	kafkaTopic := getEnv("KAFKA_TOPIC_NAME", "nyc_taxi_trips")
	csvFilePath := "../../data/yellow_tripdata_2016-03.csv"
	recordsPerHour := 1000

	sleepDuration := time.Duration(3600.0/float64(recordsPerHour)*1000) * time.Millisecond

	log.Printf("Starting Taxi Data Producer...")
	log.Printf("Broker: %s, Topic: %s", kafkaBroker, kafkaTopic)
	log.Printf("Target Rate: %d records/hour (1 record every %v)", recordsPerHour, sleepDuration)

	// Open CSV
	file, err := os.Open(csvFilePath)
	if err != nil {
		log.Fatalf("Failed to open CSV file: %v. Ensure it exists at %s", err, csvFilePath)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	// Read headers
	headers, err := reader.Read()
	if err != nil {
		log.Fatalf("Failed to read headers: %v", err)
	}

	// Create Kafka writer
	w := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    kafkaTopic,
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()

	log.Println("Successfully connected to Kafka. Starting streaming...")

	const layout = "2006-01-02 15:04:05"
	count := 0

	for {
		record, err := reader.Read()
		if err != nil {
			log.Printf("Finished reading CSV or encountered error: %v", err)
			break
		}

		rowMap := make(map[string]interface{})
		var originalPickup time.Time
		var originalDropoff time.Time

		for i, header := range headers {
			val := record[i]

			if header == "tpep_pickup_datetime" {
				t, err := time.Parse(layout, val)
				if err == nil {
					originalPickup = t
				}
			} else if header == "tpep_dropoff_datetime" {
				t, err := time.Parse(layout, val)
				if err == nil {
					originalDropoff = t
				}
			} else {
				if floatVal, err := strconv.ParseFloat(val, 64); err == nil {
					rowMap[header] = floatVal
				} else {
					rowMap[header] = val
				}
			}
		}

		// Shift timestamps to current time
		now := time.Now()
		duration := originalDropoff.Sub(originalPickup)
		newPickup := now
		newDropoff := now.Add(duration)

		rowMap["tpep_pickup_datetime"] = newPickup.Format(layout)
		rowMap["tpep_dropoff_datetime"] = newDropoff.Format(layout)

		jsonData, err := json.Marshal(rowMap)
		if err != nil {
			log.Printf("Failed to marshal JSON: %v", err)
			continue
		}

		err = w.WriteMessages(context.Background(),
			kafka.Message{
				Value: jsonData,
			},
		)
		if err != nil {
			log.Printf("Failed to write message to Kafka: %v", err)
		} else {
			count++
			if count%10 == 0 {
				log.Printf("Sent %d records so far. Latest Pick-up: %s", count, rowMap["tpep_pickup_datetime"])
			}
		}

		time.Sleep(sleepDuration)
	}

	log.Printf("Producer finished. Total records sent: %d", count)
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
