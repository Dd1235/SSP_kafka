package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

const (
	broker  = "localhost:9092"
	topic   = "benchmark-test"
	records = 100000
	msgSize = 1024
)

func main() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	// Force a timeout so it doesn't hang forever
	config.Net.DialTimeout = 5 * time.Second
	config.Producer.Timeout = 5 * time.Second
	config.Metadata.Retry.Max = 3

	producer, err := sarama.NewAsyncProducer([]string{broker}, config)
	if err != nil {
		log.Fatalf("Failed to start producer: %v", err)
	}
	defer producer.Close()

	payload := make([]byte, msgSize)
	start := time.Now()

	fmt.Printf("🚀 Starting benchmark: %d messages...\n", records)

	// Background sender
	go func() {
		for i := 0; i < records; i++ {
			producer.Input() <- &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.ByteEncoder(payload),
			}
		}
	}()

	// Success listener with progress reporting
	successes := 0
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for successes < records {
		select {
		case <-producer.Successes():
			successes++
		case err := <-producer.Errors():
			log.Fatalf("❌ CRITICAL ERROR: %v", err)
		case <-ticker.C:
			fmt.Printf("... Progress: %d/%d (%.1f%%)\n", successes, records, float64(successes)/records*100)
		case <-time.After(10 * time.Second):
			log.Fatal("TIMEOUT: No messages acknowledged for 10 seconds. Check Docker logs.")
		}
	}

	// ... after the loop finishes ...
	duration := time.Since(start)
	throughput := float64(records) / duration.Seconds()
	dataRate := (throughput * float64(msgSize)) / 1024 / 1024

	fmt.Println("\n" + strings.Repeat("-", 30))
	fmt.Printf("%-15s : %s\n", "Metric", "Value")
	fmt.Println(strings.Repeat("-", 30))
	fmt.Printf("%-15s : %d\n", "Total Msgs", records)
	fmt.Printf("%-15s : %d bytes\n", "Msg Size", msgSize)
	fmt.Printf("%-15s : %.2f sec\n", "Duration", duration.Seconds())
	fmt.Printf("%-15s : %.2f msg/sec\n", "Throughput", throughput)
	fmt.Printf("%-15s : %.2f MB/sec\n", "Data Rate", dataRate)
	fmt.Println(strings.Repeat("-", 30))
}
