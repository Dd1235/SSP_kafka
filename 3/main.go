package main

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

const (
	broker        = "localhost:9092"
	topic         = "backpressure-test"
	totalMessages = 10000
	producerRate  = 1000                 // Messages per second
	consumerDelay = 5 * time.Millisecond // Simulate a slow consumer
)

type LatencyResult struct {
	latencies []time.Duration
	mu        sync.Mutex
}

func main() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// 1. Setup Admin to clear topic
	admin, _ := sarama.NewClusterAdmin([]string{broker}, config)
	admin.DeleteTopic(topic)
	time.Sleep(1 * time.Second) // Wait for deletion
	admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false)

	results := &LatencyResult{latencies: make([]time.Duration, 0, totalMessages)}
	var wg sync.WaitGroup
	wg.Add(2)

	// 2. PRODUCER: Controlled Rate Injection
	go func() {
		defer wg.Done()
		producer, _ := sarama.NewSyncProducer([]string{broker}, config)
		defer producer.Close()

		ticker := time.NewTicker(time.Second / producerRate)
		defer ticker.Stop()

		fmt.Printf("📢 Producer started: %d msg/sec\n", producerRate)
		for i := 0; i < totalMessages; i++ {
			<-ticker.C
			timestamp := time.Now().UnixNano()
			msg := &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.StringEncoder(fmt.Sprintf("%d", timestamp)),
			}
			producer.SendMessage(msg)
		}
		fmt.Println("✅ Producer finished sending.")
	}()

	// 3. CONSUMER: Intentional Slowdown & Measurement
	go func() {
		defer wg.Done()
		consumer, _ := sarama.NewConsumer([]string{broker}, config)
		defer consumer.Close()

		partConsumer, _ := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)

		fmt.Printf("🐌 Consumer started: %v delay per message\n", consumerDelay)
		count := 0
		for count < totalMessages {
			msg := <-partConsumer.Messages()

			// Measure Latency (Time in Pipeline)
			var sentTime int64
			fmt.Sscanf(string(msg.Value), "%d", &sentTime)
			latency := time.Duration(time.Now().UnixNano() - sentTime)

			results.mu.Lock()
			results.latencies = append(results.latencies, latency)
			results.mu.Unlock()

			// Simulate "Work" / Bottleneck
			time.Sleep(consumerDelay)

			count++
			if count%1000 == 0 {
				fmt.Printf("... Consumer reached: %d/%d (Lag is growing)\n", count, totalMessages)
			}
		}
		calculateMetrics(results.latencies)
	}()

	wg.Wait()
}

func calculateMetrics(latencies []time.Duration) {
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	p50 := latencies[len(latencies)*50/100]
	p95 := latencies[len(latencies)*95/100]
	p99 := latencies[len(latencies)*99/100]

	fmt.Println("\n--- 📊 Preliminary Results ---")
	fmt.Printf("P50 Latency: %v\n", p50)
	fmt.Printf("P95 Latency: %v (Tail)\n", p95)
	fmt.Printf("P99 Latency: %v (Tail)\n", p99)
	fmt.Printf("Max Latency: %v\n", latencies[len(latencies)-1])
	fmt.Println("------------------------------")
}
