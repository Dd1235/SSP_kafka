package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	// This is the Kafka topic we want to read from.
	topic := "comments"

	// Connect to the Kafka broker cluster.
	// Even though there is only one broker here, Sarama expects a slice.
	worker, err := connectConsumer([]string{"localhost:29092"})
	if err != nil {
		panic(err)
	}
	defer worker.Close()

	// Consume from partition 0, starting from the oldest available message.
	// This means the consumer begins at the start of the log, like rewinding
	// a cassette tape to the first song.
	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	fmt.Println("Consumer started...")

	// sigchan listens for Ctrl+C or termination signals from the OS.
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	msgCount := 0
	doneCh := make(chan struct{})

	// Start a goroutine so the main thread can wait cleanly on doneCh.
	go func() {
		for {
			select {
			// If Kafka reports an error while consuming, print it.
			case err := <-consumer.Errors():
				fmt.Println("Consumer error:", err)

			// When a message arrives, print some useful details.
			case msg := <-consumer.Messages():
				msgCount++
				fmt.Printf(
					"Received message count: %d | Topic: %s | Partition: %d | Offset: %d | Message: %s\n",
					msgCount,
					msg.Topic,
					msg.Partition,
					msg.Offset,
					string(msg.Value),
				)

			// If user presses Ctrl+C, signal graceful shutdown.
			case <-sigchan:
				fmt.Println("Interruption detected")
				doneCh <- struct{}{}
				return
			}
		}
	}()

	// Wait until the goroutine tells us to stop.
	<-doneCh
	fmt.Println("Processed", msgCount, "messages")
}

// connectConsumer creates a Kafka consumer connection.
//
// This is the doorway into Kafka for reading messages.
func connectConsumer(brokersURL []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()

	// Enable the Errors() channel so we can listen for consumer failures.
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(brokersURL, config)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}
