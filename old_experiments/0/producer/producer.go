package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2"
)

// Comment is the shape of the incoming request body.
// Example JSON:
//
//	{
//	  "text": "hello kafka"
//	}
type Comment struct {
	Text string `json:"text"`
}

func main() {
	// Create the Fiber app: this is the HTTP server that will listen for requests.
	app := fiber.New()

	// Group routes under /api/v1 so the API has a clean versioned prefix.
	api := app.Group("/api/v1")

	// Register POST /api/v1/comments
	// When a client sends a comment here, createComment will handle it.
	api.Post("/comments", createComment)

	// Start the server on port 3000.
	log.Fatal(app.Listen(":3000"))
}

// ConnectProducer creates a synchronous Kafka producer.
//
// "Synchronous" means SendMessage waits until Kafka acknowledges whether
// the message was successfully written or not. This is nice for learning
// and debugging because the request path can know if the push worked.
func ConnectProducer(brokersURL []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()

	// We want SendMessage to return partition and offset information.
	config.Producer.Return.Successes = true

	// WaitForAll means Kafka waits for all in-sync replicas to acknowledge.
	// Good for durability, though a little slower.
	config.Producer.RequiredAcks = sarama.WaitForAll

	// Retry a few times if sending fails temporarily.
	config.Producer.Retry.Max = 5

	// Create the producer connection to the Kafka brokers.
	producer, err := sarama.NewSyncProducer(brokersURL, config)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

// PushCommentToQueue sends the given message bytes into the specified Kafka topic.
//
// Picture this as tossing a sealed envelope onto a moving conveyor belt.
// Kafka is the conveyor belt, and "topic" is the labeled lane it goes into.
func PushCommentToQueue(topic string, message []byte) error {
	brokersURL := []string{"localhost:29092"}

	producer, err := ConnectProducer(brokersURL)
	if err != nil {
		return err
	}
	defer producer.Close()

	// Build the Kafka message.
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
	}

	// Send the message and get back where Kafka stored it.
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf(
		"Message stored in topic=%s partition=%d offset=%d\n",
		topic, partition, offset,
	)

	return nil
}

// createComment handles POST /api/v1/comments
//
// Flow:
// 1. Read JSON body from client
// 2. Convert it into Comment struct
// 3. Marshal it back into JSON bytes
// 4. Push bytes into Kafka topic "comments"
// 5. Return HTTP response to client
func createComment(c *fiber.Ctx) error {
	// Create an empty Comment struct to fill from the request body.
	cmt := new(Comment)

	// Parse request JSON into cmt.
	// Example incoming body: {"text":"nice post"}
	if err := c.BodyParser(cmt); err != nil {
		log.Println("body parse error:", err)

		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"message": "invalid request body",
			"error":   err.Error(),
		})
	}

	// Convert the Go struct into raw JSON bytes so it can be sent to Kafka.
	cmtInBytes, err := json.Marshal(cmt)
	if err != nil {
		log.Println("marshal error:", err)

		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"message": "failed to serialize comment",
			"error":   err.Error(),
		})
	}

	// Push the serialized comment into the Kafka topic named "comments".
	if err := PushCommentToQueue("comments", cmtInBytes); err != nil {
		log.Println("kafka push error:", err)

		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"message": "failed to push comment to Kafka",
			"error":   err.Error(),
		})
	}

	// If everything worked, tell the client the comment was accepted.
	return c.JSON(fiber.Map{
		"success": true,
		"message": "Comment pushed successfully",
		"comment": cmt,
	})
}
