// Package topic handles topic lifecycle: ensure, reset, describe.
//
// Reset-before-run is the default — the interim version didn't reset and that
// masked consumer bugs (old data from previous runs was consumed as if new).
package topic

import (
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"kafka-bench-v4/internal/config"
)

// Ensure creates the topic if missing. If reset=true, deletes and recreates.
func Ensure(cfg *config.BenchConfig) error {
	scfg := sarama.NewConfig()
	scfg.Version = sarama.V3_6_0_0
	admin, err := sarama.NewClusterAdmin(cfg.Brokers, scfg)
	if err != nil {
		return fmt.Errorf("cluster admin: %w", err)
	}
	defer admin.Close()

	topics, err := admin.ListTopics()
	if err != nil {
		return fmt.Errorf("list topics: %w", err)
	}

	if _, exists := topics[cfg.Topic]; exists && cfg.ResetTopic {
		fmt.Printf("[setup] deleting topic %q for clean slate\n", cfg.Topic)
		if err := admin.DeleteTopic(cfg.Topic); err != nil {
			return fmt.Errorf("delete topic: %w", err)
		}
		// Broker needs a moment for deletion to finalize.
		time.Sleep(2 * time.Second)
		topics, _ = admin.ListTopics()
	}

	if _, exists := topics[cfg.Topic]; exists {
		fmt.Printf("[setup] topic %q exists, reusing\n", cfg.Topic)
		return nil
	}

	err = admin.CreateTopic(cfg.Topic, &sarama.TopicDetail{
		NumPartitions:     int32(cfg.Partitions),
		ReplicationFactor: 1,
	}, false)
	if err != nil {
		return fmt.Errorf("create topic: %w", err)
	}
	fmt.Printf("[setup] created topic %q (%d partitions)\n", cfg.Topic, cfg.Partitions)
	return nil
}
