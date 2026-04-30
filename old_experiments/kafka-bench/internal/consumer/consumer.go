package consumer

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/IBM/sarama"
	"kafka-bench/internal/config"
	"kafka-bench/internal/metrics"
)

// Pool manages N consumer goroutines spread across partitions
type Pool struct {
	cfg       *config.BenchConfig
	collector *metrics.Collector
	group     sarama.ConsumerGroup
}

func NewPool(cfg *config.BenchConfig, collector *metrics.Collector) (*Pool, error) {
	scfg := sarama.NewConfig()
	scfg.Version = sarama.V3_6_0_0
	scfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}
	scfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	// Fetch settings tuned for high throughput
	scfg.Consumer.Fetch.Min = 1
	scfg.Consumer.Fetch.Default = 1 << 20 // 1MB per fetch
	scfg.Consumer.Fetch.Max = 10 << 20    // 10MB max
	scfg.ChannelBufferSize = 4096

	group, err := sarama.NewConsumerGroup(cfg.Brokers, "bench-consumer-group", scfg)
	if err != nil {
		return nil, fmt.Errorf("consumer group: %w", err)
	}
	return &Pool{cfg: cfg, collector: collector, group: group}, nil
}

func (p *Pool) Run(ctx context.Context) {
	topics := []string{p.cfg.Topic}
	handler := &benchHandler{collector: p.collector}

	var wg sync.WaitGroup
	for i := 0; i < p.cfg.ConsumerWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if err := p.group.Consume(ctx, topics, handler); err != nil {
					log.Printf("consumer error: %v", err)
				}
				if ctx.Err() != nil {
					return
				}
			}
		}()
	}
	wg.Wait()
	p.group.Close()
}

type benchHandler struct {
	collector *metrics.Collector
}

func (h *benchHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *benchHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *benchHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			h.collector.RecordReceived()
			sess.MarkMessage(msg, "")
		case <-sess.Context().Done():
			return nil
		}
	}
}
