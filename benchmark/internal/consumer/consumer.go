// Package consumer implements a Sarama ConsumerGroup pool that:
//   - extracts the producer timestamp from each message header
//   - records end-to-end (enqueue→consume) latency
//   - optionally sleeps per-message to simulate slow processing
//   - supports a phased slowdown: high delay for the first PhaseDuration, then
//     switch to PhaseDelay for the rest of the run (recovery test)
//
// Fixes compared to the interim:
//   - Initial offset defaults to `oldest` so no messages are missed during
//     consumer-group rebalance (which takes ~3–5s).
//   - RecordReceived is paired with E2E-latency recording, not just a counter.
package consumer

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"kafka-bench-v4/internal/config"
	"kafka-bench-v4/internal/metrics"
	"kafka-bench-v4/internal/payload"
)

type Pool struct {
	cfg   *config.BenchConfig
	col   *metrics.Collector
	group sarama.ConsumerGroup
	// shared by all worker goroutines; updated by Phaser.
	currentDelayNs atomic.Int64
	jitterNs       int64
}

func NewPool(cfg *config.BenchConfig, col *metrics.Collector) (*Pool, error) {
	scfg := buildSaramaConfig(cfg)
	grp, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupID, scfg)
	if err != nil {
		return nil, fmt.Errorf("consumer group: %w", err)
	}
	p := &Pool{cfg: cfg, col: col, group: grp, jitterNs: cfg.ConsumerJitter.Nanoseconds()}
	p.currentDelayNs.Store(cfg.ConsumerDelay.Nanoseconds())
	return p, nil
}

func buildSaramaConfig(cfg *config.BenchConfig) *sarama.Config {
	scfg := sarama.NewConfig()
	scfg.Version = sarama.V3_6_0_0
	scfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}
	switch cfg.InitialOffset {
	case "newest":
		scfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	default:
		scfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
	scfg.Consumer.Fetch.Min = 1
	scfg.Consumer.Fetch.Default = 1 << 20
	scfg.Consumer.Fetch.Max = 10 << 20
	scfg.ChannelBufferSize = 4096
	// Shorter session timeout so lost workers are detected fast
	scfg.Consumer.Group.Session.Timeout = 10 * time.Second
	scfg.Consumer.Group.Heartbeat.Interval = 3 * time.Second
	return scfg
}

// Run blocks until ctx is cancelled.
func (p *Pool) Run(ctx context.Context) {
	topics := []string{p.cfg.Topic}
	h := &handler{pool: p}

	// Phaser: if configured, switches the processing delay mid-run.
	if p.cfg.PhaseDuration > 0 {
		go p.phaser(ctx)
	}

	var wg sync.WaitGroup
	for i := 0; i < p.cfg.ConsumerWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if err := p.group.Consume(ctx, topics, h); err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Printf("consumer error: %v", err)
				}
				if ctx.Err() != nil {
					return
				}
			}
		}()
	}
	wg.Wait()
	_ = p.group.Close()
}

func (p *Pool) phaser(ctx context.Context) {
	t := time.NewTimer(p.cfg.PhaseDuration)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return
	case <-t.C:
		newDelay := p.cfg.PhaseDelay
		p.currentDelayNs.Store(newDelay.Nanoseconds())
		fmt.Printf("\n[phase] consumer delay switched: %v -> %v  (recovery begins)\n\n",
			p.cfg.ConsumerDelay, newDelay)
	}
}

type handler struct{ pool *Pool }

func (h *handler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *handler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *handler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	r := rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(claim.Partition())))
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			// End-to-end latency from the producer's stamp.
			if _, _, sentNs, ok := payload.ReadHeader(msg.Value); ok && sentNs > 0 {
				lat := time.Duration(time.Now().UnixNano() - sentNs)
				if lat >= 0 {
					h.pool.col.RecordE2ELatency(lat)
				}
			}
			h.pool.col.RecordReceived(len(msg.Value))
			sess.MarkMessage(msg, "")

			// Simulated work
			if d := h.pool.currentDelayNs.Load(); d > 0 {
				j := h.pool.jitterNs
				sleep := d
				if j > 0 {
					sleep += r.Int63n(j)
				}
				time.Sleep(time.Duration(sleep))
			}
		case <-sess.Context().Done():
			return nil
		}
	}
}
