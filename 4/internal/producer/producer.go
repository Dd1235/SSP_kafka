// Package producer — goroutine-per-producer pool using sarama.AsyncProducer.
//
// Each producer has:
//   - a sender goroutine that emits messages on a time.Ticker-paced loop
//   - an ack-reader goroutine that drains Successes()/Errors(), records latency
//
// These are siblings, not sharers — each owns its AsyncProducer exclusively, so
// there's no mutex on the hot path.
//
// To add a producer knob:
//  1. Add a field to config.BenchConfig.
//  2. Thread it into buildSaramaConfig() below.
package producer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"kafka-bench-v4/internal/config"
	"kafka-bench-v4/internal/metrics"
	"kafka-bench-v4/internal/payload"

	"github.com/IBM/sarama"
)

type Pool struct {
	cfg       *config.BenchConfig
	col       *metrics.Collector
	producers []sarama.AsyncProducer
	gen       *payload.Generator
}

// inFlight tracks enqueue time per message so we can compute
// enqueue-to-ack latency precisely. Sarama's ProducerMessage.Metadata
// is the designated slot for this.
type inFlight struct{ enqueuedAt time.Time }

func NewPool(cfg *config.BenchConfig, col *metrics.Collector, gen *payload.Generator) (*Pool, error) {
	scfg := buildSaramaConfig(cfg)
	ps := make([]sarama.AsyncProducer, cfg.ProducerWorkers)
	for i := range ps {
		p, err := sarama.NewAsyncProducer(cfg.Brokers, scfg)
		if err != nil {
			for j := 0; j < i; j++ {
				ps[j].Close()
			}
			return nil, fmt.Errorf("producer[%d]: %w", i, err)
		}
		ps[i] = p
	}
	return &Pool{cfg: cfg, col: col, producers: ps, gen: gen}, nil
}

func (p *Pool) Run(ctx context.Context) {
	ratePerWorker := float64(p.cfg.TargetRate) / float64(p.cfg.ProducerWorkers)
	if ratePerWorker < 1 {
		ratePerWorker = 1
	}
	intervalNs := int64(float64(time.Second) / ratePerWorker)

	bursty := p.cfg.BurstRate > 0 && p.cfg.BurstDuration > 0 && p.cfg.BurstPeriod > 0
	var burstIntervalNs int64
	if bursty {
		burstPerWorker := float64(p.cfg.BurstRate) / float64(p.cfg.ProducerWorkers)
		if burstPerWorker < 1 {
			burstPerWorker = 1
		}
		burstIntervalNs = int64(float64(time.Second) / burstPerWorker)
	}

	var wg sync.WaitGroup
	for i, prod := range p.producers {
		wg.Add(2)
		if bursty {
			go func(id int, prod sarama.AsyncProducer) {
				defer wg.Done()
				p.runBurstySender(ctx, id, prod, intervalNs, burstIntervalNs)
			}(i, prod)
		} else {
			go func(id int, prod sarama.AsyncProducer) {
				defer wg.Done()
				p.runSender(ctx, id, prod, intervalNs)
			}(i, prod)
		}
		go func(prod sarama.AsyncProducer) {
			defer wg.Done()
			p.runAckReader(ctx, prod)
		}(prod)
	}
	wg.Wait()
	for _, prod := range p.producers {
		prod.AsyncClose()
	}
}

func (p *Pool) runSender(ctx context.Context, id int, prod sarama.AsyncProducer, intervalNs int64) {
	ticker := time.NewTicker(time.Duration(intervalNs))
	defer ticker.Stop()

	workerID := uint32(id)
	var seq uint32

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Fresh allocation each message: Sarama retains the buffer after
			// enqueue for async batching/compression.
			msg := make([]byte, p.cfg.MessageSize)
			// Generate body FIRST (fills whole buffer), then stamp header
			// over first 16 bytes.
			p.gen.Next(msg, uint64(workerID)<<32|uint64(seq))
			now := time.Now()
			payload.WriteHeader(msg, workerID, seq, now.UnixNano())
			seq++

			pm := &sarama.ProducerMessage{
				Topic:    p.cfg.Topic,
				Value:    sarama.ByteEncoder(msg),
				Metadata: inFlight{enqueuedAt: now},
			}
			select {
			case prod.Input() <- pm:
			case <-ctx.Done():
				return
			}
		}
	}
}

// runBurstySender alternates between baseIntervalNs and burstIntervalNs in a
// duty cycle defined by cfg.BurstDuration / cfg.BurstPeriod. Default config
// values do not enter this path; existing constant-rate runs are unaffected.
//
// Implementation note: a fresh ticker is created on each phase change. This
// is cheaper than retuning a running ticker, and the maximum tickers per
// second is BurstPeriod^-1 (typically O(0.1 Hz)).
func (p *Pool) runBurstySender(ctx context.Context, id int, prod sarama.AsyncProducer,
	baseIntervalNs, burstIntervalNs int64) {

	workerID := uint32(id)
	var seq uint32

	emit := func() bool {
		msg := make([]byte, p.cfg.MessageSize)
		p.gen.Next(msg, uint64(workerID)<<32|uint64(seq))
		now := time.Now()
		payload.WriteHeader(msg, workerID, seq, now.UnixNano())
		seq++
		pm := &sarama.ProducerMessage{
			Topic:    p.cfg.Topic,
			Value:    sarama.ByteEncoder(msg),
			Metadata: inFlight{enqueuedAt: now},
		}
		select {
		case prod.Input() <- pm:
			return true
		case <-ctx.Done():
			return false
		}
	}

	// One burst cycle = on-window then off-window. We schedule against the
	// pool's start-time so all workers stay phase-aligned.
	startNs := time.Now().UnixNano()
	periodNs := p.cfg.BurstPeriod.Nanoseconds()
	onNs := p.cfg.BurstDuration.Nanoseconds()

	for {
		if ctx.Err() != nil {
			return
		}
		// Determine current phase based on time since start.
		elapsed := time.Now().UnixNano() - startNs
		offset := elapsed % periodNs
		var intervalNs int64
		var until int64
		if offset < onNs {
			intervalNs = burstIntervalNs
			until = onNs - offset
		} else {
			intervalNs = baseIntervalNs
			until = periodNs - offset
		}

		ticker := time.NewTicker(time.Duration(intervalNs))
		phaseEnd := time.Now().Add(time.Duration(until))
	phaseLoop:
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				if !emit() {
					ticker.Stop()
					return
				}
				if time.Now().After(phaseEnd) {
					break phaseLoop
				}
			}
		}
		ticker.Stop()
	}
}

func (p *Pool) runAckReader(ctx context.Context, prod sarama.AsyncProducer) {
	for {
		select {
		case msg, ok := <-prod.Successes():
			if !ok {
				return
			}
			if msg != nil {
				if md, ok := msg.Metadata.(inFlight); ok {
					p.col.RecordAckLatency(time.Since(md.enqueuedAt))
				}
				p.col.RecordSent(p.cfg.MessageSize)
			}
		case err, ok := <-prod.Errors():
			if !ok {
				return
			}
			if err != nil {
				p.col.RecordError()
				log.Printf("producer error (topic=%s partition=%d): %v",
					err.Msg.Topic, err.Msg.Partition, err.Err)
			}
		case <-ctx.Done():
			// Drain
			for {
				select {
				case msg, ok := <-prod.Successes():
					if !ok {
						return
					}
					if msg != nil {
						p.col.RecordSent(p.cfg.MessageSize)
					}
				case _, ok := <-prod.Errors():
					if !ok {
						return
					}
					p.col.RecordError()
				default:
					return
				}
			}
		}
	}
}

func buildSaramaConfig(cfg *config.BenchConfig) *sarama.Config {
	scfg := sarama.NewConfig()
	scfg.Version = sarama.V3_6_0_0
	scfg.Producer.Return.Successes = true
	scfg.Producer.Return.Errors = true

	switch cfg.Acks {
	case 0:
		scfg.Producer.RequiredAcks = sarama.NoResponse
	case -1:
		scfg.Producer.RequiredAcks = sarama.WaitForAll
		scfg.Producer.Idempotent = true
		scfg.Net.MaxOpenRequests = 1
	default:
		scfg.Producer.RequiredAcks = sarama.WaitForLocal
	}

	switch cfg.Compression {
	case "gzip":
		scfg.Producer.Compression = sarama.CompressionGZIP
		scfg.Producer.CompressionLevel = 1
	case "snappy":
		scfg.Producer.Compression = sarama.CompressionSnappy
	case "lz4":
		scfg.Producer.Compression = sarama.CompressionLZ4
	case "zstd":
		scfg.Producer.Compression = sarama.CompressionZSTD
	default:
		scfg.Producer.Compression = sarama.CompressionNone
	}

	// Real batch knob: Bytes threshold, not MaxMessageBytes.
	scfg.Producer.Flush.Bytes = cfg.BatchBytes
	scfg.Producer.Flush.Frequency = time.Duration(cfg.LingerMs) * time.Millisecond
	scfg.Producer.Flush.MaxMessages = 10_000
	scfg.Producer.MaxMessageBytes = cfg.MaxMessageBytes

	scfg.Producer.Retry.Max = 3
	scfg.Producer.Retry.Backoff = 100 * time.Millisecond

	if scfg.Net.MaxOpenRequests == 0 {
		scfg.Net.MaxOpenRequests = 20
	}
	scfg.Net.DialTimeout = 10 * time.Second
	scfg.Net.ReadTimeout = 30 * time.Second
	scfg.Net.WriteTimeout = 30 * time.Second
	scfg.ChannelBufferSize = 8192

	return scfg
}
