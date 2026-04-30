package producer

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log"
	"sync"
	"time"

	"kafka-bench/internal/config"
	"kafka-bench/internal/metrics"

	"github.com/IBM/sarama"
)

/*

implements kafka producer pool using sarama async producers

create multiple producer workers, each sends messages at a controlled rate, each worker has its own sarama.AsyncProducer, separate goroutines reads success/error acks

records metrics like sent count, error count, latency

each producer is independently feeding kafka, nice for throughput

for a produer there is a sender and an ack reader


*/

// Pool manages N async producer goroutines, each with its own sarama.AsyncProducer.
// No shared mutex on the hot path — each goroutine owns its producer exclusively.
type Pool struct {
	cfg         *config.BenchConfig
	collector   *metrics.Collector
	producers   []sarama.AsyncProducer
	basePayload []byte
}

func NewPool(cfg *config.BenchConfig, collector *metrics.Collector) (*Pool, error) {
	scfg := buildSaramaConfig(cfg)
	producers := make([]sarama.AsyncProducer, cfg.ProducerWorkers)
	for i := range producers {
		p, err := sarama.NewAsyncProducer(cfg.Brokers, scfg)
		if err != nil {
			// Close already-opened producers on failure
			for j := 0; j < i; j++ {
				producers[j].Close()
			}
			return nil, fmt.Errorf("producer[%d]: %w", i, err)
		}
		producers[i] = p
	}

	// Pre-generate a random base payload once.
	// Layout: [0:8] worker+seq  [8:16] send-timestamp (ns)  [16:] fixed body
	// The random body defeats compression ratio inflation (benchmarks should
	// reflect real workloads, not compressor best-case).
	basePayload := make([]byte, cfg.MessageSize)
	rand.Read(basePayload)

	return &Pool{
		cfg:         cfg,
		collector:   collector,
		producers:   producers,
		basePayload: basePayload,
	}, nil
}

// Run launches sender + ack-reader pairs for every producer, then blocks until
// ctx is cancelled and all goroutines have returned.
func (p *Pool) Run(ctx context.Context) {
	// Distribute target rate across workers.
	// Use float64 to avoid integer truncation at high worker counts.
	ratePerWorker := float64(p.cfg.TargetRate) / float64(p.cfg.ProducerWorkers)
	if ratePerWorker < 1 {
		ratePerWorker = 1
	}
	intervalNs := int64(float64(time.Second) / ratePerWorker)

	var wg sync.WaitGroup
	for i, prod := range p.producers {
		wg.Add(2)
		go func(id int, prod sarama.AsyncProducer) {
			defer wg.Done()
			p.runSender(ctx, id, prod, intervalNs)
		}(i, prod)
		go func(prod sarama.AsyncProducer) {
			defer wg.Done()
			p.runAckReader(ctx, prod)
		}(prod)
	}
	wg.Wait()

	for _, prod := range p.producers {
		// AsyncClose drains the queue before closing
		prod.AsyncClose()
	}
}

// runSender pumps messages at the configured rate using a time.Ticker.
// Each message gets a unique 8-byte header: [4B worker-id | 4B seq] + 8B timestamp.
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
			// Build message: fresh allocation required because sarama owns the
			// buffer after enqueueing (it may batch/compress asynchronously).
			msg := make([]byte, p.cfg.MessageSize)
			copy(msg, p.basePayload)

			// Stamp header
			binary.LittleEndian.PutUint32(msg[0:4], workerID)
			binary.LittleEndian.PutUint32(msg[4:8], seq)
			binary.LittleEndian.PutUint64(msg[8:16], uint64(time.Now().UnixNano()))
			seq++

			pm := &sarama.ProducerMessage{
				Topic: p.cfg.Topic,
				Value: sarama.ByteEncoder(msg),
			}

			select {
			case prod.Input() <- pm:
				// enqueued; ack-reader will call RecordSent
			case <-ctx.Done():
				return
			}
		}
	}
}

// runAckReader drains the success and error channels.
// It is the only place RecordSent/RecordError/RecordLatency are called for this producer,
// so there's no contention between sender and ack-reader on shared state.
func (p *Pool) runAckReader(ctx context.Context, prod sarama.AsyncProducer) {
	for {
		select {
		case msg, ok := <-prod.Successes():
			if !ok {
				return
			}
			// Decode the send timestamp from the message body to measure
			// end-to-end latency (enqueue → broker ack).
			if msg != nil {
				if data, err := msg.Value.Encode(); err == nil && len(data) >= 16 {
					sentNs := int64(binary.LittleEndian.Uint64(data[8:16]))
					lat := time.Duration(time.Now().UnixNano() - sentNs)
					p.collector.RecordLatency(lat)
				}
			}
			p.collector.RecordSent(p.cfg.MessageSize)

		case err, ok := <-prod.Errors():
			if !ok {
				return
			}
			if err != nil {
				p.collector.RecordError()
				log.Printf("producer error (topic=%s partition=%d): %v",
					err.Msg.Topic, err.Msg.Partition, err.Err)
			}

		case <-ctx.Done():
			// Drain remaining acks before exiting so RecordSent stays accurate
			for {
				select {
				case msg, ok := <-prod.Successes():
					if !ok {
						return
					}
					if msg != nil {
						p.collector.RecordSent(p.cfg.MessageSize)
					}
				case _, ok := <-prod.Errors():
					if !ok {
						return
					}
					p.collector.RecordError()
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

	// Must return successes so we can measure latency
	scfg.Producer.Return.Successes = true
	scfg.Producer.Return.Errors = true

	// Acks
	switch cfg.Acks {
	case 0:
		scfg.Producer.RequiredAcks = sarama.NoResponse
	case -1:
		scfg.Producer.RequiredAcks = sarama.WaitForAll
		// Idempotent requires acks=all + MaxOpenRequests=1
		scfg.Producer.Idempotent = true
		scfg.Net.MaxOpenRequests = 1
	default:
		scfg.Producer.RequiredAcks = sarama.WaitForLocal
	}

	// Compression
	switch cfg.Compression {
	case "gzip":
		scfg.Producer.Compression = sarama.CompressionGZIP
		scfg.Producer.CompressionLevel = 1 // fastest
	case "snappy":
		scfg.Producer.Compression = sarama.CompressionSnappy
	case "lz4":
		scfg.Producer.Compression = sarama.CompressionLZ4
	case "zstd":
		scfg.Producer.Compression = sarama.CompressionZSTD
	default:
		scfg.Producer.Compression = sarama.CompressionNone
	}

	// Batching — flush when either limit is hit
	scfg.Producer.Flush.Messages = 500
	scfg.Producer.Flush.Frequency = time.Duration(cfg.LingerMs) * time.Millisecond
	scfg.Producer.Flush.MaxMessages = 10_000
	scfg.Producer.MaxMessageBytes = cfg.BatchSize

	// Retry
	scfg.Producer.Retry.Max = 3
	scfg.Producer.Retry.Backoff = 100 * time.Millisecond

	// Network
	if scfg.Net.MaxOpenRequests == 0 {
		scfg.Net.MaxOpenRequests = 20
	}
	scfg.Net.DialTimeout = 10 * time.Second
	scfg.Net.ReadTimeout = 30 * time.Second
	scfg.Net.WriteTimeout = 30 * time.Second

	// Buffered channels — size for burst headroom at 10k msg/s
	scfg.ChannelBufferSize = 8192

	return scfg
}
