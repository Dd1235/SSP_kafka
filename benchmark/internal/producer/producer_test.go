package producer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"kafka-bench-v4/internal/config"
	"kafka-bench-v4/internal/metrics"
)

func TestBuildSaramaConfigAcksAndCompression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		acks        int
		compression string
		wantAcks    sarama.RequiredAcks
		wantCodec   sarama.CompressionCodec
		wantIdem    bool
		wantOpenReq int
	}{
		{name: "acks0 none", acks: 0, compression: "none", wantAcks: sarama.NoResponse, wantCodec: sarama.CompressionNone, wantOpenReq: 5},
		{name: "acks1 snappy", acks: 1, compression: "snappy", wantAcks: sarama.WaitForLocal, wantCodec: sarama.CompressionSnappy, wantOpenReq: 5},
		{name: "acks all zstd", acks: -1, compression: "zstd", wantAcks: sarama.WaitForAll, wantCodec: sarama.CompressionZSTD, wantIdem: true, wantOpenReq: 1},
		{name: "gzip", acks: 1, compression: "gzip", wantAcks: sarama.WaitForLocal, wantCodec: sarama.CompressionGZIP, wantOpenReq: 5},
		{name: "lz4", acks: 1, compression: "lz4", wantAcks: sarama.WaitForLocal, wantCodec: sarama.CompressionLZ4, wantOpenReq: 5},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := producerTestConfig()
			cfg.Acks = tt.acks
			cfg.Compression = tt.compression
			scfg := buildSaramaConfig(cfg)

			if scfg.Producer.RequiredAcks != tt.wantAcks {
				t.Fatalf("RequiredAcks = %v, want %v", scfg.Producer.RequiredAcks, tt.wantAcks)
			}
			if scfg.Producer.Compression != tt.wantCodec {
				t.Fatalf("Compression = %v, want %v", scfg.Producer.Compression, tt.wantCodec)
			}
			if scfg.Producer.Idempotent != tt.wantIdem {
				t.Fatalf("Idempotent = %v, want %v", scfg.Producer.Idempotent, tt.wantIdem)
			}
			if scfg.Net.MaxOpenRequests != tt.wantOpenReq {
				t.Fatalf("MaxOpenRequests = %d, want %d", scfg.Net.MaxOpenRequests, tt.wantOpenReq)
			}
			if !scfg.Producer.Return.Successes || !scfg.Producer.Return.Errors {
				t.Fatal("producer return channels disabled")
			}
			if scfg.Producer.Flush.Bytes != cfg.BatchBytes {
				t.Fatalf("Flush.Bytes = %d", scfg.Producer.Flush.Bytes)
			}
			if scfg.Producer.Flush.Frequency != time.Duration(cfg.LingerMs)*time.Millisecond {
				t.Fatalf("Flush.Frequency = %v", scfg.Producer.Flush.Frequency)
			}
			if scfg.Producer.MaxMessageBytes != cfg.MaxMessageBytes {
				t.Fatalf("MaxMessageBytes = %d", scfg.Producer.MaxMessageBytes)
			}
		})
	}
}

func TestRunAckReaderRecordsSuccessesAndErrors(t *testing.T) {
	cfg := producerTestConfig()
	col := metrics.New()
	col.Start()
	pool := &Pool{cfg: cfg, col: col}
	prod := newFakeAsyncProducer()

	done := make(chan struct{})
	go func() {
		defer close(done)
		pool.runAckReader(context.Background(), prod)
	}()

	prod.successes <- &sarama.ProducerMessage{Metadata: inFlight{enqueuedAt: time.Now().Add(-time.Millisecond)}}
	eventually(t, func() bool { return col.FinalSnapshot().Sent == 1 })
	prod.errors <- &sarama.ProducerError{
		Msg: &sarama.ProducerMessage{Topic: cfg.Topic, Partition: 2},
		Err: errors.New("boom"),
	}
	eventually(t, func() bool { return col.FinalSnapshot().Errors == 1 })
	close(prod.successes)
	close(prod.errors)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runAckReader did not return")
	}

	col.MarkEnd()
	final := col.FinalSnapshot()
	if final.Sent != 1 || final.Errors != 1 || final.BytesSent != int64(cfg.MessageSize) {
		t.Fatalf("final counters = sent %d errors %d bytes %d", final.Sent, final.Errors, final.BytesSent)
	}
	if final.AckLatency.Count != 1 {
		t.Fatalf("AckLatency.Count = %d", final.AckLatency.Count)
	}
}

func TestRunAckReaderDrainsAfterContextCancel(t *testing.T) {
	cfg := producerTestConfig()
	col := metrics.New()
	col.Start()
	pool := &Pool{cfg: cfg, col: col}
	prod := newFakeAsyncProducer()

	ctx, cancel := context.WithCancel(context.Background())
	prod.successes <- &sarama.ProducerMessage{}
	cancel()
	pool.runAckReader(ctx, prod)

	col.MarkEnd()
	if got := col.FinalSnapshot().Sent; got != 1 {
		t.Fatalf("sent = %d, want 1", got)
	}
}

func TestThrottleControllerPausesAndResumes(t *testing.T) {
	cfg := producerTestConfig()
	cfg.MaxLag = 10
	cfg.ResumeLag = 5
	cfg.BPPollInterval = time.Millisecond
	lag := &fakeLagSnapshot{}
	lag.current.Store(11)
	pool := &Pool{cfg: cfg, lagPoller: lag}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go pool.runThrottleController(ctx, done)

	eventually(t, func() bool { return pool.paused.Load() })
	lag.current.Store(3)
	eventually(t, func() bool { return !pool.paused.Load() })
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("controller did not stop")
	}
	if pool.ThrottleEvents() != 1 {
		t.Fatalf("ThrottleEvents = %d, want 1", pool.ThrottleEvents())
	}
	if pool.ThrottlePaused() <= 0 {
		t.Fatalf("ThrottlePaused = %v, want > 0", pool.ThrottlePaused())
	}
}

func TestWaitForCreditReturnsOnContextCancel(t *testing.T) {
	cfg := producerTestConfig()
	cfg.BPPollInterval = time.Millisecond
	pool := &Pool{cfg: cfg}
	pool.paused.Store(true)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	pool.waitForCredit(ctx)
	if time.Since(start) > 50*time.Millisecond {
		t.Fatal("waitForCredit did not return promptly")
	}
}

type fakeLagSnapshot struct {
	current atomic.Int64
}

func (f *fakeLagSnapshot) Current() int64 { return f.current.Load() }

func producerTestConfig() *config.BenchConfig {
	return &config.BenchConfig{
		Brokers:         []string{"localhost:9092"},
		Topic:           "topic",
		MessageSize:     128,
		TargetRate:      100,
		ProducerWorkers: 1,
		BatchBytes:      2048,
		LingerMs:        7,
		Acks:            1,
		Compression:     "none",
		MaxMessageBytes: 4096,
		BPPollInterval:  time.Millisecond,
	}
}

func eventually(t *testing.T, fn func() bool) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met")
}

type fakeAsyncProducer struct {
	input     chan *sarama.ProducerMessage
	successes chan *sarama.ProducerMessage
	errors    chan *sarama.ProducerError
}

func newFakeAsyncProducer() *fakeAsyncProducer {
	return &fakeAsyncProducer{
		input:     make(chan *sarama.ProducerMessage, 4),
		successes: make(chan *sarama.ProducerMessage, 4),
		errors:    make(chan *sarama.ProducerError, 4),
	}
}

func (f *fakeAsyncProducer) AsyncClose()                               {}
func (f *fakeAsyncProducer) Close() error                              { return nil }
func (f *fakeAsyncProducer) Input() chan<- *sarama.ProducerMessage     { return f.input }
func (f *fakeAsyncProducer) Successes() <-chan *sarama.ProducerMessage { return f.successes }
func (f *fakeAsyncProducer) Errors() <-chan *sarama.ProducerError      { return f.errors }
func (f *fakeAsyncProducer) IsTransactional() bool                     { return false }
func (f *fakeAsyncProducer) TxnStatus() sarama.ProducerTxnStatusFlag   { return 0 }
func (f *fakeAsyncProducer) BeginTxn() error                           { return nil }
func (f *fakeAsyncProducer) CommitTxn() error                          { return nil }
func (f *fakeAsyncProducer) AbortTxn() error                           { return nil }
func (f *fakeAsyncProducer) AddOffsetsToTxn(map[string][]*sarama.PartitionOffsetMetadata, string) error {
	return nil
}
func (f *fakeAsyncProducer) AddMessageToTxn(*sarama.ConsumerMessage, string, *string) error {
	return nil
}
