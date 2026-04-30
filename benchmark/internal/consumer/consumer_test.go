package consumer

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"kafka-bench-v4/internal/config"
	"kafka-bench-v4/internal/metrics"
	"kafka-bench-v4/internal/payload"
)

func TestBuildSaramaConfigInitialOffset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want int64
	}{
		{name: "oldest", in: "oldest", want: sarama.OffsetOldest},
		{name: "newest", in: "newest", want: sarama.OffsetNewest},
		{name: "unknown defaults oldest", in: "anything", want: sarama.OffsetOldest},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := consumerTestConfig()
			cfg.InitialOffset = tt.in
			scfg := buildSaramaConfig(cfg)
			if scfg.Consumer.Offsets.Initial != tt.want {
				t.Fatalf("Initial = %d, want %d", scfg.Consumer.Offsets.Initial, tt.want)
			}
			if scfg.Consumer.Fetch.Min != 1 || scfg.ChannelBufferSize != 4096 {
				t.Fatalf("fetch/buffer = %d/%d", scfg.Consumer.Fetch.Min, scfg.ChannelBufferSize)
			}
		})
	}
}

func TestConsumeClaimRecordsReceivedLatencyAndMarks(t *testing.T) {
	t.Parallel()

	cfg := consumerTestConfig()
	col := metrics.New()
	col.Start()
	pool := &Pool{cfg: cfg, col: col}
	h := &handler{pool: pool}

	msgWithHeader := make([]byte, 32)
	payload.WriteHeader(msgWithHeader, 1, 2, time.Now().Add(-time.Millisecond).UnixNano())
	msgWithoutHeader := []byte{1, 2, 3}

	claim := newFakeClaim([]*sarama.ConsumerMessage{
		{Topic: cfg.Topic, Partition: 0, Offset: 1, Value: msgWithHeader},
		{Topic: cfg.Topic, Partition: 0, Offset: 2, Value: msgWithoutHeader},
	})
	sess := &fakeSession{ctx: context.Background()}

	if err := h.ConsumeClaim(sess, claim); err != nil {
		t.Fatalf("ConsumeClaim error = %v", err)
	}

	col.MarkEnd()
	final := col.FinalSnapshot()
	if final.Received != 2 || final.BytesReceived != int64(len(msgWithHeader)+len(msgWithoutHeader)) {
		t.Fatalf("received = %d bytes %d", final.Received, final.BytesReceived)
	}
	if final.E2ELatency.Count != 1 {
		t.Fatalf("E2ELatency.Count = %d", final.E2ELatency.Count)
	}
	if sess.marked != 2 {
		t.Fatalf("marked = %d, want 2", sess.marked)
	}
}

func TestConsumeClaimAppliesDelay(t *testing.T) {
	cfg := consumerTestConfig()
	cfg.ConsumerDelay = 2 * time.Millisecond
	cfg.ConsumerJitter = time.Millisecond
	col := metrics.New()
	col.Start()
	pool := &Pool{cfg: cfg, col: col, jitterNs: cfg.ConsumerJitter.Nanoseconds()}
	pool.currentDelayNs.Store(cfg.ConsumerDelay.Nanoseconds())
	h := &handler{pool: pool}
	claim := newFakeClaim([]*sarama.ConsumerMessage{{Topic: cfg.Topic, Partition: 0, Value: make([]byte, 16)}})
	sess := &fakeSession{ctx: context.Background()}

	start := time.Now()
	if err := h.ConsumeClaim(sess, claim); err != nil {
		t.Fatalf("ConsumeClaim error = %v", err)
	}
	if time.Since(start) < cfg.ConsumerDelay {
		t.Fatalf("ConsumeClaim elapsed less than delay: %v", time.Since(start))
	}
}

func TestPhaserSwitchesDelay(t *testing.T) {
	cfg := consumerTestConfig()
	cfg.ConsumerDelay = 20 * time.Millisecond
	cfg.PhaseDuration = time.Millisecond
	cfg.PhaseDelay = 3 * time.Millisecond
	pool := &Pool{cfg: cfg}
	pool.currentDelayNs.Store(cfg.ConsumerDelay.Nanoseconds())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		pool.phaser(ctx)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("phaser did not return")
	}
	if got := time.Duration(pool.currentDelayNs.Load()); got != cfg.PhaseDelay {
		t.Fatalf("delay = %v, want %v", got, cfg.PhaseDelay)
	}
}

func consumerTestConfig() *config.BenchConfig {
	return &config.BenchConfig{
		Brokers:        []string{"localhost:9092"},
		Topic:          "topic",
		GroupID:        "group",
		InitialOffset:  "oldest",
		ConsumerDelay:  0,
		ConsumerJitter: 0,
	}
}

type fakeSession struct {
	ctx    context.Context
	marked int
}

func (f *fakeSession) Claims() map[string][]int32 { return nil }
func (f *fakeSession) MemberID() string           { return "member" }
func (f *fakeSession) GenerationID() int32        { return 1 }
func (f *fakeSession) MarkOffset(string, int32, int64, string) {
}
func (f *fakeSession) Commit() {}
func (f *fakeSession) ResetOffset(string, int32, int64, string) {
}
func (f *fakeSession) MarkMessage(*sarama.ConsumerMessage, string) { f.marked++ }
func (f *fakeSession) Context() context.Context                    { return f.ctx }

type fakeClaim struct {
	topic     string
	partition int32
	messages  chan *sarama.ConsumerMessage
}

func newFakeClaim(messages []*sarama.ConsumerMessage) *fakeClaim {
	ch := make(chan *sarama.ConsumerMessage, len(messages))
	for _, msg := range messages {
		ch <- msg
	}
	close(ch)
	return &fakeClaim{topic: "topic", messages: ch}
}

func (f *fakeClaim) Topic() string                            { return f.topic }
func (f *fakeClaim) Partition() int32                         { return f.partition }
func (f *fakeClaim) InitialOffset() int64                     { return 0 }
func (f *fakeClaim) HighWaterMarkOffset() int64               { return 0 }
func (f *fakeClaim) Messages() <-chan *sarama.ConsumerMessage { return f.messages }
