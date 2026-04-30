package config

import (
	"reflect"
	"testing"
	"time"
)

func TestParseArgsDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := ParseArgs(nil)
	if err != nil {
		t.Fatalf("ParseArgs(nil) error = %v", err)
	}

	if !reflect.DeepEqual(cfg.Brokers, []string{"localhost:9092"}) {
		t.Fatalf("Brokers = %v", cfg.Brokers)
	}
	if cfg.Topic != "bench-topic-v4" || cfg.GroupID != "bench-group-v4" {
		t.Fatalf("topic/group = %q/%q", cfg.Topic, cfg.GroupID)
	}
	if !cfg.ResetTopic {
		t.Fatal("ResetTopic = false, want true")
	}
	if cfg.MessageSize != 512 || cfg.TargetRate != 10000 || cfg.Partitions != 12 {
		t.Fatalf("workload defaults = size %d rate %d partitions %d", cfg.MessageSize, cfg.TargetRate, cfg.Partitions)
	}
	if cfg.Duration != 30*time.Second || cfg.WarmupDuration != 3*time.Second {
		t.Fatalf("duration defaults = %v/%v", cfg.Duration, cfg.WarmupDuration)
	}
	if cfg.PayloadMode != "mixed" || cfg.PayloadSeed != 42 {
		t.Fatalf("payload defaults = %q/%d", cfg.PayloadMode, cfg.PayloadSeed)
	}
}

func TestParseArgsCustomFlags(t *testing.T) {
	t.Parallel()

	cfg, err := ParseArgs([]string{
		"-broker", "k1:9092,k2:9092",
		"-topic", "events",
		"-group", "benchers",
		"-reset-topic=false",
		"-msg-size", "1024",
		"-rate", "1234",
		"-duration", "2m",
		"-warmup", "500ms",
		"-partitions", "3",
		"-producers", "2",
		"-batch-bytes", "8192",
		"-linger-ms", "9",
		"-acks", "-1",
		"-compression", "zstd",
		"-max-msg-bytes", "2048",
		"-consumers", "5",
		"-initial-offset", "newest",
		"-consumer-delay", "10ms",
		"-consumer-jitter", "3ms",
		"-phase-duration", "1s",
		"-phase-delay", "2ms",
		"-max-lag", "100",
		"-resume-lag", "20",
		"-bp-poll", "25ms",
		"-burst-rate", "5000",
		"-burst-duration", "2s",
		"-burst-period", "10s",
		"-payload", "json",
		"-payload-seed", "99",
		"-report-interval", "750ms",
		"-lag-interval", "800ms",
		"-sysperf-interval", "900ms",
		"-output", "out.json",
		"-json",
		"-producer-only",
	})
	if err != nil {
		t.Fatalf("ParseArgs(custom) error = %v", err)
	}

	if !reflect.DeepEqual(cfg.Brokers, []string{"k1:9092", "k2:9092"}) {
		t.Fatalf("Brokers = %v", cfg.Brokers)
	}
	if cfg.Topic != "events" || cfg.GroupID != "benchers" || cfg.ResetTopic {
		t.Fatalf("topic/group/reset = %q/%q/%v", cfg.Topic, cfg.GroupID, cfg.ResetTopic)
	}
	if cfg.MessageSize != 1024 || cfg.TargetRate != 1234 || cfg.Partitions != 3 {
		t.Fatalf("workload = size %d rate %d partitions %d", cfg.MessageSize, cfg.TargetRate, cfg.Partitions)
	}
	if cfg.Duration != 2*time.Minute || cfg.WarmupDuration != 500*time.Millisecond {
		t.Fatalf("durations = %v/%v", cfg.Duration, cfg.WarmupDuration)
	}
	if cfg.ProducerWorkers != 2 || cfg.BatchBytes != 8192 || cfg.LingerMs != 9 || cfg.Acks != -1 {
		t.Fatalf("producer tuning = workers %d batch %d linger %d acks %d", cfg.ProducerWorkers, cfg.BatchBytes, cfg.LingerMs, cfg.Acks)
	}
	if cfg.Compression != "zstd" || cfg.MaxMessageBytes != 2048 {
		t.Fatalf("compression/max = %q/%d", cfg.Compression, cfg.MaxMessageBytes)
	}
	if cfg.ConsumerWorkers != 5 || cfg.InitialOffset != "newest" {
		t.Fatalf("consumer workers/offset = %d/%q", cfg.ConsumerWorkers, cfg.InitialOffset)
	}
	if cfg.ConsumerDelay != 10*time.Millisecond || cfg.ConsumerJitter != 3*time.Millisecond {
		t.Fatalf("consumer delay/jitter = %v/%v", cfg.ConsumerDelay, cfg.ConsumerJitter)
	}
	if cfg.PhaseDuration != time.Second || cfg.PhaseDelay != 2*time.Millisecond {
		t.Fatalf("phase = %v/%v", cfg.PhaseDuration, cfg.PhaseDelay)
	}
	if cfg.MaxLag != 100 || cfg.ResumeLag != 20 || cfg.BPPollInterval != 25*time.Millisecond {
		t.Fatalf("backpressure = %d/%d/%v", cfg.MaxLag, cfg.ResumeLag, cfg.BPPollInterval)
	}
	if cfg.BurstRate != 5000 || cfg.BurstDuration != 2*time.Second || cfg.BurstPeriod != 10*time.Second {
		t.Fatalf("burst = %d/%v/%v", cfg.BurstRate, cfg.BurstDuration, cfg.BurstPeriod)
	}
	if cfg.PayloadMode != "json" || cfg.PayloadSeed != 99 {
		t.Fatalf("payload = %q/%d", cfg.PayloadMode, cfg.PayloadSeed)
	}
	if cfg.ReportInterval != 750*time.Millisecond || cfg.LagPollInterval != 800*time.Millisecond || cfg.SysPerfInterval != 900*time.Millisecond {
		t.Fatalf("intervals = %v/%v/%v", cfg.ReportInterval, cfg.LagPollInterval, cfg.SysPerfInterval)
	}
	if cfg.OutputFile != "out.json" || !cfg.JSONOutput || !cfg.ProducerOnly || cfg.ConsumerOnly {
		t.Fatalf("output/switches = %q/%v/%v/%v", cfg.OutputFile, cfg.JSONOutput, cfg.ProducerOnly, cfg.ConsumerOnly)
	}
}

func TestParseArgsClampsMessageSize(t *testing.T) {
	t.Parallel()

	cfg, err := ParseArgs([]string{"-msg-size", "3"})
	if err != nil {
		t.Fatalf("ParseArgs error = %v", err)
	}
	if cfg.MessageSize != 16 {
		t.Fatalf("MessageSize = %d, want 16", cfg.MessageSize)
	}
}

func TestParseArgsInvalidDuration(t *testing.T) {
	t.Parallel()

	if _, err := ParseArgs([]string{"-duration", "nope"}); err == nil {
		t.Fatal("ParseArgs invalid duration error = nil")
	}
}
