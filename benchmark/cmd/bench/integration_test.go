//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kafka-bench-v4/internal/config"
	"kafka-bench-v4/internal/lag"
	"kafka-bench-v4/internal/metrics"
	"kafka-bench-v4/internal/output"
	"kafka-bench-v4/internal/payload"
	"kafka-bench-v4/internal/sysperf"
	"kafka-bench-v4/internal/topic"
)

func TestIntegrationSmallKafkaRun(t *testing.T) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("set KAFKA_BROKERS to run Kafka integration test")
	}

	cfg := &config.BenchConfig{
		Brokers:         strings.Split(brokers, ","),
		Topic:           fmt.Sprintf("bench-it-%d", time.Now().UnixNano()),
		GroupID:         fmt.Sprintf("bench-it-group-%d", time.Now().UnixNano()),
		ResetTopic:      true,
		MessageSize:     64,
		TargetRate:      20,
		Duration:        8 * time.Second,
		WarmupDuration:  0,
		Partitions:      1,
		MaxLag:          0,
		BPPollInterval:  50 * time.Millisecond,
		BurstRate:       0,
		ProducerWorkers: 1,
		BatchBytes:      1024,
		LingerMs:        1,
		Acks:            1,
		Compression:     "none",
		MaxMessageBytes: 1 << 20,
		ConsumerWorkers: 1,
		InitialOffset:   "oldest",
		PayloadMode:     "mixed",
		PayloadSeed:     1,
		ReportInterval:  time.Second,
		LagPollInterval: 500 * time.Millisecond,
		SysPerfInterval: 500 * time.Millisecond,
	}

	if err := topic.Ensure(cfg); err != nil {
		t.Fatalf("topic ensure error = %v", err)
	}
	gen, err := payload.New(cfg.PayloadMode, cfg.MessageSize, cfg.PayloadSeed)
	if err != nil {
		t.Fatalf("payload error = %v", err)
	}
	samples := make([][]byte, 10)
	for i := range samples {
		samples[i] = gen.SampleFor(i)
	}
	comp := metrics.BuildCompressionReport(samples)
	if comp.RawBytes == 0 || comp.SampleCount != len(samples) {
		t.Fatalf("compression report = %+v", comp)
	}

	lagPoll, err := lag.NewPoller(cfg)
	if err != nil {
		t.Fatalf("lag poller error = %v", err)
	}
	defer lagPoll.Close()
	lagCtx, lagCancel := context.WithCancel(context.Background())
	go lagPoll.Run(lagCtx)

	col := metrics.New()
	col.Start()
	benchCtx, cancel := context.WithTimeout(context.Background(), cfg.Duration)
	prodPool := runBench(benchCtx, cfg, col, gen, lagPoll)
	cancel()
	col.MarkEnd()

	lagCancel()
	lagPoll.Wait()

	final := col.FinalSnapshot()
	if final.Sent == 0 || final.Received == 0 {
		t.Fatalf("final sent/received = %d/%d", final.Sent, final.Received)
	}
	lagSamples := lagPoll.Samples()
	if len(lagSamples) == 0 {
		t.Fatal("no lag samples captured")
	}

	var bpEvents int64
	var bpPaused time.Duration
	if prodPool != nil {
		bpEvents = prodPool.ThrottleEvents()
		bpPaused = prodPool.ThrottlePaused()
	}
	rep := output.Build(cfg, final, col.Timeline(), lagSamples, lagPoll.Summary(), nil, sysperf.Summary{}, comp, bpEvents, bpPaused)
	path := filepath.Join(t.TempDir(), "result.json")
	if err := output.Write(rep, false, path); err != nil {
		t.Fatalf("write output error = %v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read output error = %v", err)
	}
	var decoded output.Report
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("json output error = %v", err)
	}
	if decoded.Final.Sent == 0 || decoded.Compress.RawBytes == 0 {
		t.Fatalf("decoded report = %+v", decoded)
	}
}
