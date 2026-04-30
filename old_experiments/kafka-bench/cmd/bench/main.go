package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"kafka-bench/internal/config"
	"kafka-bench/internal/consumer"
	"kafka-bench/internal/metrics"
	"kafka-bench/internal/producer"

	"github.com/IBM/sarama"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU()) // allow go to run goroutines on all cpu cores concurrently
	sarama.Logger = log.New(os.Stderr, "[sarama] ", log.LstdFlags)

	cfg := config.Parse()
	cfg.Print()

	if err := ensureTopic(cfg); err != nil {
		log.Fatalf("topic setup: %v", err)
	}

	col := metrics.New()

	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		fmt.Println("\n  interrupt — shutting down gracefully...")
		rootCancel()
	}()

	// Warmup
	if cfg.WarmupDuration > 0 {
		fmt.Printf("\n[warmup] %s — results discarded\n", cfg.WarmupDuration)
		col.Start()
		warmupCtx, warmupCancel := context.WithTimeout(rootCtx, cfg.WarmupDuration)
		runBench(warmupCtx, cfg, col)
		warmupCancel()
		col.SetWarmupEnd()
		fmt.Println("[warmup] done — counters reset\n")
	} else {
		col.Start()
	}

	// Benchmark
	fmt.Printf("[bench] %s at %d msg/s\n\n", cfg.Duration, cfg.TargetRate)
	benchCtx, benchCancel := context.WithTimeout(rootCtx, cfg.Duration)
	defer benchCancel()

	// Interval reporter (sole caller of IntervalReport — no races)
	reportDone := make(chan struct{})
	go func() {
		defer close(reportDone)
		ticker := time.NewTicker(cfg.ReportInterval)
		defer ticker.Stop()
		printHeader()
		for {
			select {
			case <-benchCtx.Done():
				return
			case <-ticker.C:
				col.IntervalReport().PrintInterval()
			}
		}
	}()

	runBench(benchCtx, cfg, col)
	<-reportDone

	final := col.FinalSnapshot()
	final.PrintFinal()
	col.PrintTimeline()

	if cfg.JSONOutput || cfg.OutputFile != "" {
		writeJSON(cfg, col.GetTimeline(), final)
	}
}

func runBench(ctx context.Context, cfg *config.BenchConfig, col *metrics.Collector) {
	var wg sync.WaitGroup
	if !cfg.ConsumerOnly {
		pool, err := producer.NewPool(cfg, col)
		if err != nil {
			log.Fatalf("producer pool: %v", err)
		}
		wg.Add(1)
		go func() { defer wg.Done(); pool.Run(ctx) }()
	}
	if !cfg.ProducerOnly {
		pool, err := consumer.NewPool(cfg, col)
		if err != nil {
			log.Fatalf("consumer pool: %v", err)
		}
		wg.Add(1)
		go func() { defer wg.Done(); pool.Run(ctx) }()
	}
	wg.Wait()
}

func ensureTopic(cfg *config.BenchConfig) error {
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
	if meta, exists := topics[cfg.Topic]; exists {
		fmt.Printf("[setup] topic '%s' exists (%d partitions)\n",
			cfg.Topic, meta.NumPartitions)
		return nil
	}
	err = admin.CreateTopic(cfg.Topic, &sarama.TopicDetail{
		NumPartitions:     int32(cfg.Partitions),
		ReplicationFactor: 1,
	}, false)
	if err != nil {
		return fmt.Errorf("create topic: %w", err)
	}
	fmt.Printf("[setup] created topic '%s' (%d partitions)\n", cfg.Topic, cfg.Partitions)
	return nil
}

func printHeader() {
	fmt.Printf("%-10s %-12s %-10s %-10s %-8s %-6s  latency\n",
		"elapsed", "sent", "inst/s", "avg/s", "MB/s", "err%")
	fmt.Println("────────────────────────────────────────────────────────────────────────────────")
}

func writeJSON(cfg *config.BenchConfig, timeline []metrics.IntervalSnapshot, s metrics.Stats) {
	out := map[string]any{
		"config": map[string]any{
			"msg_size_bytes":          cfg.MessageSize,
			"target_rate_msg_per_sec": cfg.TargetRate,
			"duration_s":              cfg.Duration.Seconds(),
			"producer_goroutines":     cfg.ProducerWorkers,
			"consumer_goroutines":     cfg.ConsumerWorkers,
			"partitions":              cfg.Partitions,
			"acks":                    cfg.Acks,
			"compression":             cfg.Compression,
			"linger_ms":               cfg.LingerMs,
			"batch_size_bytes":        cfg.BatchSize,
		},
		"results": map[string]any{
			"messages_sent":         s.Sent,
			"messages_received":     s.Received,
			"errors":                s.Errors,
			"avg_rate_msg_per_sec":  s.AvgRate,
			"throughput_mb_per_sec": s.ThroughputMB,
			"total_data_mb":         float64(s.BytesSent) / 1024 / 1024,
			"latency_ms": map[string]float64{
				"min":    s.Latency.Min,
				"mean":   s.Latency.Mean,
				"p50":    s.Latency.P50,
				"p90":    s.Latency.P90,
				"p95":    s.Latency.P95,
				"p99":    s.Latency.P99,
				"p999":   s.Latency.P999,
				"max":    s.Latency.Max,
				"stddev": s.Latency.StdDev,
			},
			"latency_sample_count": s.Latency.Count,
		},
		"timeline": timeline,
	}

	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		log.Printf("json marshal: %v", err)
		return
	}
	if cfg.JSONOutput {
		fmt.Println(string(b))
	}
	if cfg.OutputFile != "" {
		if err := os.WriteFile(cfg.OutputFile, b, 0644); err != nil {
			log.Printf("write %s: %v", cfg.OutputFile, err)
		} else {
			fmt.Printf("[output] %s written\n", cfg.OutputFile)
		}
	}
}
