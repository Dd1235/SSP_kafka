// Package config centralizes all runtime knobs.
//
// To add a new flag:
//  1. Add a field to BenchConfig.
//  2. Register the flag in Parse().
//  3. If it should appear in the banner, add a line to Print().
//  4. If it should land in result JSON, add it in internal/output.
//
// Nothing in this package imports Kafka/Sarama on purpose — config is a pure
// data type that every other module can depend on without pulling in I/O.
package config

import (
	"flag"
	"fmt"
	"strings"
	"time"
)

type BenchConfig struct {
	// --- Connection / topic ---
	Brokers   []string
	Topic     string
	GroupID   string
	ResetTopic bool // delete & recreate topic before run (clean slate)

	// --- Workload shape ---
	MessageSize   int
	TargetRate    int // msgs/sec total (split across producer workers)
	Duration      time.Duration
	WarmupDuration time.Duration
	Partitions    int

	// --- Producer tuning ---
	ProducerWorkers int
	BatchBytes      int    // Sarama Producer.Flush.Bytes (true batch threshold)
	LingerMs        int    // Sarama Producer.Flush.Frequency
	Acks            int    // 0,1,-1
	Compression     string // none,gzip,snappy,lz4,zstd
	MaxMessageBytes int    // per-message hard cap (Sarama MaxMessageBytes)

	// --- Consumer tuning ---
	ConsumerWorkers int
	InitialOffset   string        // oldest | newest
	ConsumerDelay   time.Duration // per-message sleep (slow-consumer sim)
	ConsumerJitter  time.Duration // uniform jitter added to ConsumerDelay
	PhaseDuration   time.Duration // if > 0, at T=PhaseDuration switch to PhaseDelay
	PhaseDelay      time.Duration // new consumer delay after phase switch (recovery test)

	// --- Payload ---
	PayloadMode string // random | zeros | text | json | logline | mixed
	PayloadSeed int64  // deterministic generation

	// --- Observability ---
	ReportInterval time.Duration
	LagPollInterval time.Duration
	SysPerfInterval time.Duration
	OutputFile     string
	JSONOutput     bool

	// --- Scope switches ---
	ProducerOnly bool
	ConsumerOnly bool
}

func Parse() *BenchConfig {
	c := &BenchConfig{}

	broker := flag.String("broker", "localhost:9092", "Kafka broker (comma-separated for list)")
	flag.StringVar(&c.Topic, "topic", "bench-topic-v4", "Kafka topic")
	flag.StringVar(&c.GroupID, "group", "bench-group-v4", "Consumer group id")
	flag.BoolVar(&c.ResetTopic, "reset-topic", true, "Delete+recreate topic before run for a clean slate")

	flag.IntVar(&c.MessageSize, "msg-size", 512, "Message size in bytes (min 16 for header)")
	flag.IntVar(&c.TargetRate, "rate", 10000, "Target total messages/sec")
	duration := flag.Duration("duration", 30*time.Second, "Benchmark duration")
	warmup := flag.Duration("warmup", 3*time.Second, "Warmup duration (excluded from stats)")
	flag.IntVar(&c.Partitions, "partitions", 12, "Topic partition count")

	flag.IntVar(&c.ProducerWorkers, "producers", 8, "Producer goroutines")
	flag.IntVar(&c.BatchBytes, "batch-bytes", 65536, "Producer flush byte threshold")
	flag.IntVar(&c.LingerMs, "linger-ms", 5, "Producer linger (flush frequency) ms")
	flag.IntVar(&c.Acks, "acks", 1, "Acks: 0 (none), 1 (leader), -1 (all)")
	flag.StringVar(&c.Compression, "compression", "snappy", "none|gzip|snappy|lz4|zstd")
	flag.IntVar(&c.MaxMessageBytes, "max-msg-bytes", 1<<20, "Per-message hard cap (bytes)")

	flag.IntVar(&c.ConsumerWorkers, "consumers", 4, "Consumer goroutines in group")
	flag.StringVar(&c.InitialOffset, "initial-offset", "oldest",
		"oldest | newest — oldest ensures no messages missed while group rebalances")
	flag.DurationVar(&c.ConsumerDelay, "consumer-delay", 0,
		"Per-message processing delay (simulate slow consumer)")
	flag.DurationVar(&c.ConsumerJitter, "consumer-jitter", 0,
		"Uniform 0..jitter added to consumer-delay per message")
	flag.DurationVar(&c.PhaseDuration, "phase-duration", 0,
		"If > 0, after this wall time, switch consumer-delay to phase-delay (recovery test)")
	flag.DurationVar(&c.PhaseDelay, "phase-delay", 0,
		"Consumer delay after phase switch (e.g. set to 0 to drain backlog)")

	flag.StringVar(&c.PayloadMode, "payload", "mixed",
		"random|zeros|text|json|logline|mixed — affects compressibility, not size")
	flag.Int64Var(&c.PayloadSeed, "payload-seed", 42, "Seed for payload RNG (reproducible)")

	flag.DurationVar(&c.ReportInterval, "report-interval", 5*time.Second, "Live stats interval")
	flag.DurationVar(&c.LagPollInterval, "lag-interval", 2*time.Second, "How often to query broker/consumer lag")
	flag.DurationVar(&c.SysPerfInterval, "sysperf-interval", 2*time.Second, "How often to sample runtime CPU/mem/GC")
	flag.StringVar(&c.OutputFile, "output", "", "Write result JSON to this file")
	flag.BoolVar(&c.JSONOutput, "json", false, "Also print JSON to stdout")

	flag.BoolVar(&c.ProducerOnly, "producer-only", false, "Skip consumer")
	flag.BoolVar(&c.ConsumerOnly, "consumer-only", false, "Skip producer")

	flag.Parse()

	c.Brokers = strings.Split(*broker, ",")
	c.Duration = *duration
	c.WarmupDuration = *warmup

	if c.MessageSize < 16 {
		c.MessageSize = 16 // header-only
	}
	return c
}

func (c *BenchConfig) Print() {
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("  Kafka Bench v4 — modular benchmark")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("  Brokers:        %v\n", c.Brokers)
	fmt.Printf("  Topic:          %s  (%d partitions, reset=%v)\n", c.Topic, c.Partitions, c.ResetTopic)
	fmt.Printf("  Group:          %s  (initial-offset=%s)\n", c.GroupID, c.InitialOffset)
	fmt.Printf("  Message size:   %d B   payload=%s (seed=%d)\n", c.MessageSize, c.PayloadMode, c.PayloadSeed)
	fmt.Printf("  Target rate:    %d msg/s across %d producers\n", c.TargetRate, c.ProducerWorkers)
	fmt.Printf("  Duration:       %s  (warmup: %s)\n", c.Duration, c.WarmupDuration)
	fmt.Printf("  Acks:           %d  compression=%s  batch-bytes=%d  linger=%dms\n",
		c.Acks, c.Compression, c.BatchBytes, c.LingerMs)
	fmt.Printf("  Consumers:      %d  delay=%v jitter=%v\n",
		c.ConsumerWorkers, c.ConsumerDelay, c.ConsumerJitter)
	if c.PhaseDuration > 0 {
		fmt.Printf("  Phase switch:   at %v -> delay=%v (recovery test)\n", c.PhaseDuration, c.PhaseDelay)
	}
	fmt.Printf("  Report/Lag/Sys: %v / %v / %v\n",
		c.ReportInterval, c.LagPollInterval, c.SysPerfInterval)
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
}
