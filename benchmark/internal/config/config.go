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
	"io"
	"os"
	"strings"
	"time"
)

type BenchConfig struct {
	// --- Connection / topic ---
	Brokers    []string
	Topic      string
	GroupID    string
	ResetTopic bool // delete & recreate topic before run (clean slate)

	// --- Workload shape ---
	MessageSize    int
	TargetRate     int // msgs/sec total (split across producer workers)
	Duration       time.Duration
	WarmupDuration time.Duration
	Partitions     int

	// --- Adaptive backpressure (optional, off by default) ---
	// When MaxLag > 0, the producer pool watches total consumer lag (via
	// the existing lag.Poller) and pauses sending when lag exceeds MaxLag,
	// resuming only once lag falls below ResumeLag. Hysteresis avoids
	// oscillation around the threshold. Default zero values fully disable
	// the feature: the existing constant-rate code path is unchanged.
	MaxLag         int           // pause-above threshold (0 = disabled)
	ResumeLag      int           // resume-below threshold (0 → MaxLag/2)
	BPPollInterval time.Duration // controller tick (default 200 ms)

	// --- Bursty workload (optional, off by default) ---
	// When BurstRate > 0 and BurstDuration > 0 and BurstPeriod > 0, the
	// producer alternates between TargetRate (off-burst) and BurstRate
	// (during burst) on a periodic cycle: BurstDuration "on" then
	// (BurstPeriod - BurstDuration) "off". Default zero values preserve
	// the original constant-rate behaviour exactly.
	BurstRate     int
	BurstDuration time.Duration
	BurstPeriod   time.Duration

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
	ReportInterval  time.Duration
	LagPollInterval time.Duration
	SysPerfInterval time.Duration
	OutputFile      string
	JSONOutput      bool

	// --- Scope switches ---
	ProducerOnly bool
	ConsumerOnly bool
}

func Parse() *BenchConfig {
	c, err := parseArgs(os.Args[1:], os.Stderr)
	if err != nil {
		if err == flag.ErrHelp {
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	return c
}

func ParseArgs(args []string) (*BenchConfig, error) {
	return parseArgs(args, io.Discard)
}

func parseArgs(args []string, output io.Writer) (*BenchConfig, error) {
	c := &BenchConfig{}
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	fs.SetOutput(output)

	broker := fs.String("broker", "localhost:9092", "Kafka broker (comma-separated for list)")
	fs.StringVar(&c.Topic, "topic", "bench-topic-v4", "Kafka topic")
	fs.StringVar(&c.GroupID, "group", "bench-group-v4", "Consumer group id")
	fs.BoolVar(&c.ResetTopic, "reset-topic", true, "Delete+recreate topic before run for a clean slate")

	fs.IntVar(&c.MessageSize, "msg-size", 512, "Message size in bytes (min 16 for header)")
	fs.IntVar(&c.TargetRate, "rate", 10000, "Target total messages/sec")
	duration := fs.Duration("duration", 30*time.Second, "Benchmark duration")
	warmup := fs.Duration("warmup", 3*time.Second, "Warmup duration (excluded from stats)")
	fs.IntVar(&c.Partitions, "partitions", 12, "Topic partition count")

	fs.IntVar(&c.ProducerWorkers, "producers", 8, "Producer goroutines")
	fs.IntVar(&c.BatchBytes, "batch-bytes", 65536, "Producer flush byte threshold")
	fs.IntVar(&c.LingerMs, "linger-ms", 5, "Producer linger (flush frequency) ms")
	fs.IntVar(&c.Acks, "acks", 1, "Acks: 0 (none), 1 (leader), -1 (all)")
	fs.StringVar(&c.Compression, "compression", "snappy", "none|gzip|snappy|lz4|zstd")
	fs.IntVar(&c.MaxMessageBytes, "max-msg-bytes", 1<<20, "Per-message hard cap (bytes)")

	fs.IntVar(&c.ConsumerWorkers, "consumers", 4, "Consumer goroutines in group")
	fs.StringVar(&c.InitialOffset, "initial-offset", "oldest",
		"oldest | newest — oldest ensures no messages missed while group rebalances")
	fs.DurationVar(&c.ConsumerDelay, "consumer-delay", 0,
		"Per-message processing delay (simulate slow consumer)")
	fs.DurationVar(&c.ConsumerJitter, "consumer-jitter", 0,
		"Uniform 0..jitter added to consumer-delay per message")
	fs.DurationVar(&c.PhaseDuration, "phase-duration", 0,
		"If > 0, after this wall time, switch consumer-delay to phase-delay (recovery test)")
	fs.DurationVar(&c.PhaseDelay, "phase-delay", 0,
		"Consumer delay after phase switch (e.g. set to 0 to drain backlog)")

	fs.IntVar(&c.MaxLag, "max-lag", 0,
		"Adaptive backpressure: pause producer when total consumer lag exceeds this (0 = disabled).")
	fs.IntVar(&c.ResumeLag, "resume-lag", 0,
		"Adaptive backpressure: resume sending once lag falls below this (0 -> max-lag/2).")
	fs.DurationVar(&c.BPPollInterval, "bp-poll", 200*time.Millisecond,
		"Adaptive backpressure: controller tick interval.")

	fs.IntVar(&c.BurstRate, "burst-rate", 0,
		"Peak msgs/sec during burst (>0 enables bursty mode; default 0 = constant rate)")
	fs.DurationVar(&c.BurstDuration, "burst-duration", 0,
		"On-duration of each burst (e.g. 3s). Requires -burst-rate and -burst-period.")
	fs.DurationVar(&c.BurstPeriod, "burst-period", 0,
		"Period between burst start times (e.g. 12s). Off-burst portion = burst-period - burst-duration.")

	fs.StringVar(&c.PayloadMode, "payload", "mixed",
		"random|zeros|text|json|logline|mixed — affects compressibility, not size")
	fs.Int64Var(&c.PayloadSeed, "payload-seed", 42, "Seed for payload RNG (reproducible)")

	fs.DurationVar(&c.ReportInterval, "report-interval", 5*time.Second, "Live stats interval")
	fs.DurationVar(&c.LagPollInterval, "lag-interval", 2*time.Second, "How often to query broker/consumer lag")
	fs.DurationVar(&c.SysPerfInterval, "sysperf-interval", 2*time.Second, "How often to sample runtime CPU/mem/GC")
	fs.StringVar(&c.OutputFile, "output", "", "Write result JSON to this file")
	fs.BoolVar(&c.JSONOutput, "json", false, "Also print JSON to stdout")

	fs.BoolVar(&c.ProducerOnly, "producer-only", false, "Skip consumer")
	fs.BoolVar(&c.ConsumerOnly, "consumer-only", false, "Skip producer")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	c.Brokers = strings.Split(*broker, ",")
	c.Duration = *duration
	c.WarmupDuration = *warmup

	if c.MessageSize < 16 {
		c.MessageSize = 16 // header-only
	}
	return c, nil
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
	if c.MaxLag > 0 {
		resume := c.ResumeLag
		if resume <= 0 {
			resume = c.MaxLag / 2
		}
		fmt.Printf("  Backpressure:   pause @ %d msgs, resume @ %d msgs, poll %v\n",
			c.MaxLag, resume, c.BPPollInterval)
	}
	if c.BurstRate > 0 && c.BurstDuration > 0 && c.BurstPeriod > 0 {
		fmt.Printf("  Burst:          %d msg/s for %v every %v (duty %.0f%%)\n",
			c.BurstRate, c.BurstDuration, c.BurstPeriod,
			100*c.BurstDuration.Seconds()/c.BurstPeriod.Seconds())
	}
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
