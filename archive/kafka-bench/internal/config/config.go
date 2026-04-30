package config

import (
	"flag"
	"fmt"
	"time"
)

type BenchConfig struct {
	Brokers         []string
	Topic           string
	MessageSize     int
	TargetRate      int
	Duration        time.Duration
	WarmupDuration  time.Duration
	ProducerWorkers int
	ConsumerWorkers int
	Partitions      int
	BatchSize       int
	LingerMs        int
	Acks            int
	Compression     string
	ProducerOnly    bool
	ConsumerOnly    bool
	ReportInterval  time.Duration
	JSONOutput      bool
	OutputFile      string
}

func Parse() *BenchConfig {
	c := &BenchConfig{}
	broker := flag.String("broker", "localhost:9092", "Kafka broker address")
	flag.StringVar(&c.Topic, "topic", "bench-topic", "Kafka topic")
	flag.IntVar(&c.MessageSize, "msg-size", 512, "Message size in bytes")
	flag.IntVar(&c.TargetRate, "rate", 10000, "Target messages/sec")
	duration := flag.Duration("duration", 30*time.Second, "Benchmark duration")
	warmup := flag.Duration("warmup", 3*time.Second, "Warmup duration")
	flag.IntVar(&c.ProducerWorkers, "producers", 8, "Producer goroutines")
	flag.IntVar(&c.ConsumerWorkers, "consumers", 4, "Consumer goroutines")
	flag.IntVar(&c.Partitions, "partitions", 12, "Topic partitions")
	flag.IntVar(&c.BatchSize, "batch-size", 65536, "Sarama batch bytes")
	flag.IntVar(&c.LingerMs, "linger-ms", 5, "Producer linger ms")
	flag.IntVar(&c.Acks, "acks", 1, "Acks: 0,1,-1")
	flag.StringVar(&c.Compression, "compression", "snappy", "none,gzip,snappy,lz4,zstd")
	flag.BoolVar(&c.ProducerOnly, "producer-only", false, "Producer only")
	flag.BoolVar(&c.ConsumerOnly, "consumer-only", false, "Consumer only")
	flag.DurationVar(&c.ReportInterval, "report-interval", 5*time.Second, "Stats interval")
	flag.BoolVar(&c.JSONOutput, "json", false, "JSON output")
	flag.StringVar(&c.OutputFile, "output", "", "Output file")
	flag.Parse()
	c.Brokers = []string{*broker}
	c.Duration = *duration
	c.WarmupDuration = *warmup
	return c
}

func (c *BenchConfig) Print() {
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("  Kafka Goroutine Benchmark")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("  Brokers:          %v\n", c.Brokers)
	fmt.Printf("  Topic:            %s (%d partitions)\n", c.Topic, c.Partitions)
	fmt.Printf("  Message Size:     %d bytes\n", c.MessageSize)
	fmt.Printf("  Target Rate:      %d msg/s\n", c.TargetRate)
	fmt.Printf("  Duration:         %s (warmup: %s)\n", c.Duration, c.WarmupDuration)
	fmt.Printf("  Producers:        %d goroutines\n", c.ProducerWorkers)
	fmt.Printf("  Consumers:        %d goroutines\n", c.ConsumerWorkers)
	fmt.Printf("  Acks:             %d | Compression: %s\n", c.Acks, c.Compression)
	fmt.Printf("  Batch:            %d bytes | Linger: %dms\n", c.BatchSize, c.LingerMs)
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
}
