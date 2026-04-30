# kafka-bench

High-throughput Kafka benchmark in Go, optimized for Apple M2 (8GB RAM).

## Features

- **Goroutine-per-producer/consumer** — fully concurrent, zero blocking hot path
- **Token-bucket rate limiter** — per-worker ticker, no shared mutex
- **Lock-free counters** — `atomic.Int64` with cache-line padding
- **Sharded latency histogram** — percentiles up to p99.9
- **IBM/sarama** async producer with configurable batching
- **Consumer group** with round-robin rebalancing
- **Auto topic creation** via Kafka admin API
- **KRaft Kafka** (no Zookeeper) via Docker with persistence enabled

## Quick Start

```bash
# 1. Build
go mod tidy
./run.sh build

# 2. Start Kafka (KRaft, persistence enabled)
./run.sh start

# 3. Smoke test (5s)
./run.sh quick

# 4. Full benchmark (30s, 10k msg/s)
./run.sh bench

# 5. 10-minute run
./run.sh long
```

## Sweep Tests

```bash
# Compare compression codecs
./run.sh sweep

# Compare ack levels (0=none / 1=leader / -1=all)
./run.sh sweep-acks
```

## Manual Usage

```bash
./bin/bench [flags]

Flags:
  -broker       string  Kafka broker (default "localhost:9092")
  -topic        string  Topic name   (default "bench-topic")
  -msg-size     int     Bytes/msg    (default 512)
  -rate         int     Target msg/s (default 10000)
  -duration     dur     Run duration (default 30s)
  -warmup       dur     Warmup       (default 3s)
  -producers    int     Producer goroutines (default 8)
  -consumers    int     Consumer goroutines (default 4)
  -partitions   int     Topic partitions    (default 12)
  -acks         int     0/1/-1       (default 1)
  -compression  string  none/snappy/lz4/gzip/zstd (default snappy)
  -batch-size   int     Sarama batch bytes  (default 65536)
  -linger-ms    int     Flush linger        (default 5)
  -report-interval dur  Stats interval      (default 5s)
  -json               Print JSON results
  -output       file    Write JSON to file
  -producer-only      Producer benchmark only
  -consumer-only      Consumer benchmark only
```

## Architecture

```
main
 ├── ensureTopic()          admin API, auto-create if missing
 ├── Warmup phase           discarded from final stats
 └── Bench phase
      ├── producer.Pool
      │    ├── goroutine[0]  sender  ──► sarama.AsyncProducer[0]
      │    ├── goroutine[0]  ackReader ◄── successes/errors chan
      │    ├── goroutine[1]  sender  ──► sarama.AsyncProducer[1]
      │    └── ... (N=producers)
      ├── consumer.Pool
      │    └── goroutine[0..N]  ConsumerGroup.Consume()
      └── metrics.Collector
           ├── atomic sent/recv/error counters
           └── sharded latency histogram → p50/p90/p95/p99/p99.9
```

## M2 Tuning Notes

| Setting         | Value        | Why                  |
| --------------- | ------------ | -------------------- |
| GOMAXPROCS      | NumCPU() (8) | Use all P-cores      |
| producers       | 8            | One per CPU core     |
| partitions      | 12           | >cores, parallel I/O |
| batch-size      | 64KB         | Amortize syscalls    |
| linger-ms       | 5            | Batch fill window    |
| Kafka heap      | 1GB          | Leave RAM for Go     |
| G1GC MaxGCPause | 20ms         | Low-latency GC       |

## Expected Results (M2, persistence, snappy, acks=1)

| Metric      | Expected           |
| ----------- | ------------------ |
| Throughput  | 8,000–12,000 msg/s |
| MB/s        | 4–6 MB/s           |
| p50 latency | 5–20ms             |
| p99 latency | 30–80ms            |
| Error rate  | < 0.01%            |

Disk I/O is the primary bottleneck with persistence enabled.
