# Kafka Benchmark Runner

Reproducible Kafka benchmark written in Go. It measures producer ack latency,
end-to-end latency, consumer lag, recovery behavior, compression tradeoffs,
runtime CPU/GC overhead, burst handling, and adaptive backpressure.

## Quick Start

```bash
./run.sh build
./run.sh start
./run.sh bench
```

Result JSON files are written as `results-*.json`. The submitted dataset used
by the dashboard lives in `../paper/data/`.

## Experiments

### Baseline

Numbers: 9,924 msg/s sent, 9,925 msg/s received, 100.0% delivery, 30.8 ms e2e p99, 16.0 ms ack p99.

Explanation:

### Compression Sweep

| Codec | Send rate | E2E p99 | Ack p99 | GC pause |
| --- | ---: | ---: | ---: | ---: |
| none | 9,728 msg/s | 17.9 ms | 11.1 ms | 4.1 ms |
| snappy | 9,724 msg/s | 74.4 ms | 29.5 ms | 5.8 ms |
| lz4 | 9,769 msg/s | 27.8 ms | 10.7 ms | 4.3 ms |
| gzip | 9,844 msg/s | 22.4 ms | 12.2 ms | 3.3 ms |
| zstd | 9,886 msg/s | 41.0 ms | 22.2 ms | 61.2 ms |

Explanation:

### Acks Sweep

| Acks | Send rate | E2E p99 | Ack p99 | Delivery |
| --- | ---: | ---: | ---: | ---: |
| 0 | 9,404 msg/s | 13.9 ms | 7.0 ms | 100.0% |
| 1 | 9,680 msg/s | 48.1 ms | 20.6 ms | 100.0% |
| -1 | 9,763 msg/s | 11.7 ms | 8.1 ms | 100.0% |

Explanation:

### Payload Sweep

| Payload | Send rate | E2E p99 | zstd ratio | snappy ratio |
| --- | ---: | ---: | ---: | ---: |
| random | 7,945 msg/s | 20.6 ms | 1.0x | 1.0x |
| mixed | 7,962 msg/s | 17.3 ms | 4.5x | 3.8x |
| json | 7,970 msg/s | 49.4 ms | 22.9x | 8.9x |
| logline | 7,949 msg/s | 32.0 ms | 30.3x | 11.8x |
| text | 7,979 msg/s | 48.3 ms | 2775.1x | 19.9x |
| zeros | 7,924 msg/s | 45.2 ms | 8904.3x | 20.0x |

Explanation:

### Message Size Sweep

| Size | Send rate | E2E p99 | Ack p99 |
| --- | ---: | ---: | ---: |
| 64 B | 7,918 msg/s | 41.3 ms | 9.8 ms |
| 256 B | 7,922 msg/s | 11.3 ms | 6.7 ms |
| 1024 B | 7,781 msg/s | 27.4 ms | 14.0 ms |
| 4096 B | 7,861 msg/s | 28.7 ms | 15.8 ms |
| 16384 B | 7,977 msg/s | 223.1 ms | 124.4 ms |

Explanation:

### Slow Consumer

| Scenario | Send rate | Receive rate | Delivery | Peak lag | E2E p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| slow-consumer | 7,883 msg/s | 7,884 msg/s | 100.0% | 7,933 | 208.0 ms |
| slow-aggressive | 9,701 msg/s | 2,579 msg/s | 26.6% | 309,435 | 31.4 s |

Explanation:

### Recovery

| Scenario | Send rate | Peak lag | Drain rate | Time to drain | E2E p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| recovery | 7,791 msg/s | 7,911 | 12.9 msg/s | 30.0 s | 42.7 ms |
| recovery-aggressive | 9,529 msg/s | 195,598 | 3,726 msg/s | 50.0 s | 19.7 s |

Explanation:

### Bursty Workload

| Scenario | Send rate | Peak lag | E2E p99 | Delivery |
| --- | ---: | ---: | ---: | ---: |
| light | 7,454 msg/s | 14,575 | 15.3 ms | 100.0% |
| heavy | 11,200 msg/s | 29,356 | 19.1 ms | 100.0% |
| overload | 11,132 msg/s | 110,445 | 7.0 s | 107.2% |

Explanation:

### Adaptive Backpressure

| Scenario | Send rate | Peak lag | E2E p99 | Delivery | Throttle |
| --- | ---: | ---: | ---: | ---: | ---: |
| off | 9,701 msg/s | 309,435 | 31.4 s | 26.6% | 0 events |
| loose | 2,269 msg/s | 25,956 | 9.2 s | 114.0% | 4 events / 30.8 s |
| tight | 1,938 msg/s | 12,177 | 3.8 s | 103.6% | 8 events / 32.0 s |

Explanation:

## Custom Run

```bash
./bin/bench \
  -duration 60s \
  -rate 20000 \
  -msg-size 1024 \
  -payload json \
  -compression zstd \
  -output my-run.json
```

Run `./bin/bench -help` for all flags.
