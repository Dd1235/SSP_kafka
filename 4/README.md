# kafka-bench-v4

A modular Kafka benchmark in Go that fixes the consumer-side issues from the
interim version and adds the metrics the project actually needs: end-to-end
latency, consumer lag, backpressure behaviour, recovery after slowdown,
offline compression ratios across realistic payloads, and runtime
CPU/GC/memory sampling.

Every moving part lives in a small package under `internal/`. To add a knob,
add a payload class, or swap in a new metric, you touch exactly one file and
the rest of the code keeps working.

---

## Quick start

```bash
# 1. build
./run.sh build

# 2. start Kafka (KRaft, single-node, persistent)
./run.sh start

# 3. 5-second smoke test
./run.sh quick

# 4. 30-second baseline bench
./run.sh bench

# 5. slow-consumer scenario (builds lag)
./run.sh slow-consumer

# 6. recovery scenario (build lag then drain)
./run.sh recovery
```

Result JSONs are written to `results-*.json` alongside the binary.

The public GitHub Pages dashboard is a static viewer over the submitted result
JSONs in `paper/data/`. To reproduce or extend the experiments locally, use the
commands in this directory; to publish updated results, copy the new JSON files
into `paper/data/` and rebuild the frontend from `site/`.

---

## Directory layout

```
4/
├── cmd/bench/main.go                # wiring only; no logic
├── internal/
│   ├── config/        # CLI flag definitions + BenchConfig struct
│   ├── payload/       # random / zeros / text / json / logline / mixed
│   ├── producer/      # sarama async producer pool
│   ├── consumer/      # sarama consumer group pool + phased slowdown
│   ├── lag/           # broker offsets vs committed offsets, recovery metrics
│   ├── metrics/       # collector, percentiles, offline compression
│   ├── sysperf/       # CPU / goroutines / heap / GC sampling
│   ├── topic/         # ensure / reset
│   └── output/        # JSON writer + pretty printer
├── scenarios/                       # room for custom scenario scripts
├── docker-compose.yml
├── run.sh                           # preset scenarios
├── go.mod / go.sum
├── README.md                        # (this file)
├── CHANGES.md                       # change log vs the interim version
└── NEXT_PHASE.md                    # real-world application ideas
```

---

## How to run custom benchmarks

Call the binary directly:

```bash
./bin/bench \
  -duration 60s \
  -warmup 5s \
  -rate 20000 \
  -msg-size 1024 \
  -payload json \
  -producers 8 \
  -consumers 4 \
  -partitions 12 \
  -acks 1 \
  -compression zstd \
  -consumer-delay 200us \
  -consumer-jitter 100us \
  -lag-interval 1s \
  -output my-run.json
```

Every flag is listed by `./bin/bench -help`.

---

## Recipes — what to change where

| Want to…                                    | Touch                                                      |
| ------------------------------------------- | ---------------------------------------------------------- |
| Add a new CLI flag                          | `internal/config/config.go` (1 field, 1 `flag.*Var`)       |
| Add a new payload class                     | `internal/payload/payload.go` (Mode + generator fn)        |
| Add a new latency percentile                | `internal/metrics/percentiles.go`                          |
| Add a new runtime metric (e.g. OS-level)    | `internal/sysperf/sysperf.go` (`Sample` field + `takeSample`) |
| Change consumer-side slowdown behaviour     | `internal/consumer/consumer.go` (`phaser` / `ConsumeClaim`) |
| Add a new preset scenario                   | `run.sh` (new `case`) — no Go changes needed               |
| Add compression codec for offline analysis  | `internal/metrics/compression.go` (`codecs` map)           |
| Export more fields to JSON                  | `internal/output/output.go` (struct field + Build())       |

---

## Metrics captured

### Latency
Separate histograms for two meanings of latency:
- **ack latency** — producer enqueue → broker ack (what the interim version measured)
- **e2e latency** — producer enqueue → consumer receive (what the project goal requires)

Each histogram reports: `min, p50, p75, p90, p95, p99, p99.5, p99.9, p99.99, max,
mean, stddev, variance, MAD (median absolute deviation)`, plus log-bucketed
CDF counts.

### Throughput
- Messages sent / received per second (instant & average)
- Bytes/sec (payload, pre-compression)
- Delivery ratio = received / sent
- Error count (producer side)

### Consumer lag
- Per-partition and total lag at every `-lag-interval` tick
- Peak lag, final lag
- Drain rate (msg/s) if the run includes a recovery phase
- Time-to-drain from peak

### Compression (offline)
Before the bench starts, 2000 sample payloads are compressed through
`none / gzip / snappy / lz4 / zstd`. Report shows:
- compressed size
- compression ratio (raw / compressed)
- space-saved %
- encode time (µs)

This works even with random payloads — random mode will show ratio ≈ 1.00
for every codec, which is the correct answer. Use `-payload json` or
`-payload text` to see realistic ratios.

### SysPerf
Sampled every `-sysperf-interval`:
- CPU fraction (% of all cores, process-wide, via `runtime/metrics`)
- Goroutine count
- Heap alloc, heap in-use, stack in-use, total Sys memory (MB)
- Number of GC cycles, total GC pause time, last pause (ms)

---

## Scenarios in run.sh

| Scenario           | What it does                                                   |
| ------------------ | -------------------------------------------------------------- |
| `quick`            | 5s sanity check                                                |
| `bench`            | 30s, 10k msg/s, realistic payload mix                          |
| `slow-consumer`    | Consumer has a 500µs/msg delay — lag grows throughout          |
| `recovery`         | 30s with 800µs delay, then drop to 0 — measures drain         |
| `sweep-compression`| Loops over `none/snappy/lz4/zstd/gzip`                         |
| `sweep-acks`       | Loops over acks `0/1/-1`                                       |
| `sweep-payload`    | Loops over payload classes — compression report varies         |
| `sweep-msgsize`    | 64/256/1024/4096/16384-byte messages                           |

---

## Known limitations

- Single-node broker; replication, ISR, failover are out of scope.
- `bytes_sent` is pre-compression payload size, not actual wire/disk bytes.
  The offline compression report fills this gap for reasoning about codec choice.
- E2E latency uses `time.Now()` on the same machine for producer and consumer
  — no clock skew, but also means the numbers aren't cross-host-transferable.
- SysPerf samples the **bench process**, not Kafka. If you want broker metrics,
  pipe `docker stats kafka-bench-v4` alongside. Consider adding a Prometheus
  exporter in a future phase.

See [CHANGES.md](CHANGES.md) for the diff vs. the interim version and
[NEXT_PHASE.md](NEXT_PHASE.md) for ideas on pointing this bench at a real-world task.
