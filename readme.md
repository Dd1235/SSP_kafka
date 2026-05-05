# Kafka Bench

> **A modular Go benchmark for Apache Kafka that measures end-to-end latency,
> consumer lag, backpressure, and recovery — and a credit-based adaptive
> backpressure controller that bounds peak consumer lag 96 % under sustained
> overload.**

[![Paper (LaTeX source)](https://img.shields.io/badge/paper-IEEE--6pp-blue)](paper/main.tex)
[![Dashboard](https://img.shields.io/badge/results-live%20dashboard-brightgreen)](https://dd1235.github.io/SSP_kafka/)

---

## Headline result

The benchmark recreates the canonical Kafka failure mode (sustained
slow-consumer overload) and demonstrates that an opt-in,
hysteretic, credit-based producer backpressure controller turns it
into a bounded, healthy system:

| Scenario (40 s, 10 k msg/s, 4 ms consumer delay) | **Without bp** | **With bp (tight)** |                            Δ |
| ------------------------------------------------ | -------------: | ------------------: | ---------------------------: |
| Peak consumer lag                                |   309 435 msgs |     **12 177 msgs** |                    **−96 %** |
| End-to-end p50                                   |      17 035 ms |        **1 579 ms** |                        −91 % |
| End-to-end p99                                   |      31 434 ms |        **3 811 ms** |                        −88 % |
| Delivery ratio                                   |         26.6 % |           **100 %** |                       +73 pp |
| Producer rate                                    |    9 701 msg/s |         1 938 msg/s | bounded by consumer capacity |

All numbers come from a single 40 s run per configuration on a
single Apple M2 (8 GB) running cp-kafka 7.7.0 in KRaft mode.
[Methodology](#methodology) below documents exactly how each is
measured.

[Live dashboard](https://dd1235.github.io/SSP_kafka/) ·
[IEEE format paper (LaTeX)](paper/main.tex) ·

---

## What this project is

Go benchmark
that **answers a question off-the-shelf Kafka tools don't:** _what
happens to user-visible latency, consumer lag, and delivery ratio
when the consumer falls behind, and how does the system recover?_

It does six things existing benchmarks (`kafka-producer-perf-test`,
`librdkafka-bench`) don't:

1. **End-to-end latency** via 16-byte stamped headers (producer's
   `time.Now()` decoded on the consumer).
2. **Consumer lag as a polled time series**, not just a final number,
   with peak / drain-rate / time-to-drain summaries.
3. **A phased slow-consumer/recovery controller** that builds and
   drains a backlog mid-run.
4. **Six payload classes** (random / zeros / text / json / logline /
   mixed) instead of random-only, surfacing real compression ratios
   from 1× to 8 904×.
5. **Runtime/metrics GC sampling** correlated with the latency timeline,
   which is how the paper finds zstd's 16× GC pressure.
6. **Adaptive credit-based backpressure** with hysteretic threshold
   control — the headline contribution.

Why Go: cheap goroutines,
mature Sarama Kafka client, native `runtime/metrics` GC visibility,
no JVM warmup distortion.

---

## Architecture

### Component view (build-time dependencies)

```
                       cmd/bench/main.go
                       (wiring only;
                        no business logic)
                              │
       ┌──────────────────────┼──────────────────────┐
       │                      │                      │
       ▼                      ▼                      ▼
   internal/             internal/             internal/
   producer              consumer              lag
       │                      │                      │
       ▼                      ▼                      ▼
   ┌────────────────┐   ┌────────────────┐   ┌────────────────┐
   │ AsyncProducer  │   │ ConsumerGroup  │   │ Sarama client  │
   │ pool +         │   │ pool +         │   │ + admin        │
   │ bursty +       │   │ phased delay   │   │ HWM/committed  │
   │ backpressure   │   │                │   │ poller         │
   └────────────────┘   └────────────────┘   └────────────────┘
       │     │                  │                      │
       │     │                  │                      │
       │     └─► metrics ◄──────┘                      │
       │        (atomic counters,                      │
       │         percentiles, CDF)                     │
       │                                               │
       ├────► payload (6 generators + 16-B header)    │
       │                                               │
       ├────► sysperf (runtime/metrics CPU+GC) ◄──────┘
       │                                               │
       └────► output (JSON + human report) ◄──────────┘

       config (pure data, no Kafka import) ◄──── used by all
```

Every package has a one-paragraph header doc. The graph is acyclic;
`config` and `payload` are leaves; `cmd/bench/main.go` is wiring only.

### Runtime view (one bench run)

```
   ┌─ cmd/bench/main.go  (parse → ensure topic → warmup → bench → JSON) ─┐
   │                                                                     │
   │   8× producer goroutines  ──► AsyncProducer ──► broker ──► partitions
   │      │                                                         ▲
   │      └─ stamp 16-B header (worker, seq, ts)                    │
   │                                                                │
   │   4× consumer goroutines  ◄── ConsumerGroup ◄──────────────────┘
   │      │                          (round-robin partition assignment)
   │      └─ decode header, record E2E latency, MarkMessage
   │
   │   1× lag poller goroutine  ──► sarama.Client + admin
   │      │                                  │
   │      └─ every 1 s: HWM − committed → atomic.Int64 snapshot
   │
   │   1× throttle controller  ── reads atomic, hysteretic pause toggle
   │      │
   │      └─ flips paused atomic.Bool; senders waitForCredit()
   │
   │   1× sysperf sampler  ──► runtime/metrics + runtime.MemStats
   │      │
   │      └─ every 2 s: CPU%, heap MB, GC count, pause ms
   │
   │   1× interval reporter  ──► stdout + timeline append
   │
   └─► On benchCtx cancel: MarkEnd() freezes window, drain channels,
       FinalSnapshot() → output.Build → JSON file + human-readable report
```

The eight collaborating goroutine types share state through atomic
snapshots (no mutexes on the hot path). `sync.Mutex` only appears
where it pays for itself: latency-sample slice append, payload RNG
slow paths.

---

## Methodology

### Test machine and Kafka config

| Component                 | Value                                             |
| ------------------------- | ------------------------------------------------- |
| **CPU**                   | Apple Silicon M2 (8 cores, ARMv8.5)               |
| **Memory**                | 8 GB unified                                      |
| **OS**                    | macOS Darwin 24.6.0 (arm64)                       |
| **Go**                    | 1.26.1                                            |
| **Container runtime**     | Docker Desktop                                    |
| **Kafka image**           | `confluentinc/cp-kafka:7.7.0`                     |
| **Kafka mode**            | KRaft single-node (no ZooKeeper)                  |
| **Kafka heap**            | `-Xmx1g -Xms1g`, G1GC, MaxGCPauseMillis=20        |
| **Replication factor**    | 1 (single broker)                                 |
| **Default partitions**    | 12                                                |
| **Topic retention**       | 1 h                                               |
| **Group rebalance delay** | `0 ms` (`KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS`) |

The full broker config is in [`benchmark/docker-compose.yml`](benchmark/docker-compose.yml).
Single-node, replication factor 1 is intentional: this benchmark
characterises _consumer-side_ behaviour, not replication or ISR.
Multi-broker generalisation is called out as future work in the
paper.

### How latency is measured (two distinct metrics)

**1. Producer-ack latency** — `producer.Input()` enqueue → broker `Successes()` event.

We attach a wall-clock timestamp to every `sarama.ProducerMessage`'s
`Metadata` field at the moment we enqueue it, then read it back when
the message arrives on `prod.Successes()`:

```go
// at enqueue (producer goroutine)
pm := &sarama.ProducerMessage{
    Topic:    cfg.Topic,
    Value:    sarama.ByteEncoder(msg),
    Metadata: inFlight{enqueuedAt: time.Now()},
}
prod.Input() <- pm

// at ack (ack-reader goroutine)
case msg := <-prod.Successes():
    md := msg.Metadata.(inFlight)
    col.RecordAckLatency(time.Since(md.enqueuedAt))
```

This is "time until the broker said it got the write." It is bounded
above by network RTT plus broker write latency.

**2. End-to-end latency** — producer enqueue → consumer receive.

The producer stamps a 16-byte header into the message body before
sending: `(workerID uint32, seq uint32, sendNs int64)`. The consumer
decodes the same bytes:

```go
// producer side
binary.LittleEndian.PutUint32(buf[0:4], workerID)
binary.LittleEndian.PutUint32(buf[4:8], seq)
binary.LittleEndian.PutUint64(buf[8:16], uint64(time.Now().UnixNano()))

// consumer side (in ConsumeClaim)
sentNs := int64(binary.LittleEndian.Uint64(msg.Value[8:16]))
lat := time.Duration(time.Now().UnixNano() - sentNs)
col.RecordE2ELatency(lat)
```

**Key caveat:** producer and consumer run on the same machine, so
there is **no clock skew**. Cross-host measurement would require NTP

- skew correction. End-to-end numbers in this paper are not directly
  comparable to multi-host deployments.

**Both histograms** are stored as raw `int64` microseconds and at
report time produce: `min, p50, p75, p90, p95, p99, p99.5, p99.9, p99.99, max, mean, std-dev, MAD, 22-bucket log-spaced CDF`.
See [`benchmark/internal/metrics/percentiles.go`](benchmark/internal/metrics/percentiles.go).

### How consumer lag is sampled

A dedicated goroutine on a separate `sarama.Client` polls every
`-lag-interval` (default 1 s):

```go
for each partition p in the topic:
    HWM[p]      = client.GetOffset(topic, p, sarama.OffsetNewest)
    committed[p] = admin.ListConsumerGroupOffsets(group, ...)[p]
    lag[p]      = HWM[p] - committed[p]
total_lag = sum(lag[p])
```

The most recent total is also stored in an `atomic.Int64` accessible
to the producer pool's adaptive-backpressure controller; everything
else (`samples` slice, `Summary` struct) is read at report time.

Three caveats this metric inherits:

- Sarama auto-commits offsets every ~1 s. At producer rate `r`, a
  steady-state ~`r × 1 s` of "processed but not committed" messages
  is always visible as lag. **The 9 000-message floor in the
  baseline is offset-commit cadence, not real backlog.**
- The poller's tick is 1 Hz; finer cadence is configurable but
  default 1 Hz keeps broker load low.
- Lag is measured per partition then summed; per-partition lag is
  also persisted in JSON for hot-key diagnosis.

### What "delivery %" means (and why it sometimes exceeds 100 %)

`delivery = received_bench / sent_bench × 100 %` where both counts
are taken from atomic counters reset at warmup-end.

Three regimes:

| Range                  | Reading                                                                                                                                                                                                                                           |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ~100 % (within 0.1 pp) | Healthy: consumer kept up.                                                                                                                                                                                                                        |
| < 100 %                | Consumer fell behind permanently within the bench window; the "missing" messages are still on the broker, undelivered within our measurement window.                                                                                              |
| > 100 %                | **Warmup-window edge effect** (not a bug): the consumer drained leftover warmup-phase messages during the bench window. Those go into `received` (the counter was zeroed at warmup-end) but not into `sent` (they were emitted before the reset). |

Backpressured runs amplify the > 100 % effect because the producer
is paused for most of the bench window, so the _bench-phase_ sent
count is small while the consumer keeps draining warmup leftovers
at full speed.

### Bench-window discipline

Every run has three phases:

1. **Warmup** (3--5 s, default): producer + consumer run; results
   discarded. `Collector.ResetAfterWarmup()` zeroes counters and
   wipes latency slices at the boundary.
2. **Bench** (configured duration): the only measured window.
3. **Teardown** (a few seconds while goroutines drain): excluded
   from `avg_rate` via `Collector.MarkEnd()`, called the instant
   the bench context cancels.

### Number of runs per scenario

**Single run per configuration**, of the duration listed in each
section of the paper (20--80 s). Reporting:

- **Within-run percentiles** (p50, p99, p99.9, p99.99) computed from
  the full sample stream of the run. Sample sizes range from ~25 k
  (5 s smoke tests) to ~300 k (30 s baseline). p99.99 is shown only
  where n ≥ 100 k, so each percentile estimate is statistically tight.
- **Final summary fields** (`messages_sent`, `peak_lag`,
  `time_to_drain`) are point measurements from the same run.
- Claims are framed as "the system's behaviour at the configured
  operating point," and the JSON output is fully reproducible from
  the recorded `config` block.

The full 30+ result JSONs are in [`paper/data/`](paper/data/); every
figure is generated from them by
[`paper/make_plots.py`](paper/make_plots.py).

### Compression measurement

Each run also runs an **offline compression report** _before_ the
bench begins: 2 000 sample payloads are encoded through
`none / snappy / lz4 / gzip / zstd` to give per-codec compression
ratios on the actual workload. This is independent of which codec
the producer is configured to use; it lets one run answer
"what would I get with each codec?" See
[`benchmark/internal/metrics/compression.go`](benchmark/internal/metrics/compression.go).

The on-the-wire byte count is not scraped from the broker; the
offline report is used as the per-codec compression proxy.

### Sysperf measurement

A `sysperf` sampler runs every 2 s reading:

- `runtime.MemStats.HeapAlloc, HeapInuse, NumGC, PauseTotalNs, PauseNs[lastGC]`
- `runtime/metrics` `/cpu/classes/total:cpu-seconds` (delta divided by `runtime.NumCPU()`)
- `runtime.NumGoroutine()`

This samples the bench process. Broker-side JVM metrics are out of
the project scope.

---

## Detailed results (single-run, 19 scenarios + 3 backpressure)

### Baseline — 30 s healthy run

| Metric                   | Value                   |
| ------------------------ | ----------------------- |
| Messages sent / received | 297 899 / 297 927       |
| Send rate (bench window) | 9 924 msg/s             |
| Payload throughput       | 4.85 MB/s               |
| Ack p50 / p99 / p99.9    | 3.32 / 15.96 / 35.81 ms |
| E2E p50 / p99 / p99.9    | 3.89 / 30.77 / 97.19 ms |
| Process CPU (avg / max)  | 99.4 % / 121.7 %        |
| GC cycles / total pause  | 79 / 7.2 ms             |

### Compression sweep (mixed payload, 20 s, e2e p99)

`gzip` 22.4 ms · `lz4` 27.8 ms · `none` 17.9 ms · `zstd` 41.0 ms · `snappy` **74.4 ms (worst)**.
zstd triggered 842 GC cycles vs 40--52 for the others — 16× higher GC
pressure. The full table is in [paper §V-B](paper/main.tex).

### Slow consumer / recovery / bursty

| Scenario                              | Peak lag |   E2E p99 | Delivery |
| ------------------------------------- | -------: | --------: | -------: |
| Healthy baseline                      |      --- |   30.8 ms |    100 % |
| Slow consumer (4 ms delay, 40 s)      |  309 435 | 31 434 ms |   26.6 % |
| Recovery (3 ms → 0 mid-run, 80 s)     |  195 598 | 19 674 ms |    102 % |
| Bursty light (5 k → 15 k, 60 s)       |   14 575 |   15.3 ms |    100 % |
| Bursty heavy (5 k → 30 k, no delay)   |   29 356 |   19.2 ms |    100 % |
| Bursty overload (5 k → 30 k + 800 µs) |  110 445 |  6 971 ms |    107 % |

### Adaptive backpressure (the headline contribution)

Same workload as the slow-consumer row above; only `-max-lag` /
`-resume-lag` flags differ.

| Setting |   Lmax |   Peak lag |      E2E p99 |  Delivery | Send rate |
| ------- | -----: | ---------: | -----------: | --------: | --------: |
| Off     |    --- |    309 435 |    31 434 ms |    26.6 % |     9 701 |
| Loose   | 20 000 |     25 956 |     9 171 ms |     100 % |     2 269 |
| Tight   |  5 000 | **12 177** | **3 811 ms** | **100 %** |     1 938 |

The producer self-throttles to roughly the consumer's sustained
capacity. The "missing" producer throughput in the off-row was
already being lost (delivery 26.6 %); backpressure trades **dropped
throughput for guaranteed responsiveness**.

The full data is in [`paper/data/`](paper/data/); the tables are
generated from JSON by [`paper/make_plots.py`](paper/make_plots.py).

---

## Repository tour

```
SSP_kafka/
├── benchmark/                        # the working Go benchmark
│   ├── cmd/bench/main.go             # wiring only — no logic
│   ├── internal/
│   │   ├── config/                   # CLI flags + BenchConfig (no Kafka import)
│   │   ├── payload/                  # 6 payload classes + 16-B header
│   │   ├── producer/                 # AsyncProducer pool + bursty + bp
│   │   ├── consumer/                 # ConsumerGroup pool + phased delay
│   │   ├── lag/                      # broker-offset poller (atomic snapshot)
│   │   ├── metrics/                  # collector, percentiles, compression
│   │   ├── sysperf/                  # runtime/metrics CPU/GC sampling
│   │   ├── topic/                    # topic lifecycle (ensure/reset)
│   │   └── output/                   # JSON + human report
│   ├── docker-compose.yml            # KRaft single-node Kafka
│   ├── run.sh                        # preset scenarios
│   └── README.md                     # benchmark-specific docs
├── paper/
│   ├── main.tex                      # 6-page IEEE conference format
│   ├── make_plots.py                 # all figures (matplotlib)
│   ├── data/                         # 30+ result JSONs (one per scenario)
│   └── assets/                       # 13 PDF figures
├── site/                             # React dashboard for results
└── archive/                          # earlier experimental iterations
```

---

## How to run

Prerequisites: Docker Desktop with Docker Compose, Go 1.26+, and a shell that can
run `benchmark/run.sh`.

```bash
# 1. enter the benchmark runner
cd benchmark

# 2. start Kafka (KRaft single-node, persistent volume)
./run.sh start

# 3. confirm the container is healthy and listening on localhost:9092
docker ps --filter name=kafka-bench-v4

# 4. run the baseline 30 s benchmark
./run.sh bench

# 5. try the headline scenario
./run.sh slow-consumer            # builds 309 K of lag
./run.sh recovery                 # build then drain
./bin/bench -duration 40s -rate 10000 -consumer-delay 4ms \
            -max-lag 5000 -resume-lag 1000 -payload json \
            -output results-bp.json    # backpressure on

# 6. clean shutdown: stop Kafka and remove the running container/network
./run.sh stop
```

Every run produces a JSON in `benchmark/results-*.json` with the
full schema documented in [`benchmark/internal/output/output.go`](benchmark/internal/output/output.go).
Plot it with `paper/make_plots.py` or pipe it into the
[live dashboard](https://dd1235.github.io/SSP_kafka/).

Use `./run.sh stop` for a normal shutdown; it runs `docker compose down` and
keeps the Kafka volume for the next run. Use `./run.sh clean` when you want a
fresh local Kafka state; it runs `docker compose down -v` and deletes the
`kafka-data` volume.

---

## Engineering decisions

Selected design choices and where they live in the codebase.

| Decision                                             | Rationale                                                                                                                                                           | Where it lives                                |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------- |
| **One AsyncProducer per goroutine, no shared mutex** | Hot-path lock-free; ack-readers are siblings.                                                                                                                       | `benchmark/internal/producer/producer.go`     |
| **Cache-line padded atomic counters**                | Avoid false sharing on 8-counter Collector. 56 B padding → each counter on its own 64-B line.                                                                       | `benchmark/internal/metrics/metrics.go`       |
| **16-byte producer-stamped header**                  | Lets consumer compute e2e latency with no clock skew (single-machine).                                                                                              | `benchmark/internal/payload/payload.go:192`   |
| **Lag poller on its own goroutine + Sarama client**  | 1-Hz polling can't share a client with 10-kHz data plane. Atomic snapshot exposed for backpressure.                                                                 | `benchmark/internal/lag/lag.go`               |
| **Hysteresis on backpressure thresholds**            | Single-threshold control oscillates on every poll tick. Two thresholds (high pause, low resume) give one event per cycle.                                           | `benchmark/internal/producer/producer.go:208` |
| **Acyclic package graph; pure-data `config`**        | `config` and `payload` import nothing internal; `cmd/bench/main.go` is wiring-only. Adding a flag is one struct field, one `flag.*Var`.                             | `benchmark/internal/config/config.go`         |
| **MarkEnd() to freeze bench window**                 | Excludes goroutine teardown from `elapsed`, which the interim version got wrong by ~25 %.                                                                           | `benchmark/internal/metrics/metrics.go:127`   |
| **Six payload classes, not random-only**             | Random bytes are the worst case for every codec; real workloads compress 5--30×. The interim's "snappy is best" conclusion was an artifact of random-only payloads. | `benchmark/internal/payload/payload.go`       |
| **`runtime/metrics` GC sampling alongside latency**  | Reveals zstd's 16× GC pressure that the latency histogram alone hides.                                                                                              | `benchmark/internal/sysperf/sysperf.go`       |
| **JSON schema is the contract; additive-only**       | Plotting and analysis tools never break when new flags are added. Burst and backpressure additions touched zero existing figures.                                   | `benchmark/internal/output/output.go`         |
| **Race-detector hardened**                           | `go run -race` clean. Caught a real bug in the payload generator (shared `*rand.Rand`); fix was per-mode `sync.Mutex`.                                              | `benchmark/internal/payload/payload.go:38`    |

---

## What was actually built (full list)

- **Modular Go benchmark, ~1 300 lines, 10 internal packages.**
  Acyclic dependency graph; one wiring file (`cmd/bench/main.go`)
  with zero business logic.
- **Producer pool** with three opt-in modes (constant rate, bursty,
  adaptive backpressure), all gated by default-zero flags so existing
  configurations are byte-identical when you don't use them.
- **Consumer pool** with phased per-message delay (mid-run swap from
  high delay to zero delay drives the recovery scenario).
- **Lag poller** with atomic snapshot accessor for the backpressure
  controller.
- **Metrics collector** with cache-line padded atomic counters,
  bench-window accounting (`MarkEnd`), warmup reset, two latency
  histograms (ack + e2e), 8 percentiles + MAD + 22-bucket CDF.
- **Sysperf sampler** reading `runtime/metrics` and
  `runtime.MemStats` every 2 s.
- **Six payload classes** (random / zeros / text / json / logline /
  mixed) with reproducible RNG seed and 16-B timestamp header.
- **Offline compression report** that runs before each bench, encoding
  2 000 sample payloads through every codec.
- **Adaptive credit-based backpressure** with hysteretic thresholds,
  the headline new feature; gated by `-max-lag`.
- **Topic lifecycle management** (ensure / reset) using Sarama admin.
- **JSON output schema** (additive-only) covering config, final
  stats, timeline, lag samples, sysperf samples, compression report,
  backpressure stats.
- **Six preset scenarios** in `run.sh` plus three sweeps and two
  bursty/backpressure presets.
- **30+ controlled experiment runs** producing the JSONs in
  `paper/data/`.
- **13 figures and 4 tables** in
  [`paper/make_plots.py`](paper/make_plots.py) and
  [`paper/main.tex`](paper/main.tex).
- **A 6-page IEEE-format paper** covering motivation, design,
  methodology, ten experimental sections (baseline, compression,
  acks, payload, message-size, slow-consumer, recovery, bursty,
  backpressure, discussion).

- **A live React dashboard** at
  <https://dd1235.github.io/SSP_kafka/>.
- **Five interim defects fixed and documented**: zero-receive
  consumer (offset bug); incorrect bench-window accounting;
  mis-named `-batch-size` flag; data race in payload generator;
  random-only-payload mis-conclusion about codecs.

---

## Limitations

- **Single-broker, replication factor 1.** No replication, ISR
  shrink/expand, leader election, or cross-host RTT. The
  `acks=-1`-is-smoother-than-`acks=1` finding is a single-broker
  artifact and would not hold on a real RF=3 cluster.
- **Single machine, single run per configuration.** Within-run
  percentiles are well-sampled; cross-run variance is not reported.
  Order-of-magnitude estimates, not ±1 % production forecasts.
- **`bytes_sent` is pre-compression payload bytes**, not actual
  wire/disk bytes. The offline compression report is the proxy.
- **Sysperf samples the bench process**, not the Kafka JVM. Broker-
  side metrics require JMX exporter (out of scope).
- **Synthetic payloads.** Real-event replay (paper §X.A) is the
  obvious credibility upgrade and the recommended next step.

---

## Tech used

Go 1.26.1 · Apache Kafka 7.7.0 (KRaft mode) ·
[Sarama](https://github.com/IBM/sarama) (pure-Go Kafka client) ·
Docker Compose ·
[klauspost/compress](https://github.com/klauspost/compress)
(zstd / s2-snappy) · [pierrec/lz4](https://github.com/pierrec/lz4)
· Python 3.12 + matplotlib (figures) · LaTeX (`IEEEtran` paper class).

---
