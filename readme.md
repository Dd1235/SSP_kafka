# Kafka Bench Explorer

Go-based Apache Kafka benchmarking project focused on consumer lag, end-to-end
latency, backpressure, recovery, payload compressibility, and runtime overhead.

## What This Includes

- `benchmark/` — reproducible Go benchmark runner with Docker Compose Kafka.
- `paper/` — final report source, plots, and canonical benchmark JSON data.
- `site/` — static React dashboard for exploring the results.
- `old_experiments/` — earlier experiments, some for just picking up go

## Highlights

- Baseline: 9,924 msg/s with 30.8 ms p99 end-to-end latency.
- Slow consumer stress: lag reached 309,435 messages.
- Recovery test: drained a 195,598-message backlog at 3,726 msg/s.
- Adaptive backpressure: reduced peak lag from 309,435 to 12,177 messages in the tight setting.

## Links

- Dashboard: https://dd1235.github.io/SSP_kafka/
- Runner: `benchmark/README.md`
- Paper: `paper/main.tex`
- Will include the pdf on finalizing

## Run Locally

```bash
cd benchmark
./run.sh start
./run.sh bench
```
