#!/usr/bin/env bash
# run.sh — thin wrapper around `bin/bench` with preset scenarios.
#
# Every scenario is just a set of flags. If you want a custom run, call
# `./bin/bench` directly or copy one of the cases below.
set -euo pipefail

CMD=${1:-"help"}
BIN="./bin/bench"

build() {
  echo "→ Building..."
  mkdir -p bin
  go build -ldflags="-s -w" -o "$BIN" ./cmd/bench
  echo "✓ Built $BIN"
}

kafka_up() {
  echo "→ Starting Kafka..."
  docker compose up -d
  echo "→ Waiting for Kafka to be healthy..."
  until docker compose exec kafka kafka-broker-api-versions \
    --bootstrap-server localhost:9092 >/dev/null 2>&1; do
    sleep 2
    echo "  ...waiting"
  done
  echo "✓ Kafka ready"
}

kafka_down() { docker compose down; }
kafka_clean() { docker compose down -v; echo "✓ volume removed"; }

case "$CMD" in

  build)  build ;;
  start)  kafka_up ;;
  stop)   kafka_down ;;
  clean)  kafka_clean ;;

  # 5s smoke test
  quick)
    build; kafka_up
    "$BIN" \
      -duration 5s -warmup 2s \
      -rate 5000 -msg-size 512 \
      -producers 4 -consumers 2 -partitions 12 \
      -acks 1 -compression snappy \
      -payload mixed \
      -report-interval 1s -lag-interval 1s -sysperf-interval 1s
    ;;

  # Baseline bench — realistic payload mix, clean topic.
  bench)
    build; kafka_up
    "$BIN" \
      -duration "${DURATION:-30s}" -warmup 5s \
      -rate 10000 -msg-size 512 \
      -producers 8 -consumers 4 -partitions 12 \
      -acks 1 -compression snappy \
      -payload mixed \
      -report-interval 5s \
      -output results-bench.json
    ;;

  # Slow consumer — builds lag throughout the run.
  slow-consumer)
    build; kafka_up
    "$BIN" \
      -duration 30s -warmup 3s \
      -rate 8000 -msg-size 512 \
      -producers 8 -consumers 2 -partitions 12 \
      -acks 1 -compression snappy \
      -payload json \
      -consumer-delay 500us \
      -consumer-jitter 200us \
      -report-interval 3s -lag-interval 1s \
      -output results-slow-consumer.json
    ;;

  # Two-phase recovery test: burn lag, then cut delay to 0 and measure drain.
  recovery)
    build; kafka_up
    "$BIN" \
      -duration 60s -warmup 3s \
      -rate 8000 -msg-size 512 \
      -producers 8 -consumers 4 -partitions 12 \
      -acks 1 -compression snappy \
      -payload json \
      -consumer-delay 800us \
      -phase-duration 30s \
      -phase-delay 0s \
      -report-interval 5s -lag-interval 1s \
      -output results-recovery.json
    ;;

  # Sweep compression codecs with a realistic payload.
  sweep-compression)
    build; kafka_up
    for codec in none snappy lz4 zstd gzip; do
      echo; echo "══ compression=$codec ══"
      "$BIN" \
        -duration 20s -warmup 3s \
        -rate 10000 -msg-size 512 \
        -producers 8 -consumers 4 -partitions 12 \
        -acks 1 -compression "$codec" \
        -payload mixed \
        -report-interval 20s -lag-interval 2s \
        -output "results-comp-$codec.json"
    done
    ;;

  # Sweep acknowledgment modes.
  sweep-acks)
    build; kafka_up
    for ack in 0 1 -1; do
      echo; echo "══ acks=$ack ══"
      "$BIN" \
        -duration 20s -warmup 3s \
        -rate 10000 -msg-size 512 \
        -producers 8 -consumers 4 -partitions 12 \
        -acks "$ack" -compression snappy \
        -payload mixed \
        -report-interval 20s \
        -output "results-acks$ack.json"
    done
    ;;

  # Sweep payload modes — shows compression swings across workloads.
  sweep-payload)
    build; kafka_up
    for p in random zeros text json logline mixed; do
      echo; echo "══ payload=$p ══"
      "$BIN" \
        -duration 15s -warmup 2s \
        -rate 8000 -msg-size 512 \
        -producers 8 -consumers 4 -partitions 12 \
        -acks 1 -compression zstd \
        -payload "$p" \
        -report-interval 15s \
        -output "results-payload-$p.json"
    done
    ;;

  # Message-size sweep
  sweep-msgsize)
    build; kafka_up
    for sz in 64 256 1024 4096 16384; do
      echo; echo "══ msg-size=$sz ══"
      "$BIN" \
        -duration 15s -warmup 2s \
        -rate 8000 -msg-size "$sz" \
        -producers 8 -consumers 4 -partitions 12 \
        -acks 1 -compression snappy \
        -payload mixed \
        -report-interval 15s \
        -output "results-size-${sz}.json"
    done
    ;;

  # Bursty workload — periodic spikes that the system must absorb.
  # Two presets: light (system absorbs) and heavy (each burst exceeds drain capacity).
  bursty-light)
    build; kafka_up
    "$BIN" \
      -duration 60s -warmup 3s \
      -rate 5000 -burst-rate 15000 -burst-duration 3s -burst-period 12s \
      -msg-size 512 \
      -producers 8 -consumers 4 -partitions 12 \
      -acks 1 -compression snappy \
      -payload json \
      -report-interval 1s -lag-interval 1s \
      -output results-bursty-light.json
    ;;

  bursty-heavy)
    build; kafka_up
    "$BIN" \
      -duration 60s -warmup 3s \
      -rate 5000 -burst-rate 30000 -burst-duration 3s -burst-period 12s \
      -msg-size 512 \
      -producers 8 -consumers 4 -partitions 12 \
      -acks 1 -compression snappy \
      -payload json \
      -report-interval 1s -lag-interval 1s \
      -output results-bursty-heavy.json
    ;;

  # Adaptive backpressure: same workload as the aggressive slow-consumer
  # scenario, but the producer is allowed to throttle itself once total
  # consumer lag exceeds 20k messages (resumes at 5k, hysteresis).
  # Compare against results-slow-aggr.json (bp-off baseline).
  bp-on)
    build; kafka_up
    "$BIN" \
      -duration 40s -warmup 3s \
      -rate 10000 -msg-size 512 \
      -producers 8 -consumers 2 -partitions 12 \
      -acks 1 -compression snappy \
      -payload json \
      -consumer-delay 4ms -consumer-jitter 1ms \
      -max-lag 20000 -resume-lag 5000 -bp-poll 200ms \
      -report-interval 4s -lag-interval 1s \
      -output results-bp-on.json
    ;;

  reset-topic)
    docker compose exec kafka kafka-topics \
      --bootstrap-server localhost:9092 \
      --delete --topic bench-topic-v4 2>/dev/null || true
    sleep 2
    echo "✓ topic deleted"
    ;;

  help|*)
    cat <<EOF

  kafka-bench-v4 — benchmark runner

  Usage: ./run.sh <command>

  Lifecycle:
    build               Compile bin/bench
    start               docker compose up Kafka
    stop                docker compose down
    clean               stop + remove volume
    reset-topic         drop topic (useful between sweeps)

  Scenarios:
    quick               5s smoke test
    bench               30s baseline  (DURATION=60s overrides)
    slow-consumer       build lag throughout the run
    recovery            build lag for 30s, then drain for 30s
    sweep-compression   none/snappy/lz4/zstd/gzip
    sweep-acks          acks 0 / 1 / -1
    sweep-payload       random/zeros/text/json/logline/mixed
    sweep-msgsize       64 .. 16384 bytes

  Raw usage:
    ./bin/bench -help   to see all flags
EOF
    ;;
esac
