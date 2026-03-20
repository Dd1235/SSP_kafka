#!/usr/bin/env bash
set -euo pipefail

CMD=${1:-"help"}
BINARY="./bin/bench"

build() {
  echo "→ Building..."
  mkdir -p bin
  go build -ldflags="-s -w" -o "$BINARY" ./cmd/bench
  echo "✓ Built $BINARY"
}

kafka_up() {
  echo "→ Starting Kafka (KRaft, persistence enabled)..."
  docker compose up -d
  echo "→ Waiting for Kafka to be healthy..."
  until docker compose exec kafka kafka-broker-api-versions \
    --bootstrap-server localhost:9092 &>/dev/null 2>&1; do
    sleep 2
    echo "  ...waiting"
  done
  echo "✓ Kafka ready"
}

kafka_down() {
  docker compose down
}

kafka_clean() {
  docker compose down -v
  echo "✓ Kafka data volume removed"
}

case "$CMD" in
  build)
    build
    ;;

  start)
    kafka_up
    ;;

  stop)
    kafka_down
    ;;

  clean)
    kafka_clean
    ;;

  quick)
    # 5s smoke test
    build
    kafka_up
    "$BINARY" \
      -duration 5s \
      -warmup 2s \
      -rate 5000 \
      -msg-size 512 \
      -producers 4 \
      -consumers 2 \
      -partitions 12 \
      -acks 1 \
      -compression snappy \
      -report-interval 1s
    ;;

  bench)
    # Full 10k msg/s, 30s benchmark
    build
    kafka_up
    "$BINARY" \
      -duration "${DURATION:-30s}" \
      -warmup 5s \
      -rate 10000 \
      -msg-size 512 \
      -producers 8 \
      -consumers 4 \
      -partitions 12 \
      -acks 1 \
      -compression snappy \
      -report-interval 5s \
      -output results.json
    ;;

  long)
    # 10-minute run
    build
    kafka_up
    "$BINARY" \
      -duration 10m \
      -warmup 10s \
      -rate 10000 \
      -msg-size 512 \
      -producers 8 \
      -consumers 4 \
      -partitions 12 \
      -acks 1 \
      -compression snappy \
      -report-interval 30s \
      -output results-10m.json
    ;;

  sweep)
    # Sweep compression codecs
    build
    kafka_up
    for codec in none snappy lz4 zstd; do
      echo ""
      echo "══ compression=$codec ══"
      "$BINARY" \
        -duration 20s \
        -warmup 3s \
        -rate 10000 \
        -msg-size 512 \
        -producers 8 \
        -consumers 4 \
        -partitions 12 \
        -acks 1 \
        -compression "$codec" \
        -report-interval 20s \
        -output "results-$codec.json"
    done
    ;;

  sweep-acks)
    # Sweep ack levels
    build
    kafka_up
    for ack in 0 1 -1; do
      echo ""
      echo "══ acks=$ack ══"
      "$BINARY" \
        -duration 20s \
        -warmup 3s \
        -rate 10000 \
        -msg-size 512 \
        -producers 8 \
        -consumers 4 \
        -partitions 12 \
        -acks "$ack" \
        -compression snappy \
        -report-interval 20s \
        -output "results-acks$ack.json"
    done
    ;;

    producer-only)
    build
    kafka_up
    "$BINARY" \
      -duration "${DURATION:-30s}" \
      -warmup 5s \
      -rate 10000 \
      -msg-size 512 \
      -producers 8 \
      -partitions 12 \
      -acks 1 \
      -compression snappy \
      -producer-only \
      -report-interval 5s
    ;;

  reset-topic)
    # Delete and recreate the bench topic (useful between sweep runs)
    docker compose exec kafka kafka-topics \
      --bootstrap-server localhost:9092 \
      --delete --topic bench-topic 2>/dev/null || true
    sleep 2
    echo "✓ topic deleted (will be recreated on next run)"
    ;;

  help|*)
    echo ""
    echo "  Kafka Goroutine Benchmark"
    echo ""
    echo "  Usage: ./run.sh <command>"
    echo ""
    echo "  Commands:"
    echo "    build         Build the binary"
    echo "    start         Start Kafka"
    echo "    stop          Stop Kafka"
    echo "    clean         Stop Kafka and remove data"
    echo "    quick         5-second smoke test"
    echo "    bench         Full 30s / 10k msg/s benchmark"
    echo "    long          10-minute benchmark"
    echo "    sweep         Benchmark all compression codecs"
    echo "    sweep-acks    Benchmark acks 0/1/-1"
    echo "    producer-only Producer-only benchmark"
    echo "    reset-topic   Delete bench-topic"
    echo ""
    echo "  Example:"
    echo "    DURATION=60s ./run.sh bench"
    ;;
esac