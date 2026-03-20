#!/usr/bin/env bash
# compare.sh — parse JSON result files and print a comparison table
set -euo pipefail

FILES=("$@")
if [[ ${#FILES[@]} -eq 0 ]]; then
  FILES=(results-*.json)
fi

if [[ ${#FILES[@]} -eq 0 ]]; then
  echo "Usage: ./compare.sh results-*.json"
  echo "       (or run ./run.sh sweep first)"
  exit 1
fi

printf "%-22s %10s %9s %9s %8s %8s %8s %8s\n" \
  "file" "rate/s" "MB/s" "p50ms" "p99ms" "p99.9ms" "maxms" "errors"
printf '%.0s─' {1..90}; echo

for f in "${FILES[@]}"; do
  [[ -f "$f" ]] || continue
  name="${f%.json}"
  name="${name#results-}"

  rate=$(jq -r '.results.avg_rate_msg_per_sec // 0 | floor' "$f")
  mb=$(jq -r '.results.throughput_mb_per_sec // 0 | . * 100 | floor | . / 100' "$f")
  p50=$(jq -r '.results.latency_ms.p50 // 0 | . * 100 | floor | . / 100' "$f")
  p99=$(jq -r '.results.latency_ms.p99 // 0 | . * 100 | floor | . / 100' "$f")
  p999=$(jq -r '.results.latency_ms.p999 // 0 | . * 100 | floor | . / 100' "$f")
  max=$(jq -r '.results.latency_ms.max // 0 | . * 100 | floor | . / 100' "$f")
  errs=$(jq -r '.results.errors // 0' "$f")

  printf "%-22s %10s %9s %9s %8s %8s %8s %8s\n" \
    "$name" "$rate" "$mb" "$p50" "$p99" "$p999" "$max" "$errs"
done
