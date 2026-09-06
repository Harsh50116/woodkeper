#!/usr/bin/env bash
# Drives traffic against nginx and postgres so they produce a continuous log stream.
# Usage: ./scripts/traffic.sh [delay_seconds]   (default 0.5)

set -u

DELAY="${1:-0.5}"
i=0

cleanup() { echo; echo "stopped after $i requests"; exit 0; }
trap cleanup INT

echo "driving traffic every ${DELAY}s — ctrl-c to stop"

while true; do
  i=$((i + 1))

  # Mostly successful requests, with a steady minority of 404s and 405s.
  case $((RANDOM % 10)) in
    0|1) curl -s -o /dev/null "localhost:8080/missing-$i" ;;
    2)   curl -s -o /dev/null -X POST "localhost:8080/" ;;
    *)   curl -s -o /dev/null "localhost:8080/" ;;
  esac

  # Postgres every 5th iteration — docker exec is slow, and query logs are verbose.
  if [ $((i % 5)) -eq 0 ]; then
    docker exec postgres psql -U postgres -q -c "select count(*) from pg_class;" >/dev/null 2>&1
  fi

  sleep "$DELAY"
done
