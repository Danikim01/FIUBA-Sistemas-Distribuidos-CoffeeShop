#!/bin/bash

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <worker-count>" >&2
  exit 1
fi

WORKER_COUNT=$1
CONFIG_PATH=workers_config.json

if ! [[ "$WORKER_COUNT" =~ ^[0-9]+$ ]] || [[ "$WORKER_COUNT" -le 0 ]]; then
  echo "Worker count must be a positive integer (got '$WORKER_COUNT')." >&2
  exit 1
fi

# Stop all containers except rabbitmq-server
echo "Stopping existing containers (keeping rabbitmq-server running)..."
docker compose stop $(docker compose ps --services | grep -v rabbitmq) 2>/dev/null || true
docker compose rm -f $(docker compose ps --services | grep -v rabbitmq) 2>/dev/null || true

scripts/generate_scaled_compose.py --config "${CONFIG_PATH}" --output docker-compose.yml --scale "$WORKER_COUNT"
docker compose -f docker-compose.yml up --build -d
