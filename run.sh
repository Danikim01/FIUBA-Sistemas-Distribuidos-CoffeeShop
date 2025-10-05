#!/bin/bash

set -euo pipefail

WORKER_COUNT=$1
CONFIG_PATH=workers_config.json

if ! [[ "$WORKER_COUNT" =~ ^[0-9]+$ ]] || [[ "$WORKER_COUNT" -le 0 ]]; then
  echo "Worker count must be a positive integer (got '$WORKER_COUNT')." >&2
  exit 1
fi

scripts/generate_scaled_compose.py --config "${CONFIG_PATH}" --output docker-compose.yml --scale "$WORKER_COUNT"
docker compose up --build -d --force-recreate --remove-orphans
