#!/bin/bash

set -euo pipefail

CONFIG_PATH=workers_config.json

if [ $# -eq 0 ]; then
    echo "Using workers_config.json scaling..."
    scripts/generate_scaled_compose.py --config "${CONFIG_PATH}" --output docker-compose.yml
else
    WORKER_COUNT=$1
    if ! [[ "$WORKER_COUNT" =~ ^[0-9]+$ ]] || [[ "$WORKER_COUNT" -le 0 ]]; then
        echo "Worker count must be a positive integer (got '$WORKER_COUNT')." >&2
        exit 1
    fi
    echo "Scaling all workers to $WORKER_COUNT instances..."
    scripts/generate_scaled_compose.py --config "${CONFIG_PATH}" --output docker-compose.yml --scale "$WORKER_COUNT"
fi

docker compose up --build -d --force-recreate --remove-orphans
