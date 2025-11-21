#!/bin/bash

set -euo pipefail

CONFIG_PATH=workers_config.json

if [ $# -eq 0 ]; then
    echo "Using workers_config.json scaling..."
    scripts/generate_scaled_compose.py --config "${CONFIG_PATH}" --output docker-compose.yml
elif [ $# -eq 1 ]; then
    WORKER_COUNT=$1
    if ! [[ "$WORKER_COUNT" =~ ^[0-9]+$ ]] || [[ "$WORKER_COUNT" -le 0 ]]; then
        echo "Worker count must be a positive integer (got '$WORKER_COUNT')." >&2
        exit 1
    fi
    echo "Scaling all workers to $WORKER_COUNT instances..."
    scripts/generate_scaled_compose.py --config "${CONFIG_PATH}" --output docker-compose.yml --scale "$WORKER_COUNT"
elif [ $# -eq 2 ]; then
    WORKER_COUNT=$1
    CLIENT_COUNT=$2
    if ! [[ "$WORKER_COUNT" =~ ^[0-9]+$ ]] || [[ "$WORKER_COUNT" -le 0 ]]; then
        echo "Worker count must be a positive integer (got '$WORKER_COUNT')." >&2
        exit 1
    fi
    if ! [[ "$CLIENT_COUNT" =~ ^[0-9]+$ ]] || [[ "$CLIENT_COUNT" -le 0 ]]; then
        echo "Client count must be a positive integer (got '$CLIENT_COUNT')." >&2
        exit 1
    fi
    echo "Scaling all workers to $WORKER_COUNT instances and clients to $CLIENT_COUNT instances..."
    scripts/generate_scaled_compose.py --config "${CONFIG_PATH}" --output docker-compose.yml --scale "$WORKER_COUNT" --clients "$CLIENT_COUNT"
else
    echo "Usage: $0 [WORKER_COUNT] [CLIENT_COUNT]" >&2
    echo "  No args: Use workers_config.json scaling" >&2
    echo "  1 arg:   Scale workers to WORKER_COUNT, use config for clients" >&2
    echo "  2 args:  Scale workers to WORKER_COUNT and clients to CLIENT_COUNT" >&2
    exit 1
fi

docker compose up --build -d --force-recreate --remove-orphans
sleep 3
./kill_random_workers.sh