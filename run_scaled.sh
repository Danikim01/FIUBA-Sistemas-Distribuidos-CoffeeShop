#!/bin/bash

set -euo pipefail

CONFIG_PATH=${1:-workers_config.json}

# Stop all containers except rabbitmq-server
echo "Stopping existing containers (keeping rabbitmq-server running)..."
docker compose stop $(docker compose ps --services | grep -v rabbitmq) 2>/dev/null || true
docker compose rm -f $(docker compose ps --services | grep -v rabbitmq) 2>/dev/null || true

scripts/generate_scaled_compose.py --config "${CONFIG_PATH}" --output docker-compose.yml
docker compose -f docker-compose.yml up --build -d
