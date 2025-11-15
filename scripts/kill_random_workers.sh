#!/usr/bin/env bash

set -euo pipefail

COMPOSE_CMD=${COMPOSE_CMD:-"docker compose"}
KILL_INTERVAL=5      # seconds between kills
KILL_COUNT=0         # 0 => infinite
WORKER_PATTERNS=("*-worker-*")

get_all_services() {
  $COMPOSE_CMD ps --services
}

is_worker_service() {
  local service=$1
  for pattern in "${WORKER_PATTERNS[@]}"; do
    if [[ $service == $pattern ]]; then
      return 0
    fi
  done
  return 1
}

ALL_SERVICES=()
while IFS= read -r svc; do
  [[ -z $svc ]] && continue
  ALL_SERVICES+=("$svc")
done < <(get_all_services)
WORKER_SERVICES=()
for svc in "${ALL_SERVICES[@]}"; do
  if is_worker_service "$svc"; then
    WORKER_SERVICES+=("$svc")
  fi
done

if [[ ${#WORKER_SERVICES[@]} -eq 0 ]]; then
  echo "No worker services found to kill." >&2
  exit 1
fi

choose_running_worker() {
  local running=()
  while IFS= read -r svc; do
    [[ -z $svc ]] && continue
    running+=("$svc")
  done < <($COMPOSE_CMD ps --services --filter status=running)
  local candidates=()
  for svc in "${running[@]}"; do
    for worker in "${WORKER_SERVICES[@]}"; do
      if [[ $svc == "$worker" ]]; then
        candidates+=("$svc")
      fi
    done
  done

  if [[ ${#candidates[@]} -eq 0 ]]; then
    echo ""
  else
    local idx=$((RANDOM % ${#candidates[@]}))
    echo "${candidates[$idx]}"
  fi
}

perform_kill() {
  local target=$1
  if [[ -z $target ]]; then
    echo "No running worker services available to kill."
    return 1
  fi
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Killing worker service: $target"
  $COMPOSE_CMD kill "$target"
}

kills_done=0
while :; do
  worker=$(choose_running_worker)
  if ! perform_kill "$worker"; then
    sleep "$KILL_INTERVAL"
    continue
  fi

  kills_done=$((kills_done + 1))
  if [[ $KILL_COUNT -ne 0 && $kills_done -ge $KILL_COUNT ]]; then
    echo "Reached requested kill count ($KILL_COUNT). Exiting."
    break
  fi

  sleep "$KILL_INTERVAL"
done
