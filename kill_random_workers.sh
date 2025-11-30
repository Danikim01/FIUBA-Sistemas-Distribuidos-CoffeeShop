#!/usr/bin/env bash

set -euo pipefail

COMPOSE_CMD=${COMPOSE_CMD:-"docker compose"}
KILL_INTERVAL=1    # seconds between kills
KILL_COUNT=0         # 0 => infinite

# Worker type flags
INCLUDE_SHARDED=false
INCLUDE_FILTER=false
INCLUDE_EOF_BARRIER=false
INCLUDE_ROUTERS=false
INCLUDE_AGGREGATORS=false
INCLUDE_HEALTHCHECKERS=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    -s|--sharded)
      INCLUDE_SHARDED=true
      shift
      ;;
    -f|--filter)
      INCLUDE_FILTER=true
      shift
      ;;
    -e|--eof-barrier)
      INCLUDE_EOF_BARRIER=true
      shift
      ;;
    -r|--routers)
      INCLUDE_ROUTERS=true
      shift
      ;;
    -g|--aggregators)
      INCLUDE_AGGREGATORS=true
      shift
      ;;
    -c|--healthcheckers)
      INCLUDE_HEALTHCHECKERS=true
      shift
      ;;
    -a|--all)
      INCLUDE_SHARDED=true
      INCLUDE_FILTER=true
      INCLUDE_EOF_BARRIER=true
      INCLUDE_ROUTERS=true
      INCLUDE_AGGREGATORS=true
      INCLUDE_HEALTHCHECKERS=true
      shift
      ;;
    -h|--help)
      echo "Usage: $0 [OPTIONS]"
      echo ""
      echo "Options (puedes combinar múltiples opciones):"
      echo "  -s, --sharded          Incluir workers sharded (ej: tpv-worker-sharded-*, items-worker-sharded-*)"
      echo "  -f, --filter           Incluir filter workers (ej: *-filter-worker-*)"
      echo "  -e, --eof-barrier     Incluir EOF barrier workers (ej: *-eof-barrier)"
      echo "  -r, --routers          Incluir sharding routers (ej: *-sharding-router)"
      echo "  -g, --aggregators      Incluir aggregators (ej: *-aggregator)"
      echo "  -c, --healthcheckers   Incluir healthcheckers (ej: healthchecker-*), máximo 2 de 3"
      echo "  -a, --all             Incluir todos los tipos de workers"
      echo "  -h, --help            Mostrar este mensaje de ayuda"
      echo ""
      echo "Si no se especifica ninguna opción, se usa el comportamiento por defecto:"
      echo "  Solo workers sharded y filter (equivalente a: -s -f)"
      echo ""
      echo "Environment variables:"
      echo "  COMPOSE_CMD            Docker compose command (default: 'docker compose')"
      echo "  KILL_INTERVAL          Segundos entre kills (default: 0.25)"
      echo "  KILL_COUNT             Número de kills antes de salir (default: 0 = infinito)"
      echo ""
      echo "Ejemplos:"
      echo "  $0                     # Solo sharded y filter workers (default)"
      echo "  $0 -s                  # Solo sharded workers"
      echo "  $0 -f                   # Solo filter workers"
      echo "  $0 -e                   # Solo EOF barrier workers"
      echo "  $0 -c                   # Solo healthcheckers (máximo 2 de 3)"
      echo "  $0 -s -f -e            # Sharded, filter y EOF barrier"
      echo "  $0 -a                  # Todos los tipos de workers"
      exit 0
      ;;
    *)
      echo "Opción desconocida: $1" >&2
      echo "Usa -h o --help para ver información de uso" >&2
      exit 1
      ;;
  esac
done

# Set worker patterns based on options
# Si no se especifica ninguna opción, usar comportamiento por defecto (sharded + filter)
if [[ "$INCLUDE_SHARDED" == "false" && \
      "$INCLUDE_FILTER" == "false" && \
      "$INCLUDE_EOF_BARRIER" == "false" && \
      "$INCLUDE_ROUTERS" == "false" && \
      "$INCLUDE_AGGREGATORS" == "false" && \
      "$INCLUDE_HEALTHCHECKERS" == "false" ]]; then
  INCLUDE_SHARDED=true
  INCLUDE_FILTER=true
fi

WORKER_PATTERNS=()
if [[ "$INCLUDE_SHARDED" == "true" ]]; then
  WORKER_PATTERNS+=("*-worker-sharded-*")
fi
if [[ "$INCLUDE_FILTER" == "true" ]]; then
  WORKER_PATTERNS+=("*-filter-worker-*")
fi
if [[ "$INCLUDE_EOF_BARRIER" == "true" ]]; then
  WORKER_PATTERNS+=("*-eof-barrier")
fi
if [[ "$INCLUDE_ROUTERS" == "true" ]]; then
  WORKER_PATTERNS+=("*-sharding-router")
fi
if [[ "$INCLUDE_AGGREGATORS" == "true" ]]; then
  WORKER_PATTERNS+=("*-aggregator")
fi
if [[ "$INCLUDE_HEALTHCHECKERS" == "true" ]]; then
  WORKER_PATTERNS+=("healthchecker-*")
fi
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
  echo "No se encontraron servicios worker que coincidan con los patrones seleccionados." >&2
  echo "Patrones activos: ${WORKER_PATTERNS[*]}" >&2
  exit 1
fi

# Track killed healthcheckers (máximo 2 de 3)
KILLED_HEALTHCHECKERS=()
MAX_HEALTHCHECKER_KILLS=2

is_healthchecker() {
  local service=$1
  [[ $service == healthchecker-* ]]
}

is_healthchecker_killed() {
  local service=$1
  for killed in "${KILLED_HEALTHCHECKERS[@]}"; do
    if [[ $killed == "$service" ]]; then
      return 0
    fi
  done
  return 1
}

can_kill_healthchecker() {
  if [[ ${#KILLED_HEALTHCHECKERS[@]} -ge $MAX_HEALTHCHECKER_KILLS ]]; then
    return 1
  fi
  return 0
}

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
        # Si es un healthchecker, verificar que no esté ya matado y que no se hayan matado ya 2
        if is_healthchecker "$svc"; then
          if is_healthchecker_killed "$svc"; then
            continue  # Ya fue matado, saltarlo
          fi
          if ! can_kill_healthchecker; then
            continue  # Ya se mataron 2 healthcheckers, no matar más
          fi
        fi
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
    echo "No hay servicios worker corriendo disponibles para matar."
    return 1
  fi
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Matando servicio worker: $target"
  $COMPOSE_CMD kill "$target"
  
  # Si es un healthchecker, agregarlo a la lista de matados
  if is_healthchecker "$target"; then
    KILLED_HEALTHCHECKERS+=("$target")
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Healthchecker matado. Total matados: ${#KILLED_HEALTHCHECKERS[@]}/$MAX_HEALTHCHECKER_KILLS"
  fi
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
    echo "Se alcanzó el número de kills solicitado ($KILL_COUNT). Saliendo."
    break
  fi

  sleep "$KILL_INTERVAL"
done
