#!/bin/bash
# Script para detener el sistema de manera ordenada
# Detiene los healthcheckers primero para evitar que intenten reiniciar servicios que se están deteniendo

set -e

COMPOSE_FILE="${1:-docker-compose.yml}"

if [ ! -f "$COMPOSE_FILE" ]; then
    echo "Error: archivo docker-compose no encontrado: $COMPOSE_FILE"
    exit 1
fi

echo "Deteniendo el sistema de manera ordenada..."
echo ""

# Paso 1: Detener healthcheckers primero
echo "1. Deteniendo healthcheckers..."
HEALTHCHECKERS=$(docker compose -f "$COMPOSE_FILE" ps --services | grep "^healthchecker-") || true

if [ -n "$HEALTHCHECKERS" ]; then
    echo "$HEALTHCHECKERS" | while read -r hc; do
        echo "   Deteniendo $hc..."
        docker compose -f "$COMPOSE_FILE" stop "$hc" || true
    done
    echo ""
    echo "   Esperando 3 segundos para que los healthcheckers se detengan completamente..."
    sleep 3
    echo ""
else
    echo "   No se encontraron healthcheckers para detener."
    echo ""
fi

# Paso 2: Detener el resto de servicios
echo "2. Deteniendo servicios de aplicación (workers, gateway, etc.)..."
docker compose -f "$COMPOSE_FILE" stop || true
echo ""

# Paso 3: Opcional: eliminar contenedores
if [ "${2:-}" = "--down" ]; then
    echo "3. Eliminando contenedores..."
    docker compose -f "$COMPOSE_FILE" down
    echo ""
    echo "Sistema detenido y contenedores eliminados."
else
    echo "3. Contenedores detenidos pero no eliminados."
    echo "   Para eliminar los contenedores, ejecuta: $0 $COMPOSE_FILE --down"
fi

echo ""
echo "Sistema detenido correctamente."

