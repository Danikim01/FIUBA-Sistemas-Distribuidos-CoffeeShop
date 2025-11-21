#!/bin/bash

# Script para repetir pruebas automáticamente
# Uso: ./repeat.sh X (donde X es el número de iteraciones)

if [ $# -ne 1 ]; then
    echo "Uso: $0 <número_de_iteraciones>"
    echo "Ejemplo: $0 3"
    exit 1
fi

NUM_ITERATIONS=$1

if ! [[ "$NUM_ITERATIONS" =~ ^[0-9]+$ ]] || [ "$NUM_ITERATIONS" -lt 1 ]; then
    echo "Error: El número de iteraciones debe ser un entero positivo"
    exit 1
fi

LOGS_DIR="test_logs"
mkdir -p "$LOGS_DIR"

echo "================================================"
echo "Iniciando $NUM_ITERATIONS iteraciones de prueba"
echo "Cada iteración ejecutará: ./run.sh 3 5"
echo "Tiempo de espera por iteración: 30 minutos"
echo "================================================"
echo ""

for i in $(seq 1 $NUM_ITERATIONS); do
    echo "========================================"
    echo "Iteración $i de $NUM_ITERATIONS"
    echo "Hora de inicio: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "========================================"
    
    echo "Ejecutando ./run.sh 3 5..."
    ./run.sh 3 5
    
    if [ $? -ne 0 ]; then
        echo "Error: ./run.sh falló en la iteración $i"
        echo "Continuando con la siguiente iteración..."
    fi
    
    echo "Esperando 30 minutos para que terminen los procesos..."
    echo "Finalización estimada: $(date -d '+30 minutes' '+%Y-%m-%d %H:%M:%S')"
    sleep 1800
    
    echo "Capturando logs de los clientes..."
    
    CLIENT_CONTAINERS=$(docker ps --format '{{.Names}}' | grep -i client | sort)
    
    if [ -z "$CLIENT_CONTAINERS" ]; then
        echo "Advertencia: No se encontraron contenedores de clientes en la iteración $i"
    else
        CLIENT_NUM=1
        for container in $CLIENT_CONTAINERS; do
            LOG_FILE="$LOGS_DIR/intento${i}-cliente${CLIENT_NUM}.txt"
            echo "Guardando logs de $container en $LOG_FILE..."
            docker logs --tail 16 "$container" > "$LOG_FILE" 2>&1
            CLIENT_NUM=$((CLIENT_NUM + 1))
        done
        echo "Logs guardados para la iteración $i"
    fi
    
    echo "Iteración $i completada"
    echo ""
done

echo "================================================"
echo "Todas las iteraciones completadas"
echo "Logs guardados en el directorio: $LOGS_DIR/"
echo "Hora de finalización: $(date '+%Y-%m-%d %H:%M:%S')"
echo "================================================"
