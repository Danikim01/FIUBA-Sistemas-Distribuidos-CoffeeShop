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
echo "Tiempo de espera por iteración: 15 minutos"
echo "================================================"
echo ""

for i in $(seq 1 $NUM_ITERATIONS); do
    echo "========================================"
    echo "Iteración $i de $NUM_ITERATIONS"
    echo "Hora de inicio: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "========================================"
    
    echo "Ejecutando ./run.sh 3 5 en background..."
    ./run.sh 3 5 > "$LOGS_DIR/intento${i}-run.log" 2>&1 &
    RUN_PID=$!
    echo "Proceso ./run.sh iniciado con PID: $RUN_PID"
    echo "Log del run.sh guardado en: $LOGS_DIR/intento${i}-run.log"
    
    echo "Esperando 15 minutos para que terminen los procesos..."
    echo "Finalización estimada: $(date -d '+15 minutes' '+%Y-%m-%d %H:%M:%S')"
    sleep 900
    
    echo "Capturando logs de los clientes..."
    
    CLIENT_CONTAINERS=$(docker ps -a --format '{{.Names}}' | grep -E '^coffee-client-[0-9]+$' | sort -V)
    
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
        echo "Logs guardados para la iteración $i (${CLIENT_NUM} clientes)"
    fi
    
    if [ $i -lt $NUM_ITERATIONS ]; then
        if kill -0 $RUN_PID 2>/dev/null; then
            echo "Deteniendo proceso run.sh (PID: $RUN_PID)..."
            pkill -P $RUN_PID 2>/dev/null
            kill $RUN_PID 2>/dev/null
        fi
        
        echo ""
        echo "Esperando 10 segundos antes de la siguiente iteración..."
        sleep 10
    fi
    
    echo "Iteración $i completada"
    echo ""
done

# Limpieza final
echo ""
echo "Deteniendo procesos run.sh restantes..."
pkill -f "./run.sh" 2>/dev/null

echo "================================================"
echo "Todas las iteraciones completadas"
echo "Logs guardados en el directorio: $LOGS_DIR/"
echo "Hora de finalización: $(date '+%Y-%m-%d %H:%M:%S')"
echo "================================================"
