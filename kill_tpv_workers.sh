#!/bin/bash

# Script para matar los workers sharded TPV usando SIGTERM
# Espera poco tiempo inicial para que empiecen a procesar, luego mata cada worker
# mientras están procesando batches (durante el procesamiento de transacciones)
# Al final mata ambos workers a la vez y termina

set -euo pipefail

WORKERS=("tpv-worker-sharded-0" "tpv-worker-sharded-1")
WAIT_INITIAL=1 # Tiempo corto para que empiecen a procesar batches
WAIT_BETWEEN_KILLS=0.01  # Tiempo corto entre kills para aumentar probabilidad de matar durante procesamiento
KILLS_PER_WORKER=30

echo "Esperando ${WAIT_INITIAL} segundos antes de comenzar a matar workers..."
sleep ${WAIT_INITIAL}

echo "Iniciando ciclo: ${KILLS_PER_WORKER} kills por worker con ${WAIT_BETWEEN_KILLS} segundos de intervalo..."

# Contadores para cada worker
declare -A kill_counts
kill_counts["tpv-worker-sharded-0"]=0
kill_counts["tpv-worker-sharded-1"]=0

worker_index=0
total_kills=0

# Matar cada worker 4 veces alternando entre ellos
while [ $total_kills -lt $((KILLS_PER_WORKER * ${#WORKERS[@]})) ]; do
    worker=${WORKERS[$worker_index]}
    
    # Verificar si este worker ya alcanzó su límite
    if [ ${kill_counts[$worker]} -lt $KILLS_PER_WORKER ]; then
        kill_count=$((kill_counts[$worker] + 1))
        
        
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Enviando SIGTERM a contenedor: ${worker} (kill #${kill_count}/${KILLS_PER_WORKER})"
        docker kill -s TERM ${worker} 2>/dev/null || echo "  ⚠️  No se pudo enviar SIGTERM a ${worker} (puede que no exista o ya esté muerto)"
        
        # Esperar un poco antes de revivir
        sleep 2
        
        # Revivir el worker
        echo "  Reviviendo contenedor: ${worker}"
        docker start ${worker} 2>/dev/null || echo "    ⚠️  No se pudo revivir ${worker} (puede que no exista)"
        
        kill_counts[$worker]=$kill_count
        total_kills=$((total_kills + 1))
        
        # Si no es el último kill, esperar
        if [ $total_kills -lt $((KILLS_PER_WORKER * ${#WORKERS[@]})) ]; then
            echo "Esperando ${WAIT_BETWEEN_KILLS} segundos antes del siguiente kill..."
            sleep ${WAIT_BETWEEN_KILLS}
        fi
    fi
    
    # Alternar entre los workers
    worker_index=$(( (worker_index + 1) % ${#WORKERS[@]} ))
done


echo "Script terminado."

