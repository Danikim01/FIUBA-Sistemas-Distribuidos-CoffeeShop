# Coffee Shop Analysis - Sistema de Procesamiento Distribuido

Este proyecto implementa un sistema de procesamiento distribuido para analizar datos de transacciones de una cafetería usando RabbitMQ como middleware de mensajería.

## Descripción

El sistema procesa transacciones de una cafetería y genera análisis distribuidos sobre:
- **TPV (Total de Ventas)**: Ventas totales por sucursal y semestre
- **Top Items**: Ítems más vendidos por cantidad y por ganancia
- **Top Clientes**: Clientes con más compras por sucursal
- **Filtros**: Filtrado de transacciones por año, horario y monto

## Arquitectura

El sistema está compuesto por:
- **Gateway**: Punto de entrada que recibe datos de los clientes y distribuye el procesamiento
- **Workers de Filtrado**: Filtran transacciones por año, horario y monto
- **Workers Sharded**: Procesan datos distribuidos por sharding (TPV, Top Items, Top Clientes)
- **Aggregators**: Consolidan resultados parciales de los workers sharded
- **Healthcheckers**: Monitorean y reinician automáticamente servicios caídos
- **Clientes**: Envían datos y reciben resultados

## Requisitos

- Docker y Docker Compose
- Python 3.x (para desarrollo local)
- RabbitMQ (se levanta automáticamente con Docker Compose)

## Instrucciones de ejecución

### Corrida normal

```bash
docker compose up --build -d
```

En los logs de cada cliente (ingresar comando para ver cliente nombre contenedor `coffe-client-n`) podrá ver sus resultados de la ejecución para validar si el sistema funcionó bien.

Para ver los logs de un cliente:
```bash
docker logs coffe-client-1
```

### Corrida con tolerancia a fallos

Ejecuta el sistema escalando workers y matándolos aleatoriamente para probar la tolerancia a fallos:

```bash
./run.sh n
```

Donde `n` es el parámetro que indica la cantidad de workers a escalar. Este script:
1. Genera un `docker-compose.yml` escalado con `n` instancias de cada worker
2. Levanta el sistema
3. Comienza a matar workers aleatoriamente usando `kill_random_workers.sh`

### Matar workers manualmente

Para matar workers de forma aleatoria con opciones específicas:

```bash
# Ver ayuda
./kill_random_workers.sh --help

# Matar todos los tipos de workers
./kill_random_workers.sh -a
```

## Configuración

La configuración del sistema se encuentra en `workers_config.json`. Este archivo define:
- Cantidad de réplicas de cada tipo de worker
- Variables de entorno para cada servicio
- Configuración de colas y exchanges de RabbitMQ

Para modificar la escala del sistema, edita el campo `count` de cada worker en el archivo de configuración.

## Tests

Para ejecutar los tests, ver la documentación en [tests/README.md](tests/README.md).

## Características Principales

- **Tolerancia a Fallos**: El sistema puede recuperarse automáticamente cuando workers fallan
- **Escalabilidad**: Workers pueden escalarse horizontalmente mediante sharding
- **Persistencia**: Estado de procesamiento se guarda en disco para recuperación después de crashes
- **Detección de Duplicados**: Evita procesar mensajes duplicados
- **Monitoreo**: Healthcheckers monitorean servicios y los reinician automáticamente

## Resultados

Los resultados se generan en el directorio `results/` de cada cliente, incluyendo:
- `amount_filter_transactions.txt`: Transacciones filtradas por monto y horario
- `top_items_by_quantity.txt`: Top ítems por cantidad vendida
- `top_items_by_profit.txt`: Top ítems por ganancia
- `tpv_summary.txt`: Resumen de TPV por sucursal y semestre
- `top_clients_birthdays.txt`: Top clientes por sucursal