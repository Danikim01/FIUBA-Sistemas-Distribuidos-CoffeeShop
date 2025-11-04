# Tests para ShardedTPVWorker

Suite exhaustiva de tests para el `ShardedTPVWorker` que cubre todos los casos borde y escenarios de falla.

## Instalación

Instala las dependencias necesarias:

```bash
pip install pytest pytest-timeout
```

## Estructura de Tests

### Tests Unitarios/Comprehensivos (`test_tpv_sharded_comprehensive.py`)

Cubre los siguientes escenarios:

1. **Gestión de Estado (State Management)**
   - Persistencia y carga del estado desde disco
   - Manejo de archivos de estado inválidos
   - Restauración desde backup
   - Sincronización del estado después de cargar

2. **Mensajes Duplicados (Duplicate Messages)**
   - Detección de batches duplicados
   - Omisión correcta de mensajes ya procesados
   - Recuperación después de restart
   - Múltiples batches del mismo cliente

3. **Manejo de EOF**
   - Limpieza del estado después de EOF
   - EOFs repetidos
   - EOF sin UUID
   - Emisión de resultados después de EOF

4. **Transacciones Después de EOF**
   - Creación de nuevo estado después de EOF
   - Independencia entre sesiones del mismo cliente

5. **Race Conditions y Concurrencia**
   - Procesamiento concurrente de batches
   - EOF concurrente con transacciones
   - Thread safety del estado

6. **Simulación de Fallos**
   - Rollback en caso de excepciones
   - Manejo de fallos de persistencia
   - Recuperación después de fallos

7. **Casos Borde**
   - Batches vacíos
   - Transacciones con campos faltantes
   - Transacciones del shard incorrecto
   - Múltiples clientes independientes

### Tests de Integración (`test_tpv_sharded_integration.py`)

Tests que requieren Docker y RabbitMQ para ejecutarse.

## Ejecución

### Ejecutar todos los tests

```bash
cd tests
./run_tests.sh all
```

O directamente con pytest:

```bash
pytest tests/test_tpv_sharded_comprehensive.py -v
```

### Ejecutar tests específicos

El script `run_tests.sh` permite ejecutar diferentes categorías de tests:

```bash
# Tests unitarios/comprehensivos
./run_tests.sh unit

# Tests de integración (requiere Docker)
./run_tests.sh integration

# Tests de estado
./run_tests.sh state

# Tests de mensajes duplicados
./run_tests.sh duplicate

# Tests de EOF
./run_tests.sh eof

# Tests de race conditions
./run_tests.sh race

# Tests de fallos
./run_tests.sh failure
```

### Ejecutar un test específico

```bash
pytest tests/test_tpv_sharded_comprehensive.py::TestTPVShardedStateManagement::test_state_persistence_and_load -v
```

### Ejecutar con marcadores

```bash
# Solo tests marcados como "state"
pytest -m state -v

# Excluir tests lentos
pytest -m "not slow" -v
```

## Marcadores de Tests

Los tests están marcados con los siguientes marcadores:

- `unit`: Tests unitarios que no requieren servicios externos
- `integration`: Tests de integración que pueden requerir Docker/RabbitMQ
- `slow`: Tests que tardan mucho en ejecutarse
- `state`: Tests relacionados con gestión de estado
- `duplicate`: Tests para manejo de mensajes duplicados
- `eof`: Tests para manejo de EOF
- `race`: Tests para race conditions
- `failure`: Tests para escenarios de fallo

## Variables de Entorno

Los tests configuran automáticamente las siguientes variables de entorno:

- `NUM_SHARDS`: Número de shards (default: 2)
- `WORKER_ID`: ID del worker (default: 0)
- `STATE_DIR`: Directorio para archivos de estado (usado en tests)
- `SKIP_INTEGRATION_TESTS`: Si está activado, omite tests de integración

## Ejemplos de Uso

### Verificar que el estado se persiste correctamente

```bash
pytest tests/test_tpv_sharded_comprehensive.py::TestTPVShardedStateManagement::test_state_persistence_and_load -v
```

### Verificar detección de duplicados

```bash
pytest tests/test_tpv_sharded_comprehensive.py::TestTPVShardedDuplicateMessages -v
```

### Verificar manejo de EOF

```bash
pytest tests/test_tpv_sharded_comprehensive.py::TestTPVShardedEOFHandling -v
```

### Verificar race conditions

```bash
pytest tests/test_tpv_sharded_comprehensive.py::TestTPVShardedRaceConditions -v
```

## Debugging

Para ejecutar tests con más información:

```bash
pytest -vv -s tests/test_tpv_sharded_comprehensive.py
```

El flag `-s` muestra los prints y logs, `-vv` muestra más detalles.

## Notas

- Los tests usan mocks para el middleware de RabbitMQ, por lo que no requieren RabbitMQ para ejecutarse
- Los tests de integración (`test_tpv_sharded_integration.py`) sí requieren Docker y RabbitMQ
- Los tests crean directorios temporales para los archivos de estado que se limpian automáticamente
- Algunos tests pueden tardar más tiempo debido a operaciones de disco o concurrencia

## Troubleshooting

### Error: "Module not found"

Asegúrate de que el directorio `src` está en el PYTHONPATH:

```bash
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
```

### Error: "pytest not found"

Instala pytest:

```bash
pip install pytest pytest-timeout
```

### Tests de integración fallan

Los tests de integración requieren Docker. Verifica que Docker esté corriendo:

```bash
docker ps
```

Para omitir los tests de integración:

```bash
export SKIP_INTEGRATION_TESTS=true
pytest tests/test_tpv_sharded_comprehensive.py -v
```

