# Tests

Activar el entorno virtual corriendo: `source venv/bin/activate`, si no existe crear uno.

## Tests de persistencia a disco

### Aggregator state store

`pytest tests/test_aggregator_state_store.py`

Verifica que los agregadores puedan guardar y recuperar correctamente su estado intermedio (totales parciales) en disco. Incluye tests de recuperación después de reinicios, manejo de archivos corruptos, y operaciones concurrentes.

### Processed message store

`pytest tests/test_processed_message_store.py`

Verifica que el sistema pueda rastrear qué mensajes ya fueron procesados para evitar duplicados. Incluye tests de persistencia, detección de duplicados, y manejo de caracteres especiales en los identificadores.

### State manager

`pytest tests/test_state_manager.py`

Verifica que los workers puedan guardar y recuperar su estado de procesamiento en disco. Incluye tests de persistencia por cliente, recuperación después de crashes, detección de mensajes duplicados, y rendimiento con grandes volúmenes de datos.

## Tests del middleware

Es requisito tener levantado rabbitmq con docker

`pytest tests/test_middleware_basic.py`

Verifica la comunicación a través de RabbitMQ en diferentes escenarios: un cliente, múltiples clientes, un productor a múltiples consumidores. Incluye tests para colas (queues) y exchanges, verificando orden de mensajes y distribución correcta.