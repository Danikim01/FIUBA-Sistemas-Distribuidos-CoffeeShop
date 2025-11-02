# Healthcheck System

Sistema de monitoreo de salud para servicios basado en UDP que detecta y reinicia automáticamente servicios caídos.

## Arquitectura

El sistema está compuesto por dos componentes principales:

### 1. Healthcheck Service (`service.py`)
Servicio UDP que cada worker (incluyendo el gateway) expone para responder a las verificaciones de salud. Escucha en un puerto UDP configurable y responde con ACK cuando recibe un mensaje de healthcheck.

### 2. Healthchecker (`healthchecker.py`)
Proceso centralizado que monitorea todos los servicios configurados, envía mensajes de healthcheck periódicamente y ejecuta `docker restart` cuando detecta que un servicio ha fallado.

## Protocolo UDP

El protocolo es simple y basado en UDP:

- **Request**: 1 byte con valor `1` (HEALTHCHECK_REQUEST)
- **ACK**: 1 byte con valor `2` (HEALTHCHECK_ACK)

Flujo:
1. El healthchecker envía un mensaje de request (byte 1) al puerto UDP del servicio
2. El servicio recibe el request y responde con ACK (byte 2)
3. Si el healthchecker no recibe ACK dentro del timeout, cuenta como error

## Variables de Entorno

### Healthchecker

Configuración del proceso healthchecker (contenedor `healthchecker-1`):

| Variable | Default | Descripción |
|----------|---------|-------------|
| `HEALTHCHECK_PORT` | `9290` | Puerto UDP donde escuchan los servicios de healthcheck |
| `HEALTHCHECK_INTERVAL_MS` | `1000` | Intervalo entre checks en milisegundos |
| `HEALTHCHECK_TIMEOUT_MS` | `1500` | Timeout para esperar respuesta ACK en milisegundos |
| `HEALTHCHECK_MAX_ERRORS` | `3` | Número de errores consecutivos antes de reiniciar un servicio |
| `HEALTHCHECK_INITIAL_DELAY_SECONDS` | `10` | Segundos de espera antes de empezar a monitorear (permite que los servicios se inicien) |
| `NODES_TO_CHECK` | *(requerido)* | Lista de nombres de contenedores a monitorear, separados por espacios |

### Healthcheck Service

Configuración del servicio de healthcheck en cada worker/gateway:

| Variable | Default | Descripción |
|----------|---------|-------------|
| `HEALTHCHECK_PORT` | `9290` | Puerto UDP donde escuchar requests de healthcheck |

## Funcionamiento

### Inicialización

1. **Servicios (Workers/Gateway)**:
   - Al inicializar, cada worker/gateway crea una instancia de `HealthcheckService`
   - El servicio se inicia en un thread separado y escucha en el puerto UDP configurado
   - Los logs mostrarán: `"Healthcheck service started on port 9290"`

2. **Healthchecker**:
   - Espera `HEALTHCHECK_INITIAL_DELAY_SECONDS` segundos antes de empezar a monitorear
   - Crea un thread por cada servicio a monitorear
   - Los logs mostrarán: `"Starting health monitoring..."`

### Monitoreo Continuo

Para cada servicio monitoreado:

1. **Conexión**: Intenta resolver el DNS del contenedor y crear un socket UDP
2. **Envío de Healthcheck**: Envía el mensaje de request (byte 1)
3. **Espera de ACK**: Espera la respuesta ACK (byte 2) dentro del timeout
4. **Conteo de Errores**:
   - Si recibe ACK: resetea el contador de errores
   - Si no recibe ACK o hay error de conexión: incrementa el contador
   - Si alcanza `max_errors` consecutivos: ejecuta `docker restart <container_name>`

### Reinicio Automático

Cuando se detecta que un servicio ha fallado:

1. El healthchecker ejecuta `docker restart <container_name>`
2. Espera el intervalo configurado antes de verificar nuevamente
3. El contador de errores se resetea después del reinicio

## Integración

El healthcheck service se integra automáticamente en:

- **BaseWorker**: Todos los workers heredan de `BaseWorker`, que inicia el servicio automáticamente
- **Gateway**: El `CoffeeShopGateway` inicia el servicio en su `__init__`

No requiere configuración adicional en el código de los workers o gateway.

## Notas

- El healthchecker requiere acceso al socket de Docker (`/var/run/docker.sock`) para poder reiniciar contenedores
- Los servicios deben estar en la misma red Docker para que la resolución DNS funcione
- El sistema es tolerante a fallos transitorios (requiere múltiples errores consecutivos antes de reiniciar)
- Por ahora, solo hay un healthchecker sin consenso. En el futuro se puede extender para múltiples healthcheckers con algoritmo de consenso.

