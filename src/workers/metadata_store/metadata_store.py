from abc import ABC, abstractmethod
import logging
import threading
from typing import Any, Optional
from message_utils import (
    ClientId,
    extract_data_and_client_id,
    is_client_reset_message,
    is_eof_message,
    is_reset_all_clients_message,
)
from middleware.rabbitmq_middleware import RabbitMQMiddlewareExchange, RabbitMQMiddlewareQueue

logger = logging.getLogger(__name__)

class MetadataStore(ABC):
    def __init__(
        self, 
        name: str, 
        middleware: RabbitMQMiddlewareQueue | RabbitMQMiddlewareExchange,
        eof_state_store: Optional[Any] = None,
        metadata_type: Optional[str] = None
    ):
        """Initialize an metadata store for the worker.
        
        Args:
            name: Name of the metadata
            middleware: Middleware for consuming messages
            eof_state_store: Optional MetadataEOFStateStore for tracking EOFs
            metadata_type: Optional metadata type ('users', 'stores', 'menu_items') for EOF tracking
        """
        self.name = name
        self.middleware = middleware
        self.current_client_id = ''
        self.eof_state_store = eof_state_store
        self.metadata_type = metadata_type
        self._stop_event = threading.Event()
        self.consuming_thread = threading.Thread(target=self._start_consuming, daemon=True)
        
    def close(self):
        """Close the middleware connection."""
        self._stop_event.set()
        self.middleware.close()
        if self.consuming_thread.is_alive():
            self.consuming_thread.join(timeout=10.0)

    def _start_consuming(self):
        """Start consuming messages from the metadata queue."""
        import time

        def on_message(message):
            try:
                client_id, data, _ = extract_data_and_client_id(message)

                if is_reset_all_clients_message(message):
                    logger.info(
                        f"[METADATA-STORE {self.name}] Global reset control message received, clearing all metadata"
                    )
                    try:
                        self.reset_all()
                    except Exception as exc:
                        logger.error(
                            f"[METADATA-STORE {self.name}] Failed to reset all metadata: {exc}",
                            exc_info=True,
                        )
                    if self.eof_state_store:
                        self.eof_state_store.clear_all()
                    return

                if is_client_reset_message(message):
                    if not client_id or client_id.strip() == '':
                        logger.warning(
                            f"[METADATA-STORE {self.name}] Client reset message missing client_id, ignoring"
                        )
                        return
                    logger.info(
                        f"[METADATA-STORE {self.name}] Client reset control message received for {client_id}"
                    )
                    try:
                        self.reset_state(client_id)
                    except Exception as exc:
                        logger.error(
                            f"[METADATA-STORE {self.name}] Failed to reset client {client_id}: {exc}",
                            exc_info=True,
                        )
                    if self.eof_state_store:
                        self.eof_state_store.clear_client(client_id)
                    return
                
                # Validar que el client_id esté presente
                if not client_id or client_id.strip() == '':
                    logger.error(
                        f"Received message without client_id in metadata store {self.name}. "
                        f"Message keys: {list(message.keys()) if isinstance(message, dict) else 'N/A'}. "
                        f"Discarding message."
                    )
                    return
                
                self.current_client_id = client_id

                # Verificar si ya se procesó el EOF usando eof_state_store
                if self.eof_state_store and self.metadata_type:
                    state = self.eof_state_store.get_metadata_state(client_id)
                    if state.get(self.metadata_type, False):
                        logger.info(f"Extra source {self.name} already done for client {client_id}, ignoring message")
                        return
                
                if is_eof_message(message):
                    logger.info(f"EOF from client {client_id} received from extra source {self.name}")
                    # Notificar al MetadataEOFStateStore
                    if self.eof_state_store and self.metadata_type:
                        self.eof_state_store.mark_metadata_done(client_id, self.metadata_type)
                    return

                self._handle_data(data)

            except InterruptedError:
                logger.info(
                    "Processing interrupted for extra source %s, message will be requeued",
                    self.name,
                )
                raise
            except Exception as e:
                logger.error(
                    "Error interno iniciando consumo: Error procesando mensaje en extra source %s para cliente %s: %s",
                    self.name,
                    getattr(self, "current_client_id", ""),
                    e,
                    exc_info=True,
                )
                raise

        # Loop de reintento para el thread de metadata
        retry_count = 0
        max_retry_delay = 60
        base_retry_delay = 2
        
        while not self._stop_event.is_set():  # Loop infinito para thread daemon
            try:
                if retry_count > 0:
                    logger.info(f"[METADATA-STORE {self.name}] Starting consumption (reconnection attempt {retry_count})")
                else:
                    logger.info(f"[METADATA-STORE {self.name}] Starting consumption")
                
                self.middleware.start_consuming(on_message)
                if self._stop_event.is_set():
                    logger.info(f"[METADATA-STORE {self.name}] Stop requested, ending consumption loop")
                    break
                
                # Si llegamos aquí, el consumo se detuvo
                logger.warning(f"Consumption stopped for metadata store {self.name}, will retry...")
                retry_count += 1
                wait_time = min(max_retry_delay, base_retry_delay * (2 ** min(retry_count - 1, 5)))
                logger.info(f"Waiting {wait_time} seconds before retrying metadata consumption for {self.name}...")
                time.sleep(wait_time)
                if self._stop_event.is_set():
                    break
            except KeyboardInterrupt:
                logger.info(f"Metadata store {self.name} interrupted")
                break
            except Exception as exc:
                logger.error(f"Error consuming from {self.name}: {exc}", exc_info=True)
                retry_count += 1
                wait_time = min(max_retry_delay, base_retry_delay * (2 ** min(retry_count - 1, 5)))
                logger.info(f"Error in metadata store {self.name}, waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
            else:
                # Reset retry count on successful connection
                if retry_count > 0:
                    logger.info(f"Successfully reconnected metadata store {self.name}, resetting retry count")
                    retry_count = 0

    def start_consuming(self):
        """Start the consuming thread."""
        if not self.consuming_thread.is_alive():
            self.consuming_thread.start()

    def _handle_data(self, data: Any):
        if isinstance(data, list):
            return self.save_batch(data)
        if isinstance(data, dict):
            return self.save_data(data)
        logger.warning(f"Unexpected data type in extra source {self.name}: {type(data)}")

    @abstractmethod
    def save_data(self, data: dict):
        """Handle and persist a message from the metadata queue.
        
        Args:
            message: The message to handle
        """
        raise NotImplementedError
    
    @abstractmethod
    def save_batch(self, data: list):
        """Handle and persist a batch of messages from the metadata queue.

        Args:
            message: The message to handle
        """
        raise NotImplementedError

    @abstractmethod
    def reset_all(self) -> None:
        """Reset metadata for every client."""
        raise NotImplementedError
    
    @abstractmethod
    def _get_item(self, client_id: ClientId, item_id: str) -> str:
        raise NotImplementedError
    
    def reset_state(self, client_id: ClientId):
        """Reset the internal state of the metadata store.
        
        Nota: El estado de EOF en MetadataEOFStateStore se limpia en el aggregator
        cuando llama a reset_state().
        """
        # Llamar al método abstracto para limpiar la metadata específica
        self._reset_metadata_state(client_id)
    
    @abstractmethod
    def _reset_metadata_state(self, client_id: ClientId):
        """Reset the metadata-specific state (data and persistence).
        
        This method should be implemented by subclasses to clear their specific
        metadata (data cache and persistence store).
        """
        raise NotImplementedError

    def get_item_when_done(
        self,
        client_id: ClientId,
        item_id: str,
    ) -> str:
        """
        Obtiene un item de metadata.
        
        Nota: Este método se llama después de que are_all_metadata_done() es True,
        por lo que el EOF de este metadata store ya llegó y el item debería estar
        disponible (en cache o persistencia). Si no está, es porque no existe en los datos.
        """
        # Intentar obtener el item directamente (debe estar en cache o persistencia)
        # ya que are_all_metadata_done() garantiza que el EOF ya llegó
        return self._get_item(client_id, item_id)
