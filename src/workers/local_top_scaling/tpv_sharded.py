#!/usr/bin/env python3

"""Sharded TPV worker that aggregates semester totals per store based on store_id sharding."""

import logging
import os
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict
from message_utils import ClientId # pyright: ignore[reportMissingImports]
from worker_utils import run_main, safe_float_conversion, safe_int_conversion, extract_year_half # pyright: ignore[reportMissingImports]
from workers.local_top_scaling.aggregator_worker import AggregatorWorker
from workers.utils.sharding_utils import get_routing_key, extract_store_id_from_payload
from workers.utils.state_manager import TPVStateManager

# Configurar logging básico
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurar FileHandler para escribir logs a archivo
def setup_file_logging(worker_id: int):
    """Configura un FileHandler para escribir logs a un archivo basado en worker_id."""
    # Crear directorio de logs si no existe
    log_dir = Path("/app/logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Crear archivo de log con nombre basado en worker_id
    log_file = log_dir / f"tpv_sharded_worker_{worker_id}.log"
    
    # Crear FileHandler
    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    
    # Formato para el archivo (más detallado)
    # El worker_id ya está en el nombre del archivo, así que no es necesario incluirlo en cada línea
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    
    # Agregar handler al logger raíz para capturar todos los logs
    root_logger = logging.getLogger()
    root_logger.addHandler(file_handler)
    
    # También agregar al logger específico
    logger.addHandler(file_handler)
    
    logger.info(f"File logging configured: {log_file}")

YearHalf = str
StoreId = int

class ShardedTPVWorker(AggregatorWorker):
    """
    Sharded version of TPVWorker that processes transactions based on store_id sharding.
    Each worker processes a specific shard of stores.
    """
    
    def __init__(self) -> None:
        super().__init__()
        
        # Get sharding configuration from environment
        self.num_shards = int(os.getenv('NUM_SHARDS', '2'))
        self.worker_id = int(os.getenv('WORKER_ID', '0'))
        
        # Setup file logging for this worker
        setup_file_logging(self.worker_id)
        
        # Validate worker_id is within shard range
        if self.worker_id >= self.num_shards:
            raise ValueError(f"WORKER_ID {self.worker_id} must be less than NUM_SHARDS {self.num_shards}")
        
        # Configure routing key for this worker's shard
        self.expected_routing_key = f"shard_{self.worker_id}"
        
        logger.info(f"ShardedTPVWorker initialized: worker_id={self.worker_id}, num_shards={self.num_shards}, routing_key={self.expected_routing_key}")

        # Configure state manager
        state_path_env = os.getenv('STATE_FILE')
        state_dir_env = os.getenv('STATE_DIR')
        
        state_path = None
        state_dir = None
        
        if state_path_env:
            state_path = Path(state_path_env)
        elif state_dir_env:
            state_dir = Path(state_dir_env)

        self.state_manager = TPVStateManager(
            state_data=None,
            state_path=state_path,
            state_dir=state_dir,
            worker_id=str(self.worker_id)
        )
        
        from collections import defaultdict
        state_data = self.state_manager.state_data
        if state_data is None or not isinstance(state_data, defaultdict):
            state_data = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
            self.state_manager.state_data = state_data
        
        self.partial_tpv = self.state_manager.state_data
        
        # Track clients that have already received EOF to reject batches that arrive after EOF
        self._clients_with_eof: set[ClientId] = set()

    def reset_state(self, client_id: ClientId) -> None:
        try:
            del self.partial_tpv[client_id]
        except KeyError:
            pass

    def should_process_transaction(self, payload: Dict[str, Any]) -> bool:
        """
        Determine if this worker should process the transaction based on store_id sharding.
        Also handles coordinated EOF messages that don't have store_id.
        
        Args:
            payload: Transaction data
            
        Returns:
            True if this worker should process the transaction
        """
        store_id = extract_store_id_from_payload(payload)
        if store_id is None:
            return False
            
        expected_routing_key = get_routing_key(store_id, self.num_shards)
        if expected_routing_key != self.expected_routing_key:
            logger.warning(f"Received transaction for wrong shard: store_id={store_id}, expected={self.expected_routing_key}, got={expected_routing_key}")
            return False
            
        return True

    def accumulate_transaction(self, client_id: str, payload: Dict[str, Any]) -> None:
        # Only process transactions that belong to this worker's shard
        if not self.should_process_transaction(payload):
            return
            
        # Skip control messages (EOF, etc.) - they don't have store_id
        store_id = extract_store_id_from_payload(payload)
        if store_id is None:
            logger.debug(f"Skipping control message in accumulate_transaction: {payload}")
            return
            
        year_half: YearHalf | None = extract_year_half(payload.get('created_at'))
        if not year_half:
            return
        
        store_id = safe_int_conversion(payload.get('store_id'), minimum=0)
        amount: float = safe_float_conversion(payload.get('final_amount'), 0.0)

        # logger.info(f"Processing TPV transaction for store_id={store_id}, year_half={year_half}, amount={amount}")
        self.partial_tpv[client_id][year_half][store_id] += amount

    def process_batch(self, batch: list[Dict[str, Any]], client_id: ClientId):
        if self.shutdown_requested:
            logger.info("[BATCH-FLOW] Shutdown requested, rejecting batch to requeue")
            raise InterruptedError("Shutdown requested before batch processing")
        
        # Reject batches that arrive after EOF for this client
        # NOTE: This indicates a protocol error - batches should never arrive after EOF.
        # This is a safety check, but the root cause should be fixed in the sharding router.
        if client_id in self._clients_with_eof:
            message_uuid = self._get_current_message_uuid()
            logger.info(
                "[PROCESSING - BATCH] [PROTOCOL-ERROR] Batch %s for client %s arrived AFTER EOF (protocol violation). "
                "This indicates the sharding router sent EOF before all batches were delivered. "
                "Discarding batch to prevent incorrect processing.",
                message_uuid,
                client_id,
            )
            raise Exception(f"Batch arrived after EOF for client {client_id} (protocol error), discarding")
            
        message_uuid = self._get_current_message_uuid()

        if message_uuid and self.state_manager.get_last_processed_message(client_id) == message_uuid:
            logger.info(
                "[PROCESSING - BATCH] [DUPLICATE] Skipping duplicate batch %s for client %s (already processed)",
                message_uuid,
                client_id,
            )
            return

        with self._state_lock:
            previous_state = self.state_manager.clone_client_state(client_id)
            previous_uuid = self.state_manager.get_last_processed_message(client_id)

            try:
                for idx, entry in enumerate(batch):
                    if self.shutdown_requested:
                        logger.info("[PROCESSING - BATCH] [INTERRUPT] Shutdown requested during batch processing, rolling back")
                        raise InterruptedError("Shutdown requested during batch processing")

                    self.accumulate_transaction(client_id, entry)

                logger.info(f"All {len(batch)} transactions accumulated successfully")

                if message_uuid:
                    self.state_manager.set_last_processed_message(client_id, message_uuid)

                if not self.shutdown_requested:
                    logger.info(f"[BATCH-FLOW] [PERSIST] About to persist state for client {client_id}, message_uuid={message_uuid}")
                    self.state_manager.persist_state(client_id)  # Persist only this client
                    logger.info(f"[BATCH-FLOW] [PERSIST] State persisted successfully for client {client_id}, message_uuid={message_uuid}")
                else:
                    logger.info("[PROCESSING - BATCH] [INTERRUPT] Shutdown requested after batch processing, rolling back state")
                    raise InterruptedError("Shutdown requested, preventing state persistence")
                
                logger.info(f"[BATCH-FLOW] [SUCCESS] Batch processing completed successfully for client {client_id}, message_uuid={message_uuid}. Returning from process_batch.")
            except Exception as e:
                logger.error(f"[BATCH-FLOW] [ERROR] Exception during batch processing: {type(e).__name__}: {e}")
                logger.info("[BATCH-FLOW] [ROLLBACK] Restoring previous state and UUID")
                self.state_manager.restore_client_state(client_id, previous_state)
                if message_uuid:
                    if previous_uuid is None:
                        logger.info("[BATCH-FLOW] [ROLLBACK] Clearing last_processed_message (was None)")
                        self.state_manager.clear_last_processed_message(client_id)
                    else:
                        logger.info(f"[BATCH-FLOW] [ROLLBACK] Restoring previous UUID: {previous_uuid}")
                        self.state_manager.set_last_processed_message(client_id, previous_uuid)
                raise

    def create_payload(self, client_id: str) -> list[Dict[str, Any]]:
        totals = self.partial_tpv.pop(client_id, {})
        results: list[Dict[str, Any]] = []

        for year_half, stores in totals.items():
            for store_id, tpv_value in stores.items():
                results.append(
                    {
                        'year_half_created_at': year_half,
                        'store_id': store_id,
                        'tpv': tpv_value,
                    }
                )

        return results

    def handle_eof(self, message: Dict[str, Any], client_id: ClientId):
        """
        Handle EOF by sending data to aggregator and then sending EOF directly to output.
        
        Sharded workers do NOT use the consensus mechanism (requeue) between them.
        Each sharded worker sends EOF directly to the TPV aggregator after sending its data.
        The aggregator will wait for EOFs from all sharded workers (using REPLICA_COUNT).
        """
        if self.shutdown_requested:
            logger.info("Shutdown requested, rejecting EOF to requeue")
            raise InterruptedError("Shutdown requested before EOF handling")
        
        # Mark this client as having received EOF to reject any batches that arrive after
        with self._state_lock:
            self._clients_with_eof.add(client_id)
            logger.info(f"[EOF] Marking client {client_id} as having received EOF. Future batches for this client will be rejected.")
        
        # Obtener la data que se va a enviar antes de que create_payload la elimine del estado
        with self._state_lock:
            # Crear una copia de los datos para logging sin modificar el estado
            totals_for_logging = {}
            for year_half, stores in self.partial_tpv.get(client_id, {}).items():
                totals_for_logging[year_half] = dict(stores)
            
            # Log de la data que se va a enviar
            if totals_for_logging:
                logger.info(f"[EOF] [DATA-TO-SEND] Client {client_id}: Preparando data para enviar al TPV aggregator")
                total_tpv = 0.0
                store_count = 0
                for year_half, stores in totals_for_logging.items():
                    for store_id, tpv_value in stores.items():
                        total_tpv += tpv_value
                        store_count += 1
                        logger.info(f"[EOF] [DATA-TO-SEND]   - year_half={year_half}, store_id={store_id}, tpv={tpv_value}")
                logger.info(f"[EOF] [DATA-TO-SEND] Client {client_id}: Resumen - {store_count} stores, total_tpv={total_tpv}")
            else:
                logger.info(f"[EOF] [DATA-TO-SEND] Client {client_id}: No hay data para enviar (estado vacío)")
            
        with self._pause_message_processing():
            try:
                # Send data to aggregator (this is done by AggregatorWorker.handle_eof)
                # But we need to avoid calling BaseWorker.handle_eof which uses consensus mechanism
                payload_batches: list[list[Dict[str, Any]]] = []
                with self._state_lock:
                    payload = self.create_payload(client_id)
                    if payload:
                        self.reset_state(client_id)
                        if self.chunk_payload:
                            payload_batches = [payload]
                        else:
                            payload_batches = self._chunk_payload(payload, self.chunk_size)

                for chunk in payload_batches:
                    self.send_payload(chunk, client_id)
                
                # Send EOF directly to output WITHOUT consensus mechanism
                # This is the key change: sharded workers send EOF directly to aggregator
                logger.info(f"[EOF] [SHARDED-WORKER] Sending EOF directly to TPV aggregator for client {client_id} (no consensus)")
                self.eof_handler.output_eof(client_id=client_id)
                
            except Exception:
                raise

            with self._state_lock:
                previous_state = self.state_manager.clone_client_state(client_id)
                previous_uuid = self.state_manager.get_last_processed_message(client_id)
                message_uuid = self._get_current_message_uuid()
                try:
                    if message_uuid:
                        self.state_manager.set_last_processed_message(client_id, message_uuid)
                    else:
                        logger.warning(f"EOF message for client {client_id} has no UUID; clearing last processed message")
                        self.state_manager.clear_last_processed_message(client_id)

                    self.state_manager.drop_empty_client_state(client_id)
                    
                    if not self.shutdown_requested:
                        self.state_manager.persist_state(client_id)  # Persist only this client
                    else:
                        logger.info("Shutdown requested during EOF handling, skipping state persistence")
                except Exception:
                    self.state_manager.restore_client_state(client_id, previous_state)
                    if message_uuid:
                        if previous_uuid is None:
                            self.state_manager.clear_last_processed_message(client_id)
                        else:
                            self.state_manager.set_last_processed_message(client_id, previous_uuid)
                    elif previous_uuid is not None:
                        self.state_manager.set_last_processed_message(client_id, previous_uuid)
                    raise

    def _get_current_message_uuid(self) -> str | None:
        metadata = self._get_current_message_metadata()
        if not metadata:
            return None
        message_uuid = metadata.get('message_uuid')
        if not message_uuid:
            logger.warning("Missing message_uuid in metadata: %s", metadata.keys())
            return None
        return str(message_uuid)


if __name__ == '__main__':
    run_main(ShardedTPVWorker)
