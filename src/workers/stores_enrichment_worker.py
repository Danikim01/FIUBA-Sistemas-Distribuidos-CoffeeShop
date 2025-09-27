#!/usr/bin/env python3

import os
import sys
import logging
import threading
from typing import Any, Dict, List
from middleware.rabbitmq_middleware import RabbitMQMiddlewareQueue

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _is_eof(message: Any) -> bool:
    """Detecta mensajes EOF"""
    return isinstance(message, dict) and str(message.get('type', '')).upper() == 'EOF'


class StoresEnrichmentWorker:
    """
    Worker que enriquece transacciones con datos de stores.
    Sigue el patrón de joiner de proyectos anteriores.
    """

    def __init__(self):
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.rabbitmq_port = int(os.getenv('RABBITMQ_PORT', 5672))

        # Colas de entrada y salida
        self.stores_input_queue = os.getenv('STORES_INPUT_QUEUE', 'stores_raw')
        self.transactions_input_queue = os.getenv('TRANSACTIONS_INPUT_QUEUE', 'transactions_year_filtered')
        self.output_queue = os.getenv('OUTPUT_QUEUE', 'transactions_enriched')

        # Configuración de prefetch
        self.prefetch_count = int(os.getenv('PREFETCH_COUNT', 10))

        # Middleware para stores
        self.stores_middleware = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.stores_input_queue,
            port=self.rabbitmq_port,
            prefetch_count=self.prefetch_count,
        )

        # Middleware para transacciones
        self.transactions_middleware = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.transactions_input_queue,
            port=self.rabbitmq_port,
            prefetch_count=self.prefetch_count,
        )

        # Middleware de salida
        self.output_middleware = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.output_queue,
            port=self.rabbitmq_port,
        )

        # Almacenamiento de stores (patrón joiner) - solo metadata pequeña
        self.stores_lookup = {}  # {store_id: store_name} - solo metadata esencial
        self.stores_loaded = False
        self.stores_eof_received = False
        self.transactions_eof_received = False

        logger.info(
            "StoresEnrichmentWorker inicializado - Stores: %s, Transactions: %s, Output: %s",
            self.stores_input_queue,
            self.transactions_input_queue,
            self.output_queue,
        )

    def process_stores_batch(self, stores_batch: List[Dict[str, Any]]) -> None:
        """Procesa un lote de stores y almacena solo metadata esencial"""
        logger.info(f"Procesando lote de stores: {len(stores_batch)} stores")
        for store in stores_batch:
            store_id = store.get('store_id')
            store_name = store.get('store_name', '')
            if store_id is not None:
                # Solo almacenar metadata esencial
                self.stores_lookup[int(store_id)] = store_name
                logger.info("Store metadata almacenada: ID=%s, Name=%s", store_id, store_name)
        
        logger.info("Total stores en lookup: %s", len(self.stores_lookup))
    def process_stores_message(self, message: Any) -> None:
        """Procesa mensajes de stores"""
        if _is_eof(message):
            logger.info("EOF recibido para stores")
            self.stores_eof_received = True
            self.stores_loaded = True
            self._check_completion()
            return

        if isinstance(message, list):
            self.process_stores_batch(message)

        self.stores_middleware.stop_consuming()
        

    def enrich_transaction(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """Enriquece una transacción con datos de store"""
        try:
            store_id = transaction.get('store_id')
            if store_id is None:
                return transaction

            # Lookup en metadata almacenada (como en proyectos anteriores)
            store_name = self.stores_lookup.get(int(store_id), '')
            
            # Crear transacción enriquecida
            enriched_transaction = {
                **transaction,
                'store_name': store_name,
            }
            
            return enriched_transaction
            
        except Exception as exc:
            logger.error("Error enriqueciendo transacción: %s", exc)
            return transaction

    def process_transactions_batch(self, transactions_batch: List[Dict[str, Any]]) -> None:
        """Procesa un lote de transacciones y las enriquece"""
        # Esperar a que el procesamiento de stores esté completo        
        try:
            enriched_transactions = []
            for transaction in transactions_batch:
                enriched_transaction = self.enrich_transaction(transaction)
                enriched_transactions.append(enriched_transaction)

            self.output_middleware.send(enriched_transactions)
            
        except Exception as exc:
            logger.error("Error procesando lote de transacciones: %s", exc)

    def process_transactions_message(self, message: Any) -> None:
        """Procesa mensajes de transacciones"""
        if _is_eof(message):
            logger.info("EOF recibido para transacciones")
            self.transactions_eof_received = True
            self._check_completion()
            return

        if isinstance(message, list):
            self.process_transactions_batch(message)
        else:
            # Procesar transacción individual
            enriched_transaction = self.enrich_transaction(message)
            self.output_middleware.send(enriched_transaction)

    def _check_completion(self) -> None:
        """Verifica si se han recibido EOF de ambos tipos de datos"""
        if self.stores_eof_received and self.transactions_eof_received:
            logger.info("Todos los EOF recibidos, enviando EOF de salida")
            self.output_middleware.send({'type': 'EOF'})
            self.transactions_middleware.stop_consuming()

    def start_consuming(self) -> None:
        """Inicia el consumo de mensajes de ambas colas en paralelo"""
        logger.info("Iniciando consumo de stores y transacciones")

        def on_stores_message(message: Any) -> None:
            try:
                self.process_stores_message(message)
            except Exception as exc:
                logger.error("Error en callback de stores: %s", exc)

        def on_transactions_message(message: Any) -> None:
            try:
                self.process_transactions_message(message)
            except Exception as exc:
                logger.error("Error en callback de transacciones: %s", exc)

        try:
            # Iniciar consumo de stores en un hilo separado
            # stores_thread = threading.Thread(
            #     target=self.stores_middleware.start_consuming,
            #     args=(on_stores_message,),
            #     daemon=True
            # )
            # stores_thread.start()

            # Procesar stores primero
            logger.info("Iniciando consumo de stores")
            self.stores_middleware.start_consuming(on_stores_message)
            logger.info("Iniciando consumo de transacciones")
            # Después procesar transacciones (stores ya están cargados)
            self.transactions_middleware.start_consuming(on_transactions_message)
            
        except KeyboardInterrupt:
            logger.info("StoresEnrichmentWorker interrumpido por el usuario")
        except Exception as exc:
            logger.error("Error iniciando StoresEnrichmentWorker: %s", exc)
        finally:
            self.cleanup()

    def cleanup(self) -> None:
        """Limpia recursos"""
        try:
            self.stores_middleware.close()
        finally:
            try:
                self.transactions_middleware.close()
            finally:
                self.output_middleware.close()


def main() -> None:
    try:
        worker = StoresEnrichmentWorker()
        worker.start_consuming()
    except Exception as exc:
        logger.error("Error fatal en StoresEnrichmentWorker: %s", exc)
        sys.exit(1)


if __name__ == '__main__':
    main()
