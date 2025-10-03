#!/usr/bin/env python3

import os
import sys
import logging
import signal
from datetime import datetime
from collections import defaultdict
from typing import Any, Dict, List, Tuple

from middleware.rabbitmq_middleware import RabbitMQMiddlewareQueue


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _is_eof(message: Any) -> bool:
    return isinstance(message, dict) and str(message.get('type', '')).upper() == 'EOF'


def _semester_from_month(month: int) -> str:
    return 'H1' if month <= 6 else 'H2'


class TPVWorker:
    """Calcula el TPV por semestre y sucursal usando el monto final y enriquece con store names."""

    def __init__(self) -> None:
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.rabbitmq_port = int(os.getenv('RABBITMQ_PORT', 5672))
        self.shutdown_requested = False
        
        # Configurar manejo de SIGTERM
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        self.input_queue = os.getenv('INPUT_QUEUE', 'transactions_time_filtered_tpv')
        self.stores_queue = os.getenv('STORES_QUEUE', 'stores_raw')
        self.output_queue = os.getenv('OUTPUT_QUEUE', 'transactions_final_results')

        self.prefetch_count = int(os.getenv('PREFETCH_COUNT', 10))

        self.input_middleware = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.input_queue,
            port=self.rabbitmq_port,
            prefetch_count=self.prefetch_count,
        )

        self.stores_middleware = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.stores_queue,
            port=self.rabbitmq_port,
            prefetch_count=self.prefetch_count,
        )

        self.output_middleware = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.output_queue,
            port=self.rabbitmq_port,
        )

        # Lookup table para stores
        self.stores_lookup: Dict[int, str] = {}
        self.stores_loaded = False

        # Estructura: {(store_id, year, semester) -> total}
        self._totals: Dict[Tuple[int, int, str], float] = defaultdict(float)

        logger.info(
            "TPVWorker inicializado - Input: %s, Stores: %s, Output: %s",
            self.input_queue,
            self.stores_queue,
            self.output_queue,
        )

    def _handle_sigterm(self, signum, frame):
        """Maneja la señal SIGTERM para terminar ordenadamente"""
        logger.info("SIGTERM recibido, iniciando shutdown ordenado...")
        self.input_middleware.stop_consuming()
        self.stores_middleware.stop_consuming()
        self.shutdown_requested = True

    def process_stores_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Procesa un lote de stores y construye el lookup table"""
        for store in batch:
            try:
                store_id = int(float(store.get('store_id')))
                store_name = store.get('store_name', '')
                self.stores_lookup[store_id] = store_name
            except (TypeError, ValueError):
                logger.debug("Store invalida omitida: %s", store)

    def _consume_stores(self) -> None:
        """Carga todos los stores para construir el lookup table"""
        logger.info("Cargando metadata de stores")

        def on_message(message: Any) -> None:
            if self.shutdown_requested:
                logger.info("Shutdown solicitado, deteniendo carga de stores")
                self.stores_middleware.stop_consuming()
                return

            if _is_eof(message):
                self.stores_loaded = True
                self.stores_middleware.stop_consuming()
                return

            if isinstance(message, list):
                self.process_stores_batch(message)
            else:
                self.process_stores_batch([message])

        try:
            self.stores_middleware.start_consuming(on_message)
        except KeyboardInterrupt:
            logger.info("Carga de stores interrumpida")
        except Exception as exc:  # noqa: BLE001
            logger.error("Error cargando stores: %s", exc)
        finally:
            logger.info("Stores procesados: %s", len(self.stores_lookup))

    def _update_totals(self, transaction: Dict[str, Any]) -> None:
        try:
            created_at = transaction.get('created_at')
            store_id_raw = transaction.get('store_id')
            final_amount_raw = transaction.get('final_amount')

            if not created_at or store_id_raw is None or final_amount_raw is None:
                return

            dt = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')
            
            store_id = int(store_id_raw)
            final_amount = float(final_amount_raw)
            semester = _semester_from_month(dt.month)

            # Obtener store_name del lookup
            store_name = self.stores_lookup.get(store_id, '')

            key = (store_id, store_name, dt.year, semester)
            self._totals[key] += final_amount
        except (ValueError, TypeError) as exc:
            logger.warning('Transacción omitida por datos inválidos: %s', exc)
        except Exception as exc:  # noqa: BLE001
            logger.error('Error inesperado actualizando TPV: %s', exc)

    def _process_transaction(self, transaction: Dict[str, Any]) -> None:
        self._update_totals(transaction)

    def _process_batch(self, batch: Any) -> None:
        try:
            for transaction in batch:
                self._update_totals(transaction)
        except Exception as exc:  # noqa: BLE001
            logger.error('Error procesando lote en TPVWorker: %s', exc)

    def _emit_summary(self) -> None:
        if not self._totals:
            logger.info('TPVWorker sin datos para emitir resumen')
            payload = {'type': 'tpv_summary', 'results': []}
            self._send_payload(payload)
            return

        results = []
        for (store_id, store_name, year, semester), total in sorted(
            self._totals.items(),
            key=lambda item: (item[0][1], item[0][2], item[0][3], item[0][0]),
        ):
            results.append(
                {
                    'store_name': store_name,
                    'store_id': store_id,
                    'year': year,
                    'semester': semester,
                    'tpv': total,
                }
            )

        payload = {
            'type': 'tpv_summary',
            'results': results,
        }

        self._send_payload(payload)
        self._totals.clear()

    def _send_payload(self, payload: Dict[str, Any]) -> None:
        try:
            self.output_middleware.send(payload)
        except Exception as exc:  # noqa: BLE001
            logger.error('Error enviando mensaje desde TPVWorker: %s', exc)

    def start_consuming(self) -> None:
        try:
            # Primero cargar stores
            self._consume_stores()
            
            # Luego procesar transacciones
            logger.info('TPVWorker consumiendo transacciones filtradas por tiempo')

            def on_message(message: Any) -> None:
                try:
                    if self.shutdown_requested:
                        logger.info("Shutdown requested, stopping message processing")
                        return
                        
                    if _is_eof(message):
                        self._emit_summary()
                        self._send_payload({'type': 'EOF', 'source': 'tpv'})
                        self.input_middleware.stop_consuming()
                        return

                    if isinstance(message, list):
                        self._process_batch(message)
                    else:
                        self._process_transaction(message)

                except Exception as exc:  # noqa: BLE001
                    logger.error('Error en callback TPVWorker: %s', exc)

            self.input_middleware.start_consuming(on_message)
        except KeyboardInterrupt:
            logger.info('TPVWorker interrumpido por el usuario')
        except Exception as exc:  # noqa: BLE001
            logger.error('Error iniciando TPVWorker: %s', exc)
        finally:
            self.cleanup()

    def cleanup(self) -> None:
        try:
            self.input_middleware.close()
        finally:
            try:
                self.stores_middleware.close()
            finally:
                self.output_middleware.close()


def main() -> None:
    try:
        worker = TPVWorker()
        worker.start_consuming()
    except Exception as exc:  # noqa: BLE001
        logger.error('Error fatal en TPVWorker: %s', exc)
        sys.exit(1)


if __name__ == '__main__':
    main()
