#!/usr/bin/env python3

import os
import sys
import logging
from collections import defaultdict
from datetime import datetime, time
from typing import Any, Dict, List, Tuple
import signal
import threading

from middleware.rabbitmq_middleware import RabbitMQMiddlewareQueue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _is_eof(message: Any) -> bool:
    return isinstance(message, dict) and str(message.get('type', '')).upper() == 'EOF'


class ItemsTopWorker:
    """Calcula el top de ítems por mes según cantidad y beneficio."""

    def __init__(self) -> None:
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.rabbitmq_port = int(os.getenv('RABBITMQ_PORT', 5672))
        self.shutdown_requested = False
        
        # Configurar manejo de SIGTERM
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        
        self.items_input_queue = os.getenv('ITEMS_INPUT_QUEUE', 'transaction_items_raw')
        self.menu_items_input_queue = os.getenv('MENU_ITEMS_INPUT_QUEUE', 'menu_items_raw')
        self.output_queue = os.getenv('OUTPUT_QUEUE', 'transactions_final_results')

        self.prefetch_count = int(os.getenv('PREFETCH_COUNT', 10))

        self.items_middleware = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.items_input_queue,
            port=self.rabbitmq_port,
            prefetch_count=self.prefetch_count,
        )

        self.menu_items_middleware = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.menu_items_input_queue,
            port=self.rabbitmq_port,
            prefetch_count=self.prefetch_count,
        )

        self.output_middleware = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.output_queue,
            port=self.rabbitmq_port,
        )

        self.allowed_years = {2024, 2025}
        self.start_time = time(6, 0)
        self.end_time = time(23, 0)

        self.menu_items_lookup: Dict[int, str] = {}
        self.quantity_totals: defaultdict[Tuple[str, int], int] = defaultdict(int)
        self.profit_totals: defaultdict[Tuple[str, int], float] = defaultdict(float)

        self.menu_items_eof_received = False
        self.items_eof_received = False
        self.results_emitted = False


        logger.info(
            "ItemsTopWorker inicializado - Items: %s, MenuItems: %s, Output: %s",
            self.items_input_queue,
            self.menu_items_input_queue,
            self.output_queue,
        )

    def _handle_sigterm(self, signum, frame):
        """Maneja la señal SIGTERM para terminar ordenadamente"""
        logger.info("SIGTERM recibido, iniciando shutdown ordenado...")
        self.menu_items_middleware.stop_consuming()
        self.items_middleware.stop_consuming()
        self.shutdown_requested = True


    def _parse_datetime(self, value: str) -> datetime | None:
        try:
            return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        except (TypeError, ValueError):
            return None

    def _should_include(self, created_at: str) -> Tuple[str, datetime] | None:
        dt = self._parse_datetime(created_at)
        if not dt:
            return None

        if dt.year not in self.allowed_years:
            return None

        if not (self.start_time <= dt.time() <= self.end_time):
            return None

        return dt.strftime('%Y-%m'), dt

    def _update_metrics(self, item: Dict[str, Any]) -> None:
        created_at = item.get('created_at')
        result = self._should_include(created_at)
        if not result:
            return

        year_month, _ = result

        try:
            item_id = int(float(item.get('item_id')))
        except (TypeError, ValueError):
            logger.debug("Item sin item_id válido: %s", item)
            return

        try:
            subtotal = float(item.get('subtotal', 0.0))
        except (TypeError, ValueError):
            subtotal = 0.0

        key = (year_month, item_id)
        self.quantity_totals[key] += 1
        self.profit_totals[key] += subtotal

    def process_menu_items_batch(self, batch: List[Dict[str, Any]]) -> None:
        for entry in batch:
            try:
                item_id = int(entry.get('item_id'))
                item_name = entry.get('item_name', '') or ''
                self.menu_items_lookup[item_id] = item_name
            except (TypeError, ValueError):
                logger.debug("Menu item inválido: %s", entry)

    def process_items_batch(self, batch: List[Dict[str, Any]]) -> None:
        for item in batch:
            self._update_metrics(item)

    def _build_top_results(
        self,
        primary_map: Dict[Tuple[str, int], float | int],
        metric_key: str,
        secondary_map: Dict[Tuple[str, int], float | int],
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        months = sorted({month for month, _ in primary_map.keys()})

        for month in months:
            entries: List[Tuple[int, float | int, float | int]] = []
            for (year_month, item_id), value in primary_map.items():
                if year_month != month:
                    continue
                secondary_value = secondary_map.get((year_month, item_id), 0)
                entries.append((item_id, value, secondary_value))

            if not entries:
                continue

            entries.sort(key=lambda data: (data[1], data[2], -data[0]), reverse=True)
            item_id, primary_value, _ = entries[0]
            item_name = self.menu_items_lookup.get(item_id, f"Item {item_id}")

            results.append(
                {
                    'year_month_created_at': month,
                    'item_id': item_id,
                    'item_name': item_name,
                    metric_key: primary_value,
                }
            )

        return results

    def _emit_results(self) -> None:
        if self.results_emitted:
            return

        quantity_results = self._build_top_results(
            self.quantity_totals,
            'sellings_qty',
            self.profit_totals,
        )
        profit_results = self._build_top_results(
            self.profit_totals,
            'profit_sum',
            self.quantity_totals,
        )

        payload_quantity = {
            'type': 'top_items_by_quantity',
            'results': quantity_results,
        }
        payload_profit = {
            'type': 'top_items_by_profit',
            'results': profit_results,
        }

        try:
            self.output_middleware.send(payload_quantity)
            self.output_middleware.send(payload_profit)
        finally:
            self.results_emitted = True

    def _finalize_if_ready(self) -> None:
        if self.menu_items_eof_received and self.items_eof_received:
            self._emit_results()
            try:
                self.output_middleware.send({'type': 'EOF', 'source': 'items_top'})
            finally:
                self.items_middleware.stop_consuming()

    def start_consuming(self) -> None:
        logger.info("ItemsTopWorker iniciando consumo")

        def on_menu_items(message: Any) -> None:
            if self.shutdown_requested:
                logger.info("Shutdown requested, stopping menu items processing")
                return
                
            if _is_eof(message):
                self.menu_items_eof_received = True
                self.menu_items_middleware.stop_consuming()
                return

            if isinstance(message, list):
                self.process_menu_items_batch(message)
            else:
                self.process_menu_items_batch([message])

        def on_items(message: Any) -> None:
            if self.shutdown_requested:
                logger.info("Shutdown requested, stopping items processing")
                return
                
            if _is_eof(message):
                self.items_eof_received = True
                self._finalize_if_ready()
                return

            if isinstance(message, list):
                self.process_items_batch(message)
            else:
                self.process_items_batch([message])

        try:
            self.menu_items_middleware.start_consuming(on_menu_items)
            self.items_middleware.start_consuming(on_items)
        except KeyboardInterrupt:
            logger.info("ItemsTopWorker interrumpido")
        except Exception as exc:
            logger.error("Error en ItemsTopWorker: %s", exc)
        finally:
            self.cleanup()

    def cleanup(self) -> None:
        try:
            if hasattr(self, 'menu_items_middleware') and self.menu_items_middleware:
                self.menu_items_middleware.close()
        except Exception as exc:
            logger.debug("Error cerrando menu_items middleware: %s", exc)
        finally:
            try:
                if hasattr(self, 'items_middleware') and self.items_middleware:
                    self.items_middleware.close()
            finally:
                if hasattr(self, 'output_middleware') and self.output_middleware:
                    self.output_middleware.close()


def main() -> None:
    try:
        worker = ItemsTopWorker()
        worker.start_consuming()
    except Exception as exc:
        logger.error("Error fatal en ItemsTopWorker: %s", exc)
        sys.exit(1)


if __name__ == '__main__':
    main()
