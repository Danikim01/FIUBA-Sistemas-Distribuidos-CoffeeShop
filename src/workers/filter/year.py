#!/usr/bin/env python3

import os
import logging
from datetime import datetime
from typing import Any, List

from filter_worker import FilterWorker
from middleware.rabbitmq_middleware import RabbitMQMiddlewareQueue
from message_utils import create_message_with_metadata
from worker_config import WorkerConfig
from worker_utils import run_main

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class YearFilterWorker(FilterWorker):
    """
    Worker que filtra transacciones por año (2024 y 2025).
    Recibe transacciones y las filtra según el año en created_at.
    """

    def __init__(self):
        """Initialize worker-specific configuration."""
        super().__init__()

        min_year = int(os.getenv('MIN_YEAR', '2024'))
        max_year = int(os.getenv('MAX_YEAR', '2025'))

        self.min_year = min_year
        self.max_year = max_year

        secondary_queue = os.getenv('SECONDARY_OUTPUT_QUEUE', '').strip()
        if secondary_queue:
            params = self.config.get_rabbitmq_connection_params()
            self.secondary_output = RabbitMQMiddlewareQueue(
                host=params['host'],
                port=params['port'],
                queue_name=secondary_queue,
            )
        else:
            self.secondary_output = None

        logger.info(f"TimeFilterWorker configured with time range: {self.min_year} - {self.max_year}")

    def apply_filter(self, item: Any) -> bool:
        try:
            # Suponiendo que item es un dict con string ISO8601 en 'created_at'
            created_at = item.get("created_at")
            if not created_at:
                return False
            
            dt = datetime.fromisoformat(created_at)
            return dt.year in (self.min_year, self.max_year)
        except Exception as e:
            logger.error(f"No se pudo parsear created_at en item {item}: {e}")
            return False

    def _forward_filtered(self, payload: Any) -> None:
        self.send_message(payload)
        if self.secondary_output:
            message = create_message_with_metadata(self.current_client_id, payload)
            self.secondary_output.send(message)

    def process_message(self, message: Any):
        if self.apply_filter(message):
            self._forward_filtered(message)

    def process_batch(self, batch: List[Any]):
        filtered_items = [item for item in batch if self.apply_filter(item)]
        if filtered_items:
            self._forward_filtered(filtered_items)

    def cleanup(self):
        super().cleanup()
        if self.secondary_output:
            try:
                self.secondary_output.close()
            except Exception:  # noqa: BLE001
                pass

if __name__ == "__main__":
    run_main(YearFilterWorker)
