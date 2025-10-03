#!/usr/bin/env python3

import os
import logging
from typing import Any
from datetime import datetime
from filter_worker import FilterWorker
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
    def _initialize_worker(self):
        """Initialize worker-specific configuration."""
        min_year = int(os.getenv('MIN_YEAR', '2024'))
        max_year = int(os.getenv('MAX_YEAR', '2025'))

        self.min_year = min_year
        self.max_year = max_year

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
            logger.warning(f"No se pudo parsear created_at en item {item}: {e}")
            return False

if __name__ == "__main__":
    config = WorkerConfig(
        input_queue='transactions_year_filtered',
        output_queue='transactions_time_filtered'
    )
    run_main(YearFilterWorker, config)
