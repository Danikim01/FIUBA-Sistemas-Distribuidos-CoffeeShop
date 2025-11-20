#!/usr/bin/env python3

import os
import logging
from datetime import datetime
from typing import Any, Union

from filter_worker import FilterWorker
from worker_utils import run_main

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from common.models import (
    Transaction,
    TransactionItem,
)
created_at_field = 'created_at'

class YearFilterWorker(FilterWorker):
    """
    Worker que filtra transacciones por año (2024 y 2025).
    Recibe transacciones y las filtra según el año en created_at.
    """

    def __init__(self):
        """Initialize worker-specific configuration."""
        super().__init__()
        self.min_year = int(os.getenv('MIN_YEAR', '2024'))
        self.max_year = int(os.getenv('MAX_YEAR', '2025'))
        logger.info(f"TimeFilterWorker configured with time range: {self.min_year} - {self.max_year}")

    def apply_filter(self, item: Union[Transaction, TransactionItem]) -> bool:
        try:
            # Suponiendo que item es un dict con string ISO8601 en 'created_at'
            created_at = item.get(created_at_field)
            if not created_at:
                return False
            dt = datetime.fromisoformat(created_at)
            return dt.year in (self.min_year, self.max_year)
        except Exception as e:
            logger.error(f"No se pudo parsear created_at en item {item}: {e}")
            return False

if __name__ == "__main__":
    run_main(YearFilterWorker)
