#!/usr/bin/env python3

import os
import logging
from typing import Any, Union
from datetime import time
from filter_worker import FilterWorker
from worker_utils import extract_time_safe, run_main
from common.models import (
    Transaction,
    TransactionItem,
)
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TimeFilterWorker(FilterWorker):
    """
    Worker that filters transactions by time (06:00 AM - 11:00 PM).
    Receives year-filtered transactions and filters them by time.
    """
    
    def __init__(self):
        """Initialize worker-specific configuration."""
        super().__init__()
        start_hour = int(os.getenv('START_HOUR', '6'))
        end_hour = int(os.getenv('END_HOUR', '23'))
        self.start_time = time(start_hour, 0)   # 06:00 AM
        self.end_time = time(end_hour, 0)       # 11:00 PM (23:00)
        logger.info(f"TimeFilterWorker configured with time range: {self.start_time} - {self.end_time}")
    
    def apply_filter(self, item: Union[Transaction, TransactionItem]) -> bool:
        try:
            created_at = item.get('created_at')
            if not created_at:
                return False
            transaction_time = extract_time_safe(created_at)
            if not transaction_time:
                return False
        
            return self.start_time <= transaction_time <= self.end_time
        except Exception as e:
            logger.error(f"Error parsing transaction time: {e}")
            return False

if __name__ == "__main__":
    run_main(TimeFilterWorker)