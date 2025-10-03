#!/usr/bin/env python3

import os
import logging
from typing import Any
from datetime import time
from worker_utils import FilterWorker, WorkerConfig, create_worker_main, extract_time_safe

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TimeFilterWorker(FilterWorker):
    """
    Worker that filters transactions by time (06:00 AM - 11:00 PM).
    Receives year-filtered transactions and filters them by time.
    """
    
    def _initialize_worker(self):
        """Initialize worker-specific configuration."""
        # Define time range (06:00 AM - 11:00 PM)
        start_hour = int(os.getenv('START_HOUR', '6'))
        end_hour = int(os.getenv('END_HOUR', '23'))
        
        self.start_time = time(start_hour, 0)   # 06:00 AM
        self.end_time = time(end_hour, 0)       # 11:00 PM (23:00)
        
        logger.info(f"TimeFilterWorker configured with time range: {self.start_time} - {self.end_time}")
    
    def apply_filter(self, item: Any) -> bool:
        """
        Filter a transaction by time (06:00 AM - 11:00 PM).
        
        Args:
            item: Dictionary with transaction data
            
        Returns:
            bool: True if transaction meets the time filter
        """
        try:
            # Extract the date and time from created_at
            created_at = item.get('created_at', '')
            if not created_at:
                return False
            
            # Parse the time using utility function
            transaction_time = extract_time_safe(created_at)
            if not transaction_time:
                return False
            
            # Check if it's in the time range
            # Consider that 11:00 PM is 23:00, so range is 06:00-23:00
            return self.start_time <= transaction_time <= self.end_time
            
        except Exception as e:
            logger.debug(f"Error parsing transaction time: {e}")
            return False


def main():
    """Main entry point."""
    config = WorkerConfig(
        input_queue_default='transactions_enriched',
        output_queue_default='transactions_time_filtered',
        prefetch_count_default=10
    )
    
    worker = TimeFilterWorker(config)
    worker.start_consuming()


if __name__ == "__main__":
    # Use the helper to create a robust main function
    main_func = create_worker_main(
        TimeFilterWorker,
        WorkerConfig(
            input_queue_default='transactions_enriched',
            output_queue_default='transactions_time_filtered',
            prefetch_count_default=10
        )
    )
    main_func()