#!/usr/bin/env python3

import os
import logging
from typing import Any
from worker_utils import FilterWorker, WorkerConfig, create_worker_main, safe_float_conversion

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AmountFilterWorker(FilterWorker):
    """
    Worker that filters transactions by amount (>= 75).
    Receives time-filtered transactions and filters them by amount.
    """
    
    def _initialize_worker(self):
        """Initialize worker-specific configuration."""
        # Minimum amount required
        self.min_amount = safe_float_conversion(os.getenv('MIN_AMOUNT', '75.0'))
        
        logger.info(f"AmountFilterWorker configured with min_amount: {self.min_amount}")
    
    def apply_filter(self, item: Any) -> bool:
        """
        Filter a transaction by amount (>= 75).
        
        Args:
            item: Dictionary with transaction data
            
        Returns:
            bool: True if transaction meets the amount filter
        """
        try:
            # Get the final amount from the transaction
            # According to schema, we have original_amount, discount_applied and final_amount
            # We'll use final_amount as the total amount
            final_amount = item.get('final_amount')
            
            if final_amount is None:
                # If no final_amount, try to calculate it
                original_amount = safe_float_conversion(item.get('original_amount', 0))
                discount_applied = safe_float_conversion(item.get('discount_applied', 0))
                final_amount = original_amount - discount_applied
            else:
                final_amount = safe_float_conversion(final_amount)
            
            # Check if it meets the minimum amount
            return final_amount >= self.min_amount
            
        except Exception as e:
            logger.debug(f"Error parsing transaction amount: {e}")
            return False


def main():
    """Main entry point."""
    config = WorkerConfig(
        input_queue='transactions_time_filtered',
        output_queue='transactions_final_results'
    )
    
    worker = AmountFilterWorker(config)
    worker.start_consuming()


if __name__ == "__main__":
    # Use the helper to create a robust main function
    main_func = create_worker_main(
        AmountFilterWorker,
        WorkerConfig(
            input_queue='transactions_time_filtered',
            output_queue='transactions_final_results'
        )
    )
    main_func()