#!/usrfrom utils.worker_config import WorkerConfig
from utils.worker_utils import run_main, safe_float_conversion
from filter_worker import FilterWorker

import os
import logging
from typing import Any

from worker_config import WorkerConfig
from worker_utils import run_main, safe_float_conversion
from workers.filter.filter_worker import FilterWorker

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
            final_amount = item.get('final_amount')
            final_amount = safe_float_conversion(final_amount)
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
    config = WorkerConfig(
            input_queue='transactions_time_filtered',
            output_queue='transactions_final_results'
        )
    run_main(AmountFilterWorker, config)
