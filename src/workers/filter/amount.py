import os
import logging
from filter_worker import FilterWorker
from typing import Any
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

    def __init__(self):
        """Initialize worker-specific configuration."""
        super().__init__()
        self.min_amount = safe_float_conversion(os.getenv('MIN_AMOUNT', '75.0'))
        logger.info(f"AmountFilterWorker configured with min_amount: {self.min_amount}")
    
    def apply_filter(self, item: Any) -> bool:
        try:
            final_amount = safe_float_conversion(item.get('final_amount'))
            return final_amount >= self.min_amount
        except Exception as e:
            logger.error(f"Error parsing transaction amount: {e}")
            return False

if __name__ == "__main__":
    run_main(AmountFilterWorker)
