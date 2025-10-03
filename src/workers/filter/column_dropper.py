#!/usr/bin/env python3

import os
import sys
import logging
from typing import Any, Dict, List, Optional, Set

from workers.worker_utils import BaseWorker, WorkerConfig
from workers.filter.column_dropper_config import QUEUE_TRANSFORMATION_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ColumnDropper(BaseWorker):
    """
    Data transformation layer that converts RAW queue data into typed, clean data.
    
    Takes raw string data from various input queues and:
    1. Selects only required columns based on queue configuration
    2. Converts string data to proper types (int, float, etc.)
    3. Applies validation filters
    4. Sends clean, typed data to output queues
    """

    def __init__(self) -> None:
        # Get configuration from environment
        input_queue = os.getenv('INPUT_QUEUE', 'transactions_raw_compact')
        output_queue = os.getenv('OUTPUT_QUEUE', 'transactions_compact')
        
        # Initialize worker config
        config = WorkerConfig(
            input_queue=input_queue,
            output_queue=output_queue
        )
        
        # Initialize base worker
        super().__init__(config)
        
        # Get queue-specific configuration from external config
        self.queue_config = QUEUE_TRANSFORMATION_CONFIG.get(input_queue, {})
        if not self.queue_config:
            logger.error(f"No configuration found for queue {input_queue}")
            logger.info(f"Available queues: {list(QUEUE_TRANSFORMATION_CONFIG.keys())}")
            raise ValueError(f"Unsupported input queue: {input_queue}")
        
        self.output_columns: Set[str] = self.queue_config.get('output_columns', set())
        self.type_converters: Dict[str, Any] = self.queue_config.get('type_converters', {})
        self.filters: Dict[str, Any] = self.queue_config.get('filters', {})
        
        logger.info(
            "ColumnDropper initialized - Input: %s, Output: %s, Columns: %s",
            input_queue,
            output_queue,
            list(self.output_columns)
        )

    def _transform_item(self, raw_item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Transform a raw item into clean, typed data.
        
        Args:
            raw_item: Raw data dictionary with string values
            
        Returns:
            Transformed item with proper types, or None if invalid
        """
        try:
            # Extract only required columns
            transformed_item = {}
            
            for column in self.output_columns:
                raw_value = raw_item.get(column)
                if raw_value is None:
                    logger.debug(f"Missing required column '{column}' in item: {raw_item}")
                    return None
                
                # Apply type conversion
                converter = self.type_converters.get(column, str)
                try:
                    converted_value = converter(raw_value)
                    transformed_item[column] = converted_value
                except (ValueError, TypeError) as e:
                    logger.debug(f"Failed to convert '{column}' value '{raw_value}': {e}")
                    return None
            
            # Apply validation filters
            for column, filter_func in self.filters.items():
                if column in transformed_item:
                    value = transformed_item[column]
                    if not filter_func(value):
                        logger.debug(f"Item failed filter for column '{column}' with value '{value}'")
                        return None
            
            return transformed_item
            
        except Exception as exc:
            logger.error(f"Unexpected error transforming item: {exc}")
            return None

    def process_message(self, message: Any) -> None:
        """Process a single message (required by BaseWorker)."""
        if isinstance(message, dict):
            transformed = self._transform_item(message)
            if transformed is not None:
                self.send_message(transformed)
        else:
            logger.warning(f"Received non-dict message: {type(message)}")

    def process_batch(self, batch: List[Any]) -> None:
        """Process a batch of messages (required by BaseWorker)."""
        transformed_batch = []
        
        for item in batch:
            if isinstance(item, dict):
                transformed = self._transform_item(item)
                if transformed is not None:
                    transformed_batch.append(transformed)
            else:
                logger.warning(f"Skipping non-dict item in batch: {type(item)}")
        
        if transformed_batch:
            self.send_message(transformed_batch)


def main() -> None:
    try:
        worker = ColumnDropper()
        worker.start_consuming()
    except Exception as exc:
        logger.error(f"Error fatal en ColumnDropper: {exc}")
        sys.exit(1)


if __name__ == '__main__':
    main()
