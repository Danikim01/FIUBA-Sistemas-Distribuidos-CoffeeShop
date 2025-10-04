"""Filter worker implementation for data filtering operations."""

from abc import abstractmethod
from typing import Any, List

from workers.base_worker import BaseWorker


class FilterWorker(BaseWorker):
    """Base class for filter workers that apply filtering logic to messages."""

    @abstractmethod
    def apply_filter(self, item: Any) -> bool:
        """Apply filter logic to determine if an item should pass through.
        
        Args:
            item: Data item to filter
            
        Returns:
            True if item should pass through, False otherwise
        """
        pass
    
    def process_message(self, message: Any):
        """Process a single message by applying filter.
        
        Args:
            message: Message to process
        """
        if self.apply_filter(message):
            self.send_message(message)
    
    def process_batch(self, batch: List[Any]):
        """Process a batch by filtering and sending filtered results.
        
        Args:
            batch: List of messages to process
        """
        filtered_items = [item for item in batch if self.apply_filter(item)]
        if filtered_items:
            self.send_message(filtered_items)