import logging
import os
from middleware_config import MiddlewareConfig
from workers.aggregator.extra_source.extra_source import ExtraSource

logger = logging.getLogger(__name__)

UserId = str
Birthday = str
    
class ClientsExtraSource(ExtraSource):
    def __init__(self, middleware_config: MiddlewareConfig):
        """Initialize an extra source for the worker.
        
        Args:
            name: Name of the extra source
            queue: Queue name for the extra source (optional)
        """
        clients_queue = os.getenv('CLIENTS_QUEUE', 'clients_raw').strip()
        middleware = middleware_config.create_queue(clients_queue)
        super().__init__(clients_queue, middleware)
    
    def save_message(self, message: dict):
        """Save the message to disk or process it as needed."""
        # Implement saving logic if required
        pass

    def get_item(self, index: int = 0):
        """Retrieve item from the extra source.
        Returns a dict or None if out of range.
        """
        # Implement retrieval logic if required
        return None