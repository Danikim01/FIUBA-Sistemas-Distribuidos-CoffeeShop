import logging
import os
from message_utils import ClientId
from middleware_config import MiddlewareConfig
from workers.aggregator.extra_source.extra_source import ExtraSource

logger = logging.getLogger(__name__)

ItemId = str
ItemName = str
    
class MenuItemsExtraSource(ExtraSource):
    def __init__(self, middleware_config: MiddlewareConfig):
        """Initialize an extra source for the worker.
        
        Args:
        """ 
        menu_items_queue = os.getenv('MENU_ITEMS_QUEUE', 'menu_items_raw').strip()
        middleware = middleware_config.create_queue(menu_items_queue)
        super().__init__(menu_items_queue, middleware)
        self.data: dict[ClientId, list[dict[ItemId, ItemName]]] = {}
    
    def save_message(self, data: dict):
        """Save the message to disk or process it as needed."""
        if isinstance(data, list):
            for item in data:
                self.save_message(item)

        if isinstance(data, dict):
            self.data.setdefault(self.current_client_id, []).append(data)


    def _get_item(self, client_id: ClientId, item_id: str) -> ItemName:
        """Retrieve item from the extra source.
        Returns a dict or None if out of range.
        """
        stores = self.data.get(client_id, [])
        return next((store[item_id] for store in stores if item_id in store), 'Unknown Store')
