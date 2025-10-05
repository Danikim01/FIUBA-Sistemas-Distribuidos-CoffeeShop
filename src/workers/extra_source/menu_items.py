import logging
import os
from typing import Any
from message_utils import ClientId
from middleware_config import MiddlewareConfig
from workers.extra_source.extra_source import ExtraSource

logger = logging.getLogger(__name__)

id_column = 'item_id'
name_column = 'item_name'

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
        self.data: dict[ClientId, dict[ItemId, ItemName]] = {}
    
    def save_message(self, data: Any):
        """Save the message to disk or process it as needed."""
        if isinstance(data, list):
            for item in data:
                self.save_message(item)

        if isinstance(data, dict):
            stores = self.data.setdefault(self.current_client_id, {})
            id = str(data.get(id_column, '')).strip()
            name = data.get(name_column, '').strip()
            stores[id] = name


    def _get_item(self, client_id: ClientId, item_id: ItemId) -> ItemName:
        """Retrieve item from the extra source.
        Returns a dict or None if out of range.
        """
        menu_items = self.data.get(client_id, {})
        return menu_items.get(item_id, 'Unknown Item')
