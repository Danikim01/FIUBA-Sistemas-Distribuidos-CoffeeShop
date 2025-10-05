import logging
import os
from venv import logger
from message_utils import ClientId
from middleware_config import MiddlewareConfig
from workers.aggregator.extra_source.extra_source import ExtraSource

logger = logging.getLogger(__name__)

class _MenuItem:
    def __init__(self, item: dict):
        # Accept both legacy (id/name) and current (item_id/item_name) payloads
        raw_id = item.get('item_id', item.get('id', ''))
        raw_name = item.get('item_name', item.get('name', ''))
        self.id = str(raw_id) if raw_id is not None else ''
        self.name = str(raw_name).strip() if raw_name else ''

ItemName = str
    
class MenuItemsExtraSource(ExtraSource):
    def __init__(self, middleware_config: MiddlewareConfig):
        """Initialize an extra source for the worker.
        
        Args:
        """ 
        menu_items_queue = os.getenv('MENU_ITEMS_QUEUE', 'menu_items_raw').strip()
        middleware = middleware_config.create_exchange(menu_items_queue)
        super().__init__(menu_items_queue, middleware)
        self.data: dict[ClientId, list[_MenuItem]] = {}
    
    def save_message(self, message: dict):
        """Save the message to disk or process it as needed."""
        client_id = message.get('client_id')
        if client_id is None:
            return  

        if client_id not in self.data:
            self.data[client_id] = []
        
        data = message.get('data', [])

        if isinstance(data, list):
            for item in data:
                self.data[client_id].append(_MenuItem(item))

        if isinstance(data, dict):
            self.data[client_id].append(_MenuItem(data))  
        

    def get_item(self, client_id: ClientId, item_id: str) -> ItemName:
        """Retrieve item from the extra source.
        Returns a dict or None if out of range.
        """
        stores = self.data.get(client_id, [])
        return next((store.name for store in stores if store.id == item_id), 'Unknown Store')
