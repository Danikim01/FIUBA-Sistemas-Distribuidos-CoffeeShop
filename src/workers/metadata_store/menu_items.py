import logging
import os
from typing import Any, Dict
from workers.utils.message_utils import ClientId
from common.middleware.middleware_config import MiddlewareConfig
from workers.metadata_store.metadata_store import MetadataStore
from common.persistence.metadata.metadata_persistence_store import MetadataPersistenceStore

logger = logging.getLogger(__name__)

id_column = 'item_id'
name_column = 'item_name'

ItemId = str
ItemName = str
    
class MenuItemsMetadataStore(MetadataStore):
    """Stores {item_id: item_name} per client, backed by JSON Lines persistence on disk."""
    
    def __init__(self, middleware_config: MiddlewareConfig, eof_state_store=None):
        """Initialize a menu items metadata store for the worker.
        
        Args:
            middleware_config: Middleware configuration
            eof_state_store: Optional MetadataEOFStateStore for tracking EOFs
        """ 
        menu_items_queue = os.getenv('MENU_ITEMS_QUEUE', 'menu_items_raw').strip()
        middleware = middleware_config.create_queue(menu_items_queue)
        super().__init__(menu_items_queue, middleware, eof_state_store=eof_state_store, metadata_type='menu_items')
        
        self.data: dict[ClientId, dict[ItemId, ItemName]] = {}
        
        self._persistence = MetadataPersistenceStore(
            store_type='menu_items',
            id_key='item_id',
            value_key='item_name'
        )

    def _reset_metadata_state(self, client_id: ClientId):
        """Reset the metadata-specific state (data and persistence)."""
        self.data.pop(client_id, None)
        self._persistence.clear_client(client_id)

    def reset_all(self):
        """Reset the metadata state for all clients."""
        self.data.clear()
        self._persistence.clear_all()

    def save_data(self, data: dict):
        """Save the message to disk or process it as needed."""
        item_id = str(data.get(id_column, '')).strip()
        item_name = data.get(name_column, '').strip()
        
        if not item_id or not item_name:
            return
        
        self.data.setdefault(self.current_client_id, {})[item_id] = item_name
        
        self._persistence.save_item(self.current_client_id, item_id, item_name)

    def save_batch(self, data: list):
        """Save a batch of messages to disk or process them as needed."""
        items: Dict[str, str] = {}
        
        for item in data:
            item_id = str(item.get(id_column, '')).strip()
            item_name = str(item.get(name_column, '')).strip()
            if item_id and item_name:
                items[item_id] = item_name
        
        if not items:
            return
        
        self.data.setdefault(self.current_client_id, {}).update(items)
        
        self._persistence.save_batch(self.current_client_id, items)

    def _get_item(self, client_id: ClientId, item_id: ItemId) -> ItemName:
        """Retrieve item from cache or persistence."""
        menu_items = self.data.get(client_id, {})
        if item_id in menu_items:
            return menu_items[item_id]
        
        item_name = self._persistence.get_item(client_id, item_id)
        
        if item_name != 'Unknown Item':
            if client_id not in self.data:
                persisted_data = self._persistence.get_all_items(client_id)
                if persisted_data:
                    self.data[client_id] = persisted_data
                    return self.data[client_id].get(item_id, 'Unknown Item')
            else:
                self.data[client_id][item_id] = item_name
        
        return item_name
