import logging
import os
from typing import Any, Dict
from message_utils import ClientId
from middleware_config import MiddlewareConfig
from workers.metadata_store.metadata_store import MetadataStore
from workers.utils.metadata_persistence_store import MetadataPersistenceStore

logger = logging.getLogger(__name__)

id_column = 'item_id'
name_column = 'item_name'

ItemId = str
ItemName = str
    
class MenuItemsMetadataStore(MetadataStore):
    """Stores {item_id: item_name} per client, backed by JSON Lines persistence on disk."""
    
    def __init__(self, middleware_config: MiddlewareConfig):
        """Initialize a menu items metadata store for the worker.
        
        Args:
        """ 
        menu_items_queue = os.getenv('MENU_ITEMS_QUEUE', 'menu_items_raw').strip()
        middleware = middleware_config.create_queue(menu_items_queue)
        super().__init__(menu_items_queue, middleware)
        
        # Cache en memoria para acceso rápido
        self.data: dict[ClientId, dict[ItemId, ItemName]] = {}
        
        self._persistence = MetadataPersistenceStore(
            store_type='menu_items',
            id_key='item_id',
            value_key='item_name'
        )

    def reset_state(self, client_id: ClientId):
        """Reset the internal state of the metadata store."""
        self.data.pop(client_id, None)
        self._persistence.clear_client(client_id)

    def save_data(self, data: dict):
        """Save the message to disk or process it as needed."""
        item_id = str(data.get(id_column, '')).strip()
        item_name = data.get(name_column, '').strip()
        
        if not item_id or not item_name:
            return
        
        # Actualizar cache en memoria
        self.data.setdefault(self.current_client_id, {})[item_id] = item_name
        
        # Persistir en disco (append-only)
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
        
        # Actualizar cache
        self.data.setdefault(self.current_client_id, {}).update(items)
        
        # Persistir batch en disco
        self._persistence.save_batch(self.current_client_id, items)

    def _get_item(self, client_id: ClientId, item_id: ItemId) -> ItemName:
        """Retrieve item from cache or persistence."""
        # Primero buscar en cache en memoria
        menu_items = self.data.get(client_id, {})
        if item_id in menu_items:
            return menu_items[item_id]
        
        # Si no está en cache, buscar en persistencia
        item_name = self._persistence.get_item(client_id, item_id)
        
        # Si encontramos el item en persistencia, asegurarnos de que esté en cache
        if item_name != 'Unknown Item':
            # Si el cliente no está en cache, cargar todos los datos desde persistencia
            if client_id not in self.data:
                persisted_data = self._persistence.get_all_items(client_id)
                if persisted_data:
                    self.data[client_id] = persisted_data
                    # Retornar el valor del cache ahora
                    return self.data[client_id].get(item_id, 'Unknown Item')
            else:
                # El cliente está en cache pero sin este item, actualizar cache con este item
                self.data[client_id][item_id] = item_name
        
        return item_name
