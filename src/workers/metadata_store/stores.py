import logging
import os
import sys
from typing import Dict
from workers.utils.message_utils import ClientId
from common.middleware.middleware_config import MiddlewareConfig
from workers.metadata_store.metadata_store import MetadataStore
from common.persistence.metadata.metadata_persistence_store import MetadataPersistenceStore

logger = logging.getLogger(__name__)

StoreId = str
StoreName = str
    
class StoresMetadataStore(MetadataStore):
    def __init__(self, middleware_config: MiddlewareConfig, eof_state_store=None):
        """Initialize a stores metadata store for the worker.
        
        Args:
            middleware_config: Middleware configuration
            eof_state_store: Optional MetadataEOFStateStore for tracking EOFs
        """ 
        stores_exchange = os.getenv('STORES_EXCHANGE', 'stores_raw').strip()
        
        stores_queue = os.getenv('STORES_QUEUE', '').strip()
        if not stores_queue:
            script_name = sys.argv[0] if sys.argv else ''
            if 'tpv' in script_name.lower():
                worker_type = 'tpv'
            elif 'top_clients' in script_name.lower():
                worker_type = 'top_clients'
            elif 'top_items' in script_name.lower():
                worker_type = 'top_items'
            else:
                worker_type = 'aggregator'
            stores_queue = f'{stores_exchange}_{worker_type}'
        
        logger.info(f"[STORES-METADATA] Using queue '{stores_queue}' for exchange '{stores_exchange}' (fanout)")
        
        middleware = middleware_config.create_exchange(
            stores_exchange,
            queue_name=stores_queue,
            exchange_type='fanout',
            route_keys=[]  # Fanout ignores routing keys
        )
        super().__init__(stores_exchange, middleware, eof_state_store=eof_state_store, metadata_type='stores')
        
        self.data: dict[ClientId, dict[StoreId, StoreName]] = {}
        
        self._persistence = MetadataPersistenceStore(
            store_type='stores',
            id_key='store_id',
            value_key='store_name'
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
        store_id = str(data.get('store_id', '')).strip()
        store_name = str(data.get('store_name', '')).strip()
        
        if not store_id or not store_name:
            return
        
        self.data.setdefault(self.current_client_id, {})[store_id] = store_name
        
        self._persistence.save_item(self.current_client_id, store_id, store_name)
    
    def save_batch(self, data: list):
        """Save a batch of messages to disk or process them as needed."""
        items: Dict[str, str] = {}
        
        for item in data:
            store_id = str(item.get('store_id', '')).strip()
            store_name = str(item.get('store_name', '')).strip()
            if store_id and store_name:
                items[store_id] = store_name
        
        if not items:
            return
        
        self.data.setdefault(self.current_client_id, {}).update(items)
        
        self._persistence.save_batch(self.current_client_id, items)

    def _get_item(self, client_id: ClientId, item_id: str) -> StoreName:
        """Retrieve item from cache or persistence."""
        stores = self.data.get(client_id, {})
        if item_id in stores:
            return stores[item_id]
        
        store_name = self._persistence.get_item(client_id, item_id)
        
        if store_name != 'Unknown Store':
            if client_id not in self.data:
                persisted_data = self._persistence.get_all_items(client_id)
                if persisted_data:
                    self.data[client_id] = persisted_data
                    return self.data[client_id].get(item_id, 'Unknown Store')
            else:
                self.data[client_id][item_id] = store_name
        
        return store_name
