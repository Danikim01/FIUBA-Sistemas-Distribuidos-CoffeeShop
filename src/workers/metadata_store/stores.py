import logging
import os
import sys
from typing import Dict
from message_utils import ClientId
from middleware_config import MiddlewareConfig
from workers.metadata_store.metadata_store import MetadataStore
from workers.utils.metadata_persistence_store import MetadataPersistenceStore

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
        
        # Each aggregator needs its own queue to receive ALL messages (fanout behavior)
        # Determine queue name: use STORES_QUEUE env var if set, otherwise infer from script name
        stores_queue = os.getenv('STORES_QUEUE', '').strip()
        if not stores_queue:
            # Infer worker type from script name for unique queue per aggregator
            script_name = sys.argv[0] if sys.argv else ''
            if 'tpv' in script_name.lower():
                worker_type = 'tpv'
            elif 'top_clients' in script_name.lower():
                worker_type = 'top_clients'
            elif 'top_items' in script_name.lower():
                worker_type = 'top_items'
            else:
                # Fallback: use a generic name
                worker_type = 'aggregator'
            stores_queue = f'{stores_exchange}_{worker_type}'
        
        logger.info(f"[STORES-METADATA] Using queue '{stores_queue}' for exchange '{stores_exchange}' (fanout)")
        
        # Use fanout exchange so all aggregators receive all messages
        middleware = middleware_config.create_exchange(
            stores_exchange,
            queue_name=stores_queue,
            exchange_type='fanout',
            route_keys=[]  # Fanout ignores routing keys
        )
        super().__init__(stores_exchange, middleware, eof_state_store=eof_state_store, metadata_type='stores')
        
        # Cache en memoria para acceso rápido
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
        
        # Actualizar cache en memoria
        self.data.setdefault(self.current_client_id, {})[store_id] = store_name
        
        # Persistir en disco (append-only)
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
        
        # Actualizar cache
        self.data.setdefault(self.current_client_id, {}).update(items)
        
        # Persistir batch en disco
        self._persistence.save_batch(self.current_client_id, items)

    def _get_item(self, client_id: ClientId, item_id: str) -> StoreName:
        """Retrieve item from cache or persistence."""
        # Primero buscar en cache en memoria
        stores = self.data.get(client_id, {})
        if item_id in stores:
            return stores[item_id]
        
        # Si no está en cache, buscar en persistencia
        store_name = self._persistence.get_item(client_id, item_id)
        
        # Si encontramos el item en persistencia, asegurarnos de que esté en cache
        if store_name != 'Unknown Store':
            # Si el cliente no está en cache, cargar todos los datos desde persistencia
            if client_id not in self.data:
                persisted_data = self._persistence.get_all_items(client_id)
                if persisted_data:
                    self.data[client_id] = persisted_data
                    # Retornar el valor del cache ahora
                    return self.data[client_id].get(item_id, 'Unknown Store')
            else:
                # El cliente está en cache pero sin este item, actualizar cache con este item
                self.data[client_id][item_id] = store_name
        
        return store_name
