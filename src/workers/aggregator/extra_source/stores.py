import logging
import os
from message_utils import ClientId
from middleware_config import MiddlewareConfig
from workers.aggregator.extra_source.extra_source import ExtraSource

logger = logging.getLogger(__name__)

StoreId = str
StoreName = str
    
class StoresExtraSource(ExtraSource):
    def __init__(self, middleware_config: MiddlewareConfig):
        """Initialize an extra source for the worker.
        
        Args:
        """ 
        stores_exchange = os.getenv('STORES_EXCHANGE', 'stores_raw').strip()
        middleware = middleware_config.create_exchange(stores_exchange)
        super().__init__(stores_exchange, middleware)
        self.data: dict[ClientId, dict[StoreId, StoreName]] = {}
    
    def save_message(self, message: dict):
        """Save the message to disk or process it as needed."""
        client_id = message.get('client_id')
        if client_id is None:
            return  

        if client_id not in self.data:
            self.data[client_id] = {}
        
        data = message.get('data', [])

        if isinstance(data, list):
            for item in data:
                store_id = item.get('store_id', item.get('id', ''))
                store_name = item.get('store_name', item.get('name', '')).strip()
                if store_id and store_name:
                    self.data[client_id][store_id] = store_name

        if isinstance(data, dict):
            store_id = data.get('store_id', data.get('id', ''))
            store_name = data.get('store_name', data.get('name', '')).strip()
            if store_id and store_name:
                self.data[client_id][store_id] = store_name

    def _get_item(self, client_id: ClientId, item_id: str) -> StoreName:
        """Retrieve item from the extra source.
        Returns a dict or None if out of range.
        """
        stores = self.data.get(client_id, {})
        return stores.get(item_id, 'Unknown Store')
