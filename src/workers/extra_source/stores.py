import logging
import os
from message_utils import ClientId
from middleware_config import MiddlewareConfig
from workers.extra_source.extra_source import ExtraSource

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
    
    def save_message(self, data: dict):
        """Save the message to disk or process it as needed."""

        if isinstance(data, list):
            for item in data:
                self.save_message(item)

        if isinstance(data, dict):
            stores = self.data.setdefault(self.current_client_id, {})
            store_id = str(data.get('store_id', '')).strip()
            store_name = str(data.get('store_name', '')).strip()
            stores[store_id] = store_name

    def _get_item(self, client_id: ClientId, item_id: str) -> StoreName:
        """Retrieve item from the extra source.
        Returns a dict or None if out of range.
        """
        stores = self.data.get(client_id, {})
        return stores.get(item_id, 'Unknown Store')
