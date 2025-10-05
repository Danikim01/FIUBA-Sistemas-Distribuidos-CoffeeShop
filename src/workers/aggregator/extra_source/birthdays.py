import logging
import os
from typing import Dict
from message_utils import ClientId
from middleware_config import MiddlewareConfig
from workers.aggregator.extra_source.extra_source import ExtraSource

logger = logging.getLogger(__name__)

UserId = str
Birthday = str

id_column = 'user_id'
birthday_column = 'birthdate'
    
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
        self.data: dict[ClientId, Dict[UserId, Birthday]] = {}
    
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
                user_id = str(item.get(id_column, '')).strip()
                birthday = str(item.get(birthday_column, '')).strip()
                if user_id and birthday:
                    self.data[client_id][user_id] = birthday

        if isinstance(data, dict):
            user_id = str(data.get(id_column, '')).strip()
            birthday = str(data.get(birthday_column, '')).strip()
            if user_id and birthday:
                self.data[client_id][user_id] = birthday

    def _get_item(self, client_id: ClientId, item_id: str) -> Birthday:
        """Retrieve item from the extra source.
        Returns a dict or None if out of range.
        """
        clients = self.data.get(client_id, {})
        return clients.get(item_id, 'Unknown Birthday')
