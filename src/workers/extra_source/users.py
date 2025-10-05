import os
from typing import Dict

from message_utils import ClientId
from middleware_config import MiddlewareConfig
from workers.extra_source.extra_source import ExtraSource

UserId = str
Birthday = str

id_column = "user_id"
birthday_column = "birthdate"

class UsersExtraSource(ExtraSource):
    """Stores {user_id: birthdate} per client, backed by JSON on disk."""

    def __init__(self, middleware_config: MiddlewareConfig):
        clients_queue = os.getenv("CLIENTS_QUEUE", "users_raw").strip()
        middleware = middleware_config.create_queue(clients_queue)
        super().__init__(clients_queue, middleware)

        self.data: dict[ClientId, Dict[UserId, Birthday]] = {}

    def save_message(self, data: dict | list):
        if isinstance(data, list):
            for item in data:
                self.save_message(item)

        if not isinstance(data, dict):
            return
        
        user_id = UserId(data.get(id_column, ""))
        birthday = Birthday(data.get(birthday_column, ""))
        self.data.setdefault(self.current_client_id, {})[user_id] = birthday

    def _get_item(self, client_id: ClientId, item_id: str) -> Birthday:
        users = self.data.get(client_id, {})
        return users.get(item_id, "Unknown Birthday")