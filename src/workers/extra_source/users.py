import os
from typing import Dict

from message_utils import ClientId
from middleware_config import MiddlewareConfig
from workers.extra_source.extra_source import ExtraSource
from utils.file_utils import DiskJSONStore

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

        base_dir = os.getenv("EXTRA_SOURCE_DIR", "./extra_source/users").strip()
        self.store = DiskJSONStore[Birthday](base_dir)

    def save_message(self, data: dict | list):
        """Append user_id→birthdate pairs from incoming data."""
        client_id = self.current_client_id
        if not client_id:
            return

        def updater(users: Dict[UserId, Birthday]):
            def append_user(item: dict):
                uid = item.get(id_column)
                bday = item.get(birthday_column)
                if self.store.valid_uuidish(uid) and bday:
                    users[str(uid)] = str(bday)

            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        append_user(item)
            elif isinstance(data, dict):
                append_user(data)

        self.store.update(client_id, updater)

    def _get_item(self, client_id: ClientId, item_id: str) -> Birthday:
        users = self.store.load(client_id)
        return users.get(item_id, "Unknown Birthday")