import json
import os
from pathlib import Path
from typing import Dict, Optional

from message_utils import ClientId
from middleware_config import MiddlewareConfig
from workers.aggregator.extra_source.extra_source import ExtraSource

UserId = str
Birthday = str

id_column = "user_id"
birthday_column = "birthdate"


class UsersExtraSource(ExtraSource):
    def __init__(self, middleware_config: MiddlewareConfig):
        clients_queue = os.getenv("CLIENTS_QUEUE", "users_raw").strip()
        middleware = middleware_config.create_queue(clients_queue)
        super().__init__(clients_queue, middleware)

        # Directory where client files will be stored
        base_dir = os.getenv("EXTRA_SOURCE_DIR", "./extra_source/users").strip()
        self.base_path = Path(base_dir)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def _client_file(self, client_id: ClientId) -> Path:
        return self.base_path / f"{client_id}.json"

    def _load(self, client_id: ClientId) -> Dict[UserId, Birthday]:
        path = self._client_file(client_id)
        if path.exists():
            with path.open("r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def _save(self, client_id: ClientId, data: Dict[UserId, Birthday]) -> None:
        with self._client_file(client_id).open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def save_message(self, data: dict):
        users = self._load(self.current_client_id)

        def append_user(item: dict):
            uid = item.get(id_column)
            bday = item.get(birthday_column)
            if uid and bday:
                users[str(uid)] = str(bday)

        if isinstance(data, list):
            for item in data:
                append_user(item)

        if isinstance(data, dict):
            append_user(data)

        self._save(self.current_client_id, users)

    def _get_item(self, client_id: ClientId, item_id: str) -> Birthday:
        users = self._load(client_id)
        return users.get(item_id, "Unknown Birthday")