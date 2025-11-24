"""Simple per-client message UUID persistence to avoid duplicate processing."""

from __future__ import annotations

import contextlib
import json
import os
import threading
from pathlib import Path
from typing import Dict, Set

from message_utils import ClientId


class ProcessedMessageStore:
    """Track processed message UUIDs per client with filesystem persistence."""

    def __init__(self, worker_label: str):
        base_dir = os.getenv("STATE_DIR")
        if base_dir:
            self._store_dir = Path(base_dir) / "processed_messages" / worker_label
        else:
            self._store_dir = Path("state/processed_messages") / worker_label
        self._store_dir.mkdir(parents=True, exist_ok=True)

        self._cache: Dict[ClientId, Set[str]] = {}
        self._lock = threading.RLock()

    def _client_path(self, client_id: ClientId) -> Path:
        safe_id = (
            str(client_id)
            .replace("/", "_")
            .replace("\\", "_")
            .replace(":", "_")
        )
        return self._store_dir / f"{safe_id}.json"

    def _load_client(self, client_id: ClientId) -> Set[str]:
        with self._lock:
            if client_id in self._cache:
                return self._cache[client_id]

            path = self._client_path(client_id)
            if not path.exists():
                store: Set[str] = set()
                self._cache[client_id] = store
                return store

            try:
                with path.open("r", encoding="utf-8") as fh:
                    data = json.load(fh)
                if isinstance(data, list):
                    store = set(str(entry) for entry in data)
                else:
                    store = set()
            except Exception:  # noqa: BLE001
                store = set()

            self._cache[client_id] = store
            return store

    def _persist_client(self, client_id: ClientId, store: Set[str]) -> None:
        path = self._client_path(client_id)
        tmp_path = path.with_suffix(".json.tmp")
        try:
            with tmp_path.open("w", encoding="utf-8") as fh:
                json.dump(sorted(store), fh, ensure_ascii=False)
            tmp_path.replace(path)
        finally:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)

    def has_processed(self, client_id: ClientId, message_uuid: str) -> bool:
        if not message_uuid:
            return False
        store = self._load_client(client_id)
        return message_uuid in store

    def mark_processed(self, client_id: ClientId, message_uuid: str | None) -> None:
        if not message_uuid:
            return
        with self._lock:
            store = self._load_client(client_id)
            if message_uuid in store:
                return
            store.add(message_uuid)
            self._persist_client(client_id, store)

    def clear_client(self, client_id: ClientId) -> None:
        with self._lock:
            self._cache.pop(client_id, None)
            path = self._client_path(client_id)
            with contextlib.suppress(Exception):
                path.unlink()

    def clear_all(self) -> None:
        """Remove all cached UUIDs for every client."""
        with self._lock:
            self._cache.clear()
            if self._store_dir.exists():
                for path in self._store_dir.glob("*.json"):
                    with contextlib.suppress(Exception):
                        path.unlink()
