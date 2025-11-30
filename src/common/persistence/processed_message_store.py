"""Robust per-client message UUID persistence with append-only files."""

from __future__ import annotations

import contextlib
import logging
import os
import threading
from pathlib import Path
from typing import Dict, Set

from workers.utils.message_utils import ClientId

logger = logging.getLogger(__name__)


class ProcessedMessageStore:
    """
    Track processed message UUIDs per client with append-only file persistence.
    
    Each client has a plain text file with one UUID per line.
    New UUIDs are appended with fsync for durability.
    
    This ensures:
    - Fast writes (append-only, no rewrite)
    - Atomicity (fsync after each append)
    - Crash recovery (UUID is persisted before processing completes)
    
    Stores ALL processed UUIDs (not just the last one) to handle out-of-order
    message delivery from multiple filter worker replicas.
    """

    def __init__(self, worker_label: str):
        base_dir = os.getenv("STATE_DIR")
        if base_dir:
            self._store_dir = Path(base_dir) / "processed_messages" / worker_label
        else:
            self._store_dir = Path("state/processed_messages") / worker_label
        self._store_dir.mkdir(parents=True, exist_ok=True)

        self._cache: Dict[ClientId, Set[str]] = {}
        self._lock = threading.RLock()
        
        self._load_all_clients()

    def _client_path(self, client_id: ClientId) -> Path:
        """Get the path for a client's processed messages file (plain text)."""
        safe_id = (
            str(client_id)
            .replace("/", "_")
            .replace("\\", "_")
            .replace(":", "_")
        )
        return self._store_dir / f"{safe_id}.txt"

    def _load_all_clients(self) -> None:
        """Load all client processed message sets from disk on startup."""
        client_files = list(self._store_dir.glob("*.txt"))
        
        loaded_count = 0
        for client_file in client_files:
            try:
                client_id = client_file.stem
                with client_file.open('r', encoding='utf-8') as f:
                    uuids = {line.strip() for line in f if line.strip()}
                
                self._cache[client_id] = uuids
                loaded_count += 1
                logger.info(f"\033[32m[LOAD-ALL-CLIENTES] Loaded {len(uuids)} processed UUIDs for client {client_id}\033[0m")
                
            except (ValueError, OSError) as exc:
                logger.warning("[LOAD-ALL-CLIENTES] Failed to load client file %s: %s", client_file, exc)
        
        logger.info("[LOAD-ALL-CLIENTES] Loaded processed messages for %d clients", loaded_count)

    def _load_client(self, client_id: ClientId) -> Set[str]:
        """Load processed UUIDs for a client (from cache or disk)."""
        with self._lock:
            if client_id in self._cache:
                return self._cache[client_id]

            path = self._client_path(client_id)
            store: Set[str] = set()
            
            if path.exists():
                try:
                    logger.info(f"\033[32m[LOAD-CLIENT] Loading processed messages for client {client_id} from {path}\033[0m")
                    with path.open("r", encoding="utf-8") as fh:
                        store = {line.strip() for line in fh if line.strip()}
                    logger.info(f"\033[32m[LOAD-CLIENT] Loaded {len(store)} processed UUIDs for client {client_id}\033[0m")
                except (PermissionError, OSError, IOError) as exc:
                    logger.warning("[LOAD-CLIENT] Failed to load client %s: %s", client_id, exc)
                    store = set()

            self._cache[client_id] = store
            return store

    def _append_uuid(self, client_id: ClientId, message_uuid: str) -> None:
        """
        Append a UUID to the client's file atomically.
        
        Uses append mode with immediate fsync for durability.
        Much faster than rewriting the entire file.
        """
        client_path = self._client_path(client_id)
        
        try:
            with client_path.open('a', encoding='utf-8') as f:
                f.write(f"{message_uuid}\n")
                f.flush()
                os.fsync(f.fileno())
                
        except Exception as exc:
            logger.error("[APPEND] Failed to append UUID for client %s: %s", client_id, exc)
            raise

    def has_processed(self, client_id: ClientId, message_uuid: str) -> bool:
        """Check if a message UUID has been processed for a client."""
        if not message_uuid:
            return False
        store = self._load_client(client_id)
        return message_uuid in store

    def mark_processed(self, client_id: ClientId, message_uuid: str | None) -> None:
        """Mark a message UUID as processed for a client."""
        if not message_uuid:
            logger.debug(f"[MARK-PROCESSED] Message UUID is None for client {client_id}")
            return
        with self._lock:
            store = self._load_client(client_id)
            if message_uuid in store:
                logger.debug(f"[MARK-PROCESSED] Message UUID {message_uuid} already processed for client {client_id}")
                return
            
            store.add(message_uuid)
            self._cache[client_id] = store
            
            self._append_uuid(client_id, message_uuid)

    def clear_client(self, client_id: ClientId) -> None:
        """Clear processed messages for a client."""
        with self._lock:
            self._cache.pop(client_id, None)
            path = self._client_path(client_id)
            with contextlib.suppress(Exception):
                path.unlink()
            logger.info(f"\033[32m[CLEAR] Cleared processed messages for client {client_id}\033[0m")
    
    def clear_all(self) -> None:
        """Clear processed messages for all clients."""
        with self._lock:
            self._cache.clear()
            for path in self._store_dir.glob("*.txt"):
                with contextlib.suppress(Exception):
                    path.unlink()
            logger.info("\033[32m[CLEAR] Cleared processed messages for all clients\033[0m")
