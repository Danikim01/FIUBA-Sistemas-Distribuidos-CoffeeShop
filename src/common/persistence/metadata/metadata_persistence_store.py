"""Append-only persistence for metadata stores (users, stores, menu_items)."""

from __future__ import annotations

import contextlib
import csv
import logging
import os
import threading
from pathlib import Path
from typing import Dict

from workers.utils.message_utils import ClientId

logger = logging.getLogger(__name__)


class MetadataPersistenceStore:
    """
    Append-only persistence for metadata stores using CSV.
    
    Each client has a CSV file with format: id,value
    New items are added with append + fsync for durability.
    """
    
    def __init__(self, store_type: str, id_key: str, value_key: str):
        """
        Initialize the persistence store.
        
        Args:
            store_type: Store type ('users', 'stores', 'menu_items')
            id_key: ID column name (e.g., 'user_id', 'store_id', 'item_id')
            value_key: Value column name (e.g., 'birthday', 'store_name', 'item_name')
        """
        base_dir = os.getenv("STATE_DIR")
        if base_dir:
            self._store_dir = Path(base_dir) / "metadata_store" / store_type
        else:
            self._store_dir = Path("state/metadata_store") / store_type
        self._store_dir.mkdir(parents=True, exist_ok=True)
        
        self.store_type = store_type
        self.id_key = id_key
        self.value_key = value_key
        
        # Cache en memoria: dict[ClientId, dict[ItemId, Value]]
        self._cache: Dict[ClientId, Dict[str, str]] = {}
        self._lock = threading.RLock()
        
        # Cargar datos existentes al iniciar
        self._load_all_clients()
    
    def _client_path(self, client_id: ClientId) -> Path:
        """Gets the file path for a client (CSV)."""
        safe_id = (
            str(client_id)
            .replace("/", "_")
            .replace("\\", "_")
            .replace(":", "_")
        )
        return self._store_dir / f"{safe_id}.csv"
    
    def _load_all_clients(self) -> None:
        """Loads all client data from disk on startup."""
        client_files = list(self._store_dir.glob("*.csv"))
        logger.debug(
            "[LOAD-ALL] Found %d CSV files for %s in directory: %s",
            len(client_files),
            self.store_type,
            self._store_dir
        )
        
        loaded_count = 0
        total_items = 0
        
        for client_file in client_files:
            try:
                client_id = client_file.stem
                data = self._load_client_from_disk(client_id)
                
                if data:
                    self._cache[client_id] = data
                    loaded_count += 1
                    total_items += len(data)
                    # Count unique items (duplicates are handled by dict, so len(data) is already unique)
                    unique_count = len(data)
                    # Log with full client_id (no truncation)
                    logger.info(
                        "\033[92m[LOAD-ALL] Loaded %d %s for client %s\033[0m",
                        unique_count,
                        self.store_type,
                        client_id
                    )
            except (ValueError, OSError, csv.Error) as exc:
                logger.warning(
                    "[LOAD-ALL] Failed to load client file %s: %s",
                    client_file,
                    exc
                )
        
        if loaded_count > 0:
            logger.info(
                "\033[92m[LOAD-ALL] Loaded %s metadata for %d clients (%d total items)\033[0m",
                self.store_type,
                loaded_count,
                total_items
            )
    
    def _load_client_from_disk(self, client_id: ClientId) -> Dict[str, str]:
        """Loads client data from disk (CSV)."""
        path = self._client_path(client_id)
        data: Dict[str, str] = {}
        
        if not path.exists():
            return data
        
        try:
            with path.open("r", encoding="utf-8", newline='') as f:
                # Leer CSV sin header (formato: id,value)
                reader = csv.reader(f)
                for line_num, row in enumerate(reader, 1):
                    if len(row) < 2:
                        # Línea incompleta o vacía, continuar
                        continue
                    
                    item_id = str(row[0]).strip()
                    item_value = str(row[1]).strip()
                    
                    if item_id and item_value:
                        # Si hay duplicados, el último valor gana (más reciente)
                        data[item_id] = item_value
            
            logger.debug(
                f"[LOAD] Loaded {len(data)} {self.store_type} items for client {client_id}"
            )
        except (PermissionError, OSError, IOError, csv.Error) as exc:
            logger.warning(f"[LOAD] Failed to load client {client_id}: {exc}")
            data = {}
        
        return data
    
    def _load_client(self, client_id: ClientId) -> Dict[str, str]:
        """Loads client data (from cache or disk)."""
        with self._lock:
            if client_id in self._cache:
                return self._cache[client_id]
            
            data = self._load_client_from_disk(client_id)
            self._cache[client_id] = data
            return data
    
    def _append_item(self, client_id: ClientId, item_id: str, item_value: str) -> None:
        """
        Appends an item to the client's CSV file atomically.
        """
        client_path = self._client_path(client_id)
        
        try:
            with client_path.open('a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([item_id, item_value])
                f.flush()
                os.fsync(f.fileno())
                
        except Exception as exc:
            logger.error(
                f"[APPEND] Failed to append {self.store_type} item for client {client_id}: {exc}"
            )
            raise
    
    def _append_batch(self, client_id: ClientId, items: Dict[str, str]) -> None:
        """
        Appends a batch of items to the client's CSV file atomically.
        
        More efficient than multiple _append_item calls for large batches.
        """
        if not items:
            return
        
        client_path = self._client_path(client_id)
        
        try:
            with client_path.open('a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                # Escribir todos los items en un solo batch
                for item_id, item_value in items.items():
                    writer.writerow([item_id, item_value])
                f.flush()
                os.fsync(f.fileno())
                
        except Exception as exc:
            logger.error(
                f"[APPEND-BATCH] Failed to append {self.store_type} batch for client {client_id}: {exc}"
            )
            raise
    
    def save_item(self, client_id: ClientId, item_id: str, item_value: str) -> None:
        """
        Saves an item for a client.
        
        If the item already exists, it updates it (adds new line, last value wins).
        """
        if not item_id or not item_value:
            logger.debug(
                f"[SAVE] Skipping empty {self.store_type} item for client {client_id}"
            )
            return
        
        with self._lock:
            # Cargar datos del cliente
            data = self._load_client(client_id)
            
            # Si ya existe y es el mismo valor, no hacer nada
            if item_id in data and data[item_id] == item_value:
                logger.debug(
                    f"[SAVE] {self.store_type} item {item_id} already exists with same value "
                    f"for client {client_id}"
                )
                return
            
            # Actualizar cache
            data[item_id] = item_value
            self._cache[client_id] = data
            
            # Persistir en disco (append-only)
            self._append_item(client_id, item_id, item_value)
    
    def save_batch(self, client_id: ClientId, items: Dict[str, str]) -> None:
        """Saves a batch of items for a client."""
        if not items:
            return
        
        with self._lock:
            data = self._load_client(client_id)
            
            # Filtrar items válidos
            valid_items = {k: v for k, v in items.items() if k and v}
            
            if not valid_items:
                return
            
            # Actualizar cache
            data.update(valid_items)
            self._cache[client_id] = data
            
            # Persistir todos los items en un solo batch (más eficiente)
            self._append_batch(client_id, valid_items)
    
    def get_item(self, client_id: ClientId, item_id: str) -> str:
        """Gets an item for a client."""
        data = self._load_client(client_id)
        return data.get(item_id, self._get_default_value())
    
    def get_all_items(self, client_id: ClientId) -> Dict[str, str]:
        """Gets all items for a client."""
        return self._load_client(client_id).copy()
    
    def _get_default_value(self) -> str:
        """Returns the default value according to the store type."""
        defaults = {
            'users': "Unknown Birthday",
            'stores': 'Unknown Store',
            'menu_items': 'Unknown Item'
        }
        return defaults.get(self.store_type, "Unknown")
    
    def clear_client(self, client_id: ClientId) -> None:
        """Clears the data of a client."""
        with self._lock:
            self._cache.pop(client_id, None)
            path = self._client_path(client_id)
            with contextlib.suppress(Exception):
                path.unlink()
            logger.info(
                f"\033[32m[CLEAR] Cleared {self.store_type} metadata for client {client_id}\033[0m"
            )

    def clear_all(self) -> None:
        """Clears the data for all clients."""
        with self._lock:
            self._cache.clear()
            for path in self._store_dir.glob("*.csv"):
                with contextlib.suppress(Exception):
                    path.unlink()
            logger.info(
                f"\033[32m[CLEAR] Cleared {self.store_type} metadata for all clients\033[0m"
            )
    
    def has_item(self, client_id: ClientId, item_id: str) -> bool:
        """Checks if an item exists for a client."""
        data = self._load_client(client_id)
        return item_id in data
