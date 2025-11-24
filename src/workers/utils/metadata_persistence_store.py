"""Persistencia append-only para metadata stores (users, stores, menu_items)."""

from __future__ import annotations

import contextlib
import csv
import logging
import os
import threading
from pathlib import Path
from typing import Dict

from message_utils import ClientId

logger = logging.getLogger(__name__)


class MetadataPersistenceStore:
    """
    Persistencia append-only para metadata stores usando CSV.
    
    Cada cliente tiene un archivo CSV con formato: id,value
    Nuevos items se agregan con append + fsync para durabilidad.
    
    Esto asegura:
    - Escrituras rápidas (append-only CSV, más rápido que JSON)
    - Atomicidad (fsync después de cada append o batch)
    - Recuperación tras crash (items persisten antes de completar procesamiento)
    - Eficiencia para datasets grandes (users: 2M items)
    """
    
    def __init__(self, store_type: str, id_key: str, value_key: str):
        """
        Inicializa el store de persistencia.
        
        Args:
            store_type: Tipo de store ('users', 'stores', 'menu_items')
            id_key: Nombre de la columna ID (ej: 'user_id', 'store_id', 'item_id')
            value_key: Nombre de la columna valor (ej: 'birthday', 'store_name', 'item_name')
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
        """Obtiene la ruta del archivo para un cliente (CSV)."""
        safe_id = (
            str(client_id)
            .replace("/", "_")
            .replace("\\", "_")
            .replace(":", "_")
        )
        return self._store_dir / f"{safe_id}.csv"
    
    def _load_all_clients(self) -> None:
        """Carga todos los datos de clientes desde disco al iniciar."""
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
                        "[LOAD-ALL] Loaded %d unique %s items for client %s",
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
                "[LOAD-ALL] Loaded %s metadata for %d clients (%d total items)",
                self.store_type,
                loaded_count,
                total_items
            )
    
    def _load_client_from_disk(self, client_id: ClientId) -> Dict[str, str]:
        """Carga datos de un cliente desde disco (CSV)."""
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
        """Carga datos de un cliente (desde cache o disco)."""
        with self._lock:
            if client_id in self._cache:
                return self._cache[client_id]
            
            data = self._load_client_from_disk(client_id)
            self._cache[client_id] = data
            return data
    
    def _append_item(self, client_id: ClientId, item_id: str, item_value: str) -> None:
        """
        Agrega un item al archivo CSV del cliente de forma atómica.
        
        Usa append mode con fsync inmediato para durabilidad.
        CSV es más rápido que JSON para escrituras.
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
        Agrega un batch de items al archivo CSV del cliente de forma atómica.
        
        Más eficiente que múltiples _append_item para batches grandes.
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
        Guarda un item para un cliente.
        
        Si el item ya existe, lo actualiza (agrega nueva línea, el último valor gana).
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
        """Guarda un batch de items para un cliente."""
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
        """Obtiene un item para un cliente."""
        data = self._load_client(client_id)
        return data.get(item_id, self._get_default_value())
    
    def get_all_items(self, client_id: ClientId) -> Dict[str, str]:
        """Obtiene todos los items para un cliente."""
        return self._load_client(client_id).copy()
    
    def _get_default_value(self) -> str:
        """Retorna el valor por defecto según el tipo de store."""
        defaults = {
            'users': "Unknown Birthday",
            'stores': 'Unknown Store',
            'menu_items': 'Unknown Item'
        }
        return defaults.get(self.store_type, "Unknown")
    
    def clear_client(self, client_id: ClientId) -> None:
        """Limpia los datos de un cliente."""
        with self._lock:
            self._cache.pop(client_id, None)
            path = self._client_path(client_id)
            with contextlib.suppress(Exception):
                path.unlink()
            logger.info(
                f"\033[32m[CLEAR] Cleared {self.store_type} metadata for client {client_id}\033[0m"
            )
    
    def has_item(self, client_id: ClientId, item_id: str) -> bool:
        """Verifica si un item existe para un cliente."""
        data = self._load_client(client_id)
        return item_id in data

