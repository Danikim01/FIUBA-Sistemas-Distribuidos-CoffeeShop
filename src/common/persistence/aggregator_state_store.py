"""Persistence of intermediate aggregation state for final aggregators."""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import threading
from pathlib import Path
from typing import Any, Dict

from workers.utils.message_utils import ClientId

logger = logging.getLogger(__name__)


class AggregatorStateStore:
    """
    Persistence of intermediate aggregation state for final aggregators.
    
    Uses a two-phase commit protocol similar to EOFCounterStore:
    1. Writes to temporary file with fsync
    2. Validates integrity of temporary file (checksum)
    3. Atomically replaces original file with temporary (with backup)
    
    This ensures atomicity - if the worker crashes after processing a batch,
    the intermediate state is available when the worker restarts.
    """
    
    def __init__(self, worker_label: str):
        """
        Initialize the aggregation state store.
        
        Args:
            worker_label: Worker label (e.g., 'top_clients_aggregator')
        """
        base_dir = os.getenv("STATE_DIR")
        if base_dir:
            self._store_dir = Path(base_dir) / "aggregator_state" / worker_label
        else:
            self._store_dir = Path("state/aggregator_state") / worker_label
        self._store_dir.mkdir(parents=True, exist_ok=True)
        
        self.worker_label = worker_label
        self._cache: Dict[ClientId, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        
        # Limpiar archivos temporales de crashes anteriores
        self._cleanup_temp_files()
        
        # Cargar estado existente al iniciar
        self._load_all_clients()
    
    def _cleanup_temp_files(self) -> None:
        """Cleans up temporary files from previous crashes."""
        temp_files = list(self._store_dir.glob("*.temp.json"))
        for temp_file in temp_files:
            try:
                logger.info(
                    f"\033[32m[AGGREGATOR-STATE-STORE] Removing leftover temp file from previous crash: {temp_file}\033[0m"
                )
                temp_file.unlink()
            except Exception as exc:
                logger.info(
                    f"\033[33m[AGGREGATOR-STATE-STORE] Failed to remove temp file {temp_file}: {exc}\033[0m"
                )
    
    def _client_path(self, client_id: ClientId) -> Path:
        """Gets the file path for a client."""
        safe_id = (
            str(client_id)
            .replace("/", "_")
            .replace("\\", "_")
            .replace(":", "_")
        )
        return self._store_dir / f"{safe_id}.json"
    
    def _compute_checksum(self, payload: Dict[str, Any]) -> str:
        """Computes SHA256 checksum for payload validation."""
        serialized = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')
        return hashlib.sha256(serialized).hexdigest()
    
    def _load_all_clients(self) -> None:
        """Loads the state of all clients from disk on startup."""
        # Buscar todos los archivos de clientes (excluir temp y backup)
        client_files = [
            f for f in self._store_dir.glob("*.json")
            if not f.name.endswith('.temp.json') and not f.name.endswith('.backup.json')
        ]
        
        loaded_count = 0
        for client_file in client_files:
            try:
                with client_file.open('r', encoding='utf-8') as f:
                    raw = json.load(f)
                
                if not isinstance(raw, dict):
                    logger.warning(
                        f"[AGGREGATOR-STATE-STORE] Invalid format (not a dict) in file: {client_file}, skipping"
                    )
                    continue
                
                client_id = raw.get('client_id')
                state_data = raw.get('state', {})
                checksum = raw.get('checksum')
                
                if not client_id:
                    # Fallback: extraer del nombre del archivo
                    client_id = client_file.stem
                
                # Validar checksum
                payload = {
                    'client_id': client_id,
                    'state': state_data
                }
                if checksum and checksum != self._compute_checksum(payload):
                    logger.warning(
                        f"[AGGREGATOR-STATE-STORE] Checksum mismatch for client {client_id} in {client_file}, trying backup"
                    )
                    
                    # Intentar backup
                    backup_file = client_file.with_suffix('.backup.json')
                    if backup_file.exists():
                        try:
                            with backup_file.open('r', encoding='utf-8') as bf:
                                backup_raw = json.load(bf)
                            backup_client_id = backup_raw.get('client_id') or client_id
                            backup_state = backup_raw.get('state', {})
                            backup_checksum = backup_raw.get('checksum')
                            
                            backup_payload = {
                                'client_id': backup_client_id,
                                'state': backup_state
                            }
                            if backup_checksum == self._compute_checksum(backup_payload):
                                # Backup válido, usarlo
                                state_data = backup_state
                                client_id = backup_client_id
                                # Restaurar backup como archivo principal
                                os.replace(backup_file, client_file)
                                logger.info(
                                    f"\033[32m[AGGREGATOR-STATE-STORE] [RECOVERY] Recovered client {client_id} from backup\033[0m"
                                )
                            else:
                                logger.warning(
                                    f"[AGGREGATOR-STATE-STORE] [RECOVERY] Backup also corrupted for client {client_id}"
                                )
                                continue
                        except Exception as exc:
                            logger.warning(
                                f"[AGGREGATOR-STATE-STORE] [RECOVERY] Failed to recover from backup: {exc}"
                            )
                            continue
                    else:
                        logger.warning(f"[AGGREGATOR-STATE-STORE] No backup available for client {client_id}")
                        continue
                
                # Cargar en cache
                self._cache[client_id] = state_data
                loaded_count += 1
                logger.info(
                    f"\033[32m[AGGREGATOR-STATE-STORE] Loaded state for client {client_id} "
                    f"(keys: {list(state_data.keys()) if isinstance(state_data, dict) else 'N/A'})\033[0m"
                )
                
            except (json.JSONDecodeError, ValueError, KeyError, OSError) as exc:
                logger.warning(
                    f"[AGGREGATOR-STATE-STORE] Failed to load client file {client_file}: {exc}"
                )
        
        if loaded_count > 0:
            logger.info(
                f"[AGGREGATOR-STATE-STORE] Loaded aggregation state for {loaded_count} clients"
            )
    
    def _load_client(self, client_id: ClientId) -> Dict[str, Any]:
        """Loads the state of a client (from cache or disk)."""
        with self._lock:
            if client_id in self._cache:
                return self._cache[client_id]
            
            path = self._client_path(client_id)
            state_data: Dict[str, Any] = {}
            
            if path.exists():
                try:
                    with path.open("r", encoding="utf-8") as fh:
                        raw = json.load(fh)
                    
                    if isinstance(raw, dict):
                        state_data = raw.get('state', {})
                        logger.debug(
                            f"[AGGREGATOR-STATE-STORE] Loaded state for client {client_id} from disk"
                        )
                except (json.JSONDecodeError, ValueError, PermissionError, OSError, IOError) as exc:
                    logger.warning(f"[AGGREGATOR-STATE-STORE] Failed to load client {client_id}: {exc}")
                    state_data = {}
            
            self._cache[client_id] = state_data
            return state_data
    
    def _persist_client_atomic(self, client_id: ClientId, state_data: Dict[str, Any]) -> None:
        """
        Persists the state of a client atomically using two-phase commit.
        
        Protocol:
        1. Writes to temporary file with fsync
        2. Validates integrity of temporary file (checksum)
        3. Atomically replaces original file with temporary (with backup)
        """
        client_path = self._client_path(client_id)
        temp_path = client_path.with_suffix('.temp.json')
        backup_path = client_path.with_suffix('.backup.json')
        
        try:
            # Crear payload con checksum
            payload = {
                'client_id': client_id,
                'state': state_data
            }
            checksum = self._compute_checksum(payload)
            serialized = payload | {'checksum': checksum}
            
            # Fase 1: Escribir a archivo temporal con fsync
            with temp_path.open('w', encoding='utf-8') as f:
                json.dump(serialized, f, ensure_ascii=False, sort_keys=True, indent=2)
                f.flush()  # Flush buffer de Python
                os.fsync(f.fileno())  # Forzar escritura a disco
            
            # Fase 2: Validar integridad del archivo temporal
            try:
                with temp_path.open('r', encoding='utf-8') as f:
                    temp_data = json.load(f)
                
                temp_payload = {
                    'client_id': temp_data.get('client_id'),
                    'state': temp_data.get('state', {})
                }
                temp_checksum = temp_data.get('checksum')
                
                if temp_checksum != self._compute_checksum(temp_payload):
                    raise ValueError("Temp file checksum validation failed - file may be corrupted")
                    
            except (json.JSONDecodeError, ValueError, KeyError) as exc:
                logger.error(
                    f"[AGGREGATOR-STATE-STORE] [VALIDATION] Temp file validation failed for client {client_id}: {exc}. "
                    f"Aborting persist."
                )
                temp_path.unlink()
                raise ValueError(f"Temp file validation failed: {exc}") from exc
            
            # Fase 3: Reemplazo atómico (solo si el archivo temporal es válido)
            # Crear backup del archivo original si existe
            if client_path.exists():
                # Backup se crea atómicamente
                os.replace(client_path, backup_path)
            
            # Reemplazar original con archivo temporal validado (operación atómica)
            os.replace(temp_path, client_path)
            
            logger.debug(f"[AGGREGATOR-STATE-STORE] Persisted state for client {client_id}")
            
        except Exception as exc:
            logger.error(
                f"[AGGREGATOR-STATE-STORE] [ERROR] Failed to persist state for client {client_id}: {exc}"
            )
            # Limpiar archivo temporal en caso de error
            with contextlib.suppress(FileNotFoundError):
                temp_path.unlink()
            raise
    
    def get_state(self, client_id: ClientId) -> Dict[str, Any]:
        """Gets the state of a client."""
        return self._load_client(client_id).copy()
    
    def save_state(self, client_id: ClientId, state_data: Dict[str, Any]) -> None:
        """Saves the state of a client."""
        with self._lock:
            # Actualizar cache
            self._cache[client_id] = state_data.copy()
            
            # Persistir en disco (atómico)
            self._persist_client_atomic(client_id, state_data)
    
    def clear_client(self, client_id: ClientId) -> None:
        """Clears the state of a client."""
        with self._lock:
            self._cache.pop(client_id, None)
            path = self._client_path(client_id)
            backup_path = path.with_suffix('.backup.json')
            # Eliminar archivo principal y backup
            with contextlib.suppress(Exception):
                path.unlink()
            with contextlib.suppress(Exception):
                backup_path.unlink()
            logger.info(
                f"\033[32m[AGGREGATOR-STATE-STORE] Cleared state for client {client_id}\033[0m"
            )

    def clear_all(self) -> None:
        """Clears the state of all clients."""
        with self._lock:
            self._cache.clear()
            for pattern in ("*.json", "*.backup.json", "*.temp.json"):
                for path in self._store_dir.glob(pattern):
                    with contextlib.suppress(Exception):
                        path.unlink()
        logger.info("\033[32m[AGGREGATOR-STATE-STORE] Cleared state for all clients\033[0m")
    
    def has_state(self, client_id: ClientId) -> bool:
        """Checks if a client has persisted state."""
        with self._lock:
            if client_id in self._cache:
                return bool(self._cache[client_id])
            path = self._client_path(client_id)
            return path.exists()
