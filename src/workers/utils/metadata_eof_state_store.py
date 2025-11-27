"""Tracking de EOFs de metadata por cliente para aggregators finales."""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import threading
from pathlib import Path
from typing import Dict, Set

from message_utils import ClientId

logger = logging.getLogger(__name__)


class MetadataEOFStateStore:
    """
    Trackea EOFs de metadata por cliente (users, stores, menu_items).
    Permite verificar si todos los EOFs de metadata han llegado antes de procesar transacciones.
    """
    
    # Tipos de metadata que se trackean
    METADATA_TYPES = {'users', 'stores', 'menu_items'}
    
    def __init__(self, required_metadata_types: Set[str] | None = None):
        """
        Inicializa el store de estado de EOFs de metadata.
        
        Args:
            required_metadata_types: Set de tipos de metadata requeridos.
                                   Si None, usa todos los tipos disponibles.
        """
        base_dir = os.getenv("STATE_DIR")
        if base_dir:
            self._store_dir = Path(base_dir) / "metadata_eof_state"
        else:
            self._store_dir = Path("state/metadata_eof_state")
        self._store_dir.mkdir(parents=True, exist_ok=True)
        
        # Tipos de metadata requeridos para considerar "todo listo"
        if required_metadata_types is None:
            self.required_metadata_types = self.METADATA_TYPES.copy()
        else:
            self.required_metadata_types = required_metadata_types.copy()
        
        self._cache: Dict[ClientId, Dict[str, bool]] = {}
        self._lock = threading.RLock()
        
        # Limpiar archivos temporales de crashes anteriores
        self._cleanup_temp_files()
        
        # Cargar estado existente al iniciar
        self._load_all_clients()
    
    def _cleanup_temp_files(self) -> None:
        """Limpia archivos temporales de crashes anteriores."""
        temp_files = list(self._store_dir.glob("*.temp.json"))
        for temp_file in temp_files:
            try:
                logger.debug(
                    f"[METADATA-EOF-STATE] Removing leftover temp file: {temp_file}"
                )
                temp_file.unlink()
            except Exception as exc:
                logger.warning(
                    f"[METADATA-EOF-STATE] Failed to remove temp file {temp_file}: {exc}"
                )
    
    def _client_path(self, client_id: ClientId) -> Path:
        """Obtiene la ruta del archivo para un cliente."""
        safe_id = (
            str(client_id)
            .replace("/", "_")
            .replace("\\", "_")
            .replace(":", "_")
        )
        return self._store_dir / f"{safe_id}.json"
    
    def _compute_checksum(self, payload: Dict) -> str:
        """Calcula checksum SHA256 para validación de payload."""
        serialized = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')
        return hashlib.sha256(serialized).hexdigest()
    
    def _load_all_clients(self) -> None:
        """Carga el estado de todos los clientes desde disco al iniciar."""
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
                        f"[METADATA-EOF-STATE] Invalid format in file: {client_file}, skipping"
                    )
                    continue
                
                client_id = raw.get('client_id')
                metadata_state = raw.get('metadata_state', {})
                checksum = raw.get('checksum')
                
                if not client_id:
                    client_id = client_file.stem
                
                # Validar checksum
                payload = {
                    'client_id': client_id,
                    'metadata_state': metadata_state
                }
                if checksum and checksum != self._compute_checksum(payload):
                    logger.warning(
                        f"[METADATA-EOF-STATE] Checksum mismatch for client {client_id}, trying backup"
                    )
                    
                    # Intentar backup
                    backup_file = client_file.with_suffix('.backup.json')
                    if backup_file.exists():
                        try:
                            with backup_file.open('r', encoding='utf-8') as bf:
                                backup_raw = json.load(bf)
                            backup_client_id = backup_raw.get('client_id') or client_id
                            backup_state = backup_raw.get('metadata_state', {})
                            backup_checksum = backup_raw.get('checksum')
                            
                            backup_payload = {
                                'client_id': backup_client_id,
                                'metadata_state': backup_state
                            }
                            if backup_checksum == self._compute_checksum(backup_payload):
                                metadata_state = backup_state
                                client_id = backup_client_id
                                os.replace(backup_file, client_file)
                                logger.info(
                                    f"\033[32m[METADATA-EOF-STATE] [RECOVERY] Recovered client {client_id} from backup\033[0m"
                                )
                            else:
                                logger.warning(
                                    f"[METADATA-EOF-STATE] Backup also corrupted for client {client_id}"
                                )
                                continue
                        except Exception as exc:
                            logger.warning(
                                f"[METADATA-EOF-STATE] Failed to recover from backup: {exc}"
                            )
                            continue
                    else:
                        logger.warning(f"[METADATA-EOF-STATE] No backup available for client {client_id}")
                        continue
                
                # Cargar en cache
                self._cache[client_id] = metadata_state
                loaded_count += 1
                logger.debug(
                    f"[METADATA-EOF-STATE] Loaded state for client {client_id}: {metadata_state}"
                )
                
            except (json.JSONDecodeError, ValueError, KeyError, OSError) as exc:
                logger.warning(
                    f"[METADATA-EOF-STATE] Failed to load client file {client_file}: {exc}"
                )
        
        if loaded_count > 0:
            logger.info(
                f"[METADATA-EOF-STATE] Loaded EOF state for {loaded_count} clients. "
                f"Required metadata types: {self.required_metadata_types}"
            )
            # Log detailed state for debugging
            for client_id, state in self._cache.items():
                logger.info(
                    f"[METADATA-EOF-STATE] Client {client_id} state: {state}"
                )
    
    def _load_client(self, client_id: ClientId) -> Dict[str, bool]:
        """Carga el estado de un cliente (desde cache o disco)."""
        with self._lock:
            if client_id in self._cache:
                return self._cache[client_id].copy()
            
            path = self._client_path(client_id)
            metadata_state: Dict[str, bool] = {}
            
            if path.exists():
                try:
                    with path.open("r", encoding="utf-8") as fh:
                        raw = json.load(fh)
                    
                    if isinstance(raw, dict):
                        metadata_state = raw.get('metadata_state', {})
                        logger.debug(
                            f"[METADATA-EOF-STATE] Loaded state for client {client_id} from disk"
                        )
                except (json.JSONDecodeError, ValueError, PermissionError, OSError, IOError) as exc:
                    logger.warning(f"[METADATA-EOF-STATE] Failed to load client {client_id}: {exc}")
                    metadata_state = {}
            
            self._cache[client_id] = metadata_state
            return metadata_state.copy()
    
    def _persist_client_atomic(self, client_id: ClientId, metadata_state: Dict[str, bool]) -> None:
        """
        Persiste el estado de un cliente de forma atómica usando two-phase commit.
        """
        client_path = self._client_path(client_id)
        temp_path = client_path.with_suffix('.temp.json')
        backup_path = client_path.with_suffix('.backup.json')
        
        try:
            # Crear payload con checksum
            payload = {
                'client_id': client_id,
                'metadata_state': metadata_state
            }
            checksum = self._compute_checksum(payload)
            serialized = payload | {'checksum': checksum}
            
            # Fase 1: Escribir a archivo temporal con fsync
            with temp_path.open('w', encoding='utf-8') as f:
                json.dump(serialized, f, ensure_ascii=False, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            
            # Fase 2: Validar integridad del archivo temporal
            try:
                with temp_path.open('r', encoding='utf-8') as f:
                    temp_data = json.load(f)
                
                temp_payload = {
                    'client_id': temp_data.get('client_id'),
                    'metadata_state': temp_data.get('metadata_state', {})
                }
                temp_checksum = temp_data.get('checksum')
                
                if temp_checksum != self._compute_checksum(temp_payload):
                    raise ValueError("Temp file checksum validation failed")
                    
            except (json.JSONDecodeError, ValueError, KeyError) as exc:
                logger.error(
                    f"[METADATA-EOF-STATE] [VALIDATION] Temp file validation failed for client {client_id}: {exc}"
                )
                temp_path.unlink()
                raise ValueError(f"Temp file validation failed: {exc}") from exc
            
            # Fase 3: Reemplazo atómico
            if client_path.exists():
                os.replace(client_path, backup_path)
            
            os.replace(temp_path, client_path)
            
            logger.debug(f"[METADATA-EOF-STATE] Persisted state for client {client_id}")
            
        except Exception as exc:
            logger.error(
                f"[METADATA-EOF-STATE] [ERROR] Failed to persist state for client {client_id}: {exc}"
            )
            with contextlib.suppress(FileNotFoundError):
                temp_path.unlink()
            raise
    
    def mark_metadata_done(self, client_id: ClientId, metadata_type: str) -> None:
        """
        Marca un tipo de metadata como done para un cliente.
        
        Args:
            client_id: Identificador del cliente
            metadata_type: Tipo de metadata ('users', 'stores', 'menu_items')
        """
        if metadata_type not in self.METADATA_TYPES:
            logger.warning(
                f"[METADATA-EOF-STATE] Unknown metadata type: {metadata_type} for client {client_id}"
            )
            return
        
        with self._lock:
            metadata_state = self._load_client(client_id)
            if metadata_state.get(metadata_type, False):
                logger.debug(
                    f"[METADATA-EOF-STATE] Metadata {metadata_type} already marked as done for client {client_id}"
                )
                return
            
            metadata_state[metadata_type] = True
            self._cache[client_id] = metadata_state
            
            logger.info(
                f"[METADATA-EOF-STATE] Marked {metadata_type} as done for client {client_id}. "
                f"Current state: {metadata_state}, Required: {self.required_metadata_types}"
            )
            
            # Persistir atómicamente
            self._persist_client_atomic(client_id, metadata_state)
            
            logger.info(
                f"[METADATA-EOF-STATE] Persisted state for client {client_id}. "
                f"All required metadata done: {self.are_all_metadata_done(client_id)}"
            )
            
            logger.info(
                f"\033[92m[METADATA-EOF-STATE] Marked {metadata_type} as done for client {client_id}\033[0m"
            )
    
    def are_all_metadata_done(self, client_id: ClientId) -> bool:
        """
        Verifica si todos los tipos de metadata requeridos están done para un cliente.
        
        Args:
            client_id: Identificador del cliente
            
        Returns:
            True si todos los tipos de metadata requeridos están done, False en caso contrario
        """
        with self._lock:
            metadata_state = self._load_client(client_id)
            
            # Verificar que todos los tipos requeridos estén done
            for metadata_type in self.required_metadata_types:
                if not metadata_state.get(metadata_type, False):
                    logger.info(
                        f"[METADATA-EOF-STATE] Client {client_id} not ready: {metadata_type} not done yet. "
                        f"Required types: {self.required_metadata_types}, Current state: {metadata_state}"
                    )
                    return False
            
            logger.debug(
                f"[METADATA-EOF-STATE] All metadata done for client {client_id}: {metadata_state}"
            )
            return True
    
    def clear_client(self, client_id: ClientId) -> None:
        """Limpia el estado de un cliente."""
        with self._lock:
            self._cache.pop(client_id, None)
            path = self._client_path(client_id)
            backup_path = path.with_suffix('.backup.json')
            with contextlib.suppress(Exception):
                path.unlink()
            with contextlib.suppress(Exception):
                backup_path.unlink()
            logger.info(
                f"\033[32m[METADATA-EOF-STATE] Cleared state for client {client_id}\033[0m"
            )
    
    def get_metadata_state(self, client_id: ClientId) -> Dict[str, bool]:
        """Obtiene el estado de metadata para un cliente (copia)."""
        return self._load_client(client_id)

