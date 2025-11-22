"""Robust per-client EOF counter persistence with two-phase commit protocol."""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import threading
from pathlib import Path
from typing import Any, Dict

from message_utils import ClientId

logger = logging.getLogger(__name__)


class EOFCounterStore:
    """
    Track EOF counters per client with robust filesystem persistence.
    
    Uses a two-phase commit protocol similar to ProcessedMessageStore:
    1. Write to temp file with fsync
    2. Validate temp file integrity (checksum)
    3. Atomically replace original with temp file (with backup)
    
    This ensures atomicity - if the worker crashes after receiving an EOF,
    the counter is still available when the worker restarts.
    """

    def __init__(self, worker_label: str):
        base_dir = os.getenv("STATE_DIR")
        if base_dir:
            self._store_dir = Path(base_dir) / "eof_counters" / worker_label
        else:
            self._store_dir = Path("state/eof_counters") / worker_label
        self._store_dir.mkdir(parents=True, exist_ok=True)

        self._cache: Dict[ClientId, int] = {}
        self._lock = threading.RLock()
        
        # Clean up any leftover temp files from previous crashes
        self._cleanup_temp_files()
        
        # Load existing EOF counters on startup
        self._load_all_clients()

    def _cleanup_temp_files(self) -> None:
        """Clean up any leftover temporary files from previous crashes."""
        temp_files = list(self._store_dir.glob("*.temp.json"))
        for temp_file in temp_files:
            try:
                logger.info(f"\033[32m[EOF-COUNTER-STORE] Removing leftover temp file from previous crash: {temp_file}\033[0m")
                temp_file.unlink()
            except Exception as exc:
                logger.info(f"\033[33m[EOF-COUNTER-STORE] Failed to remove temp file {temp_file}: {exc}\033[0m")

    def _client_path(self, client_id: ClientId) -> Path:
        """Get the path for a client's EOF counter file."""
        safe_id = (
            str(client_id)
            .replace("/", "_")
            .replace("\\", "_")
            .replace(":", "_")
        )
        return self._store_dir / f"{safe_id}.json"

    def _compute_checksum(self, payload: Dict[str, Any]) -> str:
        """Compute SHA256 checksum for payload validation."""
        serialized = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')
        return hashlib.sha256(serialized).hexdigest()

    def _load_all_clients(self) -> None:
        """Load all client EOF counters from disk on startup."""
        # Find all client files (exclude temp and backup files)
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
                    # Invalid format - skip corrupted files
                    logger.info(f"\033[33m[EOF-COUNTER-STORE] Invalid format (not a dict) in file: {client_file}, skipping\033[0m")
                    continue
                
                client_id = raw.get('client_id')
                counter = raw.get('eof_count', 0)
                checksum = raw.get('checksum')
                
                if not client_id:
                    # Fallback: extract from filename
                    logger.info(f"\033[32m[EOF-COUNTER-STORE] No client ID found in file: {client_file}, falling back to filename\033[0m")
                    client_id = client_file.stem
                
                # Validate checksum
                payload = {
                    'client_id': client_id,
                    'eof_count': counter
                }
                if checksum and checksum != self._compute_checksum(payload):
                    logger.info(f"\033[33m[EOF-COUNTER-STORE] Checksum mismatch for client {client_id} in {client_file}, trying backup\033[0m")

                    # Try backup
                    backup_file = client_file.with_suffix('.backup.json')
                    if backup_file.exists():
                        try:
                            with backup_file.open('r', encoding='utf-8') as bf:
                                backup_raw = json.load(bf)
                            backup_client_id = backup_raw.get('client_id') or client_id
                            backup_counter = backup_raw.get('eof_count', 0)
                            backup_checksum = backup_raw.get('checksum')
                            
                            backup_payload = {
                                'client_id': backup_client_id,
                                'eof_count': backup_counter
                            }
                            if backup_checksum == self._compute_checksum(backup_payload):
                                # Backup is valid, use it
                                counter = backup_counter
                                client_id = backup_client_id
                                # Restore backup as main file
                                os.replace(backup_file, client_file)
                                logger.info(f"\033[32m[EOF-COUNTER-STORE] [RECOVERY] Recovered client {client_id} from backup with count {counter}\033[0m")
                            else:
                                logger.info(f"\033[33m[EOF-COUNTER-STORE] [RECOVERY] Backup also corrupted for client {client_id}\033[0m")
                                continue
                        except Exception as exc:
                            logger.info(f"\033[33m[EOF-COUNTER-STORE] [RECOVERY] Failed to recover from backup: {exc}\033[0m")
                            continue
                    else:
                        logger.warning("[EOF-COUNTER-STORE] No backup available for client %s", client_id)
                        continue
                
                # Load into cache
                self._cache[client_id] = int(counter)
                loaded_count += 1
                logger.info(f"\033[32m[EOF-COUNTER-STORE] Loaded EOF counter for client {client_id}: {counter}\033[0m")
                
            except (json.JSONDecodeError, ValueError, KeyError, OSError) as exc:
                logger.warning("[EOF-COUNTER-STORE] Failed to load client file %s: %s", client_file, exc)
        
        logger.info("[EOF-COUNTER-STORE] Loaded EOF counters for %d clients", loaded_count)

    def _load_client(self, client_id: ClientId) -> int:
        """Load EOF counter for a client (from cache or disk)."""
        with self._lock:
            if client_id in self._cache:
                return self._cache[client_id]

            path = self._client_path(client_id)
            counter: int = 0
            
            if path.exists():
                try:
                    with path.open("r", encoding="utf-8") as fh:
                        logger.info(f"\033[32m[EOF-COUNTER-STORE] Loading EOF counter for client {client_id} from {path}\033[0m")
                        raw = json.load(fh)
                    
                    if not isinstance(raw, dict):
                        # Invalid format - treat as empty
                        logger.info("[EOF-COUNTER-STORE] Invalid format (not a dict) in file %s for client %s, treating as 0", 
                                     path, client_id)
                        counter = 0
                    else:
                        # Format with checksum
                        counter = int(raw.get('eof_count', 0))
                        logger.info(f"\033[32m[EOF-COUNTER-STORE] Loaded EOF counter for client {client_id}: {counter}\033[0m")
                except (json.JSONDecodeError, ValueError, PermissionError, OSError, IOError) as exc:
                    logger.warning("[EOF-COUNTER-STORE] Failed to load client %s: %s", client_id, exc)
                    counter = 0

            self._cache[client_id] = counter
            return counter

    def _persist_client_atomic(self, client_id: ClientId, counter: int) -> None:
        """
        Persist EOF counter for a client atomically using two-phase commit.
        
        Protocol:
        1. Write to temp file with fsync
        2. Validate temp file integrity (checksum)
        3. Atomically replace original with temp file (with backup)
        """
        client_path = self._client_path(client_id)
        temp_path = client_path.with_suffix('.temp.json')
        backup_path = client_path.with_suffix('.backup.json')
        
        try:
            # Create payload with checksum
            payload = {
                'client_id': client_id,
                'eof_count': counter
            }
            checksum = self._compute_checksum(payload)
            serialized = payload | {'checksum': checksum}
            
            # Phase 1: Write to temp file with fsync
            with temp_path.open('w', encoding='utf-8') as f:
                json.dump(serialized, f, ensure_ascii=False, sort_keys=True)
                f.flush()  # Flush Python buffer
                os.fsync(f.fileno())  # Force write to disk
            
            # Phase 2: Validate temp file integrity
            try:
                with temp_path.open('r', encoding='utf-8') as f:
                    temp_data = json.load(f)
                
                temp_payload = {
                    'client_id': temp_data.get('client_id'),
                    'eof_count': temp_data.get('eof_count', 0)
                }
                temp_checksum = temp_data.get('checksum')
                
                if temp_checksum != self._compute_checksum(temp_payload):
                    raise ValueError("Temp file checksum validation failed - file may be corrupted")
                    
            except (json.JSONDecodeError, ValueError, KeyError) as exc:
                logger.error("[EOF-COUNTER-STORE] [VALIDATION] Temp file validation failed for client %s: %s. Aborting persist.", 
                           client_id, exc)
                temp_path.unlink()
                raise ValueError(f"Temp file validation failed: {exc}") from exc
            
            # Phase 3: Atomic replace (only if temp file is valid)
            # Create backup of original file if it exists
            if client_path.exists():
                # Backup is created atomically
                os.replace(client_path, backup_path)
            
            # Replace original with validated temp file (atomic operation)
            os.replace(temp_path, client_path)
            
            logger.debug(f"[EOF-COUNTER-STORE] Persisted EOF counter for client {client_id}: {counter}")
            
        except Exception as exc:
            logger.info(f"\033[33m[EOF-COUNTER-STORE] [ERROR] Failed to persist EOF counter for client {client_id}: {exc}\033[0m")
            # Clean up temp file on error
            with contextlib.suppress(FileNotFoundError):
                temp_path.unlink()
            raise

    def get_counter(self, client_id: ClientId) -> int:
        """Get the current EOF counter for a client."""
        return self._load_client(client_id)

    def increment_counter(self, client_id: ClientId) -> int:
        """Increment the EOF counter for a client and persist it."""
        with self._lock:
            counter = self._load_client(client_id)
            counter += 1
            self._cache[client_id] = counter
            # Persist atomically
            self._persist_client_atomic(client_id, counter)
            return counter

    def reset_counter(self, client_id: ClientId) -> None:
        """Reset the EOF counter for a client to 0."""
        with self._lock:
            self._cache[client_id] = 0
            # Persist atomically
            self._persist_client_atomic(client_id, 0)
            logger.info(f"\033[32m[EOF-COUNTER-STORE] Reset EOF counter for client {client_id}\033[0m")

    def clear_client(self, client_id: ClientId) -> None:
        """Clear EOF counter for a client (remove file)."""
        with self._lock:
            self._cache.pop(client_id, None)
            path = self._client_path(client_id)
            backup_path = path.with_suffix('.backup.json')
            # Remove both main file and backup
            with contextlib.suppress(Exception):
                path.unlink()
            with contextlib.suppress(Exception):
                backup_path.unlink()
            logger.info(f"\033[32m[EOF-COUNTER-STORE] Cleared EOF counter for client {client_id}\033[0m")

