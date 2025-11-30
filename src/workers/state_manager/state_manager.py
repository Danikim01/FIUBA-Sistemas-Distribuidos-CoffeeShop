#!/usr/bin/env python3

"""State manager with per-client persistence for better scalability."""

import contextlib
import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, TypeVar, Generic

from workers.utils.message_utils import ClientId

logger = logging.getLogger(__name__)

# Type variables for generic state management
T = TypeVar('T')  # Type for the state data structure


class StateManager(Generic[T]):
    """
    State manager with per-client persistence.
    
    Instead of persisting the entire state in a single file, this manager:
    - Persists each client's state in a separate file
    - Stores message UUIDs directly in each client's file (no separate metadata file)
    - Only persists clients that have been modified
    - Provides atomic writes per client
    
    This approach scales much better when dealing with many clients,
    as it avoids rewriting the entire state when only one client changes.
    The UUID is stored in the client file itself to ensure atomicity - if the worker
    crashes after persisting the client state, the UUID is still available for duplicate detection.
    """
    
    def __init__(self, 
                 state_data: T | None = None,
                 state_path: Path | None = None,
                 state_dir: Path | None = None,
                 worker_id: str = "0",
                 worker_type: str = "worker"):
        """
        Initialize the state manager.
        
        Args:
            state_data: The state data structure to manage (optional, will be created if None)
            state_path: Specific path for state file (not used in improved version, kept for compatibility)
            state_dir: Directory for state files
            worker_id: Worker identifier for unique state files
            worker_type: Type of worker for naming
        """
        logger.info("[STATE-MANAGER] [INIT] Initializing StateManager for worker_id=%s, worker_type=%s", 
                   worker_id, worker_type)
        self.state_data = state_data
        self.worker_id = worker_id
        self.worker_type = worker_type
        
        # Configure state directory
        if state_dir:
            self._state_dir = state_dir
        else:
            self._state_dir = Path(f"state/{worker_type}-{worker_id}")
                
        # Last processed message UUIDs (cached in memory, stored in client files)
        self._last_processed_message: Dict[ClientId, str] = {}
        
        self._ensure_state_dir()
        # Clean up any leftover temp files from previous crashes
        self._cleanup_temp_files()
        self._load_state()
        
        # If state_data was provided but we loaded from disk, update the reference
        if state_data is not None and self.state_data is not state_data:
            # Update the original reference to point to our managed state
            if hasattr(state_data, 'clear') and hasattr(state_data, 'update'):
                state_data.clear() # type: ignore
                state_data.update(self.state_data) # type: ignore
    
    def _ensure_state_dir(self) -> None:
        """Ensure the state directory exists."""
        try:
            self._state_dir.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            logger.error("Cannot create state directory %s: %s", self._state_dir, exc)
            raise
    
    def _cleanup_temp_files(self) -> None:
        """Clean up any leftover temporary files from previous crashes."""
        # Clean up temp files in state directory
        temp_files = list(self._state_dir.glob("*.temp.json"))
        for temp_file in temp_files:
            try:
                logger.warning("[CLEANUP] Removing leftover temp file from previous crash: %s", temp_file)
                temp_file.unlink()
            except Exception as exc:
                logger.warning("[CLEANUP] Failed to remove temp file %s: %s", temp_file, exc)
        
    
    def _get_client_state_path(self, client_id: ClientId) -> Path:
        """Get the path for a client's state file."""
        # Sanitize client_id for filesystem safety
        # Replace problematic characters with underscores
        safe_client_id = client_id.replace('/', '_').replace('\\', '_').replace(':', '_')
        return self._state_dir / f"client_{safe_client_id}.json"
    
    def _load_state(self) -> None:
        """Load state from disk - loads client states and UUIDs from client files."""
        self._load_all_client_states()
        
    def _load_all_client_states(self) -> None:
        """
        Load all client states from disk.
        
        Recovery strategy:
        1. Try to load from main client file
        2. If main file is corrupted, try backup file
        3. If both are corrupted, skip this client (state will be empty)
        4. Skip temp files (they indicate an incomplete write)
        """
        if self.state_data is None:
            return
        
        # Find all client state files (exclude temp and backup files)
        client_files = [
            f for f in self._state_dir.glob("client_*.json")
            if not f.name.endswith('.temp.json') and not f.name.endswith('.backup.json')
        ]
        
        if not client_files:
            logger.info("[LOAD-STATE] No client state files found")
            return
        
        loaded_count = 0
        for client_file in client_files:
            client_id = None
            try:
                # Try main file first
                with client_file.open('r', encoding='utf-8') as f:
                    raw = json.load(f)
                
                if not isinstance(raw, dict):
                    logger.warning("[LOAD-STATE] Invalid JSON structure in %s, trying backup", client_file)
                    raise ValueError("Invalid structure")
                
                # Get client_id from file (stored in payload)
                client_id = raw.get('client_id')
                if not client_id:
                    # Fallback: extract from filename (for backwards compatibility)
                    client_id = client_file.stem.replace("client_", "")
                
                state_section = raw.get('state')
                last_uuid = raw.get('last_processed_uuid')  # UUID stored in client file (may be None for old files)
                checksum = raw.get('checksum')
                
                # Validate checksum (includes UUID for atomicity, but handles old files without UUID)
                payload = {
                    'client_id': client_id,
                    'state': state_section,
                    'last_processed_uuid': last_uuid  # None for old files is OK
                }
                if checksum != self._compute_checksum(payload):
                    logger.warning("[LOAD-STATE] Checksum mismatch for client %s in %s, trying backup", 
                                 client_id, client_file)
                    raise ValueError("Checksum mismatch")
                
                # Restore client state (subclass should handle this)
                self._restore_client_state_from_map(client_id, state_section)
                
                # CRITICAL: Restore UUID from client file (UUIDs are now stored only in client files)
                if last_uuid:
                    self._last_processed_message[client_id] = last_uuid
                    logger.debug("[LOAD-STATE] Restored UUID %s for client %s from client file", 
                              last_uuid, client_id)
                
                loaded_count += 1
                
            except (json.JSONDecodeError, ValueError, KeyError, OSError) as exc:
                # Try backup file if main file failed
                backup_file = client_file.with_suffix('.backup.json')
                if backup_file.exists():
                    try:
                        logger.info("[LOAD-STATE] [RECOVERY] Attempting to recover client %s from backup file", 
                                   client_id or client_file.stem)
                        with backup_file.open('r', encoding='utf-8') as f:
                            raw = json.load(f)
                        
                        if not isinstance(raw, dict):
                            raise ValueError("Invalid backup structure")
                        
                        backup_client_id = raw.get('client_id') or client_file.stem.replace("client_", "")
                        state_section = raw.get('state')
                        backup_last_uuid = raw.get('last_processed_uuid')
                        checksum = raw.get('checksum')
                        
                        payload = {
                            'client_id': backup_client_id,
                            'state': state_section,
                            'last_processed_uuid': backup_last_uuid
                        }
                        if checksum == self._compute_checksum(payload):
                            # Backup is valid, restore from it
                            self._restore_client_state_from_map(backup_client_id, state_section)
                            # Restore backup as main file
                            os.replace(backup_file, client_file)
                            logger.info("[LOAD-STATE] [RECOVERY] Successfully recovered client %s from backup", 
                                      backup_client_id)
                            
                            # Restore UUID from backup file (UUIDs are now stored only in client files)
                            if backup_last_uuid:
                                self._last_processed_message[backup_client_id] = backup_last_uuid
                                logger.debug("[LOAD-STATE] [RECOVERY] Restored UUID %s for client %s from backup file", 
                                          backup_last_uuid, backup_client_id)
                            
                            loaded_count += 1
                        else:
                            logger.error("[LOAD-STATE] [RECOVERY] Backup file also corrupted for client %s", 
                                       backup_client_id)
                    except Exception as backup_exc:
                        logger.error("[LOAD-STATE] [RECOVERY] Failed to recover from backup for client %s: %s", 
                                   client_id or client_file.stem, backup_exc)
                else:
                    logger.warning("[LOAD-STATE] Failed to load client state from %s: %s (no backup available)", 
                                 client_file, exc)
        
        logger.info("[LOAD-STATE] Loaded %d client states from disk", loaded_count)
    
    def _restore_client_state_from_map(self, client_id: ClientId, state_map: Any) -> None:
        """
        Restore a single client's state from deserialized map.
        This method should be overridden by subclasses.
        """
        # Default implementation - subclasses should override this
        pass
    
    def _snapshot_client_state(self, client_id: ClientId) -> Any:
        """
        Create a snapshot of a single client's state for serialization.
        This method should be overridden by subclasses.
        """
        # Default implementation - subclasses should override this
        return {}
    
    def persist_state(self, client_id: ClientId | None = None) -> None:
        """
        Persist state to disk.
        
        Args:
            client_id: If provided, persist only this client's state.
                      If None, persist all modified clients.
        
        Note: UUIDs are stored directly in the client file, not in a separate
        metadata file. This ensures atomicity - if the worker crashes after
        persisting the client state, the UUID is still available for duplicate detection.
        """
        self._persist_client_state(client_id or "all")
    
    def _persist_client_state(self, client_id: ClientId) -> None:
        """
        Persist a single client's state atomically.
        
        Uses a two-phase commit approach:
        1. Write to temp file with fsync
        2. Validate temp file integrity (checksum)
        3. Atomically replace original with temp file
        
        CRITICAL: Includes the last_processed_message_uuid in the client file
        to ensure atomicity. UUIDs are stored directly in the client file (not in
        a separate metadata file) so that if the process crashes after persisting
        the client state, we can still detect duplicates by checking the UUID in the
        client file itself.
        
        If process crashes:
        - During temp write: temp file will be corrupted, but original remains intact
        - During replace: original file remains intact (os.replace is atomic)
        - On restart: temp files are cleaned up, valid backup is used
        """
        client_path = self._get_client_state_path(client_id)
        temp_path = client_path.with_suffix('.temp.json')
        backup_path = client_path.with_suffix('.backup.json')
        
        try:
            # Create snapshot of client state
            state_snapshot = self._snapshot_client_state(client_id)
            
            # CRITICAL: Include UUID in client file for atomicity
            # UUIDs are stored directly in the client file (not in a separate metadata file)
            # This ensures we can detect duplicates even if the worker crashes
            last_uuid = self._last_processed_message.get(client_id)
            
            # Include client_id and UUID in payload for recovery and duplicate detection
            payload = {
                'client_id': client_id,
                'state': state_snapshot,
                'last_processed_uuid': last_uuid  # Include UUID for atomicity
            }
            checksum = self._compute_checksum(payload)
            serialized = payload | {'checksum': checksum}
            
            # Phase 1: Write to temp file with fsync to ensure data is on disk
            with temp_path.open('w', encoding='utf-8') as f:
                json.dump(serialized, f, ensure_ascii=False, sort_keys=True)
                f.flush()  # Flush Python buffer
                os.fsync(f.fileno())  # Force write to disk
            
            # Phase 2: Validate temp file integrity before replacing
            # This ensures we don't replace a good file with a corrupted one
            try:
                with temp_path.open('r', encoding='utf-8') as f:
                    temp_data = json.load(f)
                
                temp_payload = {
                    'client_id': temp_data.get('client_id'),
                    'state': temp_data.get('state'),
                    'last_processed_uuid': temp_data.get('last_processed_uuid')  # May be None for old files
                }
                temp_checksum = temp_data.get('checksum')
                
                if temp_checksum != self._compute_checksum(temp_payload):
                    raise ValueError("Temp file checksum validation failed - file may be corrupted")
                
            except (json.JSONDecodeError, ValueError, KeyError) as exc:
                logger.error("[PERSIST-CLIENT] [VALIDATION] Temp file validation failed for client %s: %s. Aborting persist.", 
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
            
        except Exception as exc:
            logger.error("[PERSIST-CLIENT] [ERROR] Failed to persist state for client %s: %s", 
                        client_id, exc)
            # Clean up temp file on error
            with contextlib.suppress(FileNotFoundError):
                temp_path.unlink()
            raise
            
    def get_last_processed_message(self, client_id: ClientId) -> str | None:
        """
        Get the last processed message UUID for a client.
        
        Checks the client file directly (UUIDs are stored in client files, not in
        a separate metadata file). This ensures we can detect duplicates even if
        the worker crashed after persisting the client state.
        
        The result is cached in memory for performance.
        """
        # First check cache (fast path)
        uuid = self._last_processed_message.get(client_id)
        if uuid:
            logger.info(f"\033[32m[GET-UUID] Found UUID {uuid} for client {client_id} in memory cache\033[0m")
            return uuid
        
        # Read from client file (source of truth)
        client_path = self._get_client_state_path(client_id)
        if client_path.exists():
            try:
                with client_path.open('r', encoding='utf-8') as f:
                    raw = json.load(f)
                file_uuid = raw.get('last_processed_uuid')
                if file_uuid:
                    # Update cache for next time
                    self._last_processed_message[client_id] = file_uuid
                    logger.info(f"\033[32m[GET-UUID] Found UUID {file_uuid} for client {client_id} in client file\033[0m")
                    return file_uuid
            except Exception as exc:
                logger.info(f"\033[33m[GET-UUID] Failed to read UUID from client file {client_path}: {exc}\033[0m")
        
        return None
    
    def set_last_processed_message(self, client_id: ClientId, message_uuid: str) -> None:
        """Set the last processed message UUID for a client."""
        self._last_processed_message[client_id] = message_uuid
    
    def clear_last_processed_message(self, client_id: ClientId) -> None:
        """Clear the last processed message UUID for a client."""
        self._last_processed_message.pop(client_id, None)
    
    def drop_empty_client_state(self, client_id: ClientId) -> None:
        """
        Drop empty client state if it exists.
        This method should be overridden by subclasses to handle specific state structures.
        """
        # Default implementation - subclasses should override this
        pass
    
    def clone_client_state(self, client_id: ClientId) -> Any:
        """
        Create a deep copy of the client state for rollback purposes.
        This method should be overridden by subclasses to handle specific state structures.
        """
        # Default implementation - subclasses should override this
        return None
    
    def restore_client_state(self, client_id: ClientId, snapshot: Any) -> None:
        """
        Restore client state from a snapshot for rollback purposes.
        This method should be overridden by subclasses to handle specific state structures.
        """
        # Default implementation - subclasses should override this
        pass

    def clear_client_files(self, client_id: ClientId) -> None:
        """Remove persisted files for a specific client."""
        client_path = self._get_client_state_path(client_id)
        backup_path = client_path.with_suffix(".backup.json")
        temp_path = client_path.with_suffix(".temp.json")
        for path in (client_path, backup_path, temp_path):
            with contextlib.suppress(Exception):
                path.unlink()

    def clear_all_files(self) -> None:
        """Remove all persisted client files, backups, and metadata temp files."""
        patterns = ("client_*.json", "client_*.backup.json", "client_*.temp.json")
        for pattern in patterns:
            for path in self._state_dir.glob(pattern):
                with contextlib.suppress(Exception):
                    path.unlink()
        for meta_path in (self._state_dir / "metadata.json", self._state_dir / "metadata.backup.json", self._state_dir / "metadata.temp.json"):
            with contextlib.suppress(Exception):
                meta_path.unlink()

    def clear_last_processed_messages(self) -> None:
        """Clear cached message UUIDs for all clients."""
        self._last_processed_message.clear()
    
    def get_state_data(self) -> T | None:
        """Get the current state data."""
        return self.state_data
    
    def update_state_data(self, new_state: T) -> None:
        """Update the state data (used for restoration)."""
        self.state_data = new_state
    
    @staticmethod
    def _compute_checksum(payload: Dict[str, Any]) -> str:
        """Compute SHA256 checksum for payload validation."""
        serialized = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')
        return hashlib.sha256(serialized).hexdigest()


