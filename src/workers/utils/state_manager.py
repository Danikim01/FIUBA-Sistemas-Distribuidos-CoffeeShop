#!/usr/bin/env python3

"""Generic state management utilities for workers with fault tolerance."""

import contextlib
import hashlib
import json
import logging
import os
from collections import defaultdict
from pathlib import Path
from typing import Any, DefaultDict, Dict, TypeVar, Generic

from message_utils import ClientId

logger = logging.getLogger(__name__)

# Type variables for generic state management
T = TypeVar('T')  # Type for the state data structure
K = TypeVar('K')  # Type for keys in nested dictionaries
V = TypeVar('V')  # Type for values in nested dictionaries


class StateManager(Generic[T]):
    """
    Generic state manager that provides fault-tolerant state persistence and recovery.
    
    This class handles:
    - State serialization/deserialization
    - Atomic state persistence with backup
    - State recovery from disk
    - Checksum validation for data integrity
    - Thread-safe state operations
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
            state_path: Specific path for state file
            state_dir: Directory for state files
            worker_id: Worker identifier for unique state files
            worker_type: Type of worker for naming
        """
        self.state_data = state_data
        self.worker_id = worker_id
        self.worker_type = worker_type
        
        # Configure state file paths
        if state_path:
            self._state_dir = state_path.parent if state_path.parent != Path() else Path(".")
            self._state_path = state_path
        elif state_dir:
            if state_dir.suffix == '.bin':
                self._state_dir = state_dir.parent if state_dir.parent != Path() else Path(".")
                self._state_path = state_dir
            else:
                self._state_dir = state_dir
                self._state_path = self._state_dir / "state.bin"
        else:
            self._state_dir = Path(f"state/{worker_type}-{worker_id}")
            self._state_path = self._state_dir / "state.bin"
        
        self._backup_path = self._state_path.with_name("state.backup.bin")
        self._temp_path = self._state_path.with_name("state.temp.bin")
        self._last_processed_message: Dict[ClientId, str] = {}
        
        self._ensure_state_dir()
        self._load_state()
        
        # If state_data was provided but we loaded from disk, update the reference
        if state_data is not None and self.state_data is not state_data:
            # Update the original reference to point to our managed state
            state_data.clear() # pyright: ignore[reportAttributeAccessIssue]
            state_data.update(self.state_data) # pyright: ignore[reportAttributeAccessIssue]
    
    def _ensure_state_dir(self) -> None:
        """Ensure the state directory exists."""
        try:
            self._state_dir.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            logger.error("Cannot create state directory %s: %s", self._state_dir, exc)
            raise
    
    def _load_state(self) -> None:
        """Load state from disk, trying main file first, then backup."""
        for path in (self._state_path, self._backup_path):
            if not path.exists():
                continue
            try:
                id_map, state_map = self._read_state_payload(path)
                self._last_processed_message = dict(id_map)
                self._restore_state_from_map(state_map)
                logger.info("Loaded persisted state from %s", path)
                return
            except Exception as exc:
                logger.warning("Failed to load state from %s: %s", path, exc)
        logger.info("No valid state found; starting with empty state")
    
    def _read_state_payload(
        self,
        path: Path,
    ) -> tuple[Dict[ClientId, str], Dict[ClientId, Any]]:
        """Read and validate state payload from file."""
        with path.open('r', encoding='utf-8') as handle:
            raw = json.load(handle)

        if not isinstance(raw, dict):
            raise ValueError("State file is not a JSON object")

        id_section = raw.get('id', {})
        state_section = raw.get('state', {})
        checksum = raw.get('checksum')

        payload = {'id': id_section, 'state': state_section}
        if checksum != self._compute_checksum(payload):
            raise ValueError("Checksum mismatch")

        if not isinstance(id_section, dict) or not isinstance(state_section, dict):
            raise ValueError("Invalid state payload structure")

        # Sanitize client IDs
        sanitized_ids: Dict[ClientId, str] = {}
        for client_id, message_uuid in id_section.items():
            if isinstance(client_id, str) and isinstance(message_uuid, str):
                sanitized_ids[client_id] = message_uuid

        return sanitized_ids, state_section
    
    def _restore_state_from_map(self, state_map: Dict[ClientId, Any]) -> None:
        """
        Restore state from a deserialized state map.
        This method should be overridden by subclasses to handle specific state structures.
        """
        # Default implementation - subclasses should override this
        pass
    
    def _snapshot_state(self) -> Dict[ClientId, Any]:
        """
        Create a snapshot of the current state for serialization.
        This method should be overridden by subclasses to handle specific state structures.
        """
        # Default implementation - subclasses should override this
        return {}
    
    def persist_state(self) -> None:
        """Atomically persist the current state to disk."""
        payload = {
            'id': dict(self._last_processed_message),
            'state': self._snapshot_state(),
        }
        checksum = self._compute_checksum(payload)
        serialized = payload | {'checksum': checksum}

        self._ensure_state_dir()
        try:
            with self._temp_path.open('w', encoding='utf-8') as handle:
                json.dump(serialized, handle, ensure_ascii=False, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())

            if self._state_path.exists():
                os.replace(self._state_path, self._backup_path)

            os.replace(self._temp_path, self._state_path)
        except Exception as exc:
            logger.error("Failed to persist state to disk: %s", exc)
            with contextlib.suppress(FileNotFoundError):
                self._temp_path.unlink()
            raise
    
    def get_last_processed_message(self, client_id: ClientId) -> str | None:
        """Get the last processed message UUID for a client."""
        return self._last_processed_message.get(client_id)
    
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


class TPVStateManager(StateManager[DefaultDict[ClientId, DefaultDict[str, DefaultDict[int, float]]]]):
    """
    Specialized state manager for TPV worker state structure.
    
    State structure: ClientId -> YearHalf -> StoreId -> float
    """
    
    def __init__(self, 
                 state_data: DefaultDict[ClientId, DefaultDict[str, DefaultDict[int, float]]] | None = None,
                 state_path: Path | None = None,
                 state_dir: Path | None = None,
                 worker_id: str = "0"):
        # Create default state if not provided
        if state_data is None:
            from collections import defaultdict
            state_data = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
        
        super().__init__(state_data, state_path, state_dir, worker_id, "tpv-worker-sharded")
    
    def _restore_state_from_map(self, state_map: Dict[ClientId, Any]) -> None:
        """Restore TPV state from deserialized map."""
        from collections import defaultdict
        
        restored: DefaultDict[ClientId, DefaultDict[str, DefaultDict[int, float]]] = \
            defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
        
        for client_id, year_map in state_map.items():
            if not isinstance(client_id, str) or not isinstance(year_map, dict):
                continue
                
            for year_half, store_map in year_map.items():
                if not isinstance(year_half, str) or not isinstance(store_map, dict):
                    continue
                    
                store_bucket = restored[client_id][year_half]
                for store_id_str, value in store_map.items():
                    try:
                        store_bucket[int(store_id_str)] = float(value)
                    except (TypeError, ValueError):
                        continue
        
        # Update the state data
        if self.state_data is not None:
            self.state_data.clear()
            self.state_data.update(restored)
        else:
            self.state_data = restored
    
    def sync_state_after_load(self) -> None:
        """
        Sync the state after loading from disk.
        Ensures that state_data is a defaultdict structure after restoration.
        This is important because after loading from JSON, we may have regular dicts.
        """
        from collections import defaultdict
        
        # If state_data is None, create a new defaultdict structure
        if self.state_data is None:
            self.state_data = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
            return
        
        # If it's not already a defaultdict, convert it
        if not isinstance(self.state_data, defaultdict):
            # Convert to defaultdict structure
            converted = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
            for client_id, year_map in self.state_data.items():
                if isinstance(year_map, dict):
                    for year_half, store_map in year_map.items():
                        if isinstance(store_map, dict):
                            for store_id, value in store_map.items():
                                converted[client_id][year_half][store_id] = value
            self.state_data = converted
            return
        
        # If it's already a defaultdict, ensure nested structures are also defaultdicts
        # (in case only the outer dict was converted)
        for client_id in list(self.state_data.keys()):
            year_map = self.state_data[client_id]
            if not isinstance(year_map, defaultdict):
                # Convert inner dict to defaultdict
                converted_year = defaultdict(lambda: defaultdict(float))
                for year_half, store_map in year_map.items():
                    if isinstance(store_map, dict):
                        converted_year[year_half] = defaultdict(float, store_map)
                    else:
                        converted_year[year_half] = store_map
                self.state_data[client_id] = converted_year
            else:
                # Ensure innermost structures are also defaultdicts
                for year_half in list(year_map.keys()):
                    store_map = year_map[year_half]
                    if not isinstance(store_map, defaultdict):
                        year_map[year_half] = defaultdict(float, store_map)
    
    def _snapshot_state(self) -> Dict[ClientId, Dict[str, Dict[str, float]]]:
        """Create serializable snapshot of TPV state."""
        snapshot: Dict[ClientId, Dict[str, Dict[str, float]]] = {}
        
        for client_id, year_map in self.state_data.items():
            serializable_year_map: Dict[str, Dict[str, float]] = {}
            for year_half, store_map in year_map.items():
                if not store_map:
                    continue
                serializable_year_map[year_half] = {
                    str(store_id): float(value)
                    for store_id, value in store_map.items()
                }
            if serializable_year_map:
                snapshot[client_id] = serializable_year_map
        
        return snapshot
    
    def drop_empty_client_state(self, client_id: ClientId) -> None:
        """Drop empty client state for TPV structure."""
        client_state = self.state_data.get(client_id)
        if not client_state:
            self.state_data.pop(client_id, None)
            return

        has_data = any(store_map for store_map in client_state.values() if store_map)
        if not has_data:
            self.state_data.pop(client_id, None)
    
    def clone_client_state(self, client_id: ClientId) -> Dict[str, Dict[int, float]]:
        """Create deep copy of client state for rollback."""
        client_state = self.state_data.get(client_id)
        if not client_state:
            return {}

        snapshot: Dict[str, Dict[int, float]] = {}
        for year_half, store_map in client_state.items():
            snapshot[year_half] = dict(store_map)
        return snapshot
    
    def restore_client_state(self, client_id: ClientId, snapshot: Dict[str, Dict[int, float]]) -> None:
        """Restore client state from snapshot."""
        if not snapshot:
            self.state_data.pop(client_id, None)
            return

        from collections import defaultdict
        restored = defaultdict(lambda: defaultdict(float))
        for year_half, store_map in snapshot.items():
            restored_bucket = restored[year_half]
            for store_id, value in store_map.items():
                restored_bucket[int(store_id)] = float(value)

        self.state_data[client_id] = restored
