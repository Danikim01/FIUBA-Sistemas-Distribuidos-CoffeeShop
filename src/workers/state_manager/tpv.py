from workers.utils.message_utils import ClientId
from workers.state_manager.state_manager import StateManager


from collections import defaultdict
from pathlib import Path
from typing import Any, DefaultDict, Dict


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
            state_data = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))

        super().__init__(state_data, state_path, state_dir, worker_id, "tpv-worker-sharded")

    def _restore_client_state_from_map(self, client_id: ClientId, state_map: Any) -> None:
        """Restore a single client's TPV state from deserialized map."""
        if not isinstance(state_map, dict):
            return

        if self.state_data is None:
            self.state_data = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))

        client_state = self.state_data[client_id]

        for year_half, store_map in state_map.items():
            if not isinstance(year_half, str) or not isinstance(store_map, dict):
                continue

            store_bucket = client_state[year_half]
            for store_id_str, value in store_map.items():
                try:
                    store_bucket[int(store_id_str)] = float(value)
                except (TypeError, ValueError):
                    continue

    def _snapshot_client_state(self, client_id: ClientId) -> Dict[str, Dict[str, float]]:
        """Create serializable snapshot of a single client's TPV state."""
        if self.state_data is None:
            return {}

        client_state = self.state_data.get(client_id)
        if not client_state:
            return {}

        snapshot: Dict[str, Dict[str, float]] = {}
        for year_half, store_map in client_state.items():
            if not store_map:
                continue
            snapshot[year_half] = {
                str(store_id): float(value)
                for store_id, value in store_map.items()
            }

        return snapshot

    def drop_empty_client_state(self, client_id: ClientId) -> None:
        """Drop empty client state for TPV structure."""
        if self.state_data is None:
            return

        client_state = self.state_data.get(client_id)
        if not client_state:
            # Remove client state file if it exists
            client_path = self._get_client_state_path(client_id)
            if client_path.exists():
                client_path.unlink()
            return

        has_data = any(store_map for store_map in client_state.values() if store_map)
        if not has_data:
            self.state_data.pop(client_id, None)
            # Remove client state file
            client_path = self._get_client_state_path(client_id)
            if client_path.exists():
                client_path.unlink()

    def clone_client_state(self, client_id: ClientId) -> Dict[str, Dict[int, float]]:
        """Create deep copy of client state for rollback."""
        if self.state_data is None:
            return {}

        client_state = self.state_data.get(client_id)
        if not client_state:
            return {}

        snapshot: Dict[str, Dict[int, float]] = {}
        for year_half, store_map in client_state.items():
            snapshot[year_half] = dict(store_map)
        return snapshot

    def restore_client_state(self, client_id: ClientId, snapshot: Dict[str, Dict[int, float]]) -> None:
        """Restore client state from snapshot."""
        if self.state_data is None:
            self.state_data = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))

        if not snapshot:
            self.state_data.pop(client_id, None)
            return

        restored = defaultdict(lambda: defaultdict(float))
        for year_half, store_map in snapshot.items():
            restored_bucket = restored[year_half]
            for store_id, value in store_map.items():
                restored_bucket[int(store_id)] = float(value)

        # Assign restored state back to state_data
        self.state_data[client_id] = restored