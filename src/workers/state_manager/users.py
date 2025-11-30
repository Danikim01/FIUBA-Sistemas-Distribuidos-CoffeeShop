from workers.utils.message_utils import ClientId
from workers.state_manager.state_manager import StateManager


from collections import defaultdict
from pathlib import Path
from typing import Any, DefaultDict, Dict


class UsersStateManager(StateManager[DefaultDict[ClientId, DefaultDict[int, DefaultDict[int, int]]]]):
    """
    Specialized state manager for Users worker state structure.

    State structure: ClientId -> StoreId -> UserId -> int (purchase count)
    """

    def __init__(self,
                 state_data: DefaultDict[ClientId, DefaultDict[int, DefaultDict[int, int]]] | None = None,
                 state_path: Path | None = None,
                 state_dir: Path | None = None,
                 worker_id: str = "0"):
        # Create default state if not provided
        if state_data is None:
            state_data = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

        super().__init__(state_data, state_path, state_dir, worker_id, "users-worker-sharded")

    def _restore_client_state_from_map(self, client_id: ClientId, state_map: Any) -> None:
        """Restore a single client's Users state from deserialized map."""
        if not isinstance(state_map, dict):
            return

        if self.state_data is None:
            self.state_data = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

        client_state = self.state_data[client_id]

        for store_id_str, user_map in state_map.items():
            if not isinstance(store_id_str, str) or not isinstance(user_map, dict):
                continue

            try:
                store_id = int(store_id_str)
            except (TypeError, ValueError):
                continue

            user_bucket = client_state[store_id]
            for user_id_str, count in user_map.items():
                try:
                    user_bucket[int(user_id_str)] = int(count)
                except (TypeError, ValueError):
                    continue

    def _snapshot_client_state(self, client_id: ClientId) -> Dict[str, Dict[str, int]]:
        """Create serializable snapshot of a single client's Users state."""
        if self.state_data is None:
            return {}

        client_state = self.state_data.get(client_id)
        if not client_state:
            return {}

        snapshot: Dict[str, Dict[str, int]] = {}
        for store_id, user_map in client_state.items():
            if not user_map:
                continue
            snapshot[str(store_id)] = {
                str(user_id): int(count)
                for user_id, count in user_map.items()
            }

        return snapshot

    def drop_empty_client_state(self, client_id: ClientId) -> None:
        """Drop empty client state for Users structure."""
        if self.state_data is None:
            return

        client_state = self.state_data.get(client_id)
        if not client_state:
            # Remove client state file if it exists
            client_path = self._get_client_state_path(client_id)
            if client_path.exists():
                client_path.unlink()
            return

        has_data = any(user_map for user_map in client_state.values() if user_map)
        if not has_data:
            self.state_data.pop(client_id, None)
            # Remove client state file
            client_path = self._get_client_state_path(client_id)
            if client_path.exists():
                client_path.unlink()

    def clone_client_state(self, client_id: ClientId) -> Dict[int, Dict[int, int]]:
        """Create deep copy of client state for rollback."""
        if self.state_data is None:
            return {}

        client_state = self.state_data.get(client_id)
        if not client_state:
            return {}

        snapshot: Dict[int, Dict[int, int]] = {}
        for store_id, user_map in client_state.items():
            snapshot[store_id] = dict(user_map)
        return snapshot

    def restore_client_state(self, client_id: ClientId, snapshot: Dict[int, Dict[int, int]]) -> None:
        """Restore client state from snapshot."""
        if self.state_data is None:
            self.state_data = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

        if not snapshot:
            self.state_data.pop(client_id, None)
            return

        restored = defaultdict(lambda: defaultdict(int))
        for store_id, user_map in snapshot.items():
            restored_bucket = restored[store_id]
            for user_id, count in user_map.items():
                restored_bucket[int(user_id)] = int(count)

        self.state_data[client_id] = restored
        self.mark_client_modified(client_id)