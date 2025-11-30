from workers.utils.message_utils import ClientId
from workers.state_manager.state_manager import StateManager


from collections import defaultdict
from pathlib import Path
from typing import Any, DefaultDict, Dict


class ItemsStateManager(StateManager[tuple[DefaultDict[ClientId, DefaultDict[str, DefaultDict[int, int]]], DefaultDict[ClientId, DefaultDict[str, DefaultDict[int, float]]]]]):
    """
    Specialized state manager for Items worker state structure.

    State structure: Two separate structures:
    - Quantity: ClientId -> YearMonth -> ItemId -> int
    - Profit: ClientId -> YearMonth -> ItemId -> float
    """

    def __init__(self,
                 state_data: tuple[DefaultDict[ClientId, DefaultDict[str, DefaultDict[int, int]]],
                                   DefaultDict[ClientId, DefaultDict[str, DefaultDict[int, float]]]] | None = None,
                 state_path: Path | None = None,
                 state_dir: Path | None = None,
                 worker_id: str = "0"):
        # Create default state if not provided
        if state_data is None:
            quantity_totals = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
            profit_totals = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
            state_data = (quantity_totals, profit_totals)

        super().__init__(state_data, state_path, state_dir, worker_id, "items-worker-sharded")

    @property
    def quantity_totals(self) -> DefaultDict[ClientId, DefaultDict[str, DefaultDict[int, int]]]:
        """Get quantity totals structure."""
        if self.state_data is None:
            return defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        return self.state_data[0]

    @property
    def profit_totals(self) -> DefaultDict[ClientId, DefaultDict[str, DefaultDict[int, float]]]:
        """Get profit totals structure."""
        if self.state_data is None:
            return defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
        return self.state_data[1]

    def _restore_client_state_from_map(self, client_id: ClientId, state_map: Any) -> None:
        """Restore a single client's Items state from deserialized map."""
        if not isinstance(state_map, dict):
            return

        if self.state_data is None:
            quantity_totals = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
            profit_totals = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
            self.state_data = (quantity_totals, profit_totals)

        quantity_state = self.quantity_totals[client_id]
        profit_state = self.profit_totals[client_id]

        # State map contains both quantity and profit
        quantity_map = state_map.get('quantity', {})
        profit_map = state_map.get('profit', {})

        # Restore quantity totals
        if isinstance(quantity_map, dict):
            for year_month, items_map in quantity_map.items():
                if not isinstance(year_month, str) or not isinstance(items_map, dict):
                    continue

                item_bucket = quantity_state[year_month]
                for item_id_str, quantity in items_map.items():
                    try:
                        item_bucket[int(item_id_str)] = int(quantity)
                    except (TypeError, ValueError):
                        continue

        # Restore profit totals
        if isinstance(profit_map, dict):
            for year_month, items_map in profit_map.items():
                if not isinstance(year_month, str) or not isinstance(items_map, dict):
                    continue

                item_bucket = profit_state[year_month]
                for item_id_str, profit in items_map.items():
                    try:
                        item_bucket[int(item_id_str)] = float(profit)
                    except (TypeError, ValueError):
                        continue

    def _snapshot_client_state(self, client_id: ClientId) -> Dict[str, Dict[str, Dict[str, int | float]]]:
        """Create serializable snapshot of a single client's Items state."""
        if self.state_data is None:
            return {}

        quantity_state = self.quantity_totals.get(client_id)
        profit_state = self.profit_totals.get(client_id)

        if not quantity_state and not profit_state:
            return {}

        snapshot: Dict[str, Dict[str, Dict[str, int | float]]] = {}

        # Snapshot quantity
        quantity_snapshot: Dict[str, Dict[str, int]] = {}
        if quantity_state:
            for year_month, items_map in quantity_state.items():
                if not items_map:
                    continue
                quantity_snapshot[year_month] = {
                    str(item_id): int(quantity)
                    for item_id, quantity in items_map.items()
                }

        # Snapshot profit
        profit_snapshot: Dict[str, Dict[str, float]] = {}
        if profit_state:
            for year_month, items_map in profit_state.items():
                if not items_map:
                    continue
                profit_snapshot[year_month] = {
                    str(item_id): float(profit)
                    for item_id, profit in items_map.items()
                }

        if quantity_snapshot or profit_snapshot:
            snapshot = {
                'quantity': quantity_snapshot, # type: ignore
                'profit': profit_snapshot
            }

        return snapshot

    def drop_empty_client_state(self, client_id: ClientId) -> None:
        """Drop empty client state for Items structure."""
        if self.state_data is None:
            return

        quantity_state = self.quantity_totals.get(client_id)
        profit_state = self.profit_totals.get(client_id)

        has_quantity = quantity_state and any(items_map for items_map in quantity_state.values() if items_map)
        has_profit = profit_state and any(items_map for items_map in profit_state.values() if items_map)

        if not has_quantity and not has_profit:
            self.quantity_totals.pop(client_id, None)
            self.profit_totals.pop(client_id, None)
            # Remove client state file
            client_path = self._get_client_state_path(client_id)
            if client_path.exists():
                client_path.unlink()

    def clone_client_state(self, client_id: ClientId) -> tuple[Dict[str, Dict[int, int]], Dict[str, Dict[int, float]]]:
        """Create deep copy of client state for rollback."""
        if self.state_data is None:
            return ({}, {})

        quantity_state = self.quantity_totals.get(client_id)
        profit_state = self.profit_totals.get(client_id)

        quantity_snapshot: Dict[str, Dict[int, int]] = {}
        if quantity_state:
            for year_month, items_map in quantity_state.items():
                quantity_snapshot[year_month] = dict(items_map)

        profit_snapshot: Dict[str, Dict[int, float]] = {}
        if profit_state:
            for year_month, items_map in profit_state.items():
                profit_snapshot[year_month] = dict(items_map)

        return (quantity_snapshot, profit_snapshot)

    def restore_client_state(self, client_id: ClientId, snapshot: tuple[Dict[str, Dict[int, int]], Dict[str, Dict[int, float]]]) -> None:
        """Restore client state from snapshot."""
        if self.state_data is None:
            quantity_totals = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
            profit_totals = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
            self.state_data = (quantity_totals, profit_totals)

        quantity_snapshot, profit_snapshot = snapshot

        if not quantity_snapshot and not profit_snapshot:
            self.quantity_totals.pop(client_id, None)
            self.profit_totals.pop(client_id, None)
            return

        # Restore quantity
        restored_quantity = defaultdict(lambda: defaultdict(int))
        for year_month, items_map in quantity_snapshot.items():
            restored_bucket = restored_quantity[year_month]
            for item_id, quantity in items_map.items():
                restored_bucket[int(item_id)] = int(quantity)
        self.quantity_totals[client_id] = restored_quantity

        # Restore profit
        restored_profit = defaultdict(lambda: defaultdict(float))
        for year_month, items_map in profit_snapshot.items():
            restored_bucket = restored_profit[year_month]
            for item_id, profit in items_map.items():
                restored_bucket[int(item_id)] = float(profit)
        self.profit_totals[client_id] = restored_profit

        self.mark_client_modified(client_id)