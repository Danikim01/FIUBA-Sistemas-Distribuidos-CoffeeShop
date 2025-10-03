"""Utilities for top aggregation workers."""

from collections import defaultdict
from typing import Any, Dict

from workers.utils.base_worker import BaseWorker


class TopWorker(BaseWorker):
    """Base helper for top aggregation workers with per-client state storage."""

    def __init__(self) -> None:
        super().__init__()
        self._client_states: Dict[str, Dict[str, Any]] = defaultdict(dict)

    def get_client_state(self, client_id: str) -> Dict[str, Any]:
        """Return (and create if needed) the mutable state bucket for a client."""

        return self._client_states.setdefault(client_id, {})

    def reset_client_state(self, client_id: str) -> None:
        """Remove cached aggregation data for the given client."""

        self._client_states.pop(client_id, None)
