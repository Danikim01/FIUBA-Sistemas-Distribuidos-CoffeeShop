"""Shared utilities for top aggregation workers."""

from collections import defaultdict
from typing import Any, Callable, DefaultDict, Dict, TypeVar
from workers.utils.base_worker import BaseWorker

StateDict = Dict[str, Any]
T = TypeVar("T")

class TopWorker(BaseWorker):
    """Base class for single-source top workers with per-client state helpers."""

    def __init__(self) -> None:
        super().__init__()
        self._client_state: DefaultDict[str, StateDict] = defaultdict(dict)

    def get_state_bucket(self, client_id: str) -> StateDict:
        """Return (and create) the mutable state dictionary for a client."""

        return self._client_state.setdefault(client_id, {})

    def get_or_create(self, client_id: str, key: str, factory: Callable[[], T]) -> T:
        """Return a state entry creating it with ``factory`` when absent."""

        bucket = self.get_state_bucket(client_id)
        if key not in bucket:
            bucket[key] = factory()
        return bucket[key]

    def reset_client(self, client_id: str) -> None:
        """Remove cached state for ``client_id`` after emitting results."""

        self._client_state.pop(client_id, None)
