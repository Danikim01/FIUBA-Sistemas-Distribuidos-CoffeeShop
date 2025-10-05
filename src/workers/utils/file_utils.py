import json
import os
import re
import threading
from pathlib import Path
from typing import Dict, TypeVar, Generic

T = TypeVar("T")

_UUIDISH = re.compile(r"^[A-Fa-f0-9-]+$")

class DiskJSONStore(Generic[T]):
    """Atomic, crash-safe JSON key-value store per client_id."""

    _locks: Dict[str, threading.RLock] = {}

    def __init__(self, base_dir: str):
        self.base_path = Path(base_dir)
        self.base_path.mkdir(parents=True, exist_ok=True)

    # ---------- Helpers ----------

    def _lock_for(self, client_id: str) -> threading.RLock:
        if client_id not in self._locks:
            self._locks[client_id] = threading.RLock()
        return self._locks[client_id]

    def _file(self, client_id: str) -> Path:
        return self.base_path / f"{client_id}.json"

    def _tmp(self, client_id: str) -> Path:
        return self.base_path / f"{client_id}.json.tmp"

    def _bak(self, client_id: str) -> Path:
        return self.base_path / f"{client_id}.json.bak"

    # ---------- Core methods ----------

    def load(self, client_id: str) -> Dict[str, T]:
        """Load JSON file safely; restore from backup if needed."""
        path = self._file(client_id)
        bak = self._bak(client_id)

        if not path.exists():
            return {}

        try:
            with path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            # try backup
            if bak.exists():
                try:
                    with bak.open("r", encoding="utf-8") as f:
                        data = json.load(f)
                    self.save_atomic(client_id, data)
                    return data
                except Exception:
                    pass

            # rename corrupt file for inspection
            corrupt = path.with_suffix(".json.corrupt")
            try:
                if corrupt.exists():
                    corrupt.unlink()
                path.rename(corrupt)
            except Exception:
                pass
            return {}

    def save_atomic(self, client_id: str, data: Dict[str, T]) -> None:
        """Atomic write: write temp, backup, replace."""
        target = self._file(client_id)
        tmp = self._tmp(client_id)
        bak = self._bak(client_id)

        with tmp.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        if target.exists():
            try:
                bak.write_text(target.read_text(encoding="utf-8"), encoding="utf-8")
            except Exception:
                pass

        os.replace(tmp, target)

    def update(self, client_id: str, updater) -> None:
        """Thread-safe update via callback: updater(data_dict) -> None."""
        lock = self._lock_for(client_id)
        with lock:
            data = self.load(client_id)
            updater(data)
            self.save_atomic(client_id, data)

    @staticmethod
    def valid_uuidish(value: str | None) -> bool:
        return bool(value and _UUIDISH.match(str(value)))