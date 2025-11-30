import csv
import logging
import os
import threading
from pathlib import Path
from workers.utils.message_utils import ClientId

logger = logging.getLogger(__name__)


class Done:
    def __init__(self, store_name: str):
        """
        Initialize the Done state with disk persistence.
        
        Args:
            store_name: Store name (stores, users, menu_items) to identify the persistence file
        """
        self.store_name = store_name
        self.client_done: dict[ClientId, threading.Event] = {}
        self._lock = threading.RLock()
        
        # Cargar estado persistido al inicializar
        self._load_persisted_state()
    
    def _get_persistence_path(self) -> Path:
        """Gets the persistence file path for the Done state."""
        base_dir = os.getenv("STATE_DIR")
        if base_dir:
            persistence_dir = Path(base_dir) / "metadata_store" / "done"
        else:
            persistence_dir = Path("state/metadata_store/done")
        persistence_dir.mkdir(parents=True, exist_ok=True)
        
        safe_name = (
            str(self.store_name)
            .replace("/", "_")
            .replace("\\", "_")
            .replace(":", "_")
        )
        return persistence_dir / f"{safe_name}.csv"
    
    def _load_persisted_state(self) -> None:
        """Loads the persisted Done state from disk on initialization."""
        path = self._get_persistence_path()
        
        if not path.exists():
            logger.debug(f"[DONE-LOAD] No persisted state found for {self.store_name}")
            return
        
        try:
            with path.open("r", encoding="utf-8", newline='') as f:
                reader = csv.reader(f)
                done_clients = []
                
                # Leer cada línea del CSV (una línea por client_id)
                for row in reader:
                    if row and len(row) > 0:
                        client_id = str(row[0]).strip()
                        if client_id:
                            done_clients.append(client_id)
                
                # Marcar todos los clientes como done en memoria
                for client_id in done_clients:
                    logger.info(f"\033[92m[DONE-LOAD] Marking client {client_id} as done for {self.store_name}\033[0m")
                    done_event = self.client_done.setdefault(client_id, threading.Event())
                    done_event.set()
    
        except (csv.Error, OSError, IOError) as exc:
            logger.warning(
                f"[DONE-LOAD] Failed to load persisted state for {self.store_name}: {exc}"
            )
    
    def _persist_state(self) -> None:
        """Persists the current Done state to disk."""
        path = self._get_persistence_path()
        
        try:
            # Obtener todos los client_ids que están done
            done_clients = [
                client_id 
                for client_id, event in self.client_done.items() 
                if event.is_set()
            ]
            
            # Escribir de forma atómica (write + fsync)
            # Formato CSV: una línea por client_id
            with path.open("w", encoding="utf-8", newline='') as f:
                writer = csv.writer(f)
                for client_id in done_clients:
                    writer.writerow([client_id])
                f.flush()
                os.fsync(f.fileno())
                
        except (OSError, IOError, csv.Error) as exc:
            logger.error(
                f"[DONE-PERSIST] Failed to persist done state for {self.store_name}: {exc}"
            )
    
    def is_client_done(self, client_id: ClientId, block: bool = False, timeout: float | None = None) -> bool:
        """Check or wait for the extra source to finish processing.
        
        Args:
            block: If True, block until done (or until timeout if provided).
            timeout: Optional maximum seconds to wait when block=True.
        
        Returns:
            True if done (or became done within the timeout), otherwise False.
        """
        with self._lock:
            done_event = self.client_done.setdefault(client_id, threading.Event())
        return done_event.wait(timeout=timeout) if block else done_event.is_set()
    
    def set_done(self, client_id: ClientId):
        """Marks a client as done and persists the state to disk."""
        with self._lock:
            done_event = self.client_done.setdefault(client_id, threading.Event())
            if not done_event.is_set():
                done_event.set()
                # Persistir el estado después de marcar como done
                self._persist_state()
                logger.debug(
                    f"[DONE] Marked client {client_id} as done for {self.store_name} "
                    f"and persisted state"
                )
    
    def clear_client(self, client_id: ClientId) -> None:
        """Clears the done state for a client and updates persistence."""
        with self._lock:
            if client_id in self.client_done:
                self.client_done[client_id].clear()
                # Si no hay más clientes done, eliminar el archivo
                if not any(event.is_set() for event in self.client_done.values()):
                    path = self._get_persistence_path()
                    try:
                        if path.exists():
                            path.unlink()
                            logger.debug(f"[DONE-CLEAR] Removed persistence file for {self.store_name}")
                    except OSError as exc:
                        logger.warning(f"[DONE-CLEAR] Failed to remove persistence file: {exc}")
                else:
                    # Actualizar persistencia
                    self._persist_state()
                logger.debug(f"[DONE-CLEAR] Cleared done state for client {client_id} in {self.store_name}")