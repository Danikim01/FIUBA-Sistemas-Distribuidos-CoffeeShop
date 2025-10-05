import threading
from message_utils import ClientId

class Done:
    def __init__(self):
        self.client_done: dict[ClientId, threading.Event] = {}

    def is_client_done(self, client_id: ClientId, block: bool = False, timeout: float | None = None) -> bool:
        """Check or wait for the extra source to finish processing.
        
        Args:
            block: If True, block until done (or until timeout if provided).
            timeout: Optional maximum seconds to wait when block=True.
        
        Returns:
            True if done (or became done within the timeout), otherwise False.
        """
        done_event = self.client_done.setdefault(client_id, threading.Event())
        return done_event.wait(timeout=timeout) if block else done_event.is_set()
    
    def set_done(self, client_id: ClientId):
        done_event = self.client_done.setdefault(client_id, threading.Event())
        if not done_event.is_set():
            done_event.set()