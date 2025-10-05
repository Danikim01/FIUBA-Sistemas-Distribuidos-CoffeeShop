from collections.abc import Callable
import threading


class Done:
    def __init__(self):
        self.done = False
        self._done_event = threading.Event()

    def _is_done(self, block: bool = False, timeout: float | None = None) -> bool:
        """Check or wait for the extra source to finish processing.
        
        Args:
            block: If True, block until done (or until timeout if provided).
            timeout: Optional maximum seconds to wait when block=True.
        
        Returns:
            True if done (or became done within the timeout), otherwise False.
        """
        if block:
            return self._done_event.wait(timeout=timeout)
        return self._done_event.is_set()
    
    def _set_done(self):
        if not self._done_event.is_set():
            self._done_event.set()
            self.done = True