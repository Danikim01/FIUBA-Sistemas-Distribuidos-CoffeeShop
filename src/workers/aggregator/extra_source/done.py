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

    def when_done(
        self,
        name: str,
        callback: Callable[..., None],
        *args,
        timeout: float | None = None,
        **kwargs,
    ):
        """Run `callback(*args, **kwargs)` once the given client_id becomes done.
        Non-blocking: spawns a daemon thread that waits on the underlying Done event.
        If `timeout` elapses first, the callback is not invoked.
        """
        def _wait_and_call():
            if self._is_done(block=True, timeout=timeout):
                return callback(*args, **kwargs)
    
        t = threading.Thread(
            target=_wait_and_call,
            name=name,
            daemon=True,
        )
        t.start()
