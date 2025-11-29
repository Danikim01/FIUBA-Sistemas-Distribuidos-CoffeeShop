from __future__ import annotations
from collections.abc import Callable
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

FLAG_PATH = Path("booted.flag")

def _mark_boot_completed(flag_path: Path) -> None:
    try:
        flag_path.touch()
        logger.info("Persisted boot flag.")
    except Exception as exc:
        logger.error("Failed to create gateway boot flag %s: %s", flag_path, exc)

def handle_boot(propagate_reset: Callable, flag_path: Path = FLAG_PATH) -> None:
    """Invoke a callback if the gateway has booted before. Else, create the boot flag.
    Args:
        propagate_reset: Callback to invoke on prior boot
        flag_path: Path to the boot flag file (default: FLAG_PATH)
    """
    has_booted_before: bool = flag_path.exists()
    if has_booted_before:
        logger.info("Detected prior boot.")
        propagate_reset()
    else:
        logger.info("First gateway boot detected.")
        _mark_boot_completed(flag_path)
