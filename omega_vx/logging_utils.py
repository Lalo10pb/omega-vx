from __future__ import annotations

import builtins
import logging
import sys
import threading
import time
from contextlib import contextmanager
from typing import Callable, Dict, Optional

from .config import LOG_DIR

__all__ = [
    "EMOJI_LEVEL_MAP",
    "configure_logger",
    "install_print_bridge",
]

EMOJI_LEVEL_MAP: Dict[str, str] = {
    "⚠️": "warning",
    "❌": "error",
    "⛔": "info",
    "🛑": "warning",
    "🚫": "info",
    "❗": "warning",
    "ℹ️": "info",
    "✅": "info",
    "💥": "warning",
}


class _UTCFormatter(logging.Formatter):
    converter = time.gmtime


def configure_logger(name: str = "omega_vx") -> logging.Logger:
    """
    Create (or return) a module-level logger that logs both to stdout and a file
    under the shared LOG_DIR. Subsequent calls are idempotent.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    formatter = _UTCFormatter("%(asctime)sZ [%(levelname)s] [%(threadName)s] %(message)s")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(LOG_DIR / "omega_vx_bot.log")
    file_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    logger.propagate = False
    return logger


def install_print_bridge(logger: Optional[logging.Logger] = None) -> Callable[[], None]:
    """
    Override builtins.print so that emoji-prefixed messages are routed through
    the Omega logger. Returns a callable that restores the original print.
    """
    target_logger = logger or configure_logger()
    original_print = builtins.print
    lock = threading.Lock()

    def _normalize_tag(tag: Optional[str]) -> str:
        if tag:
            return str(tag).upper()
        name = threading.current_thread().name or "CORE"
        if name.lower() == "mainthread":
            name = "MAIN"
        return name.upper()

    def _infer_log_level(message: str) -> str:
        trimmed = message.lstrip()
        for prefix, level in EMOJI_LEVEL_MAP.items():
            if trimmed.startswith(prefix):
                return level
        return "info"

    def bridged_print(*args, **kwargs):
        sep = kwargs.pop("sep", " ")
        end = kwargs.pop("end", "")
        level = kwargs.pop("level", None)
        tag = kwargs.pop("tag", None)
        kwargs.pop("flush", None)
        kwargs.pop("file", None)
        message = sep.join(str(arg) for arg in args)
        if end and end != "\n":
            message += end
        inferred = str(level or _infer_log_level(message)).lower()
        prefix = f"[{_normalize_tag(tag)}] "
        with lock:
            log_fn = getattr(target_logger, inferred, target_logger.info)
            log_fn(f"{prefix}{message}")

    builtins.print = bridged_print

    def restore():
        builtins.print = original_print

    return restore


@contextmanager
def bridged_print(logger: Optional[logging.Logger] = None):
    """
    Context manager wrapper over install_print_bridge for short-lived scripts.
    """
    restore = install_print_bridge(logger=logger)
    try:
        yield
    finally:
        restore()
