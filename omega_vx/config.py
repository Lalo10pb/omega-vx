from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

__all__ = [
    "DEFAULT_ENV_PATH",
    "LOG_DIR",
    "load_environment",
    "clean",
    "get_env",
    "get_float",
    "get_int",
    "get_bool",
]

# Repository structure assumption: omega_vx package folder sits beside `.env`.
DEFAULT_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
LOG_DIR = Path(os.path.expanduser("~/omega-vx/logs"))


def load_environment(env_path: Optional[Path] = None) -> bool:
    """
    Load a dotenv file once so that scripts and the Flask app share the same
    environment defaults. Returns True when a file was found and loaded.
    """
    path = env_path or DEFAULT_ENV_PATH
    try:
        if not path.exists():
            return False
        return load_dotenv(path, override=False)
    except Exception:
        # Mirror omega_vx_bot behaviour where missing .env is non-fatal.
        return False


def clean(value: Optional[str]) -> str:
    """Trim whitespace and surrounding quotes from environment values."""
    return str(value or "").strip().strip('"').strip("'")


def get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name, default)
    if value is None:
        return None
    return clean(value)


def get_float(name: str, default: float) -> float:
    raw = os.getenv(name, None)
    try:
        return float(str(raw).strip())
    except (TypeError, ValueError):
        return float(default)


def get_int(name: str, default: int) -> int:
    raw = os.getenv(name, None)
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        return int(default)


def get_bool(name: str, default: str = "0") -> bool:
    raw = os.getenv(name, str(default))
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


# Ensure log directory exists for all imports without forcing the rest of the app.
LOG_DIR.mkdir(parents=True, exist_ok=True)
