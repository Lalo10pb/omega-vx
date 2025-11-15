from __future__ import annotations

import logging
import os
import shlex
import subprocess
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Iterable, List

import psutil

REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_BOT_PATH = (REPO_ROOT / "omega_vx_bot.py").resolve()

BOT_PATH = Path(os.getenv("OMEGA_GUARD_TARGET", DEFAULT_BOT_PATH)).resolve()
LOG_PATH = Path(os.getenv("OMEGA_GUARD_LOG", REPO_ROOT / "omega_guard.log")).resolve()
CHECK_INTERVAL = max(10, int(os.getenv("OMEGA_GUARD_CHECK_SECONDS", "60")))
BACKOFF_BASE = max(15, int(os.getenv("OMEGA_GUARD_BACKOFF_SECONDS", "45")))
BACKOFF_MAX = max(BACKOFF_BASE, int(os.getenv("OMEGA_GUARD_BACKOFF_MAX_SECONDS", "600")))
CMD_OVERRIDE = os.getenv("OMEGA_GUARD_COMMAND")

ACCOUNT_LOGGER_ENABLED = os.getenv("ACCOUNT_LOGGER_ENABLED", "1").strip().lower() in ("1", "true", "yes", "y", "on")
ACCOUNT_LOGGER_INTERVAL = max(300, int(os.getenv("ACCOUNT_LOGGER_INTERVAL_SECONDS", "900")))
ACCOUNT_LOGGER_COMMAND = os.getenv(
    "ACCOUNT_LOGGER_COMMAND",
    f"{shlex.quote(sys.executable)} {shlex.quote(str(REPO_ROOT / 'account_logger.py'))} --interval {ACCOUNT_LOGGER_INTERVAL}",
)

_ACCOUNT_LOGGER_PROCESS: subprocess.Popen | None = None


def _ensure_log_dir() -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)


def _configure_logger() -> logging.Logger:
    _ensure_log_dir()
    logger = logging.getLogger("omega_guard")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(LOG_PATH, maxBytes=1_000_000, backupCount=3)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(formatter)
    logger.addHandler(stream)
    return logger


LOGGER = _configure_logger()


def _guarded_command() -> List[str]:
    if CMD_OVERRIDE:
        return shlex.split(CMD_OVERRIDE)
    return [sys.executable, str(BOT_PATH)]


def _iter_guarded_processes() -> Iterable[psutil.Process]:
    for proc in psutil.process_iter(["pid", "cmdline"]):
        try:
            cmdline = proc.info["cmdline"] or []
            if any(str(BOT_PATH) in part for part in cmdline):
                yield proc
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue


def is_bot_running() -> bool:
    return any(True for _ in _iter_guarded_processes())


def start_bot() -> bool:
    try:
        cmd = _guarded_command()
        LOGGER.warning("🔁 omega_vx_bot.py not running — attempting restart via %s", cmd)
        subprocess.Popen(cmd, cwd=str(REPO_ROOT))
        LOGGER.info("✅ omega_vx_bot.py restart command issued.")
        return True
    except Exception as exc:
        LOGGER.exception("❌ Failed to launch omega_vx_bot.py: %s", exc)
        return False


def _ensure_account_logger() -> None:
    global _ACCOUNT_LOGGER_PROCESS
    if not ACCOUNT_LOGGER_ENABLED:
        return
    if _ACCOUNT_LOGGER_PROCESS and _ACCOUNT_LOGGER_PROCESS.poll() is None:
        return
    try:
        cmd = shlex.split(ACCOUNT_LOGGER_COMMAND)
        LOGGER.info("📊 Starting account logger via %s", cmd)
        _ACCOUNT_LOGGER_PROCESS = subprocess.Popen(cmd, cwd=str(REPO_ROOT))
    except Exception as exc:
        LOGGER.error("⚠️ Failed to launch account logger: %s", exc)


def main() -> None:
    consecutive_failures = 0
    LOGGER.info("🛡️ Omega Guard monitoring %s (interval=%ss)", BOT_PATH, CHECK_INTERVAL)
    _ensure_account_logger()

    while True:
        try:
            _ensure_account_logger()
            if is_bot_running():
                if consecutive_failures:
                    LOGGER.info("✅ omega_vx_bot.py recovered.")
                else:
                    LOGGER.info("✅ omega_vx_bot.py is running.")
                consecutive_failures = 0
                time.sleep(CHECK_INTERVAL)
                continue

            delay = min(BACKOFF_BASE * (2 ** consecutive_failures), BACKOFF_MAX)
            if consecutive_failures:
                LOGGER.warning("⚠️ Restart attempt #%s in %ss (backoff active).", consecutive_failures + 1, delay)
            else:
                LOGGER.warning("⚠️ omega_vx_bot.py offline; restart in %ss.", delay)
            time.sleep(delay)

            if start_bot():
                consecutive_failures = 0
            else:
                consecutive_failures += 1
        except KeyboardInterrupt:
            LOGGER.info("🛑 Omega Guard interrupted by user; exiting.")
            if _ACCOUNT_LOGGER_PROCESS and _ACCOUNT_LOGGER_PROCESS.poll() is None:
                LOGGER.info("🛑 Stopping managed account logger (pid=%s).", _ACCOUNT_LOGGER_PROCESS.pid)
                _ACCOUNT_LOGGER_PROCESS.terminate()
            break
        except Exception as exc:
            LOGGER.exception("❌ Guard loop error: %s", exc)
            time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
