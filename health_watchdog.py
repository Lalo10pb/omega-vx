"""
health_watchdog.py

Checks critical Omega-VX artifacts for staleness and alerts via Telegram/email
if any files are missing or older than their allowed threshold.
"""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple

from omega_vx import config as omega_config
from omega_vx.notifications import send_email, send_telegram_alert

__all__ = [
    "DEFAULT_TARGETS",
    "RENOTIFY_COOLDOWN",
    "parse_targets",
    "run_watchdog",
]


BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"
omega_config.load_environment(ENV_PATH)

LOG_DIR = Path(omega_config.LOG_DIR)
ALERT_STATE_FILE = LOG_DIR / "watchdog_alert_state.json"
RENOTIFY_COOLDOWN = timedelta(hours=6)
WATCHDOG_EMAIL_ENABLED = omega_config.get_bool("WATCHDOG_EMAIL_ENABLED", "1")
WATCHDOG_TELEGRAM_ENABLED = omega_config.get_bool("WATCHDOG_TELEGRAM_ENABLED", "1")

DEFAULT_TARGETS: List[Tuple[str, Path, int]] = [
    ("trade_log.csv", LOG_DIR / "trade_log.csv", 60 * 60 * 2),  # 2 hours
    ("portfolio_log.csv", LOG_DIR / "portfolio_log.csv", 60 * 60 * 6),
    ("equity_curve.log", LOG_DIR / "equity_curve.log", 60 * 60 * 6),
    ("omega_vx_bot.log", LOG_DIR / "omega_vx_bot.log", 60 * 15),  # 15 minutes
]


def _format_elapsed(seconds: float) -> str:
    if seconds < 0:
        seconds = 0
    hours, rem = divmod(int(seconds), 3600)
    minutes, secs = divmod(rem, 60)
    parts = []
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    parts.append(f"{secs}s")
    return " ".join(parts)


@dataclass
class TargetStatus:
    label: str
    path: Path
    max_age_seconds: int
    ok: bool
    detail: str
    age_seconds: float | None = None


def _check_target(label: str, path: Path, max_age_seconds: int) -> TargetStatus:
    if not path.exists():
        return TargetStatus(label, path, max_age_seconds, False, f"{path} missing")
    try:
        mtime = datetime.fromtimestamp(path.stat().st_mtime)
    except Exception as exc:
        return TargetStatus(label, path, max_age_seconds, False, f"Could not read {path}: {exc}")
    age = (datetime.now() - mtime).total_seconds()
    if age > max_age_seconds:
        detail = f"{_format_elapsed(age)} old (limit {_format_elapsed(max_age_seconds)})"
        return TargetStatus(label, path, max_age_seconds, False, detail, age)
    return TargetStatus(
        label,
        path,
        max_age_seconds,
        True,
        f"{_format_elapsed(age)} old",
        age,
    )


def _load_alert_state() -> tuple[list[str], datetime | None]:
    if not ALERT_STATE_FILE.exists():
        return [], None
    try:
        data = json.loads(ALERT_STATE_FILE.read_text())
        issues = data.get("issues") or []
        timestamp = data.get("timestamp")
        last_alert = datetime.fromisoformat(timestamp) if timestamp else None
        return list(issues), last_alert
    except Exception:
        return [], None


def _persist_alert_state(issue_labels: list[str], last_alert: datetime | None) -> None:
    ALERT_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "issues": issue_labels,
        "timestamp": last_alert.isoformat() if last_alert else None,
    }
    ALERT_STATE_FILE.write_text(json.dumps(payload))


def run_watchdog(targets: List[Tuple[str, Path, int]], quiet: bool = False, notify: bool = True) -> int:
    statuses: list[TargetStatus] = []
    for label, path, max_age in targets:
        status = _check_target(label, path, max_age)
        statuses.append(status)
        if not quiet:
            icon = "✅" if status.ok else "⏰"
            detail = status.detail
            if status.age_seconds is not None and status.ok:
                detail = f"{detail}"
            print(f"{icon} {label}: {detail}")

    issues = [status for status in statuses if not status.ok]
    if not issues:
        if not quiet:
            print("🟢 All monitored files are within thresholds.")
        prev_issues, _ = _load_alert_state()
        if prev_issues:
            _persist_alert_state([], None)
        return 0

    alert_lines = [f"⚠️ Omega Watchdog detected {len(issues)} issue(s):"]
    for status in issues:
        alert_lines.append(f"• {status.label}: {status.detail}")
    alert_text = "\n".join(alert_lines)

    issue_labels = sorted(status.label for status in issues)
    previous_labels, last_alert_at = _load_alert_state()
    now = datetime.now()

    should_notify = False
    if issue_labels != previous_labels:
        should_notify = True
    elif not last_alert_at or now - last_alert_at > RENOTIFY_COOLDOWN:
        should_notify = True

    if notify and should_notify:
        if WATCHDOG_TELEGRAM_ENABLED:
            try:
                send_telegram_alert(alert_text)
            except Exception:
                pass
        if WATCHDOG_EMAIL_ENABLED:
            try:
                send_email("⚠️ Omega Watchdog Alert", alert_text)
            except Exception:
                pass
        _persist_alert_state(issue_labels, now)
    else:
        # Preserve the fact that issues are still outstanding without
        # touching the last alert time so we can re-notify after cooldown.
        if issue_labels != previous_labels:
            _persist_alert_state(issue_labels, last_alert_at)

    if not quiet:
        print(alert_text)
    return len(issues)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check Omega-VX artifacts for staleness.")
    parser.add_argument("--quiet", action="store_true", help="Suppress per-file output; only report issues.")
    parser.add_argument("--no-notify", action="store_true", help="Do not send Telegram/email notifications.")
    parser.add_argument("--targets", help="Optional comma-separated overrides in form name:path:seconds")
    return parser.parse_args()


def parse_targets(raw: str | None) -> List[Tuple[str, Path, int]]:
    """Parse CLI/env target overrides in the form name:path:seconds."""
    if not raw:
        return DEFAULT_TARGETS
    items: List[Tuple[str, Path, int]] = []
    for part in raw.split(","):
        try:
            name, path_str, seconds = part.split(":")
            items.append((name, Path(path_str).expanduser(), int(seconds)))
        except ValueError:
            print(f"⚠️ Invalid target spec '{part}', expected name:path:seconds")
    return items or DEFAULT_TARGETS


if __name__ == "__main__":
    args = parse_args()
    targets = parse_targets(args.targets)
    run_watchdog(targets, quiet=args.quiet, notify=not args.no_notify)
