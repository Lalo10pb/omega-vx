from __future__ import annotations

import json
import os
import smtplib
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from typing import Optional

import requests

from . import config
from .logging_utils import configure_logger

__all__ = [
    "send_telegram_alert",
    "send_email",
    "flush_alert_queue",
]

LOGGER = configure_logger("omega_vx.notifications")
LOG_DIR = Path(config.LOG_DIR)
ALERT_QUEUE_PATH = LOG_DIR / "alert_queue.jsonl"


def _ensure_env_loaded() -> None:
    config.load_environment()


def _telegram_details() -> tuple[Optional[str], Optional[str]]:
    _ensure_env_loaded()
    token = config.get_env("TELEGRAM_BOT_TOKEN")
    chat_id = config.get_env("TELEGRAM_CHAT_ID")
    return token, chat_id


def _queue_alert(channel: str, payload: dict) -> None:
    try:
        ALERT_QUEUE_PATH.parent.mkdir(parents=True, exist_ok=True)
        entry = {
            "channel": channel,
            "payload": payload,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }
        with ALERT_QUEUE_PATH.open("a") as handle:
            handle.write(json.dumps(entry) + "\n")
    except Exception as exc:
        LOGGER.error("⚠️ Failed to persist alert to queue: %s", exc)


def send_telegram_alert(message: str, *, queue_failures: bool = True) -> bool:
    token, chat_id = _telegram_details()
    if not token or not chat_id:
        LOGGER.warning("⚠️ Telegram not configured (missing token/chat id).")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message}

    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code != 200:
            LOGGER.warning("⚠️ Telegram send failed: %s %s", response.status_code, response.text)
            if queue_failures:
                _queue_alert("telegram", {"message": message, "error": response.text})
            return False
    except Exception as exc:
        LOGGER.error("❌ Telegram alert error: %s", exc)
        if queue_failures:
            _queue_alert("telegram", {"message": message, "error": str(exc)})
        return False
    return True


def send_email(subject: str, body: str, *, queue_failures: bool = True) -> bool:
    _ensure_env_loaded()
    email_address = config.get_env("EMAIL_USER")
    email_password = config.get_env("EMAIL_PASSWORD")

    if not email_address or not email_password:
        LOGGER.warning("⚠️ Email not configured (missing EMAIL_USER / EMAIL_PASSWORD).")
        return False

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = email_address
    msg["To"] = email_address
    msg.set_content(body)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(email_address, email_password)
            smtp.send_message(msg)
        LOGGER.info("📧 Email sent: %s", subject)
    except Exception as exc:
        LOGGER.error("❌ Email send failed: %s", exc)
        if queue_failures:
            _queue_alert("email", {"subject": subject, "body": body, "error": str(exc)})
        send_telegram_alert(f"❌ Email failure: {exc}", queue_failures=False)
        return False
    return True


def flush_alert_queue(max_batch: int = 100) -> int:
    if not ALERT_QUEUE_PATH.exists():
        return 0
    try:
        lines = ALERT_QUEUE_PATH.read_text().splitlines()
    except Exception as exc:
        LOGGER.error("⚠️ Failed to read alert queue: %s", exc)
        return 0

    remaining = []
    sent = 0
    for entry in lines[:max_batch]:
        try:
            data = json.loads(entry)
        except json.JSONDecodeError:
            continue
        channel = data.get("channel")
        payload = data.get("payload") or {}
        ok = False
        if channel == "telegram":
            ok = send_telegram_alert(payload.get("message", ""), queue_failures=False)
        elif channel == "email":
            ok = send_email(payload.get("subject", "Omega Alert"), payload.get("body", ""), queue_failures=False)
        if ok:
            sent += 1
        else:
            remaining.append(entry)

    remaining.extend(lines[max_batch:])
    if remaining:
        ALERT_QUEUE_PATH.write_text("\n".join(remaining) + "\n")
    else:
        ALERT_QUEUE_PATH.unlink(missing_ok=True)
    return sent
