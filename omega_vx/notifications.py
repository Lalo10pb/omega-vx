from __future__ import annotations

import os
import smtplib
from email.message import EmailMessage
from typing import Optional

import requests

from . import config
from .logging_utils import configure_logger

__all__ = [
    "send_telegram_alert",
    "send_email",
]

LOGGER = configure_logger("omega_vx.notifications")


def _ensure_env_loaded() -> None:
    config.load_environment()


def _telegram_details() -> tuple[Optional[str], Optional[str]]:
    _ensure_env_loaded()
    token = config.get_env("TELEGRAM_BOT_TOKEN")
    chat_id = config.get_env("TELEGRAM_CHAT_ID")
    return token, chat_id


def send_telegram_alert(message: str) -> None:
    token, chat_id = _telegram_details()
    if not token or not chat_id:
        LOGGER.warning("⚠️ Telegram not configured (missing token/chat id).")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message}

    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code != 200:
            LOGGER.warning("⚠️ Telegram send failed: %s %s", response.status_code, response.text)
    except Exception as exc:
        LOGGER.error("❌ Telegram alert error: %s", exc)


def send_email(subject: str, body: str) -> None:
    _ensure_env_loaded()
    email_address = config.get_env("EMAIL_USER")
    email_password = config.get_env("EMAIL_PASSWORD")

    if not email_address or not email_password:
        LOGGER.warning("⚠️ Email not configured (missing EMAIL_USER / EMAIL_PASSWORD).")
        return

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
        send_telegram_alert(f"❌ Email failure: {exc}")
