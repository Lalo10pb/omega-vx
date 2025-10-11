from __future__ import annotations

import base64
import json
import os
from functools import lru_cache
from typing import Optional

import gspread
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.trading.client import TradingClient
from oauth2client.service_account import ServiceAccountCredentials

from . import config
from .logging_utils import configure_logger

__all__ = [
    "get_trading_client",
    "get_data_client",
    "get_raw_data_client",
    "get_gspread_client",
]

LOGGER = configure_logger("omega_vx.clients")


def _resolve_paper_mode() -> bool:
    raw = os.getenv("ALPACA_PAPER")
    if raw is None:
        base_url = config.get_env("APCA_API_BASE_URL")
        if base_url:
            return "paper" in base_url.lower()
        return True
    return str(raw).strip().lower() in {"1", "true", "yes", "paper"}


@lru_cache(maxsize=1)
def get_trading_client() -> TradingClient:
    config.load_environment()
    key = config.get_env("APCA_API_KEY_ID")
    secret = config.get_env("APCA_API_SECRET_KEY")
    if not key or not secret:
        raise RuntimeError("Missing APCA_API_KEY_ID / APCA_API_SECRET_KEY environment variables.")
    kwargs = {"paper": _resolve_paper_mode()}
    base_url = config.get_env("APCA_API_BASE_URL")
    if base_url:
        kwargs["url_override"] = base_url
    return TradingClient(key, secret, **kwargs)


@lru_cache(maxsize=1)
def get_data_client() -> StockHistoricalDataClient:
    config.load_environment()
    key = config.get_env("APCA_API_KEY_ID")
    secret = config.get_env("APCA_API_SECRET_KEY")
    if not key or not secret:
        raise RuntimeError("Missing APCA_API_KEY_ID / APCA_API_SECRET_KEY environment variables.")
    return StockHistoricalDataClient(key, secret)


@lru_cache(maxsize=1)
def get_raw_data_client() -> Optional[StockHistoricalDataClient]:
    try:
        config.load_environment()
        key = config.get_env("APCA_API_KEY_ID")
        secret = config.get_env("APCA_API_SECRET_KEY")
        if not key or not secret:
            return None
        return StockHistoricalDataClient(key, secret, raw_data=True)
    except Exception:
        return None


def get_gspread_client():
    """
    Return an authorized gspread client using one of:
      1) Credentials file (GOOGLE_CREDS_PATH, default google_credentials.json, or Render secret file mount)
      2) GOOGLE_SERVICE_ACCOUNT_JSON env var (raw JSON string)
      3) GOOGLE_SERVICE_ACCOUNT_JSON_B64 env var (base64-encoded JSON)
    """
    config.load_environment()
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    search_paths = []
    env_path = config.get_env("GOOGLE_CREDS_PATH")
    if env_path:
        search_paths.append(env_path)
    search_paths.append("/etc/secrets/google_credentials.json")
    search_paths.append("google_credentials.json")

    for path in search_paths:
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    preview = json.load(f)
                LOGGER.info("🔐 Using Google creds file at %s; service_account=%s", path, preview.get("client_email", "?"))
            except Exception:
                LOGGER.info("🔐 Using Google creds file at %s", path)
            creds = ServiceAccountCredentials.from_json_keyfile_name(path, scope)
            return gspread.authorize(creds)

    raw_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if raw_json and raw_json.strip().startswith("{"):
        info = json.loads(raw_json)
        LOGGER.info("🔐 Using GOOGLE_SERVICE_ACCOUNT_JSON; service_account=%s", info.get("client_email", "?"))
        creds = ServiceAccountCredentials.from_json_keyfile_dict(info, scope)
        return gspread.authorize(creds)

    b64_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_B64")
    if b64_json:
        try:
            decoded = base64.b64decode(b64_json).decode("utf-8")
            info = json.loads(decoded)
            LOGGER.info("🔐 Using GOOGLE_SERVICE_ACCOUNT_JSON_B64; service_account=%s", info.get("client_email", "?"))
            creds = ServiceAccountCredentials.from_json_keyfile_dict(info, scope)
            return gspread.authorize(creds)
        except Exception as exc:
            LOGGER.warning("⚠️ Failed to decode GOOGLE_SERVICE_ACCOUNT_JSON_B64: %s", exc)

    raise FileNotFoundError(
        "No Google credentials found (checked GOOGLE_CREDS_PATH, /etc/secrets/google_credentials.json, "
        "google_credentials.json, GOOGLE_SERVICE_ACCOUNT_JSON, GOOGLE_SERVICE_ACCOUNT_JSON_B64)."
    )
