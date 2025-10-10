# === Flask and Webhook Handling ===
from flask import Flask, request, jsonify

# === Core Packages ===
import os
import csv
import time
import threading
import requests
import subprocess
import sys
from typing import Optional, Set, Tuple, Iterable
from datetime import datetime, timedelta, time as dt_time
import numpy as np
import pandas as pd
from dotenv import load_dotenv
import json
import base64
from decimal import Decimal, ROUND_HALF_UP
import logging

from alpaca.trading.requests import (
    MarketOrderRequest,
    GetOrdersRequest,
    LimitOrderRequest,
    StopOrderRequest,
)
from alpaca.trading.enums import OrderSide, TimeInForce, OrderType, QueryOrderStatus
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockLatestQuoteRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.mappings import BAR_MAPPING
from alpaca.trading.client import TradingClient
from alpaca.data.enums import DataFeed
from datetime import datetime, timedelta, time as dt_time, timezone
# === Email Reporting ===
import smtplib
from email.message import EmailMessage

# === Google Sheets ===
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# === Logging Configuration ===
LOG_DIR = os.path.expanduser("~/omega-vx/logs")
os.makedirs(LOG_DIR, exist_ok=True)


class _UTCFormatter(logging.Formatter):
    converter = time.gmtime


def _configure_logger() -> logging.Logger:
    logger = logging.getLogger("omega_vx")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    formatter = _UTCFormatter("%(asctime)sZ [%(levelname)s] [%(threadName)s] %(message)s")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(os.path.join(LOG_DIR, "omega_vx_bot.log"))
    file_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    logger.propagate = False
    return logger


LOGGER = _configure_logger()


_EMOJI_LEVEL_MAP = {
    "⚠️": "warning",
    "❌": "error",
    "⛔": "error",
    "🛑": "warning",
    "🚫": "warning",
    "❗": "warning",
    "ℹ️": "info",
    "✅": "info",
    "💥": "warning",
}


def _infer_log_level(message: str) -> str:
    trimmed = message.lstrip()
    for prefix, level in _EMOJI_LEVEL_MAP.items():
        if trimmed.startswith(prefix):
            return level
    return "info"


def _normalize_tag(tag: Optional[str]) -> str:
    if tag:
        return str(tag).upper()
    name = threading.current_thread().name or "CORE"
    if name.lower() == "mainthread":
        name = "MAIN"
    return name.upper()


def _log_context(level: str, tag: Optional[str], message: str) -> None:
    safe_level = str(level or "info").lower()
    log_fn = getattr(LOGGER, safe_level, LOGGER.info)
    prefix = f"[{_normalize_tag(tag)}] "
    log_fn(f"{prefix}{message}")


def print(*args, **kwargs):  # type: ignore[override]
    sep = kwargs.pop("sep", " ")
    end = kwargs.pop("end", "")
    level = kwargs.pop("level", None)
    tag = kwargs.pop("tag", None)
    kwargs.pop("flush", None)
    kwargs.pop("file", None)
    message = sep.join(str(arg) for arg in args)
    if end and end != "\n":
        message += end
    inferred_level = level or _infer_log_level(message)
    _log_context(inferred_level, tag, message)


def _log_boot(message: str, level: str = "info"):
    _log_context(level, "BOOT", message)


_log_boot("Logger initialized with UTC timestamps and thread names.")
_log_boot("Omega-VX live build 08a8d10 (risk guard + PDT safeguards active).")
_log_boot("Application started.")

# === Load .env and Set Environment Vars ===
from pathlib import Path
ENV_PATH = Path(__file__).parent / ".env"
if not load_dotenv(ENV_PATH):
    _log_boot(f"Could not load .env at {ENV_PATH} — using environment vars only.", level="warning")


def _float_env(name: str, default: float) -> float:
    """Parse environment variable as float with a safe default."""
    raw = os.getenv(name, None)
    try:
        val = float(str(raw).strip())
    except (TypeError, ValueError):
        return float(default)
    return val


def _int_env(name: str, default: int) -> int:
    """Parse environment variable as int with a safe default."""
    raw = os.getenv(name, None)
    try:
        val = int(str(raw).strip())
    except (TypeError, ValueError):
        return int(default)
    return val


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _bool_env(name: str, default: str = "0") -> bool:
    raw = os.getenv(name, str(default))
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")
BASE_URL = os.getenv("APCA_API_BASE_URL")
WEBHOOK_SECRET_TOKEN = os.getenv("WEBHOOK_SECRET_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_CREDENTIALS_FILE = "google_credentials.json"
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

PAPER_MODE = str(os.getenv("ALPACA_PAPER", "true")).strip().lower() in ("1", "true", "yes")
OVERNIGHT_PROTECTION_ENABLED = bool(int(os.getenv("OVERNIGHT_PROTECTION_ENABLED", "1")))


# --- Alpaca Data Feed selection (force IEX to avoid SIP permission errors) ---
_DATA_FEED = DataFeed.IEX
_log_boot("Alpaca data feed: iex (forced)")

# --- Early env sanitizer (must be defined before any top-level uses) ---
def _clean_env(s: str) -> str:
    """Trim whitespace and surrounding quotes from environment variables."""
    return str(s or "").strip().strip('"').strip("'")

# --- Google Sheets Helper for Flexible Auth ---
def _get_gspread_client():
    """
    Return an authorized gspread client using one of:
      1) Secret File on disk (supports Render Secret Files and custom GOOGLE_CREDS_PATH)
      2) Env var GOOGLE_SERVICE_ACCOUNT_JSON (raw JSON)
      3) Env var GOOGLE_SERVICE_ACCOUNT_JSON_B64 (base64-encoded JSON)
    """
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]

        # 1) Preferred: a credentials file on disk
        search_paths = []
        # custom path override
        env_path = os.getenv("GOOGLE_CREDS_PATH")
        if env_path:
            search_paths.append(env_path)
        # Render Secret Files default mount
        search_paths.append("/etc/secrets/google_credentials.json")
        # repo-local fallback (if the file is committed or copied at build)
        search_paths.append("google_credentials.json")

        for path in search_paths:
            if os.path.exists(path):
                # Print minimal debug to help diagnose 404/403 issues
                try:
                    with open(path, "r") as f:
                        _creds_preview = json.load(f)
                    print(
                        f"🔐 Using Google creds file at {path}; service_account={_creds_preview.get('client_email','?')}"
                    )
                except Exception:
                    pass
                creds = ServiceAccountCredentials.from_json_keyfile_name(path, scope)
                return gspread.authorize(creds)

        # 2) Raw JSON in env
        raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
        if raw and raw.strip().startswith("{"):
            info = json.loads(raw)
            print(f"🔐 Using GOOGLE_SERVICE_ACCOUNT_JSON; service_account={info.get('client_email','?')}")
            creds = ServiceAccountCredentials.from_json_keyfile_dict(info, scope)
            return gspread.authorize(creds)

        # 3) Base64 JSON in env
        b64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_B64")
        if b64:
            try:
                decoded = base64.b64decode(b64).decode("utf-8")
                info = json.loads(decoded)
                print(f"🔐 Using GOOGLE_SERVICE_ACCOUNT_JSON_B64; service_account={info.get('client_email','?')}")
                creds = ServiceAccountCredentials.from_json_keyfile_dict(info, scope)
                return gspread.authorize(creds)
            except Exception as e:
                print(f"⚠️ Failed to decode GOOGLE_SERVICE_ACCOUNT_JSON_B64: {e}")

        raise FileNotFoundError(
            "No Google credentials found (checked GOOGLE_CREDS_PATH, /etc/secrets/google_credentials.json, google_credentials.json, GOOGLE_SERVICE_ACCOUNT_JSON, GOOGLE_SERVICE_ACCOUNT_JSON_B64)."
        )
    except Exception as e:
        print(f"❌ Google auth error: {type(e).__name__}: {e}")
        raise

# Legacy logging configuration replaced by centralized logger above.
DAILY_RISK_LIMIT = -10  # optional daily risk guard, currently unused
TRADE_COOLDOWN_SECONDS = 300
PER_SYMBOL_COOLDOWN_SECONDS = _int_env("PER_SYMBOL_COOLDOWN_SECONDS", 600)
MAX_RISK_BASE_PERCENT = _float_env("MAX_RISK_BASE_PERCENT", 3.0)
MAX_RISK_PER_TRADE_PERCENT = MAX_RISK_BASE_PERCENT
MAX_RISK_AUTO_MIN = _float_env("MAX_RISK_AUTO_MIN_PCT", MAX_RISK_BASE_PERCENT)
MAX_RISK_AUTO_MAX = _float_env("MAX_RISK_AUTO_MAX_PCT", MAX_RISK_BASE_PERCENT)
MAX_RISK_AUTO_UP_STEP = _float_env("MAX_RISK_AUTO_UP_STEP", 0.0)
MAX_RISK_AUTO_DOWN_STEP = _float_env("MAX_RISK_AUTO_DOWN_STEP", 0.0)
EQUITY_GUARD_MIN_DRAWDOWN = _float_env("EQUITY_GUARD_MIN_DRAWDOWN_PCT", 0.10)
EQUITY_GUARD_STALE_RATIO = _float_env("EQUITY_GUARD_STALE_RATIO", 5.0)
EQUITY_GUARD_MAX_EQUITY_FLOOR = _float_env("EQUITY_GUARD_MAX_EQUITY_FLOOR", 0.0)
MAX_OPEN_POSITIONS_HIGH_EQUITY = _int_env("MAX_OPEN_POSITIONS_HIGH_EQUITY", 3)
DAILY_TRADE_CAP = _int_env("DAILY_TRADE_CAP", 0)
EQUITY_DRAWDOWN_MAX_PCT = _float_env("EQUITY_DRAWDOWN_MAX_PCT", 0.0) / 100.0
PDT_GUARD_ENABLED = _bool_env("PDT_GUARD_ENABLED", "1")
PDT_MIN_DAY_TRADES_BUFFER = max(0, _int_env("PDT_MIN_DAY_TRADES_BUFFER", 0))
PDT_GLOBAL_LOCK_SECONDS = max(0, _int_env("PDT_GLOBAL_LOCK_SECONDS", 900))
PDT_SYMBOL_LOCK_SECONDS = max(0, _int_env("PDT_SYMBOL_LOCK_SECONDS", 600))
PDT_ALERT_COOLDOWN_SECONDS = max(0, _int_env("PDT_ALERT_COOLDOWN_SECONDS", 300))
PDT_STATUS_CACHE_SECONDS = max(5, _int_env("PDT_STATUS_CACHE_SECONDS", 60))
MIN_TRADE_QTY = 1

_DAILY_TRADE_COUNT = 0
_DAILY_TRADE_DATE = datetime.now().date()

# --- Trade Re-entry Guards ---
REENTRY_DIP_PCT = _float_env("REENTRY_DIP_PCT", 2.0)  # % dip required for same-day reentry
_symbol_last_trade = {}  # {"SNAP": {"date": date, "exit_price": 8.57, "count": 1, "last_trade_ts": iso}}
_symbol_peak_unrealized = {}  # track peak % gains for trailing logic
_symbol_last_trade_lock = threading.Lock()
_symbol_peak_unrealized_lock = threading.Lock()
# Thread safety for PDT lockouts and PnL alert state
_pdt_lockouts_lock = threading.Lock()
_DAILY_PNL_ALERT_STATE_LOCK = threading.Lock()
_DAILY_PNL_ALERT_STATE = {"gain": None, "loss": None}


def _prune_symbol_peak_cache(active_symbols: Set[str]) -> None:
    """Remove symbols with no active positions from the peak unrealized tracker."""
    # Thread-safe update to shared state
    with _symbol_peak_unrealized_lock:
        stale = [sym for sym in list(_symbol_peak_unrealized) if sym not in active_symbols]
        for sym in stale:
            _symbol_peak_unrealized.pop(sym, None)


def _get_peak_unrealized(symbol: str) -> Optional[float]:
    # Thread-safe read from shared state
    with _symbol_peak_unrealized_lock:
        return _symbol_peak_unrealized.get(symbol)


def _set_peak_unrealized(symbol: str, value: Optional[float]) -> None:
    # Thread-safe update to shared state
    with _symbol_peak_unrealized_lock:
        if value is None:
            _symbol_peak_unrealized.pop(symbol, None)
        else:
            _symbol_peak_unrealized[symbol] = value

def _check_daily_trade_cap():
    global _DAILY_TRADE_COUNT, _DAILY_TRADE_DATE
    today = datetime.now().date()
    if today != _DAILY_TRADE_DATE:
        _DAILY_TRADE_DATE = today
        _DAILY_TRADE_COUNT = 0
    if DAILY_TRADE_CAP > 0 and _DAILY_TRADE_COUNT >= DAILY_TRADE_CAP:
        print(f"🛑 Daily trade cap reached ({DAILY_TRADE_CAP}) — skipping.")
        try:
            send_telegram_alert(f"🛑 Daily trade cap reached ({DAILY_TRADE_CAP}) — skipping.")
        except Exception:
            pass
        return False
    return True


def _increment_daily_trade_count():
    global _DAILY_TRADE_COUNT
    _DAILY_TRADE_COUNT += 1


def _get_daily_equity_baseline(current_equity: float) -> float:
    today = datetime.now(timezone.utc).date().isoformat()
    if os.path.exists(DAILY_EQUITY_BASELINE_FILE):
        try:
            with open(DAILY_EQUITY_BASELINE_FILE, "r") as f:
                raw = f.read().strip()
            if raw:
                stored_day, stored_equity = raw.split(",")
                if stored_day == today:
                    return float(stored_equity)
        except Exception as e:
            print(f"⚠️ Could not read daily baseline: {e}")
    try:
        with open(DAILY_EQUITY_BASELINE_FILE, "w") as f:
            f.write(f"{today},{current_equity:.2f}")
    except Exception as e:
        print(f"⚠️ Could not persist daily baseline: {e}")
    # Thread-safe update to shared state
    with _DAILY_PNL_ALERT_STATE_LOCK:
        _DAILY_PNL_ALERT_STATE["gain"] = None
        _DAILY_PNL_ALERT_STATE["loss"] = None
    return current_equity


def _daily_pnl_guard_allows_trading() -> bool:
    try:
        account = trading_client.get_account()
        equity = float(account.equity)
    except Exception as e:
        print(f"⚠️ Daily PnL guard could not fetch equity: {e}")
        return True

    if equity <= 0:
        return True

    baseline = _get_daily_equity_baseline(equity)
    if baseline <= 0:
        return True

    change_pct = ((equity - baseline) / baseline) * 100.0
    # Thread-safe update to shared state
    with _DAILY_PNL_ALERT_STATE_LOCK:
        if change_pct >= DAILY_GAIN_CAP_PCT:
            if _DAILY_PNL_ALERT_STATE.get("gain") != datetime.now().date():
                msg = f"🛑 Daily gain cap hit (+{change_pct:.2f}% ≥ {DAILY_GAIN_CAP_PCT}%). Pausing new trades."
                print(msg)
                try: send_telegram_alert(msg)
                except Exception: pass
                _DAILY_PNL_ALERT_STATE["gain"] = datetime.now().date()
            return False
        if change_pct <= DAILY_LOSS_CAP_PCT:
            if _DAILY_PNL_ALERT_STATE.get("loss") != datetime.now().date():
                msg = f"🛑 Daily loss cap hit ({change_pct:.2f}% ≤ {DAILY_LOSS_CAP_PCT}%). Pausing new trades."
                print(msg)
                try: send_telegram_alert(msg)
                except Exception: pass
                _DAILY_PNL_ALERT_STATE["loss"] = datetime.now().date()
            return False
    return True


def _can_trade_symbol_today(symbol: str, entry: float) -> bool:
    today = datetime.now().date()
    # Thread-safe read from shared state
    with _symbol_last_trade_lock:
        record = _symbol_last_trade.get(symbol.upper())
        record = None if record is None else record.copy()

    if not record:
        return True

    last_ts = record.get("last_trade_ts")
    if last_ts:
        try:
            last_dt = datetime.fromisoformat(last_ts)
            if (datetime.now(timezone.utc) - last_dt).total_seconds() < PER_SYMBOL_COOLDOWN_SECONDS:
                print(f"🚫 Re-entry blocked for {symbol}: cooldown {PER_SYMBOL_COOLDOWN_SECONDS}s not met.")
                return False
        except Exception:
            pass

    if record.get("date") == today:
        count = int(record.get("count", 0) or 0)
        if count >= 2:
            print(f"🚫 Re-entry blocked for {symbol}: max daily trades reached ({count}).")
            return False
        last_exit = record.get("exit_price")
        if last_exit is None:
            print(f"🚫 Re-entry blocked for {symbol}: exit price unavailable for dip check.")
            return False
        dip_threshold = last_exit * (1 - REENTRY_DIP_PCT / 100.0)
        if entry < dip_threshold:
            print(f"🔄 Re-entry allowed for {symbol}: entry {entry} < dip threshold {dip_threshold:.2f}")
            return True
        print(f"🚫 Re-entry blocked for {symbol}: entry {entry} not below dip threshold {dip_threshold:.2f}")
        return False
    return True

# sanity clamps to keep env overrides within reasonable bounds
if MAX_RISK_AUTO_MIN < 0:
    MAX_RISK_AUTO_MIN = 0.0
if MAX_RISK_AUTO_MAX < MAX_RISK_AUTO_MIN:
    MAX_RISK_AUTO_MAX = MAX_RISK_AUTO_MIN
if EQUITY_GUARD_MIN_DRAWDOWN < 0:
    EQUITY_GUARD_MIN_DRAWDOWN = 0.0
if EQUITY_GUARD_MIN_DRAWDOWN > 1:
    EQUITY_GUARD_MIN_DRAWDOWN = 1.0
if EQUITY_GUARD_STALE_RATIO < 0:
    EQUITY_GUARD_STALE_RATIO = 0.0

_LOG_WRITE_LOCK = threading.Lock()
_BACKGROUND_WORKERS_LOCK = threading.Lock()
_BACKGROUND_WORKERS_STARTED = False

# Watchdog thresholds (percent change on position)
TRAILING_TRIGGER_PCT = _float_env("TRAILING_TRIGGER_PCT", 2.5)
TRAILING_GIVEBACK_PCT = _float_env("TRAILING_GIVEBACK_PCT", 1.0)
WATCHDOG_TAKE_PROFIT_PCT = _float_env("WATCHDOG_TAKE_PROFIT_PCT", 5.0)
WATCHDOG_HARD_STOP_PCT = _float_env("WATCHDOG_HARD_STOP_PCT", -2.5)
if WATCHDOG_HARD_STOP_PCT > 0:
    WATCHDOG_HARD_STOP_PCT = -WATCHDOG_HARD_STOP_PCT
FALLBACK_TAKE_PROFIT_PCT = _float_env("FALLBACK_TAKE_PROFIT_PCT", 5.0)
FALLBACK_STOP_LOSS_PCT = _float_env("FALLBACK_STOP_LOSS_PCT", 2.5)
WATCHDOG_LOOP_SECONDS = max(5, _int_env("WATCHDOG_LOOP_SECONDS", 30))
THREAD_AUTH_FAILURE_THRESHOLD = max(1, _int_env("THREAD_AUTH_FAILURE_THRESHOLD", 3))
DAILY_GAIN_CAP_PCT = _float_env("DAILY_GAIN_CAP_PCT", 8.0)
DAILY_LOSS_CAP_PCT = -abs(_float_env("DAILY_LOSS_CAP_PCT", 4.0))
DAILY_EQUITY_BASELINE_FILE = os.path.join(LOG_DIR, "daily_equity_baseline.txt")

_THREAD_ERROR_LOCK = threading.Lock()
_THREAD_ERROR_COUNTS = {}
_CRITICAL_THREAD_ERROR_KEYWORDS = (
    "authentication",
    "invalid api key",
    "unauthorized",
    "forbidden",
    "access key",
    "signature verification failed",
)


def _handle_thread_exception(thread_name: str, exc: Exception) -> Tuple[bool, bool]:
    """
    Track repeated thread errors and decide whether to stop the loop.
    Returns (should_stop, alert_already_sent).
    """
    message = str(exc).lower()
    critical = any(keyword in message for keyword in _CRITICAL_THREAD_ERROR_KEYWORDS)
    key = (thread_name, "critical" if critical else type(exc).__name__)

    with _THREAD_ERROR_LOCK:
        count = _THREAD_ERROR_COUNTS.get(key, 0) + 1
        _THREAD_ERROR_COUNTS[key] = count

    alert_sent = False
    if critical:
        if count >= THREAD_AUTH_FAILURE_THRESHOLD:
            alert = f"⛔ {thread_name} halted after repeated authentication failures: {exc}"
            print(alert)
            try:
                send_telegram_alert(alert)
            except Exception:
                pass
            return True, True
        if count == 1:
            warn = f"⚠️ {thread_name} authentication error: {exc} (retrying)"
            print(warn)
            try:
                send_telegram_alert(warn)
            except Exception:
                pass
            alert_sent = True
    return False, alert_sent
def _fetch_data_with_fallback(request_function, symbol, feed=_DATA_FEED):
    """Fetches data using the given request function, with fallback to IEX if permission errors occur."""
    try:
        return request_function(feed=feed)
    except Exception as e:
        if "subscription does not permit" in str(e).lower() and feed != DataFeed.IEX:
            print(f"ℹ️ Falling back to IEX for {symbol} due to feed permission.")
            try:
                return request_function(feed=DataFeed.IEX)
            except Exception as e2:
                print(f"❌ Data fetch failed for {symbol} (IEX fallback): {e2}")
                return None
        else:
            print(f"❌ Data fetch failed for {symbol}: {e}")
            return None

# === Logging ===
CRASH_LOG_FILE = os.path.join(LOG_DIR, "last_boot.txt")
LAST_BLOCK_FILE = os.path.join(LOG_DIR, "last_block.txt")
LAST_TRADE_FILE = os.path.join(LOG_DIR, "last_trade_time.txt")

# === Alpaca Clients ===
trading_client = TradingClient(API_KEY, API_SECRET, paper=PAPER_MODE)
data_client = StockHistoricalDataClient(API_KEY, API_SECRET)
try:
    _RAW_DATA_CLIENT = StockHistoricalDataClient(API_KEY, API_SECRET, raw_data=True)
except Exception:
    _RAW_DATA_CLIENT = None


def print_protection_status():
    """Prints the current protection status (TP and SL) for all open positions."""
    try:
        positions = trading_client.get_all_positions()
        if not positions:
            print("🛡 No open positions currently under protection.")
            return
        print("🛡 Active Protection:")
        for p in positions:
            symbol = getattr(p, "symbol", "?")
            qty = getattr(p, "qty", "?")
            avg_entry = getattr(p, "avg_entry_price", "?")
            try:
                req = GetOrdersRequest(status=QueryOrderStatus.OPEN, symbols=[symbol])
                open_orders = trading_client.get_orders(filter=req)
                tp = sl = None
                for o in open_orders:
                    side = str(getattr(o, "side", "")).lower()
                    if side != "sell":
                        continue
                    otype = str(getattr(o, "order_type", "")).lower()
                    price = None
                    try:
                        price = float(getattr(o, "limit_price") or getattr(o, "stop_price") or 0)
                    except Exception:
                        continue
                    if otype == "limit" and price > 0:
                        tp = price
                    elif otype == "stop" and price > 0:
                        sl = price
                print(f"   • {symbol:<5} qty={qty} entry={avg_entry} TP={tp or '-'} SL={sl or '-'}")
            except Exception as e:
                print(f"⚠️ Could not fetch orders for {symbol}: {e}")
    except Exception as e:
        print(f"⚠️ Protection status check failed: {e}")


def _bars_df_from_raw_payload(raw_payload):
    if not raw_payload:
        return pd.DataFrame()

    records = []
    for symbol, bars in raw_payload.items():
        if not bars:
            continue
        for bar in bars:
            if not isinstance(bar, dict):
                continue
            mapped = {
                BAR_MAPPING[key]: value
                for key, value in bar.items()
                if key in BAR_MAPPING and value is not None
            }
            if not mapped:
                continue
            mapped["symbol"] = symbol
            records.append(mapped)

    if not records:
        return pd.DataFrame()

    frame = pd.DataFrame(records)
    if "timestamp" in frame.columns:
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
        frame = frame.dropna(subset=["timestamp"])
        if frame.empty:
            return pd.DataFrame()
        frame = frame.set_index(["symbol", "timestamp"]).sort_index()
    return frame


def _fetch_bars_df(symbol: str, request: StockBarsRequest) -> Optional[pd.DataFrame]:
    try:
        return data_client.get_stock_bars(request).df
    except AttributeError as err:
        if "items" not in str(err):
            raise
        timeframe = getattr(request, "timeframe", None)
        feed = getattr(request, "feed", None)
        print(
            "⚠️ Alpaca returned null bars for "
            f"{symbol} (tf={getattr(timeframe, 'value', timeframe)}, feed={getattr(feed, 'value', feed)}); sanitizing raw payload."
        )
        if _RAW_DATA_CLIENT is None:
            print(f"⚠️ Raw data client unavailable; returning empty bars for {symbol}.")
            return None
        try:
            raw_payload = _RAW_DATA_CLIENT.get_stock_bars(request)
        except Exception as raw_err:
            print(f"⚠️ Raw payload fetch failed for {symbol}: {raw_err}")
            return None
        sanitized = _bars_df_from_raw_payload(raw_payload)
        if sanitized.empty:
            print(
                f"⚠️ No usable bars found for {symbol} after sanitizing raw payload "
                f"(tf={getattr(timeframe, 'value', timeframe)}, feed={getattr(feed, 'value', feed)})."
            )
            return None
        return sanitized


def _normalize_bars_for_symbol(
    bars: Optional[pd.DataFrame],
    symbol: str,
    *,
    reset_index: bool = False,
) -> pd.DataFrame:
    if bars is None or getattr(bars, "empty", True):
        return pd.DataFrame()

    subset = None

    try:
        if isinstance(bars.index, pd.MultiIndex) and "symbol" in bars.index.names:
            subset = bars.xs(symbol, level="symbol")
    except (KeyError, ValueError):
        subset = None

    if subset is None:
        try:
            if "symbol" in bars.columns:
                subset = bars[bars["symbol"] == symbol]
        except AttributeError:
            subset = None

    if subset is None:
        try:
            subset = bars.loc[symbol]
        except Exception:
            subset = bars

    if isinstance(subset, pd.Series):
        subset = subset.to_frame().T

    subset = subset.copy()
    if reset_index:
        subset = subset.reset_index()
    return subset


def _fetch_bars_with_daily_fallback(
    symbol: str,
    *,
    primary_tf: TimeFrame,
    primary_start: datetime,
    primary_end: datetime,
    feed: DataFeed = DataFeed.IEX,
    daily_lookback_days: int = 60,
    normalize_reset_index: bool = False,
) -> Tuple[pd.DataFrame, bool]:
    """Fetch bars for `symbol`, falling back to daily data if intraday is unavailable."""

    def _request(tf: TimeFrame, start_dt: datetime, end_dt: datetime, data_feed: DataFeed):
        req = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=tf,
            start=start_dt,
            end=end_dt,
            feed=data_feed,
        )
        return _fetch_bars_df(symbol, req)

    bars = _fetch_data_with_fallback(
        lambda feed: _request(primary_tf, primary_start, primary_end, feed),
        symbol,
        feed=feed,
    )
    used_daily = False

    if bars is None or getattr(bars, "empty", True):
        daily_start = primary_end - timedelta(days=daily_lookback_days)
        bars = _fetch_data_with_fallback(
            lambda feed: _request(TimeFrame.Day, daily_start, primary_end, feed),
            symbol,
            feed=feed,
        )
        used_daily = True

    normalized = _normalize_bars_for_symbol(bars, symbol, reset_index=normalize_reset_index)
    return normalized, used_daily

# === Flask App ===

app = Flask(__name__)

# --- Health check root route for Render ---
@app.route("/", methods=["GET"])
def home():
    return "Omega-VX running", 200

# === Watchdog cooldown ===
from time import monotonic
_last_close_attempt = {}
_CLOSE_COOLDOWN_SEC = 20  # consider 60–120 during market hours
_PDT_LOCKOUT_SEC = PDT_SYMBOL_LOCK_SECONDS or 600
_pdt_lockouts = {}
_last_close_attempt_lock = threading.Lock()
_pdt_lockouts_lock = threading.Lock()


def _get_last_close_attempt_ts(symbol: str) -> float:
    with _last_close_attempt_lock:
        return _last_close_attempt.get(symbol, 0.0)


def _set_last_close_attempt_ts(symbol: str, timestamp: float) -> None:
    with _last_close_attempt_lock:
        _last_close_attempt[symbol] = timestamp

_PDT_GLOBAL_LOCKOUT_UNTIL = 0.0
_PDT_LAST_ALERT_MONO = 0.0
_DAY_TRADE_STATUS_CACHE = {"expires": 0.0, "remaining": None, "is_pdt": None}


def _quantize_to_tick(price):
    """Clamp price to the allowed tick size (>= $1 → $0.01, otherwise $0.0001)."""
    if price is None:
        return None
    try:
        dec_price = Decimal(str(price))
    except (TypeError, ValueError):
        return price

    tick = Decimal("0.01") if dec_price >= Decimal("1") else Decimal("0.0001")
    quantized = dec_price.quantize(tick, rounding=ROUND_HALF_UP)
    if quantized <= 0:
        return 0.0
    return float(quantized)


def _is_pattern_day_trading_error(err: Exception) -> bool:
    msg = str(err).lower()
    if "pattern day trading protection" in msg:
        return True
    code = getattr(err, "code", None) or getattr(err, "error_code", None)
    if code and str(code) == "40310100":
        return True
    return False


def _register_pdt_lockout(symbol: str) -> int:
    """Record that PDT blocked the symbol and return the lockout duration (seconds)."""
    until = monotonic() + _PDT_LOCKOUT_SEC
    # Thread-safe update to shared state
    with _pdt_lockouts_lock:
        _pdt_lockouts[symbol.upper()] = until
    return _PDT_LOCKOUT_SEC


def _pdt_lockout_remaining(symbol: str) -> int:
    # Thread-safe read from shared state
    with _pdt_lockouts_lock:
        until = _pdt_lockouts.get(symbol.upper())
    if not until:
        return 0
    remaining = int(max(0, until - monotonic()))
    if remaining == 0:
        # Thread-safe update to shared state
        with _pdt_lockouts_lock:
            _pdt_lockouts.pop(symbol.upper(), None)
    return remaining


def _pdt_lockout_active(symbol: str) -> bool:
    return _pdt_lockout_remaining(symbol) > 0

# === Dev flags ===

FORCE_WEBHOOK_TEST = str(os.getenv("FORCE_WEBHOOK_TEST", "0")).strip().lower() in ("1", "true", "yes")


def _update_day_trade_status_from_account(account) -> tuple:
    if not PDT_GUARD_ENABLED:
        return (None, None)
    try:
        remaining = getattr(account, "day_trades_left", None)
    except Exception:
        remaining = None
    try:
        is_pdt = bool(getattr(account, "pattern_day_trader", False))
    except Exception:
        is_pdt = None
    _DAY_TRADE_STATUS_CACHE.update(
        {
            "remaining": remaining,
            "is_pdt": is_pdt,
            "expires": monotonic() + PDT_STATUS_CACHE_SECONDS,
        }
    )
    return remaining, is_pdt


def _get_day_trade_status() -> tuple:
    if not PDT_GUARD_ENABLED:
        return (None, None)
    now = monotonic()
    if now < _DAY_TRADE_STATUS_CACHE.get("expires", 0.0):
        return (
            _DAY_TRADE_STATUS_CACHE.get("remaining"),
            _DAY_TRADE_STATUS_CACHE.get("is_pdt"),
        )
    try:
        account = trading_client.get_account()
        return _update_day_trade_status_from_account(account)
    except Exception as e:
        print(f"⚠️ Failed to refresh day trade status: {e}")
        return (
            _DAY_TRADE_STATUS_CACHE.get("remaining"),
            _DAY_TRADE_STATUS_CACHE.get("is_pdt"),
        )


def _pdt_global_lockout_remaining() -> int:
    if not PDT_GUARD_ENABLED:
        return 0
    remaining = int(max(0.0, _PDT_GLOBAL_LOCKOUT_UNTIL - monotonic()))
    return remaining


def _pdt_global_lockout_active() -> bool:
    return _pdt_global_lockout_remaining() > 0


def _maybe_alert_pdt(reason: str, day_trades_left=None, pattern_flag=None):
    if not PDT_GUARD_ENABLED:
        return
    global _PDT_LAST_ALERT_MONO
    now = monotonic()
    if now - _PDT_LAST_ALERT_MONO < PDT_ALERT_COOLDOWN_SECONDS:
        return
    if day_trades_left is None or pattern_flag is None:
        cached_remaining, cached_flag = _get_day_trade_status()
        if day_trades_left is None:
            day_trades_left = cached_remaining
        if pattern_flag is None:
            pattern_flag = cached_flag
    msg = "🚫 PDT guard active"
    if reason:
        msg += f": {reason}"
    if day_trades_left is not None:
        msg += f" | day_trades_left={day_trades_left}"
    if pattern_flag is not None:
        msg += f" | pattern_day_trader={pattern_flag}"
    remaining = _pdt_global_lockout_remaining()
    if remaining:
        msg += f" | lockout={remaining}s"
    print(msg)
    try:
        send_telegram_alert(msg)
    except Exception:
        pass
    _PDT_LAST_ALERT_MONO = now


def _set_pdt_global_lockout(reason: str = "", seconds: int = None, day_trades_left=None, pattern_flag=None):
    if not PDT_GUARD_ENABLED:
        return
    global _PDT_GLOBAL_LOCKOUT_UNTIL
    duration = seconds if seconds is not None else PDT_GLOBAL_LOCK_SECONDS
    if duration <= 0:
        return
    until = monotonic() + duration
    if until > _PDT_GLOBAL_LOCKOUT_UNTIL:
        _PDT_GLOBAL_LOCKOUT_UNTIL = until
        _maybe_alert_pdt(reason, day_trades_left=day_trades_left, pattern_flag=pattern_flag)

# --- Auto‑Scanner flags ---
OMEGA_AUTOSCAN = str(os.getenv("OMEGA_AUTOSCAN", "0")).strip().lower() in ("1","true","yes","y","on")
OMEGA_AUTOSCAN_DRYRUN = str(os.getenv("OMEGA_AUTOSCAN_DRYRUN", "0")).strip().lower() in ("1","true","yes","y","on")
OMEGA_SCAN_INTERVAL_SEC = int(str(os.getenv("OMEGA_SCAN_INTERVAL_SEC", "120")).strip() or "120")

# --- Dynamic max open positions cap based on account equity ---
def get_dynamic_max_open_positions():
    try:
        account = trading_client.get_account()
        _update_day_trade_status_from_account(account)
        equity = float(account.equity)
        if equity < 25000:
            return 3
        return MAX_OPEN_POSITIONS_HIGH_EQUITY
    except Exception as e:
        print(f"⚠️ Failed to fetch equity for dynamic position cap: {e}")
        return 3  # fallback default, never less than 3

# --- Weekend Improvements Config -------------------------------------------
# End‑of‑Day (EOD) summary appender
EOD_SUMMARY = str(os.getenv("EOD_SUMMARY", "1")).strip().lower() in ("1","true","yes","y","on")
EOD_SUMMARY_HOUR_UTC = int(str(os.getenv("EOD_SUMMARY_HOUR_UTC", "20")).strip() or "20")  # default 20:00 UTC
PERF_DAILY_TAB = _clean_env(os.getenv("PERF_DAILY_TAB") or "Performance Daily")

# Open Positions → Google Sheet pusher
OPEN_POSITIONS_PUSH = str(os.getenv("OPEN_POSITIONS_PUSH", "1")).strip().lower() in ("1","true","yes","y","on")
OPEN_POSITIONS_TAB = _clean_env(os.getenv("OPEN_POSITIONS_TAB") or "Open Positions")
OPEN_POSITIONS_INTERVAL = int(str(os.getenv("OPEN_POSITIONS_INTERVAL", "90")).strip() or "90")

# Stray order janitor (cleans SELLs when no position)
ORDER_JANITOR = str(os.getenv("ORDER_JANITOR", "1")).strip().lower() in ("1","true","yes","y","on")
ORDER_JANITOR_INTERVAL = int(str(os.getenv("ORDER_JANITOR_INTERVAL", "180")).strip() or "180")

# Monday auto-flush safety layer (prevents carrying unprotected positions into the week)
WEEKLY_FLUSH_ENABLED = _bool_env("WEEKLY_FLUSH_ENABLED", "1")
WEEKLY_FLUSH_MODE = (_clean_env(os.getenv("WEEKLY_FLUSH_MODE") or "unprotected") or "unprotected").lower()
if WEEKLY_FLUSH_MODE not in {"unprotected", "all"}:
    WEEKLY_FLUSH_MODE = "unprotected"
WEEKLY_FLUSH_HOUR_UTC = int(str(os.getenv("WEEKLY_FLUSH_HOUR_UTC", "13")).strip() or "13")
WEEKLY_FLUSH_MINUTE_UTC = int(str(os.getenv("WEEKLY_FLUSH_MINUTE_UTC", "35")).strip() or "35")
WEEKLY_FLUSH_WHITELIST = {
    symbol.strip().upper()
    for symbol in (os.getenv("WEEKLY_FLUSH_WHITELIST") or "").split(",")
    if symbol.strip()
}
WEEKLY_FLUSH_LAST_RUN_FILE = os.path.join(LOG_DIR, "weekly_flush_last_run.txt")
WEEKLY_FLUSH_HOUR_UTC = max(0, min(23, WEEKLY_FLUSH_HOUR_UTC))
WEEKLY_FLUSH_MINUTE_UTC = max(0, min(59, WEEKLY_FLUSH_MINUTE_UTC))

# --- SAFE CLOSE HELPERS -------------------------------------------------------
from alpaca.trading.requests import GetOrdersRequest, MarketOrderRequest
from alpaca.trading.enums import QueryOrderStatus, OrderSide, TimeInForce

# --- SAFE CLOSE FILL HELPERS ------------------------------------------------
def _get_order_by_id(order_id: str):
    try:
        return trading_client.get_order_by_id(order_id)
    except Exception as e:
        print(f"⚠️ get_order_by_id failed {order_id}: {e}")
        return None

def poll_order_fill(order_id: str, timeout: int = 90, poll_secs: int = 2):
    """
    Poll an order until it has a filled quantity. Returns dict with
    {'filled_qty': float, 'filled_avg_price': float|None}.
    """
    import time as _t
    deadline = _t.time() + timeout
    while _t.time() < deadline:
        o = _get_order_by_id(order_id)
        if o is not None:
            status = str(getattr(o, "status", "")).lower()
            filled_qty = float(getattr(o, "filled_qty", 0) or 0)
            filled_avg_price = getattr(o, "filled_avg_price", None)
            if status in ("filled", "partially_filled") and filled_qty > 0:
                try:
                    filled_avg_price = float(filled_avg_price)
                except Exception:
                    pass
                return {"filled_qty": filled_qty, "filled_avg_price": filled_avg_price}
        _t.sleep(poll_secs)
    return {"filled_qty": 0.0, "filled_avg_price": None}

def find_recent_sell_fill(symbol: str, lookback_sec: int = 240):
    """
    Find the most recent CLOSED SELL order for `symbol` within `lookback_sec`
    and return its filled qty/avg price.
    """
    try:
        since = datetime.now(timezone.utc) - timedelta(seconds=lookback_sec)
        req = GetOrdersRequest(status=QueryOrderStatus.CLOSED, symbols=[symbol])
        orders = trading_client.get_orders(filter=req)
        # newest first
        orders = [o for o in orders if str(getattr(o, "side", "")).lower().endswith("sell")]
        orders.sort(key=lambda o: getattr(o, "submitted_at", datetime.now(timezone.utc)), reverse=True)
        for o in orders:
            sub = getattr(o, "submitted_at", None)
            try:
                # normalize to aware UTC
                if sub and sub.tzinfo is None:
                    sub = sub.replace(tzinfo=timezone.utc)
            except Exception:
                sub = None
            if sub and sub < since:
                continue
            fq = float(getattr(o, "filled_qty", 0) or 0)
            favg = getattr(o, "filled_avg_price", None)
            if fq > 0:
                try:
                    favg = float(favg)
                except Exception:
                    pass
                return {"filled_qty": fq, "filled_avg_price": favg}
    except Exception as e:
        print(f"⚠️ find_recent_sell_fill error for {symbol}: {e}")
    return {"filled_qty": 0.0, "filled_avg_price": None}

def cancel_open_sells(symbol: str) -> int:
    """Cancel all OPEN sell orders (limit/stop) for symbol to avoid wash-trade rejects."""
    n = 0
    try:
        req = GetOrdersRequest(status=QueryOrderStatus.OPEN, symbols=[symbol])
        orders = trading_client.get_orders(filter=req)
        for o in orders:
            if str(o.side).lower().endswith("sell"):
                try:
                    trading_client.cancel_order_by_id(o.id)
                    n += 1
                except Exception as ce:
                    print(f"⚠️ cancel failed {symbol} {o.id}: {ce}")
    except Exception as e:
        print(f"⚠️ list open sells failed for {symbol}: {e}")
    return n

def close_position_safely(symbol: str, *, session_type: str = "intraday", close_reason: str = "") -> bool:
    """
    Prevent “potential wash trade detected” by:
      1) Cancel all open SELL orders for the symbol
      2) Wait briefly for cancels to settle
      3) Close position with a single market SELL (reduce‑only via close_position)
    """
    if PDT_GUARD_ENABLED and _pdt_global_lockout_active():
        return False
    # Capture pre-close position details
    pre_qty = 0
    pre_avg = None
    try:
        pos = [p for p in trading_client.get_all_positions() if p.symbol.upper() == symbol.upper()]
        if pos:
            pre_qty = int(float(pos[0].qty))
            pre_avg = float(pos[0].avg_entry_price)
    except Exception as _e:
        pass

    # 1) cancel TP/SL first
    n = cancel_open_sells(symbol)
    if n:
        print(f"🧹 Canceled {n} open SELL orders for {symbol}.")
    else:
        print(f"🧹 No open SELL orders to cancel for {symbol}.")

    # 2) let cancels settle
    time.sleep(0.8)

    # 3) close via API convenience (reduce‑only semantics)
    try:
        trading_client.close_position(symbol)  # Alpaca’s safe close
        print(f"✅ Requested close for {symbol} (market).")
        send_telegram_alert(f"✅ Closed {symbol} (safe close).")
        # Try to find the resulting SELL fill and log realized P&L
        try:
            fill = find_recent_sell_fill(symbol, lookback_sec=240)
            fqty = int(float(fill.get("filled_qty") or 0))
            favg = fill.get("filled_avg_price")
            realized = None
            if pre_avg is not None and favg is not None and fqty > 0:
                realized = (float(favg) - float(pre_avg)) * float(fqty)
            try:
                log_trade(
                    symbol,
                    fqty or pre_qty,
                    pre_avg if pre_avg is not None else 0.0,
                    None,
                    None,
                    status="closed",
                    action="SELL",
                    fill_price=favg,
                    realized_pnl=realized,
                    session_type=session_type,
                    close_reason=close_reason,
                )
            except Exception as _le:
                print(f"⚠️ Trade log (SELL) failed: {_le}")

            exit_price = None
            try:
                if favg is not None:
                    exit_price = float(favg)
                elif pre_avg is not None:
                    exit_price = float(pre_avg)
            except Exception:
                exit_price = None
            if exit_price is not None:
                key = symbol.upper()
                with _symbol_last_trade_lock:
                    prev = _symbol_last_trade.get(key, {})
                    current_count = int(prev.get("count", 0) or 0)
                    if current_count <= 0:
                        current_count = 1
                    _symbol_last_trade[key] = {
                        "date": datetime.now().date(),
                        "exit_price": exit_price,
                        "count": current_count,
                        "last_trade_ts": datetime.now(timezone.utc).isoformat(),
                    }
        except Exception as _fe:
            print(f"⚠️ Could not backfill SELL fill for {symbol}: {_fe}")
        return True
    except Exception as e:
        if _is_pattern_day_trading_error(e):
            cooldown = _register_pdt_lockout(symbol)
            remaining = _pdt_lockout_remaining(symbol)
            minutes = max(1, remaining // 60) if remaining else max(1, cooldown // 60)
            _set_pdt_global_lockout(f"Close denied for {symbol}")
            print(f"⛔ Close denied for {symbol}: pattern day trading protection. Deferring retries for ~{minutes} minute(s).")
            try:
                send_telegram_alert(
                    f"⛔ Close blocked for {symbol}: pattern day trading protection. Retrying after ~{minutes} minute(s)."
                )
            except Exception:
                pass
            return False
        # Fallback: explicit market SELL sized to position
        try:
            pos = [p for p in trading_client.get_all_positions() if p.symbol.upper()==symbol.upper()]
            if not pos:
                print(f"ℹ️ No {symbol} position found; nothing to close.")
                return True
            qty = int(float(pos[0].qty))
            if qty <= 0:
                print(f"ℹ️ Non‑positive qty for {symbol}; nothing to close.")
                return True
            sell_order = trading_client.submit_order(MarketOrderRequest(
                symbol=symbol, qty=qty, side=OrderSide.SELL, time_in_force=TimeInForce.DAY
            ))
            info = {}
            try:
                info = poll_order_fill(sell_order.id, timeout=90, poll_secs=2)
            except Exception as _:
                info = {}
            fqty = int(float(info.get("filled_qty") or qty or 0))
            favg = info.get("filled_avg_price")
            realized = None
            if pre_avg is not None and favg is not None and fqty > 0:
                realized = (float(favg) - float(pre_avg)) * float(fqty)
            print(f"✅ Submitted market SELL {fqty} {symbol} (fallback).")
            send_telegram_alert(f"✅ Closed {symbol} with market SELL (fallback).")
            try:
                log_trade(
                    symbol,
                    fqty,
                    pre_avg if pre_avg is not None else 0.0,
                    None,
                    None,
                    status="closed",
                    action="SELL",
                    fill_price=favg,
                    realized_pnl=realized,
                    session_type=session_type,
                    close_reason=close_reason,
                )
            except Exception as _le:
                print(f"⚠️ Trade log (SELL) failed: {_le}")

            exit_price = None
            try:
                if favg is not None:
                    exit_price = float(favg)
                elif pre_avg is not None:
                    exit_price = float(pre_avg)
            except Exception:
                exit_price = None
            if exit_price is not None:
                key = symbol.upper()
                with _symbol_last_trade_lock:
                    prev = _symbol_last_trade.get(key, {})
                    current_count = int(prev.get("count", 0) or 0)
                    if current_count <= 0:
                        current_count = 1
                    _symbol_last_trade[key] = {
                        "date": datetime.now().date(),
                        "exit_price": exit_price,
                        "count": current_count,
                        "last_trade_ts": datetime.now(timezone.utc).isoformat(),
                    }
            return True
        except Exception as e2:
            if _is_pattern_day_trading_error(e2):
                cooldown = _register_pdt_lockout(symbol)
                remaining = _pdt_lockout_remaining(symbol)
                minutes = max(1, remaining // 60) if remaining else max(1, cooldown // 60)
                _set_pdt_global_lockout(f"Fallback close denied for {symbol}")
                print(f"⛔ Fallback close denied for {symbol}: pattern day trading protection. Deferring retries for ~{minutes} minute(s).")
                try:
                    send_telegram_alert(
                        f"⛔ Fallback close blocked for {symbol}: pattern day trading protection. Retrying after ~{minutes} minute(s)."
                    )
                except Exception:
                    pass
                return False
            print(f"❌ Safe close failed for {symbol}: {e} / fallback: {e2}")
            send_telegram_alert(f"❌ Failed to close {symbol}: {e}")
            return False

def post_trade_protection_audit(symbol, entry_price, tp_price, sl_price):
    """
    Verify that both Take-Profit (TP) and Stop-Loss (SL) are active for a symbol.
    If either is missing, cancel open SELLs and rebuild both orders.
    """
    try:
        req = GetOrdersRequest(status=QueryOrderStatus.OPEN, symbols=[symbol])
        open_orders = trading_client.get_orders(filter=req)

        existing_tp = None
        existing_sl = None

        for o in open_orders:
            side = str(getattr(o, "side", "")).lower()
            if side != "sell":
                continue
            otype = str(getattr(o, "order_type", "")).lower()
            price = float(getattr(o, "limit_price") or getattr(o, "stop_price") or 0)
            if otype == "limit" and price > 0:
                existing_tp = price
            elif otype == "stop" and price > 0:
                existing_sl = price

        if not existing_tp or not existing_sl:
            print(f"⚠️ Missing TP or SL for {symbol}. Rebuilding protection set...")
            cancel_open_sells(symbol)
            time.sleep(1.0)

            qty = 0
            try:
                pos = [p for p in trading_client.get_all_positions() if p.symbol.upper() == symbol.upper()]
                if pos:
                    qty = int(float(pos[0].qty))
            except Exception as e:
                print(f"⚠️ Could not fetch qty for {symbol}: {e}")

            if qty > 0:
                tp_order = trading_client.submit_order(
                    LimitOrderRequest(
                        symbol=symbol,
                        qty=qty,
                        side=OrderSide.SELL,
                        limit_price=_quantize_to_tick(tp_price),
                        time_in_force=TimeInForce.DAY,
                    )
                )
                sl_order = trading_client.submit_order(
                    StopOrderRequest(
                        symbol=symbol,
                        qty=qty,
                        side=OrderSide.SELL,
                        stop_price=_quantize_to_tick(sl_price),
                        time_in_force=TimeInForce.DAY,
                    )
                )
                print(f"🛡 Reattached TP={tp_price} / SL={sl_price} for {symbol}.")
                send_telegram_alert(f"🛡 Reattached protection for {symbol}: TP={tp_price}, SL={sl_price}")
            else:
                print(f"⚠️ No active qty found for {symbol} to re-attach protection.")
        else:
            print(f"🛡 Protection verified for {symbol}: TP={existing_tp}, SL={existing_sl}")

    except Exception as e:
        print(f"❌ post_trade_protection_audit failed for {symbol}: {e}")

def smart_hold_check(symbol: str) -> bool:
    """
    Decide whether to hold a position overnight.
    Only hold if both 15m and 1h Heikin-Ashi trends are bullish AND RSI > 60.
    """
    try:
        trend_15m = get_heikin_ashi_trend(symbol, interval="15m")
        trend_1h = get_heikin_ashi_trend(symbol, interval="1h")
        rsi = get_rsi_value(symbol, interval="15m")

        if trend_15m == "bullish" and trend_1h == "bullish" and rsi and rsi > 60:
            print(f"🌙 Smart Hold: {symbol} passes hold criteria (RSI={rsi}, trends bullish).")
            return True
        else:
            print(f"🌙 Smart Hold: {symbol} fails hold criteria (RSI={rsi}, trends={trend_15m}/{trend_1h}).")
            return False
    except Exception as e:
        print(f"⚠️ Smart Hold check failed for {symbol}: {e}")
        return False

# 🧠 Generate AI-based trade explanation
def generate_trade_explanation(symbol, entry, stop_loss, take_profit, rsi=None, trend=None, ha_candle=None):
    """
    Generate a human-readable explanation for why the trade was taken.
    """

    reasons = []

    if rsi is not None:
        if rsi < 30:
            reasons.append(f"RSI is oversold ({rsi}), suggesting a potential upward reversal.")
        elif rsi > 70:
            reasons.append(f"RSI is overbought ({rsi}), suggesting a potential downward reversal.")

    if trend is not None:
        if trend == "uptrend":
            reasons.append("EMA trend filter indicates a bullish market.")
        elif trend == "downtrend":
            reasons.append("EMA trend filter indicates a bearish market.")

    if ha_candle is not None:
        if ha_candle == "bullish":
            reasons.append("Heikin Ashi candle shows bullish momentum.")
        elif ha_candle == "bearish":
            reasons.append("Heikin Ashi candle shows bearish momentum.")

    explanation = f"Trade setup for {symbol} at {entry}, Stop Loss={stop_loss}, Take Profit={take_profit}."
    if reasons:
        explanation += " Reasons: " + " ".join(reasons)

    return explanation

def send_telegram_alert(message: str):
    import requests  # keep local to guarantee availability
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Telegram not configured (missing token/chat id).")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}

    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            print(f"⚠️ Telegram send failed: {r.status_code} {r.text}")
    except Exception as e:
        print(f"❌ Telegram alert error: {e}")

def get_watchlist_from_google_sheet(sheet_name="OMEGA-VX LOGS", tab_name="watchlist"):
    """
    Return a list of symbols from the first column of the watchlist tab.
    Prefers GOOGLE_SHEET_ID. Tab name can be overridden via WATCHLIST_SHEET_NAME.
    """
    try:
        client = _get_gspread_client()

        ss = None
        sheet_id = _clean_env(os.getenv("GOOGLE_SHEET_ID"))
        tab_override = _clean_env(os.getenv("WATCHLIST_SHEET_NAME"))
        if tab_override:
            tab_name = tab_override

        if sheet_id:
            try:
                print(f"📄 Opening spreadsheet by ID: {sheet_id!r}")
                ss = client.open_by_key(sheet_id)
            except Exception as e_id:
                print(f"⚠️ open_by_key failed for id {sheet_id!r}: {type(e_id).__name__}: {e_id}")

        if ss is None:
            try:
                print(f"📄 Falling back to open by title: {sheet_name}")
                ss = client.open(sheet_name)
            except Exception as e_title:
                raise Exception(
                    f"Spreadsheet not reachable. Tried id={sheet_id!r} and title={sheet_name!r}. Error: {type(e_title).__name__}: {e_title}"
                )

        try:
            ws = ss.worksheet(tab_name)
        except Exception as e_ws:
            try:
                tabs = [w.title for w in ss.worksheets()]
            except Exception:
                tabs = []
            raise Exception(
                f"Worksheet '{tab_name}' not found. Available tabs: {tabs}. Error: {type(e_ws).__name__}: {e_ws}"
            )

        symbols = ws.col_values(1)
        out = []
        for s in symbols:
            s = (s or "").strip().upper()
            if not s or s == "SYMBOL":
                continue
            out.append(s)

        if not out:
            print("⚠️ Watchlist tab reached but it is empty (after header filtering).")
        else:
            preview = ", ".join(out[:10]) + (" …" if len(out) > 10 else "")
            print(f"✅ Watchlist loaded ({len(out)}): {preview}")

        return out

    except Exception as e:
        print(f"❌ Failed to fetch watchlist: {type(e).__name__}: {e}")
        return []

def handle_restart_notification():
    now = datetime.now()
    try:
        if os.path.exists(CRASH_LOG_FILE):
            with open(CRASH_LOG_FILE, 'r') as f:
                last_boot = f.read().strip()
                if last_boot:
                    last_time = datetime.strptime(last_boot, "%Y-%m-%d %H:%M:%S")
                    diff = (now - last_time).total_seconds() / 60
                    msg = f"♻️ OMEGA-VX restarted — last boot was {diff:.1f} minutes ago."
                    print(msg)
                    send_telegram_alert(msg)
        else:
            print("🆕 First boot — no prior crash log found.")

    except Exception as e:
        print(f"⚠️ Crash log check failed: {e}")

    # ✅ Update the file with the current boot time
    try:
        with open(CRASH_LOG_FILE, 'w') as f:
            f.write(now.strftime("%Y-%m-%d %H:%M:%S"))
    except Exception as e:
        print(f"⚠️ Failed to write crash log: {e}")

def auto_adjust_risk_percent():
    try:
        path = os.path.join(LOG_DIR, "equity_curve.log")
        if not os.path.exists(path):
            print("🧠 No equity curve data found — skipping AI adjustment.")
            return

        with open(path, "r") as f:
            lines = f.readlines()[-5:]  # last 5 records
            equity_values = []
            for line in lines:
                try:
                    parts = line.strip().split("EQUITY:")
                    if len(parts) == 2:
                        equity = float(parts[1].replace("$", ""))
                        if equity > 0:
                            equity_values.append(equity)
                except Exception:
                    continue

        if len(equity_values) < 2:
            print("🧠 Not enough equity data to analyze slope.")
            return

        slope = (equity_values[-1] - equity_values[0]) / (len(equity_values) - 1)
        print(f"📈 Equity slope: {slope:.2f}")

        global MAX_RISK_PER_TRADE_PERCENT
        target = MAX_RISK_BASE_PERCENT
        if slope <= -10:
            target = MAX_RISK_BASE_PERCENT - MAX_RISK_AUTO_DOWN_STEP
            target = _clamp(target, MAX_RISK_AUTO_MIN, MAX_RISK_AUTO_MAX)
            print(f"⚠️ AI Auto-Tune: Negative slope → Risk adjusted to {target:.2f}%")
        elif slope >= 20:
            target = MAX_RISK_BASE_PERCENT + MAX_RISK_AUTO_UP_STEP
            target = _clamp(target, MAX_RISK_AUTO_MIN, MAX_RISK_AUTO_MAX)
            print(f"🚀 AI Auto-Tune: Positive slope → Risk adjusted to {target:.2f}%")
        else:
            target = _clamp(MAX_RISK_BASE_PERCENT, MAX_RISK_AUTO_MIN, MAX_RISK_AUTO_MAX)
            print(f"🧠 AI Auto-Tune: Risk normalized to {target:.2f}%")

        MAX_RISK_PER_TRADE_PERCENT = target

    except Exception as e:
        print(f"❌ AI Auto-Tune failed: {e}")
auto_adjust_risk_percent()

def get_bars(symbol, interval='15m', lookback=10):
    """
    Fetch recent bars with the configured data feed, falling back to IEX if SIP is not allowed.
    """
    from alpaca.data.requests import StockBarsRequest
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=15 * (lookback + 1))
    tf = TimeFrame.Minute if interval == '15m' else TimeFrame.Hour

    def _request(feed):
        req = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=tf,
            start=start,
            end=end,
            feed=feed,
        )
        return _fetch_bars_df(symbol, req)

    bars = _fetch_data_with_fallback(_request, symbol, feed=_DATA_FEED)
    if bars is None or bars.empty:
        print(f"⚠️ No Alpaca data returned for {symbol}")
        return None
    return bars

def is_within_trading_hours(start_hour=13, start_minute=30, end_hour=20):
    if FORCE_WEBHOOK_TEST:
        return True
    now_utc = datetime.now(timezone.utc).time()
    start = dt_time(hour=start_hour, minute=start_minute)
    end = dt_time(hour=end_hour, minute=0)
    return start <= now_utc <= end

def get_heikin_ashi_trend(symbol, interval='15m', lookback=2):
    """
    Heikin‑Ashi using Alpaca bars. We FORCE IEX. If minute/hour bars are not
    available on your plan, we fall back to DAILY bars and use the last HA candle.
    """
    tf = TimeFrame.Minute if interval == '15m' else TimeFrame.Hour
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=2)

    bars, used_daily = _fetch_bars_with_daily_fallback(
        symbol,
        primary_tf=tf,
        primary_start=start,
        primary_end=end,
        feed=DataFeed.IEX,
        daily_lookback_days=20,
        normalize_reset_index=True,
    )

    if bars.empty:
        print(f"⚠️ No Alpaca data returned for {symbol}")
        return None

    if used_daily:
        if len(bars) < 2:
            print(f"⚠️ Not enough daily data to compute Heikin Ashi for {symbol}.")
            return None
        prev_close = float(bars['close'].iloc[-2])
        curr_open = float(bars['open'].iloc[-1])
        ha_open = (curr_open + prev_close) / 2
        ha_close = (
            float(bars['open'].iloc[-1])
            + float(bars['high'].iloc[-1])
            + float(bars['low'].iloc[-1])
            + float(bars['close'].iloc[-1])
        ) / 4
        trend = 'bullish' if ha_close > ha_open else ('bearish' if ha_close < ha_open else 'neutral')
        print(f"🕊️ Heikin-Ashi DAILY fallback for {symbol}: {trend}")
        return trend

    if len(bars) < lookback + 1:
        print(f"⚠️ Not enough data to compute Heikin Ashi for {symbol}.")
        return None

    ha_candles = []
    for i in range(1, lookback + 1):
        prev_close = float(bars['close'].iloc[-(i + 1)])
        curr_open = float(bars['open'].iloc[-i])
        ha_open = (curr_open + prev_close) / 2
        ha_close = (
            float(bars['open'].iloc[-i])
            + float(bars['high'].iloc[-i])
            + float(bars['low'].iloc[-i])
            + float(bars['close'].iloc[-i])
        ) / 4
        ha_candles.append({'open': ha_open, 'close': ha_close})

    last = ha_candles[-1]
    if last['close'] > last['open']:
        return 'bullish'
    if last['close'] < last['open']:
        return 'bearish'
    return 'neutral'

def is_multi_timeframe_confirmed(symbol):
    trend_15m = get_heikin_ashi_trend(symbol, interval='15m')
    trend_1h = get_heikin_ashi_trend(symbol, interval='1h')

    print(f"🕒 MTF Check | 15m: {trend_15m} | 1h: {trend_1h}")

    if trend_15m == trend_1h and trend_15m == 'bullish':
        return True
    else:
        return False

# === AUTOSCAN ENGINE =========================================================
def _open_positions_count() -> int:
    try:
        return len(trading_client.get_all_positions())
    except Exception as e:
        print(f"⚠️ count positions failed: {e}")
        return 0


def has_open_position(symbol: str, positions: Optional[Iterable] = None) -> bool:
    """
    Return True if there is an open (non-zero) position for `symbol`.
    Accepts an optional iterable of positions to avoid redundant API calls.
    """
    try:
        if positions is None:
            positions = trading_client.get_all_positions()
    except Exception as e:
        print(f"⚠️ has_open_position failed for {symbol}: {e}")
        return False

    symbol_upper = symbol.upper()
    for pos in positions or []:
        try:
            if getattr(pos, "symbol", "").upper() != symbol_upper:
                continue
            qty_candidates = (
                getattr(pos, "qty_available", None),
                getattr(pos, "qty", None),
            )
            for raw_qty in qty_candidates:
                if raw_qty is None:
                    continue
                try:
                    if abs(float(raw_qty)) > 0:
                        return True
                except Exception:
                    continue
            return False
        except Exception:
            continue
    return False


MIN_VOLUME = 10000
MIN_VOLATILITY_PCT = 0.15
MAX_VOLATILITY_PCT = 5.0


def _get_vix_from_yahoo() -> float:
    """
    Fetch latest VIX close from Yahoo Finance as a fallback.
    Retries once with a short delay if rate-limited.
    """
    url = "https://query1.finance.yahoo.com/v8/finance/chart/%5EVIX?interval=1d&range=5d"
    for attempt in range(2):  # first try + one retry
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            result = data.get("chart", {}).get("result") or []
            if not result:
                break
            closes = (
                result[0]
                .get("indicators", {})
                .get("quote", [{}])[0]
                .get("close", [])
            )
            for value in reversed(closes):
                if value is not None:
                    vix_value = float(value)
                    print(f"🌐 Yahoo fallback: VIX={vix_value:.2f} — adaptive filters updated.")
                    return vix_value
        except requests.exceptions.HTTPError as err:
            status = getattr(err.response, "status_code", None)
            if status == 429 and attempt == 0:
                print("⚠️ Yahoo rate-limit — retrying in 10 s...")
                time.sleep(10)
                continue
            print(f"⚠️ Yahoo VIX fallback failed: {err}")
            break
        except Exception as err:
            print(f"⚠️ Yahoo VIX fetch exception: {err}")
            break
    return 0.0


def _ema_trend_confirmed(bars) -> bool:
    try:
        closes = bars['close'].astype(float)
    except Exception:
        closes = pd.Series(dtype=float)
    if len(closes) < 55:
        return False
    ema20 = closes.ewm(span=20, adjust=False).mean().iloc[-1]
    ema50 = closes.ewm(span=50, adjust=False).mean().iloc[-1]
    price = closes.iloc[-1]
    return price > ema20 > ema50


def _auto_adjust_filters_by_vix():
    """
    Automatically tune volatility/volume filters based on current VIX level.
    Ensures Omega adapts to calm or turbulent markets autonomously.
    """
    try:
        vix = get_current_vix()
        if vix <= 0:
            print("⚠️ Could not fetch VIX from Alpaca; attempting Yahoo fallback.")
            vix = _get_vix_from_yahoo()
            if vix <= 0:
                print("⚠️ Could not fetch VIX; keeping previous thresholds.")
                return

        global MIN_VOLATILITY_PCT, MIN_VOLUME, FALLBACK_STOP_LOSS_PCT, FALLBACK_TAKE_PROFIT_PCT
        if vix < 15:
            MIN_VOLATILITY_PCT = 0.04
            MIN_VOLUME = 500
            FALLBACK_STOP_LOSS_PCT = 1.0
            FALLBACK_TAKE_PROFIT_PCT = FALLBACK_STOP_LOSS_PCT * 2.0
        elif vix <= 25:
            MIN_VOLATILITY_PCT = 0.08
            MIN_VOLUME = 1000
            FALLBACK_STOP_LOSS_PCT = 2.0
            FALLBACK_TAKE_PROFIT_PCT = FALLBACK_STOP_LOSS_PCT * 2.5
        else:
            MIN_VOLATILITY_PCT = 0.15
            MIN_VOLUME = 2000
            FALLBACK_STOP_LOSS_PCT = 3.0
            FALLBACK_TAKE_PROFIT_PCT = FALLBACK_STOP_LOSS_PCT * 3.0

        print(
            "🧠 VIX="
            f"{vix:.2f} → filters: volatility≥{MIN_VOLATILITY_PCT}% | volume≥{MIN_VOLUME} | "
            f"SL={FALLBACK_STOP_LOSS_PCT:.2f}% | TP={FALLBACK_TAKE_PROFIT_PCT:.2f}%"
        )
    except Exception as e:
        print(f"⚠️ Auto-filter calibration failed: {e}")


def _best_candidate_from_watchlist(symbols):
    """Return best watchlist candidate after enforcing risk filters.

    Hard filters:
      • Average 15m volume ≥ MIN_VOLUME (auto-tuned)
      • 15m volatility between MIN_VOLATILITY_PCT and MAX_VOLATILITY_PCT (auto-tuned)
      • Price > EMA20 > EMA50
      • RSI(15m) in [35, 65]
      • ≥ 10 minutes since this ticker was last traded

    Symbols passing the filters are scored to break ties.
    """
    ranked = []
    today = datetime.now().date()
    try:
        current_positions = trading_client.get_all_positions()
    except Exception as e:
        print(f"⚠️ autoscan position fetch failed: {e}")
        current_positions = []
    for sym in symbols:
        try:
            s = sym.strip().upper()
            if not s:
                continue
            # --- Skip if already traded today (only when no open position) ---
            with _symbol_last_trade_lock:
                record = _symbol_last_trade.get(s)
                record = None if record is None else record.copy()
            if record and record.get("date") == today and not has_open_position(s, current_positions):
                last_ts = record.get("last_trade_ts")
                if last_ts:
                    try:
                        last_dt = datetime.fromisoformat(last_ts)
                        if (datetime.now(timezone.utc) - last_dt).total_seconds() < PER_SYMBOL_COOLDOWN_SECONDS:
                            print(f"🚫 Skipping {s}: traded {int((datetime.now(timezone.utc)-last_dt).total_seconds())}s ago (cooldown active).")
                            continue
                    except Exception:
                        pass
                print(f"🚫 Skipping {s}: already traded today.")
                continue
            # --- Fetch bars for volume/volatility filter ---
            bars = get_bars(s, interval='15m', lookback=70)
            if bars is None or 'volume' not in bars.columns or len(bars) < 5:
                print(f"⚠️ Skipping {s}: insufficient volume data.")
                continue
            closes = bars['close'].astype(float)
            avg_vol = float(bars['volume'].tail(20).mean())
            returns = closes.pct_change().dropna()
            volatility = returns.std() * 100
            if avg_vol < MIN_VOLUME or volatility > MAX_VOLATILITY_PCT or volatility < MIN_VOLATILITY_PCT:
                print(f"🚫 Skipping {s}: volume={avg_vol:.0f}, volatility={volatility:.2f}% — outside safe limits.")
                continue
            if not _ema_trend_confirmed(bars.tail(60)):
                print(f"🚫 Skipping {s}: EMA trend filter failed.")
                continue
            # --- MTF and RSI scoring ---
            mtf_bull = is_multi_timeframe_confirmed(s)
            rsi = get_rsi_value(s, interval='15m')
            if rsi is None or rsi < 35 or rsi > 65:
                print(f"🚫 Skipping {s}: RSI {rsi} outside 35-65.")
                continue
            score = 0
            if mtf_bull:
                score += 2
            score += 1  # RSI already validated neutral band
            # --- Volume/volatility scoring ---
            score_mod = 0
            if avg_vol > 250000:
                score_mod += 1
            if volatility > 3.5:
                score_mod -= 1
            score += score_mod
            print(f"🧪 Score {s}: score={score} (mtf_bull={mtf_bull}, rsi={rsi}, vol={avg_vol:.0f}, volat={volatility:.2f}%)")
            ranked.append((score, s))
        except Exception as e:
            print(f"⚠️ scoring {sym} failed: {e}")
            continue
    ranked.sort(reverse=True)
    return ranked[0][1] if ranked and ranked[0][0] > 0 else None

def _compute_entry_tp_sl(symbol: str):
    """
    Pull live quote; compute a conservative TP/SL if not provided using the
    configured FALLBACK_STOP_LOSS_PCT / FALLBACK_TAKE_PROFIT_PCT offsets.
    """
    from alpaca.data.requests import StockLatestQuoteRequest
    try:
        req = StockLatestQuoteRequest(symbol_or_symbols=symbol, feed=_DATA_FEED)
        q = data_client.get_stock_latest_quote(req)
        px = float(q[symbol].ask_price or q[symbol].bid_price)
    except Exception as e:
        if "subscription does not permit" in str(e).lower() and _DATA_FEED != DataFeed.IEX:
            try:
                req = StockLatestQuoteRequest(symbol_or_symbols=symbol, feed=DataFeed.IEX)
                q = data_client.get_stock_latest_quote(req)
                px = float(q[symbol].ask_price or q[symbol].bid_price)
            except Exception as e2:
                print(f"⚠️ quote fetch failed for {symbol} (IEX fallback): {e2}")
                return None
        else:
            print(f"⚠️ quote fetch failed for {symbol}: {e}")
            return None
    entry = round(px, 2)
    sl = round(entry * (1 - FALLBACK_STOP_LOSS_PCT / 100.0), 2)
    tp = round(entry * (1 + FALLBACK_TAKE_PROFIT_PCT / 100.0), 2)
    return entry, sl, tp

def autoscan_once():
    # stop if too many positions (dynamic cap)
    print(f"🧮 Open positions: {_open_positions_count()} / Max allowed: {get_dynamic_max_open_positions()}")
    if _open_positions_count() >= get_dynamic_max_open_positions():
        print(f"⛔ Position cap reached ({get_dynamic_max_open_positions()}).")
        return False

    # read watchlist
    watch = get_watchlist_from_google_sheet(sheet_name="OMEGA-VX LOGS", tab_name="watchlist")
    if not watch:
        print("⚠️ Watchlist empty or not reachable.")
        return False

    _auto_adjust_filters_by_vix()

    # pick a candidate
    sym = _best_candidate_from_watchlist(watch)
    if not sym:
        print("ℹ️ No positive‑score candidate right now.")
        return False

    # compute prices
    trio = _compute_entry_tp_sl(sym)
    if not trio:
        return False
    entry, sl, tp = trio

    # respect cooldown, hours, and equity guard
    if is_cooldown_active():
        print("⏳ Global cooldown active; skipping autoscan trade.")
        return False
    if not is_within_trading_hours():
        print("🕑 Outside trading hours; autoscan skip.")
        return False
    if should_block_trading_due_to_equity():
        print("🛑 Equity guard active; autoscan skip.")
        return False

    print(f"🤖 AUTOSCAN candidate {sym}: entry={entry} SL={sl} TP={tp}")
    return submit_order_with_retries(
        symbol=sym,
        entry=entry,
        stop_loss=sl,
        take_profit=tp,
        use_trailing=True,
        dry_run=OMEGA_AUTOSCAN_DRYRUN
    )

def start_autoscan_thread():
    if not OMEGA_AUTOSCAN:
        print("🤖 Autoscan disabled (set OMEGA_AUTOSCAN=1 to enable).")
        return
    def _loop():
        print(f"🤖 Autoscan running every {OMEGA_SCAN_INTERVAL_SEC}s "
              f"(dynamic max open positions, dryrun={OMEGA_AUTOSCAN_DRYRUN})")
        while True:
            try:
                autoscan_once()
            except Exception as e:
                print(f"⚠️ autoscan loop error: {e}")
                should_stop, alerted = _handle_thread_exception("Autoscan", e)
                if not alerted:
                    try:
                        send_telegram_alert(f"⚠️ Autoscan loop error: {e}")
                    except Exception:
                        pass
                if should_stop:
                    return
            time.sleep(OMEGA_SCAN_INTERVAL_SEC)
    t = threading.Thread(target=_loop, daemon=True, name="Autoscan")
    t.start()

def log_equity_curve():
    try:
        equity = get_account_equity()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with open(EQUITY_CURVE_LOG_PATH, "a") as f:
            f.write(f"[{now}] EQUITY: ${equity:.2f}\n")

        print(f"📈 Equity logged: ${equity:.2f}")
    except Exception as e:
        print(f"❌ Failed to log equity curve: {e}")

def get_account_equity():
    try:
        account = trading_client.get_account()  # ✅ from alpaca-py
        _update_day_trade_status_from_account(account)
        return float(account.equity)
    except Exception as e:
        print(f"❌ Failed to fetch account equity: {e}")
        return 0
def calculate_position_size(entry_price, stop_loss):
    try:
        equity = get_account_equity()
        risk_percent = MAX_RISK_PER_TRADE_PERCENT / 100  # convert 1.0 → 0.01
        risk_amount = equity * risk_percent
        risk_per_share = abs(entry_price - stop_loss)

        if risk_per_share == 0:
            print("⚠️ Risk per share is zero. Skipping.")
            return 0

        position_size = int(risk_amount // risk_per_share)
        if position_size < MIN_TRADE_QTY:
            print("⚠️ Risk budget too small for minimum position size.")
            return 0
        return position_size
    except Exception as e:
        print(f"❌ Error calculating position size: {e}")
        return 0

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        data = request.get_json(silent=True) or {}
        print(f"📥 Webhook received: {data}", tag="WEBHOOK")

        # 🔐 Validate the secret from HEADER
        header_secret = request.headers.get("X-OMEGA-SECRET")
        env_secret = os.getenv("WEBHOOK_SECRET_TOKEN")  # be explicit
        if header_secret != env_secret:
            print("🚫 Unauthorized webhook attempt blocked.", tag="WEBHOOK")
            try:
                send_telegram_alert("🚫 Unauthorized webhook attempt blocked.")
            except Exception:
                pass
            return jsonify({"status": "failed", "reason": "auth"}), 200

        # 🔎 Basic validation
        required = ("symbol", "entry", "stop_loss", "take_profit")
        missing = [k for k in required if k not in data]
        if missing:
            return jsonify({"status": "failed", "reason": f"missing_fields:{','.join(missing)}"}), 200

        symbol = str(data.get("symbol", "")).upper().strip()

        # 🧮 Safe numeric parsing
        try:
            entry = float(data.get("entry"))
            stop_loss = float(data.get("stop_loss"))
            take_profit = float(data.get("take_profit"))
        except Exception as conv_err:
            return jsonify({"status": "failed", "reason": f"bad_numbers:{conv_err}"}), 200

        # ✅ Proper boolean handling (strings "true"/"false" etc.)
        ut_raw = data.get("use_trailing", False)
        if isinstance(ut_raw, str):
            use_trailing = ut_raw.strip().lower() in ("1", "true", "yes", "y", "on")
        else:
            use_trailing = bool(ut_raw)
        # --- optional dry-run guard (header, query, or default from env) ---
        def _as_bool(x):
            return str(x).strip().lower() in ("1", "true", "yes", "y", "on")

        dry_run = (
            _as_bool(request.headers.get("X-OMEGA-DRYRUN", "0"))
            or _as_bool(request.args.get("dryrun", "0"))
            or _as_bool(os.getenv("OMEGA_WEBHOOK_DRYRUN", "0"))
        )

        if dry_run:
            msg = (
                f"🧪 Webhook DRY-RUN for {symbol} "
                f"(entry {entry}, sl {stop_loss}, tp {take_profit}, trailing {use_trailing})"
            )
            print(msg, tag="WEBHOOK")
            try:
                send_telegram_alert(msg)
            except Exception:
                pass
            return jsonify({
                "status": "ok",
                "trade_placed": False,
                "reason": "dry_run"
            }), 200
        # --- end dry-run guard ---
        print(f"🔄 Calling submit_order_with_retries for {symbol}", tag="WEBHOOK")

        try:
            success = submit_order_with_retries(
                symbol=symbol,
                entry=entry,
                 stop_loss=stop_loss,
                take_profit=take_profit,
                use_trailing=use_trailing,
                dry_run=dry_run
            )
            
        except Exception as trade_error:
            msg = f"💥 submit_order_with_retries error: {trade_error}"
            print(msg, tag="WEBHOOK", level="error")
            try:
                send_telegram_alert(msg)
            except Exception:
                pass
            return jsonify({"status": "failed", "reason": "exception", "detail": str(trade_error)}), 200

        placed = bool(success)
        print(f"✅ Webhook processed (trade_placed={placed})", tag="WEBHOOK")
        resp = {
            "status": "ok",                      # webhook processed fine
            "trade_placed": placed               # whether an order actually went in
        }
        if not placed:
           resp["reason"] = "submit_returned_false"  # e.g., qty=0 / buying power / weekend

        return jsonify(resp), 200

    except Exception as e:
        print(f"❌ Exception in webhook: {e}", tag="WEBHOOK", level="error")
        return jsonify({"status": "failed", "reason": "handler_exception", "detail": str(e)}), 200

@app.route("/ping", methods=["GET"])
def ping():
    return "pong", 200

MAX_EQUITY_FILE = os.path.join(LOG_DIR, "max_equity.txt")
SNAPSHOT_LOG_PATH = os.path.join(LOG_DIR, "last_snapshot.txt")
PORTFOLIO_LOG_PATH = os.path.join(LOG_DIR, "portfolio_log.csv")
TRADE_LOG_PATH = os.path.join(LOG_DIR, "trade_log.csv")
TRADE_LOG_HEADER = [
    "timestamp",
    "symbol",
    "qty",
    "entry",
    "stop_loss",
    "take_profit",
    "status",
    "action",
    "fill_price",
    "realized_pnl",
    "session_type",
    "hold_reason",
    "close_reason",
    "explanation",
    "context_time_of_day",
    "context_recent_outcome",
    "context_equity_change",
]
EQUITY_CURVE_LOG_PATH = os.path.join(LOG_DIR, "equity_curve.log")

def calculate_trade_qty(entry_price, stop_loss_price):
    try:
        account = trading_client.get_account()  # ✅ Updated
        _update_day_trade_status_from_account(account)
        equity = float(account.equity)
        max_risk_amount = equity * (MAX_RISK_PER_TRADE_PERCENT / 100)
        risk_per_share = abs(entry_price - stop_loss_price)

        print(f"🧮 DEBUG:")
        print(f"  • Account equity: {equity}")
        print(f"  • Max risk per trade: {MAX_RISK_PER_TRADE_PERCENT}% → {max_risk_amount}")
        print(f"  • Risk per share: {risk_per_share}")

        if risk_per_share == 0:
            print("⚠️ Risk per share is 0 — invalid stop loss?")
            return 0

        raw_qty = int(max_risk_amount // risk_per_share)
        if raw_qty < MIN_TRADE_QTY:
            print("⚠️ Risk budget too small for even the minimum quantity — skipping.")
            return 0

        qty = raw_qty
        print(f"  • Final calculated qty: {qty}")
        return qty

    except Exception as e:
        print("⚠️ Error calculating trade quantity:", e)
        send_telegram_alert(f"⚠️ Risk-based quantity error: {e}")
        return 0

def get_current_vix():
    try:
        req = StockBarsRequest(
            symbol_or_symbols="^VIX",
            timeframe=TimeFrame.Day,
            start=datetime.now(timezone.utc) - timedelta(days=5),
            end=datetime.now(timezone.utc),
            feed=_DATA_FEED,
        )
        bars = _fetch_bars_df("^VIX", req)
    except Exception as e:
        if "subscription does not permit" in str(e).lower() and _DATA_FEED != DataFeed.IEX:
            try:
                req = StockBarsRequest(
                    symbol_or_symbols="^VIX",
                    timeframe=TimeFrame.Day,
                    start=datetime.now(timezone.utc) - timedelta(days=5),
                    end=datetime.now(timezone.utc),
                    feed=DataFeed.IEX,
                )
                bars = _fetch_bars_df("^VIX", req)
            except Exception as e2:
                print(f"❌ Failed to get VIX (IEX fallback): {e2}")
                return 0
        else:
            print(f"❌ Failed to get VIX: {e}")
            return 0

    try:
        if 'symbol' in bars.columns:
            sel = bars[bars['symbol'] == "^VIX"]
        else:
            sel = bars.loc["^VIX"]
        if sel is None or len(sel) == 0:
            print("⚠️ No VIX data found.")
            return 0
        vix_value = float(sel['close'].iloc[-1])
        print(f"📊 VIX fetched from Alpaca: {vix_value}")
        return vix_value
    except Exception:
        print("⚠️ No VIX data found.")
        return 0

def _write_max_equity(value: float):
    try:
        with open(MAX_EQUITY_FILE, "w") as f:
            f.write(f"{value:.2f}")
    except Exception as e:
        print(f"⚠️ Failed to write max equity: {e}")


def get_max_equity():
    if os.path.exists(MAX_EQUITY_FILE):
        try:
            with open(MAX_EQUITY_FILE, "r") as f:
                return float(f.read().strip())
        except Exception as e:
            print(f"⚠️ Could not read max equity file: {e}")
    return 0.0


def update_max_equity(current_equity):
    current_equity = max(current_equity, EQUITY_GUARD_MAX_EQUITY_FLOOR)
    max_equity = get_max_equity()
    if current_equity > max_equity:
        _write_max_equity(current_equity)

def should_block_trading_due_to_equity():
    try:
        account = trading_client.get_account()  # ✅ Updated
        _update_day_trade_status_from_account(account)
        equity = max(float(account.equity), 0.0)
        if equity <= 0:
            print("⚠️ Equity fetch returned 0 — skipping guard update.")
            return False
        update_max_equity(equity)
        max_equity = max(get_max_equity(), EQUITY_GUARD_MAX_EQUITY_FLOOR)
        if max_equity <= 0:
            return False

        if (
            EQUITY_GUARD_STALE_RATIO > 0
            and equity > 0
            and max_equity > equity
            and (max_equity / max(equity, 1e-6)) >= EQUITY_GUARD_STALE_RATIO
        ):
            msg = (
                f"🔄 Equity guard baseline reset (max ${max_equity:.2f} ≫ equity ${equity:.2f}); "
                "assuming manual deposit/withdrawal."
            )
            print(msg)
            try:
                send_telegram_alert(msg)
            except Exception:
                pass
            _write_max_equity(equity)
            return False

        drop_percent = 0.0
        if max_equity > 0:
            drop_percent = (max_equity - equity) / max_equity

        if drop_percent >= EQUITY_GUARD_MIN_DRAWDOWN:
            msg = (
                f"🛑 Trading blocked — Portfolio dropped {drop_percent*100:.2f}% from high "
                f"(${max_equity:.2f} → ${equity:.2f})."
            )
            print(msg)
            send_telegram_alert(msg)
            send_email("🚫 Trading Disabled", msg)
            return True

        if EQUITY_DRAWDOWN_MAX_PCT > 0:
            drawdown_pct = (max_equity - equity) / max_equity if max_equity > 0 else 0
            if drawdown_pct >= EQUITY_DRAWDOWN_MAX_PCT:
                msg = (
                    "🛑 Trading blocked — Max drawdown "
                    f"{drawdown_pct*100:.2f}% exceeded (limit {EQUITY_DRAWDOWN_MAX_PCT*100:.2f}%)."
                )
                print(msg)
                try:
                    send_telegram_alert(msg)
                    send_email("🚫 Trading Disabled", msg)
                except Exception:
                    pass
                return True
        return False
    except Exception as e:
        print("⚠️ Error checking equity drop:", e)
        send_telegram_alert(f"⚠️ Equity check error: {e}")
        return False

def send_email(subject, body):
    EMAIL_ADDRESS = os.getenv("EMAIL_USER")
    EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")

    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = EMAIL_ADDRESS
    msg['To'] = EMAIL_ADDRESS
    msg.set_content(body)

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            smtp.send_message(msg)
        print(f"📧 Email sent: {subject}")
    except Exception as e:
        print(f"❌ Email send failed: {e}")  
        send_telegram_alert(f"❌ Email failure: {e}")


def get_equity_slope():
    try:
        path = EQUITY_CURVE_LOG_PATH
        if not os.path.exists(path):
            return 0
        values = []
        with open(path, "r") as f:
            for line in f:
                parts = line.strip().split("EQUITY:")
                if len(parts) == 2:
                    try:
                        values.append(float(parts[1].replace("$", "").strip()))
                    except Exception:
                        pass
        if len(values) < 5:
            return 0
        slope = (values[-1] - values[-5]) / 5
        return slope
    except Exception as e:
        print(f"❌ Failed to analyze equity slope: {e}")
        return 0

def get_rsi_value(symbol, interval='15m', period=14):
    """
    RSI from Alpaca bars. We FORCE IEX. If minute/hour bars are not
    available, fall back to DAILY bars.
    """
    tf = TimeFrame.Minute if interval == '15m' else TimeFrame.Hour
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=5)

    bars, used_daily = _fetch_bars_with_daily_fallback(
        symbol,
        primary_tf=tf,
        primary_start=start,
        primary_end=end,
        feed=DataFeed.IEX,
        daily_lookback_days=100,
        normalize_reset_index=True,
    )

    if bars.empty or 'close' not in bars.columns:
        context = " after daily fallback" if used_daily else ""
        print(f"⚠️ Not enough data to calculate RSI for {symbol}{context}.")
        return None

    bars['close'] = pd.to_numeric(bars['close'], errors='coerce')
    bars = bars.dropna(subset=['close'])
    if len(bars) < period + 1:
        print(f"⚠️ Not enough data to calculate RSI for {symbol}.")
        return None

    delta = bars['close'].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean().replace(0, np.nan)

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    latest_rsi = float(rsi.iloc[-1])
    if np.isnan(latest_rsi):
        return None
    latest_rsi = round(latest_rsi, 2)
    if used_daily:
        print(f"📈 RSI (daily fallback) for {symbol}: {latest_rsi}")
    else:
        print(f"📈 RSI ({interval}) for {symbol}: {latest_rsi}")
    return latest_rsi

def submit_order_with_retries(
    symbol, entry, stop_loss, take_profit, use_trailing,
    max_retries=3, dry_run=False
):
    """
    Flow:
      1) Risk-based qty calc + BP guard (live ask + 2% buffer)
      2) Pre-cancel SELLs to avoid wash-trade
      3) Plain market BUY (no bracket)
      4) Attach split reduce_only TP/SL (with retries if qty_available=0)
    """
    print("📌 About to calculate quantity...")

    if PDT_GUARD_ENABLED and _pdt_global_lockout_active() and not dry_run:
        remaining = _pdt_global_lockout_remaining()
        msg = f"⏳ PDT lockout active ({remaining}s) — skipping BUY {symbol}."
        _maybe_alert_pdt(msg)
        return False

    # 🚦 Extra PDT guard: block BUY if we likely can't close safely
    if PDT_GUARD_ENABLED and not dry_run:
        rem, pattern_flag = _get_day_trade_status()
        if rem is not None and rem <= PDT_MIN_DAY_TRADES_BUFFER:
            msg = f"🚫 Skipping BUY {symbol} — insufficient day trades left to safely exit later (rem={rem})."
            print(msg)
            _maybe_alert_pdt(msg, day_trades_left=rem, pattern_flag=pattern_flag)
            try:
                send_telegram_alert(msg)
                send_telegram_alert(f"📊 PDT Status: {rem} day trades left | PDT flag={pattern_flag}")
            except Exception:
                pass
            return False

    # --- Per-symbol daily trade guard ---
    if not _can_trade_symbol_today(symbol, entry):
        print(f"🚫 Skipping {symbol} — already traded today and dip rule not met.")
        try:
            send_telegram_alert(f"🚫 Skipping {symbol} — already traded today and dip rule not met.")
        except Exception:
            pass
        return False

    if not _check_daily_trade_cap():
        return False

    if not _daily_pnl_guard_allows_trading():
        return False

    qty = calculate_trade_qty(entry, stop_loss)
    if qty == 0:
        print("❌ Qty is 0 — skipping order.")
        try: send_telegram_alert("❌ Trade aborted — calculated qty was 0.")
        except Exception: pass
        return False

    # DRY-RUN
    if dry_run:
        msg = (f"🧪 [DRY-RUN] Would BUY {symbol} qty={qty} @ {entry} "
               f"(SL {stop_loss} / TP {take_profit}, trailing={use_trailing})")
        print(msg)
        try: send_telegram_alert(msg)
        except Exception: pass
        return False

    # --- Buying power guard with live ask price ---
    from alpaca.data.requests import StockLatestQuoteRequest
    live_price = entry
    try:
        req = StockLatestQuoteRequest(symbol_or_symbols=symbol, feed=_DATA_FEED)
        q = data_client.get_stock_latest_quote(req)
        live_price = float(q[symbol].ask_price or q[symbol].bid_price or entry)
    except Exception as e:
        if "subscription does not permit" in str(e).lower() and _DATA_FEED != DataFeed.IEX:
            try:
                req = StockLatestQuoteRequest(symbol_or_symbols=symbol, feed=DataFeed.IEX)
                q = data_client.get_stock_latest_quote(req)
                live_price = float(q[symbol].ask_price or q[symbol].bid_price or entry)
            except Exception as e2:
                print(f"⚠️ Could not fetch live quote for {symbol} (IEX fallback): {e2} — using entry={entry}")
        else:
            print(f"⚠️ Could not fetch live quote for {symbol}: {e} — using entry={entry}")

    # ⚖️ Use an 'effective' buying power for cash accounts where bp may show 0
    try:
        acct = trading_client.get_account()
        rem, pattern_flag = _update_day_trade_status_from_account(acct)
        bp = float(getattr(acct, "buying_power", 0) or 0)
        nmbp = float(getattr(acct, "non_marginable_buying_power", 0) or 0)
        regt = float(getattr(acct, "regt_buying_power", 0) or 0)
        cash_avail = float(getattr(acct, "cash", 0) or 0)
        multiplier = float(getattr(acct, "multiplier", 1) or 1)

        if PDT_GUARD_ENABLED and not dry_run:
            if rem is not None and rem <= PDT_MIN_DAY_TRADES_BUFFER:
                msg = f"🛑 Abort {symbol} — remaining day trades {rem} at/under buffer."
                print(msg)
                _maybe_alert_pdt(msg, day_trades_left=rem, pattern_flag=pattern_flag)
                return False

        # If margin account, prefer broker-reported BP; otherwise take the max available cash-like field.
        if multiplier > 1:
            effective_bp = bp
        else:
            effective_bp = max(cash_avail, nmbp, regt, bp)

        print(
            f"💵 BP snapshot → bp={bp:.2f} nmbp={nmbp:.2f} regt={regt:.2f} "
            f"cash={cash_avail:.2f} mult={multiplier:.0f} → effective_bp={effective_bp:.2f}"
        )
    except Exception as e:
        print(f"⚠️ Could not fetch account buying power fields: {e}")
        effective_bp = 0.0

    SAFETY = 0.98  # 2% buffer
    if live_price <= 0:
        live_price = max(entry, 0.01)

    max_qty_by_bp = int((effective_bp * SAFETY) // live_price)
    if max_qty_by_bp < MIN_TRADE_QTY:
        print(f"❌ Not enough usable buying power for even 1 share at ~${live_price:.2f} (effective_bp=${effective_bp:.2f}).")
        try:
            send_telegram_alert(f"❌ Trade aborted — usable buying power ${effective_bp:.2f} too low for {symbol}.")
        except Exception:
            pass
        return False

    if qty > max_qty_by_bp:
        print(
            f"⚠️ Reducing qty by BP cap: {qty} → {max_qty_by_bp} "
            f"(effective_bp ${effective_bp:.2f}, live ~${live_price:.2f})"
        )
        qty = max_qty_by_bp
        if qty < MIN_TRADE_QTY:
            print("⚠️ Buying power reduction pushed qty below minimum — skipping trade.")
            try:
                send_telegram_alert(f"⚠️ Trade aborted — insufficient buying power for {symbol} after reduction.")
            except Exception:
                pass
            return False

    if qty * live_price > effective_bp * SAFETY:
        bp_qty = int((effective_bp * SAFETY) // live_price)
        if bp_qty < MIN_TRADE_QTY:
            print("⚠️ Safety shave left no affordable quantity — skipping trade.")
            try:
                send_telegram_alert(f"⚠️ Trade aborted — post-safety buying power insufficient for {symbol}.")
            except Exception:
                pass
            return False
        qty = bp_qty
        print(f"🛡️ Safety shave → qty={qty}")

    print(f"📌 Quantity calculated: {qty}")
    print("🔍 Checking equity guard...")
    if should_block_trading_due_to_equity():
        print("🛑 BLOCKED: Equity drop filter triggered.")
        try: send_telegram_alert("🛑 Webhook blocked: Equity protection triggered.")
        except Exception: pass
        return False
    print("✅ Equity check passed.")

    if not is_within_trading_hours():
        print("🕑 Outside trading hours.")
        try: send_telegram_alert("🕑 Trade skipped — outside allowed trading hours.")
        except Exception: pass
        return False
    print("✅ Within trading hours.")

    # --- NEW: Pre-cancel existing SELLs to avoid wash-trade rejects ---
    try:
        n = cancel_open_sells(symbol)
        if n:
            print(f"🧹 Pre-cancelled {n} open SELLs on {symbol} before BUY")
        time.sleep(0.8)  # let exchange register cancels
    except Exception as e:
        print(f"⚠️ Pre-cancel error for {symbol}: {e}")

    # ---- BUY leg (no bracket) ----
    from alpaca.trading.enums import OrderSide, TimeInForce
    try:
        print(f"🚀 Submitting BUY {symbol} x{qty} (market, GTC)")
        buy_order = trading_client.submit_order(MarketOrderRequest(
            symbol=symbol, qty=qty, side=OrderSide.BUY, time_in_force=TimeInForce.GTC
        ))
        buy_fill_price = None
        try:
            info = poll_order_fill(buy_order.id, timeout=90, poll_secs=2)
            buy_fill_price = info.get("filled_avg_price")
        except Exception as _:
            buy_fill_price = None
    except Exception as e:
        if _is_pattern_day_trading_error(e):
            _set_pdt_global_lockout(f"BUY denied for {symbol}")
        print("🧨 BUY submit failed:", e)
        try: send_telegram_alert(f"❌ BUY failed for {symbol}: {e}")
        except Exception: pass
        return False

    # --- Sanity clamp TP/SL once (avoid nonsensical webhook values) ---
    try:
        req = StockLatestQuoteRequest(symbol_or_symbols=symbol, feed=_DATA_FEED)
        q = data_client.get_stock_latest_quote(req)
        last_px = float(q[symbol].ask_price or q[symbol].bid_price or entry)
    except Exception as e:
        if "subscription does not permit" in str(e).lower() and _DATA_FEED != DataFeed.IEX:
            try:
                req = StockLatestQuoteRequest(symbol_or_symbols=symbol, feed=DataFeed.IEX)
                q = data_client.get_stock_latest_quote(req)
                last_px = float(q[symbol].ask_price or q[symbol].bid_price or entry)
            except Exception:
                last_px = entry
        else:
            last_px = entry

    abs_tp = float(take_profit)
    abs_sl = float(stop_loss)
    min_tp_price = last_px * (1 + FALLBACK_TAKE_PROFIT_PCT / 100.0)
    min_sl_price = last_px * (1 - FALLBACK_STOP_LOSS_PCT / 100.0)
    if abs_tp <= min_tp_price:
        abs_tp = min_tp_price
    if abs_sl >= min_sl_price:
        abs_sl = min_sl_price
    abs_tp = _quantize_to_tick(abs_tp)
    abs_sl = _quantize_to_tick(abs_sl)
    print(f"🧭 TP/SL sanity → last={last_px:.2f} | TP={abs_tp} | SL={abs_sl}")

    # 🚨 Attach protection immediately (always place TP/SL after buy)
    try:
        place_split_protection(symbol, tp_price=abs_tp, sl_price=abs_sl)
        print(f"🛡️ Protection attached for {symbol}: TP={abs_tp}, SL={abs_sl}")
    except Exception as e:
        print(f"⚠️ Immediate protection attach failed for {symbol}: {e} — watchdog will retry.")

    print(f"✅ BUY order placed for {symbol} qty={qty}")
    try:
        post_trade_protection_audit(symbol, entry, take_profit, stop_loss)
    except Exception as e:
        print(f"⚠️ Protection audit skipped for {symbol}: {e}")

    # ---- Notify + log ----
    explanation = generate_trade_explanation(
        symbol=symbol, entry=entry, stop_loss=abs_sl,
        take_profit=abs_tp, rsi=None,
        trend="uptrend" if use_trailing else "neutral"
    )
    try: send_telegram_alert(f"🚀 Trade executed:\n{explanation}")
    except Exception: pass
    try: log_equity_curve()
    except Exception: pass

    print(f"✅ Order + protection finished for {symbol}.")

    # Track per-symbol trade count for re-entry guard
    today = datetime.now().date()
    key = symbol.upper()
    with _symbol_last_trade_lock:
        prev = _symbol_last_trade.get(key)
        if prev and prev.get("date") == today:
            count = int(prev.get("count", 0) or 0) + 1
            exit_price = prev.get("exit_price")
        else:
            count = 1
            exit_price = None
        _symbol_last_trade[key] = {
            "date": today,
            "exit_price": exit_price,
            "count": count,
            "last_trade_ts": datetime.now(timezone.utc).isoformat(),
        }

    _increment_daily_trade_count()
    try:
        log_trade(symbol, qty, entry, abs_sl, abs_tp, status="executed",
                  action="BUY", fill_price=buy_fill_price, realized_pnl=None)
        print("📝 Trade logged to CSV + Google Sheet.")
    except Exception as e:
        print(f"⚠️ Trade log failed: {e}")
    return True

def log_portfolio_snapshot():
    try:
        account = trading_client.get_account()
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        equity = float(account.equity)
        cash = float(account.cash)
        portfolio_value = float(account.portfolio_value)

        row = [timestamp, equity, cash, portfolio_value, "intraday", "", ""]

        # ✅ Log to local CSV file
        file_exists = os.path.exists(PORTFOLIO_LOG_PATH)
        with open(PORTFOLIO_LOG_PATH, mode='a', newline='') as file:
            writer = csv.writer(file)
            if not file_exists:
                writer.writerow(["timestamp", "equity", "cash", "portfolio_value", "session_type", "hold_reason", "close_reason"])
            writer.writerow(row)

        print("✅ Daily snapshot logged (CSV):", row)
        send_telegram_alert(f"📸 Snapshot logged: Equity ${equity:.2f}, Cash ${cash:.2f}")

        # ✅ Also log to Google Sheet
        try:
            client = _get_gspread_client()

            sheet_id = _clean_env(os.getenv("GOOGLE_SHEET_ID"))
            sheet_name = _clean_env(os.getenv("PORTFOLIO_SHEET_NAME") or "Portfolio Log")  # Default fallback

            if not sheet_id or not sheet_name:
                raise ValueError("Missing GOOGLE_SHEET_ID or PORTFOLIO_SHEET_NAME environment variable.")

            sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
            sheet.append_row(row)
            print("✅ Snapshot logged to Google Sheet.")

        except Exception as gs_error:
            print("⚠️ Failed to log snapshot to Google Sheet:", gs_error)

    except Exception as e:
        print("⚠️ Failed to log portfolio snapshot:", e)
        send_telegram_alert(f"⚠️ Snapshot failed: {e}")

def send_weekly_summary():
    try:
        if not os.path.isfile(TRADE_LOG_PATH):
            print("No trade log found.")
            return

        df = pd.read_csv(TRADE_LOG_PATH, parse_dates=["timestamp"])

        # Filter for last 7 days
        one_week_ago = datetime.now() - pd.Timedelta(days=7)
        df = df[df["timestamp"] >= one_week_ago.strftime("%Y-%m-%d")]

        if df.empty:
            send_email("📊 Weekly P&L Summary", "No trades in the last 7 days.")
            return

        df["pnl"] = df["take_profit"] - df["entry"]  # Approx P&L (simplified logic)
        total_trades = len(df)
        wins = df[df["pnl"] > 0]
        losses = df[df["pnl"] <= 0]
        win_rate = (len(wins) / total_trades) * 100
        total_pnl = df["pnl"].sum()
        avg_gain = wins["pnl"].mean() if not wins.empty else 0
        avg_loss = losses["pnl"].mean() if not losses.empty else 0

        body = (
            f"📈 Weekly Trade Summary ({total_trades} trades)\n\n"
            f"• Total P&L: ${total_pnl:.2f}\n"
            f"• Win rate: {win_rate:.2f}%\n"
            f"• Avg gain: ${avg_gain:.2f}\n"
            f"• Avg loss: ${avg_loss:.2f}\n"
        )

        send_email("📊 Weekly P&L Summary", body)
        print("✅ Weekly summary sent.")
        send_telegram_alert("📬 Weekly P&L email sent.")
        
    except Exception as e:
        print(f"⚠️ Weekly summary failed: {e}")
        send_telegram_alert(f"⚠️ Weekly summary failed: {e}")

def summarize_pnl_from_csv(path: str = TRADE_LOG_PATH) -> dict:
    """Compute realized P&L stats from local trade_log.csv using SELL rows with numeric realized_pnl."""
    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"⚠️ Unable to read trade log for summary: {e}")
        return {
            "total_realized_pnl": 0.0,
            "trades_closed": 0,
            "wins": 0,
            "losses": 0,
            "win_rate_pct": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "best": 0.0,
            "worst": 0.0,
        }

    if 'realized_pnl' not in df.columns:
        df['realized_pnl'] = np.nan
    if 'action' not in df.columns:
        df['action'] = None

    sells = df[df['action'].astype(str).str.upper() == 'SELL'].copy()
    sells['realized_pnl'] = pd.to_numeric(sells['realized_pnl'], errors='coerce')
    sells = sells.dropna(subset=['realized_pnl'])

    total = float(sells['realized_pnl'].sum()) if not sells.empty else 0.0
    wins_mask = sells['realized_pnl'] > 0
    losses_mask = sells['realized_pnl'] <= 0
    wins = int(wins_mask.sum())
    losses = int(losses_mask.sum())
    count = int(len(sells))
    win_rate = (wins / count * 100.0) if count else 0.0
    avg_win = float(sells.loc[wins_mask, 'realized_pnl'].mean() or 0.0)
    avg_loss = float(sells.loc[losses_mask, 'realized_pnl'].mean() or 0.0)
    best = float(sells['realized_pnl'].max()) if count else 0.0
    worst = float(sells['realized_pnl'].min()) if count else 0.0

    return {
        "total_realized_pnl": round(total, 2),
        "trades_closed": count,
        "wins": wins,
        "losses": losses,
        "win_rate_pct": round(win_rate, 2),
        "avg_win": round(avg_win, 2),
        "avg_loss": round(avg_loss, 2),
        "best": round(best, 2),
        "worst": round(worst, 2),
    }


# === Weekend Improvements: Daily PnL, Google Sheet Appends ===
from datetime import date as _date

def _read_trades_df(path: str = TRADE_LOG_PATH) -> pd.DataFrame:
    try:
        df = pd.read_csv(path)
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        return df
    except Exception as e:
        print(f"⚠️ Unable to read trade log: {e}")
        return pd.DataFrame()


def summarize_realized_pnl_for_day(day: _date = None, path: str = TRADE_LOG_PATH) -> dict:
    """Realized PnL for a given calendar day (UTC), using SELL rows with numeric realized_pnl."""
    day = day or _date.today()
    df = _read_trades_df(path)
    if df.empty:
        return {
            "date": str(day),
            "total_realized_pnl": 0.0,
            "trades_closed": 0,
            "wins": 0,
            "losses": 0,
            "win_rate_pct": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "best": 0.0,
            "worst": 0.0,
        }
    if 'action' not in df.columns:
        df['action'] = None
    df['realized_pnl'] = pd.to_numeric(df.get('realized_pnl'), errors='coerce')
    df_day = df[(df['action'].astype(str).str.upper() == 'SELL') & (df['timestamp'].dt.date == day)]
    df_day = df_day.dropna(subset=['realized_pnl'])
    total = float(df_day['realized_pnl'].sum()) if not df_day.empty else 0.0
    wins_mask = df_day['realized_pnl'] > 0
    losses_mask = df_day['realized_pnl'] <= 0
    wins = int(wins_mask.sum())
    losses = int(losses_mask.sum())
    count = int(len(df_day))
    win_rate = (wins / count * 100.0) if count else 0.0
    avg_win = float(df_day.loc[wins_mask, 'realized_pnl'].mean() or 0.0)
    avg_loss = float(df_day.loc[losses_mask, 'realized_pnl'].mean() or 0.0)
    best = float(df_day['realized_pnl'].max()) if count else 0.0
    worst = float(df_day['realized_pnl'].min()) if count else 0.0
    return {
        "date": str(day),
        "total_realized_pnl": round(total, 2),
        "trades_closed": count,
        "wins": wins,
        "losses": losses,
        "win_rate_pct": round(win_rate, 2),
        "avg_win": round(avg_win, 2),
        "avg_loss": round(avg_loss, 2),
        "best": round(best, 2),
        "worst": round(worst, 2),
    }


def append_daily_performance_row(metrics: dict, sheet_id: str = None, tab_name: str = None) -> None:
    """Append a dated row to a Google Sheet tab (default: 'Performance Daily')."""
    try:
        client = _get_gspread_client()
        sid = _clean_env(sheet_id or os.getenv("GOOGLE_SHEET_ID"))
        tab = _clean_env(tab_name or PERF_DAILY_TAB or "Performance Daily")
        if not sid:
            print("⚠️ No GOOGLE_SHEET_ID configured; skipping Performance append.")
            return
        ss = client.open_by_key(sid)
        try:
            ws = ss.worksheet(tab)
        except Exception:
            ws = ss.add_worksheet(title=tab, rows=100, cols=10)
        header = [
            "Date","Total Realized PnL","Trades Closed","Wins","Losses",
            "Win Rate %","Avg Win","Avg Loss","Best","Worst"
        ]
        try:
            existing = ws.get_all_values()[:1]
        except Exception:
            existing = []
        if not existing:
            ws.update(values=[header], range_name='A1')
        row = [
            metrics.get('date'),
            f"{metrics.get('total_realized_pnl', 0.0):.2f}",
            str(metrics.get('trades_closed', 0)),
            str(metrics.get('wins', 0)),
            str(metrics.get('losses', 0)),
            f"{metrics.get('win_rate_pct', 0.0):.2f}",
            f"{metrics.get('avg_win', 0.0):.2f}",
            f"{metrics.get('avg_loss', 0.0):.2f}",
            f"{metrics.get('best', 0.0):.2f}",
            f"{metrics.get('worst', 0.0):.2f}",
        ]
        ws.append_row(row)
        print("✅ Appended daily performance row.")
    except Exception as e:
        print(f"⚠️ Failed to append performance row: {e}")


def update_google_performance_sheet(metrics: dict, sheet_id: str = None, tab_name: str = None) -> None:
    """Write key P&L metrics to a dedicated Google Sheet tab (default: 'Performance')."""
    try:
        client = _get_gspread_client()
        sid = _clean_env(sheet_id or os.getenv("GOOGLE_SHEET_ID"))
        tab = _clean_env(tab_name or os.getenv("PERF_SHEET_NAME") or "Performance")
        if not sid:
            print("⚠️ No GOOGLE_SHEET_ID configured; skipping Performance sheet update.")
            return
        ss = client.open_by_key(sid)
        try:
            ws = ss.worksheet(tab)
        except Exception:
            ws = ss.add_worksheet(title=tab, rows=50, cols=4)

        rows = [
            ["Metric", "Value"],
            ["Total Realized PnL", f"{metrics.get('total_realized_pnl', 0.0):.2f}"],
            ["Trades Closed", str(metrics.get('trades_closed', 0))],
            ["Wins", str(metrics.get('wins', 0))],
            ["Losses", str(metrics.get('losses', 0))],
            ["Win Rate %", f"{metrics.get('win_rate_pct', 0.0):.2f}"],
            ["Avg Win", f"{metrics.get('avg_win', 0.0):.2f}"],
            ["Avg Loss", f"{metrics.get('avg_loss', 0.0):.2f}"],
            ["Best Trade", f"{metrics.get('best', 0.0):.2f}"],
            ["Worst Trade", f"{metrics.get('worst', 0.0):.2f}"],
        ]
        try:
            ws.clear()
        except Exception:
            pass
        ws.update(values=rows, range_name='A1')
        print("✅ Performance sheet updated.")
    except Exception as e:
        print(f"⚠️ Failed to update performance sheet: {e}")

# --- Weekend Improvements: Open Positions helpers & EOD/Janitor schedulers ---

def get_symbol_tp_sl_open_orders(symbol: str):
    """Return (tp_limit, sl_stop) from current OPEN SELL orders, if present."""
    tp, sl = None, None
    try:
        req = GetOrdersRequest(status=QueryOrderStatus.OPEN, symbols=[symbol])
        for o in trading_client.get_orders(filter=req):
            side = str(getattr(o, 'side', '')).lower()
            if not side.endswith('sell'):
                continue
            otype = str(getattr(o, 'type', '')).lower()
            if otype == 'limit':
                try: tp = float(getattr(o, 'limit_price', tp))
                except Exception: pass
            elif otype == 'stop':
                try: sl = float(getattr(o, 'stop_price', sl))
                except Exception: pass
    except Exception as e:
        print(f"⚠️ get_symbol_tp_sl_open_orders failed for {symbol}: {e}")
    return tp, sl


def push_open_positions_to_sheet():
    """Write current open positions to Google Sheet tab (clears & replaces)."""
    try:
        client = _get_gspread_client()
        sid = _clean_env(os.getenv('GOOGLE_SHEET_ID'))
        tab = OPEN_POSITIONS_TAB
        if not sid:
            print("⚠️ No GOOGLE_SHEET_ID configured; skip Open Positions push.")
            return
        ss = client.open_by_key(sid)
        try:
            ws = ss.worksheet(tab)
        except Exception:
            ws = ss.add_worksheet(title=tab, rows=200, cols=10)
        header = ["symbol","qty","avg_entry","current","pnl_pct","tp","sl","updated_at"]
        rows = [header]
        positions = trading_client.get_all_positions()
        now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        for p in positions:
            symbol = p.symbol
            qty = int(float(p.qty))
            avg_entry = float(p.avg_entry_price)
            current = float(p.current_price)
            pnl_pct = float(p.unrealized_plpc) * 100.0
            tp, sl = get_symbol_tp_sl_open_orders(symbol)
            rows.append([symbol, qty, round(avg_entry,2), round(current,2), round(pnl_pct,2), tp, sl, now])
        try:
            ws.clear()
        except Exception:
            pass
        ws.update(values=rows, range_name='A1')
        print(f"✅ Open positions pushed ({len(rows)-1} rows).")
    except Exception as e:
        print(f"⚠️ Open positions push failed: {e}")


def start_open_positions_pusher():
    if not OPEN_POSITIONS_PUSH:
        print("📄 Open Positions pusher disabled.")
        return
    def _loop():
        print(f"📄 Open Positions → Google Sheet every {OPEN_POSITIONS_INTERVAL}s → tab '{OPEN_POSITIONS_TAB}'")
        while True:
            try:
                push_open_positions_to_sheet()
            except Exception as e:
                print(f"⚠️ Pusher error: {e}")
                should_stop, alerted = _handle_thread_exception("Open Positions Pusher", e)
                if not alerted:
                    try:
                        send_telegram_alert(f"⚠️ Open Positions pusher error: {e}")
                    except Exception:
                        pass
                if should_stop:
                    return
            time.sleep(OPEN_POSITIONS_INTERVAL)
    threading.Thread(target=_loop, daemon=True, name="OpenPositions").start()


def start_eod_close_thread():
    """Automatically flat positions ~5 minutes before close while equity < $25k."""
    def _loop():
        while True:
            now_utc = datetime.now(timezone.utc)
            if now_utc.time().hour == 19 and 55 <= now_utc.time().minute <= 59:
                try:
                    account = trading_client.get_account()
                    equity = float(account.equity)
                    if equity >= 25000:
                        time.sleep(60)
                        continue
                    positions = trading_client.get_all_positions()
                    if not positions:
                        print("🌙 EOD flush: no open positions to manage.")
                        time.sleep(60)
                        continue
                    for pos in positions:
                        sym = pos.symbol.upper()
                        print(f"🌙 EOD review {sym} (equity ${equity:.2f} < $25k).")
                        send_telegram_alert(f"🌙 EOD flush for {sym} (equity ${equity:.2f} < $25k).")
                        close_position_safely(sym, close_reason="EOD equity<25k")
                        time.sleep(2)
                except Exception as e:
                    print(f"⚠️ EOD close thread error: {e}")
                    should_stop, alerted = _handle_thread_exception("EOD Close", e)
                    if not alerted:
                        try:
                            send_telegram_alert(f"⚠️ EOD close thread error: {e}")
                        except Exception:
                            pass
                    if should_stop:
                        return
            time.sleep(60)
    threading.Thread(target=_loop, daemon=True, name="EODFlush").start()


def start_eod_summary_scheduler():
    if not EOD_SUMMARY:
        print("🕗 EOD summary disabled.")
        return
    last_file = os.path.join(LOG_DIR, 'eod_last_run.txt')
    def _loop():
        print(f"🕗 EOD summary scheduler active (UTC hour={EOD_SUMMARY_HOUR_UTC}, tab='{PERF_DAILY_TAB}')")
        while True:
            try:
                now = datetime.now(timezone.utc)
                should_run = now.hour == EOD_SUMMARY_HOUR_UTC
                last_day = None
                if os.path.exists(last_file):
                    try:
                        with open(last_file, 'r') as f:
                            last_day = f.read().strip()
                    except Exception:
                        last_day = None
                already_ran_today = (last_day == now.strftime('%Y-%m-%d'))
                if should_run and not already_ran_today:
                    metrics = summarize_realized_pnl_for_day(now.date())
                    append_daily_performance_row(metrics)
                    try:
                        send_telegram_alert(
                            f"📬 EOD summary appended for {metrics['date']}: PnL ${metrics['total_realized_pnl']:.2f} (win rate {metrics['win_rate_pct']:.1f}%)"
                        )
                    except Exception:
                        pass
                    try:
                        with open(last_file, 'w') as f:
                            f.write(now.strftime('%Y-%m-%d'))
                    except Exception:
                        pass
                time.sleep(60)
            except Exception as e:
                print(f"⚠️ EOD scheduler error: {e}")
                should_stop, alerted = _handle_thread_exception("EOD Summary", e)
                if not alerted:
                    try:
                        send_telegram_alert(f"⚠️ EOD summary scheduler error: {e}")
                    except Exception:
                        pass
                if should_stop:
                    return
                time.sleep(60)
    threading.Thread(target=_loop, daemon=True, name="EODSummary").start()


def start_order_janitor():
    if not ORDER_JANITOR:
        print("🧹 Order janitor disabled.")
        return
    def _loop():
        print(f"🧹 Order janitor running every {ORDER_JANITOR_INTERVAL}s …")
        while True:
            try:
                pos_symbols = {p.symbol for p in trading_client.get_all_positions()}
                req = GetOrdersRequest(status=QueryOrderStatus.OPEN)
                open_orders = trading_client.get_orders(filter=req)
                cancelled = 0
                for o in open_orders:
                    side = str(getattr(o, 'side','')).lower()
                    sym = getattr(o, 'symbol', None)
                    if sym and side.endswith('sell') and sym not in pos_symbols:
                        try:
                            trading_client.cancel_order_by_id(o.id)
                            cancelled += 1
                        except Exception as ce:
                            print(f"⚠️ Cancel failed {sym}/{o.id}: {ce}")
                if cancelled:
                    msg = f"🧹 Cancelled {cancelled} stray SELL order(s) on symbols without positions."
                    print(msg)
                    try: send_telegram_alert(msg)
                    except Exception: pass
            except Exception as e:
                print(f"⚠️ Janitor error: {e}")
                should_stop, alerted = _handle_thread_exception("Order Janitor", e)
                if not alerted:
                    try:
                        send_telegram_alert(f"⚠️ Order janitor error: {e}")
                    except Exception:
                        pass
                if should_stop:
                    return
            time.sleep(ORDER_JANITOR_INTERVAL)
    threading.Thread(target=_loop, daemon=True, name="OrderJanitor").start()

def _weekly_flush_last_run_date():
    try:
        if not os.path.exists(WEEKLY_FLUSH_LAST_RUN_FILE):
            return None
        with open(WEEKLY_FLUSH_LAST_RUN_FILE, "r") as f:
            raw = f.read().strip()
        if not raw:
            return None
        return datetime.fromisoformat(raw).date()
    except Exception:
        return None


def _set_weekly_flush_last_run(day):
    try:
        with open(WEEKLY_FLUSH_LAST_RUN_FILE, "w") as f:
            f.write(day.isoformat())
    except Exception as e:
        print(f"⚠️ Could not persist weekly flush run date: {e}")


def _run_weekly_flush_once() -> dict:
    summary = {"closed": [], "failed": [], "skipped": []}
    print("🧼 Monday auto-flush starting — assessing open positions…")
    try:
        positions = trading_client.get_all_positions()
    except Exception as e:
        msg = f"⚠️ Weekly flush could not load positions: {e}"
        print(msg)
        summary["error"] = msg
        return summary

    if not positions:
        print("🧼 No open positions — nothing to flush.")
        return summary

    try:
        print_protection_status()
    except Exception as e:
        print(f"⚠️ Could not print protection status: {e}")

    for pos in positions:
        symbol = getattr(pos, "symbol", "").upper()
        if not symbol:
            continue
        if symbol in WEEKLY_FLUSH_WHITELIST:
            summary["skipped"].append((symbol, "whitelist"))
            print(f"🛑 Skipping {symbol}: whitelisted.")
            continue

        tp, sl = get_symbol_tp_sl_open_orders(symbol)
        has_tp = tp is not None
        has_sl = sl is not None
        try:
            has_tp = has_tp and float(tp) > 0
        except Exception:
            has_tp = False
        try:
            has_sl = has_sl and float(sl) > 0
        except Exception:
            has_sl = False

        protected = has_tp and has_sl
        should_close = False
        reason = ""

        if WEEKLY_FLUSH_MODE == "all":
            should_close = True
            reason = "mode=all"
        elif not protected:
            missing = []
            if not has_tp:
                missing.append("TP")
            if not has_sl:
                missing.append("SL")
            reason = "missing " + "/".join(missing) if missing else "unprotected"
            should_close = True
        else:
            summary["skipped"].append((symbol, "protected"))
            print(f"✅ {symbol} has active protection — skip.")
            continue

        if not should_close:
            continue

        print(f"🧼 Weekly flush closing {symbol} ({reason}).")
        ok = False
        try:
            ok = close_position_safely(symbol, close_reason=f"weekly_flush:{reason}")
        except Exception as e:
            print(f"❌ Weekly flush close failed for {symbol}: {e}")
        if ok:
            summary["closed"].append((symbol, reason))
        else:
            summary["failed"].append((symbol, reason))

    return summary


def _format_weekly_flush_summary(summary: dict) -> str:
    lines = [f"🧼 Monday auto-flush complete (mode={WEEKLY_FLUSH_MODE})."]
    closed = summary.get("closed", [])
    failed = summary.get("failed", [])
    skipped = summary.get("skipped", [])
    error = summary.get("error")

    if error:
        lines.append(error)

    if closed:
        details = ", ".join(f"{sym} [{reason}]" for sym, reason in closed)
        lines.append(f"✅ Closed: {details}")
    if failed:
        details = ", ".join(f"{sym} [{reason}]" for sym, reason in failed)
        lines.append(f"⚠️ Close failures: {details}")
    if skipped:
        grouped = {}
        for sym, why in skipped:
            grouped.setdefault(why, []).append(sym)
        for why, symbols in grouped.items():
            display = ", ".join(symbols[:8])
            if len(symbols) > 8:
                display += f" …(+{len(symbols) - 8})"
            lines.append(f"ℹ️ Skipped ({why}): {display}")

    if not (closed or failed or error):
        lines.append("ℹ️ No action required — all positions already protected.")

    return "\n".join(lines)


def start_weekly_flush_scheduler():
    if not WEEKLY_FLUSH_ENABLED:
        print("🧼 Monday auto-flush disabled.")
        return

    def _loop():
        print(
            "🧼 Monday auto-flush scheduler active "
            f"(mode={WEEKLY_FLUSH_MODE}, whitelist={len(WEEKLY_FLUSH_WHITELIST)} symbols, "
            f"target={WEEKLY_FLUSH_HOUR_UTC:02d}:{WEEKLY_FLUSH_MINUTE_UTC:02d} UTC)."
        )
        while True:
            try:
                now = datetime.now(timezone.utc)
                if now.weekday() == 0:  # Monday
                    target_dt = datetime.combine(
                        now.date(),
                        dt_time(hour=WEEKLY_FLUSH_HOUR_UTC, minute=WEEKLY_FLUSH_MINUTE_UTC, tzinfo=timezone.utc),
                    )
                    last_run = _weekly_flush_last_run_date()
                    if now >= target_dt and (last_run is None or last_run != now.date()):
                        summary = _run_weekly_flush_once()
                        _set_weekly_flush_last_run(now.date())
                        report = _format_weekly_flush_summary(summary)
                        print(report)
                        try:
                            send_telegram_alert(report)
                        except Exception as e:
                            print(f"⚠️ Telegram alert failed for weekly flush: {e}")
                time.sleep(60)
            except Exception as e:
                print(f"⚠️ Weekly flush scheduler error: {e}")
                should_stop, alerted = _handle_thread_exception("Weekly Flush Scheduler", e)
                if not alerted:
                    try:
                        send_telegram_alert(f"⚠️ Weekly flush scheduler error: {e}")
                    except Exception:
                        pass
                if should_stop:
                    return
                time.sleep(60)

    threading.Thread(target=_loop, daemon=True, name="WeeklyFlush").start()


def _check_overnight_protection():
    """Force-close all open positions before market close to avoid overnight exposure."""
    try:
        now_utc = datetime.now(timezone.utc)
        market_close_utc = datetime.combine(
            now_utc.date(),
            dt_time(hour=20, minute=55, tzinfo=timezone.utc),
        )
        window_start = market_close_utc - timedelta(minutes=5)
        if window_start <= now_utc <= market_close_utc:
            try:
                positions = trading_client.get_all_positions()
            except Exception as fetch_err:
                print(f"⚠️ Overnight protection: position fetch failed: {fetch_err}")
                return
            for pos in positions:
                try:
                    qty = float(getattr(pos, "qty", 0) or 0)
                except Exception:
                    qty = 0.0
                if abs(qty) == 0:
                    continue
                symbol = getattr(pos, "symbol", "").upper()
                side = "sell" if qty > 0 else "buy"
                print(f"🌙 [Overnight Protection] Closing {symbol} ({side} {abs(qty)} shares).")
                try:
                    close_position_safely(symbol, close_reason="overnight_protection")
                except Exception as close_err:
                    print(f"⚠️ Failed to close {symbol} before close: {close_err}")
    except Exception as e:
        print(f"⚠️ Overnight protection check failed: {e}")


def start_auto_sell_monitor():
    def monitor():
        while True:
            try:
                # Add a timeout to the API call
                positions = trading_client.get_all_positions()
                active_symbols = {p.symbol for p in positions}
                _prune_symbol_peak_cache(active_symbols)

                for position in positions:
                    symbol = position.symbol
                    qty = float(position.qty)
                    entry_price = float(position.avg_entry_price)
                    current_price = float(position.current_price)
                    percent_change = float(position.unrealized_plpc) * 100

                    print(f"📊 {symbol}: Qty={qty} Entry=${entry_price:.2f} Now=${current_price:.2f} PnL={percent_change:.2f}%")

                    # Cooldown guard (inserted)
                    now_t = monotonic()
                    last_t = _get_last_close_attempt_ts(symbol)
                    if now_t - last_t < _CLOSE_COOLDOWN_SEC:
                        continue

                    trailing_triggered = False
                    peak = _get_peak_unrealized(symbol)
                    if percent_change >= TRAILING_TRIGGER_PCT:
                        peak = max(peak or percent_change, percent_change)
                        _set_peak_unrealized(symbol, peak)
                    elif peak is not None and percent_change < TRAILING_TRIGGER_PCT:
                        # below trigger again before giveback → reset
                        _set_peak_unrealized(symbol, None)
                        peak = None

                    if peak is not None and (peak - percent_change) >= TRAILING_GIVEBACK_PCT:
                        trailing_triggered = True

                    hit_take_profit = percent_change >= WATCHDOG_TAKE_PROFIT_PCT
                    hit_stop_loss = percent_change <= WATCHDOG_HARD_STOP_PCT

                    if trailing_triggered or hit_take_profit or hit_stop_loss:
                        reason = "❗"
                        if hit_take_profit:
                            reason += "Take Profit Hit"
                        elif trailing_triggered:
                            reason += "Trailing Stop Giveback"
                        elif hit_stop_loss:
                            reason += "Hard Stop Hit"

                        print(f"💥 {symbol} closing position — {reason}")
                        now_t = monotonic()
                        ok = close_position_safely(symbol)  # cancels SELLs, then closes
                        _set_last_close_attempt_ts(symbol, now_t)  # start cooldown
                        _set_peak_unrealized(symbol, None)
                        if ok:
                            print(f"✅ Position closed for {symbol}")
                            # Update Performance sheet after a realized SELL is logged
                            try:
                                metrics = summarize_pnl_from_csv(TRADE_LOG_PATH)
                                update_google_performance_sheet(metrics)
                            except Exception as perf_e:
                                print(f"⚠️ Performance update failed: {perf_e}")
                            send_telegram_alert(f"💥 {symbol} auto-closed: {reason}")
                        else:
                            print(f"⚠️ Safe close failed for {symbol}; will retry after cooldown.")
                            send_telegram_alert(f"⚠️ Safe close failed for {symbol}; will retry after cooldown.")

            except Exception as monitor_error:
                print(f"❌ Watchdog error: {monitor_error}")
                should_stop, alerted = _handle_thread_exception("Watchdog", monitor_error)
                if not alerted:
                    try:
                        send_telegram_alert(f"⚠️ Watchdog error: {monitor_error}")
                    except Exception:
                        pass
                if should_stop:
                    return

            if OVERNIGHT_PROTECTION_ENABLED:
                _check_overnight_protection()
            time.sleep(WATCHDOG_LOOP_SECONDS)

    t = threading.Thread(target=monitor, daemon=True, name="Watchdog")
    t.start()

def is_cooldown_active():
    if not os.path.exists(LAST_TRADE_FILE):
        return False
    with open(LAST_TRADE_FILE, 'r') as f:
        last_trade_time_str = f.read().strip()
    if not last_trade_time_str:
        return False
    try:
        last_trade_time = datetime.strptime(last_trade_time_str, "%Y-%m-%d %H:%M:%S")
        elapsed = (datetime.now() - last_trade_time).total_seconds()
        return elapsed < TRADE_COOLDOWN_SECONDS
    except Exception as e:
        print("⚠️ Error reading last trade time:", e)
        return False

def is_ai_mood_bad():
    """Evaluate trade mood based on recent outcomes and equity drawdown."""
    try:
        df = pd.read_csv(TRADE_LOG_PATH, parse_dates=["timestamp"])
        recent = df.tail(5)

        loss_streak = sum(recent["status"].str.contains("loss|error|skipped", case=False))
        win_streak = sum(recent["status"].str.contains("executed", case=False))

        account = trading_client.get_account()  # ✅ New SDK method
        equity = float(account.equity)
        max_equity = get_max_equity()

        drawdown = 0
        if max_equity and equity < max_equity:
            drawdown = (max_equity - equity) / max_equity

        # 💡 Mood logic
        if drawdown >= 0.05:
            print("🧠 Mood: Drawdown triggered")
            return True
        if loss_streak >= 3 and win_streak == 0:
            print("🧠 Mood: Consecutive losses")
            return True

        return False

    except Exception as e:
        print("⚠️ AI Mood check failed:", e)
        return False

def log_trade(
    symbol,
    qty,
    entry,
    stop_loss,
    take_profit,
    status,
    action=None,
    fill_price=None,
    realized_pnl=None,
    *,
    session_type: str = "intraday",
    hold_reason: str = "",
    close_reason: str = "",
):
    explanation = generate_trade_explanation(
        symbol=symbol,
        entry=entry,
        stop_loss=stop_loss,
        take_profit=take_profit,
        rsi=None,
        trend=None,
        ha_candle=None
    )

    now = datetime.now()
    hour = now.hour
    if hour < 10:
        context_time = "morning"
    elif hour < 14:
        context_time = "midday"
    else:
        context_time = "afternoon"

    context_recent_outcome = "unknown"
    context_equity_change = "unknown"

    try:
        if os.path.isfile(TRADE_LOG_PATH):
            recent_df = pd.read_csv(TRADE_LOG_PATH)
            if not recent_df.empty:
                last_status = recent_df.iloc[-1]["status"]
                context_recent_outcome = "win" if last_status == "executed" else "loss"

                if "equity" in recent_df.columns:
                    prev_equity = float(recent_df.iloc[-1].get("equity", 0))
                    account = trading_client.get_account()
                    current_equity = float(account.equity)
                    context_equity_change = "gain" if current_equity > prev_equity else "loss"
    except Exception as e:
        print("⚠️ Failed to fetch recent outcome context:", e)

    # 🕰 Timestamp
    timestamp_str = now.strftime("%Y-%m-%d %H:%M:%S")

    if status == "executed":
        with open(LAST_TRADE_FILE, 'w') as f:
            f.write(timestamp_str)

    # 🧠 Compile and log trade data
    trade_data = {
        "timestamp": timestamp_str,
        "symbol": symbol,
        "qty": qty,
        "entry": entry,
        "stop_loss": stop_loss,
        "take_profit": take_profit,
        "status": status,
        "explanation": explanation,
        "context_time_of_day": context_time,
        "context_recent_outcome": context_recent_outcome,
        "context_equity_change": context_equity_change,
        "action": action,
        "fill_price": fill_price,
        "realized_pnl": realized_pnl,
        "session_type": session_type,
        "hold_reason": hold_reason,
        "close_reason": close_reason,
    }

    row = []
    for col in TRADE_LOG_HEADER:
        value = trade_data.get(col, "")
        row.append("" if value is None else value)

    file_exists = os.path.isfile(TRADE_LOG_PATH)
    with open(TRADE_LOG_PATH, mode='a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(TRADE_LOG_HEADER)
        writer.writerow(row)

    # ✅ Google Sheet Logging
    try:
        client = _get_gspread_client()

        sheet_id = _clean_env(os.getenv("GOOGLE_SHEET_ID"))
        sheet_name = _clean_env(os.getenv("TRADE_SHEET_NAME") or "Trade Log")

        if not sheet_id or not sheet_name:
            raise ValueError("Missing GOOGLE_SHEET_ID or TRADE_SHEET_NAME environment variable.")

        sheet = client.open_by_key(sheet_id).worksheet(sheet_name)

        sheet.append_row(row)
        print("✅ Trade logged to Google Sheet.")

    except Exception as e:
        print("⚠️ Failed to log trade to Google Sheet:", e)

    # === Update Performance sheet on SELL/close ===
    try:
        if str(action).upper() == 'SELL' or str(status).lower() in ('closed', 'sell'):
            metrics = summarize_pnl_from_csv(TRADE_LOG_PATH)
            update_google_performance_sheet(metrics)
    except Exception as perf_e:
        print(f"⚠️ Performance update failed: {perf_e}")

def _get_available_qty(symbol: str) -> float:
    """Return qty_available for an open position, or 0 if none."""
    try:
        positions = trading_client.get_all_positions()
        for p in positions:
            if p.symbol.upper() == symbol.upper():
                return float(getattr(p, "qty_available", getattr(p, "qty", "0")))
        return 0.0
    except Exception as e:
        print(f"⚠️ Could not fetch positions: {e}")
        return 0.0

def close_position_if_needed(symbol: str, reason: str) -> bool:
    """Safely close a position if qty is available. Returns True if order was submitted."""
    now = monotonic()
    last = _get_last_close_attempt_ts(symbol)
    if last is not None and (now - last) < _CLOSE_COOLDOWN_SEC:
        print(f"⏳ Skipping close for {symbol} (cooldown {int(_CLOSE_COOLDOWN_SEC - (now - last))}s).")
        return False

    qty_available = _get_available_qty(symbol)
    if qty_available <= 0:
        print(f"⚠️ No available qty for {symbol}, skipping close.")
        return False

    print(f"💥 {symbol} closing position — {reason} (qty_available={qty_available})")
    try:
        market_order = MarketOrderRequest(
            symbol=symbol,
            qty=qty_available,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.GTC
        )
        trading_client.submit_order(order_data=market_order)
        _set_last_close_attempt_ts(symbol, now)
        send_telegram_alert(f"✅ {symbol} closed at market. Reason: {reason}")
        print(f"✅ {symbol} position close submitted.")
        return True
    except Exception as e:
        _set_last_close_attempt_ts(symbol, now)
        print(f"❌ Failed to close {symbol}: {e}")
        send_telegram_alert(f"❌ Failed to close {symbol}: {e}")
        return False

def place_split_protection(
    symbol: str,
    tp_price: float = None,
    sl_price: float = None,
    tp_pct: float = 0.03,
    sl_pct: float = 0.02
) -> bool:
    """
    Cancel all SELL orders on `symbol`, then attach reduce-only style protection by
    submitting a TP (limit SELL) and SL (stop SELL), each for the full qty_available.
    If qty_available is not yet free (e.g., BUY leg still settling), return False so
    callers can retry shortly.
    """
    # 0) Cancel any existing SELLs to avoid qty conflicts
    try:
        n = cancel_open_sells(symbol)
        if n:
            print(f"🧹 Cancelled {n} existing SELLs on {symbol} before attaching protection.")
    except Exception as e:
        print(f"⚠️ Cancel SELLs failed for {symbol}: {e}")

    # 1) Check qty_available from current position
    qty_available = _get_available_qty(symbol)
    if qty_available <= 0:
        # BUY leg may still be settling; let caller retry later
        return False

    # 2) Get a recent price for % defaults if explicit prices weren't provided
    from alpaca.data.requests import StockLatestQuoteRequest
    last_px = None
    try:
        req = StockLatestQuoteRequest(symbol_or_symbols=symbol, feed=_DATA_FEED)
        q = data_client.get_stock_latest_quote(req)
        last_px = float(q[symbol].ask_price or q[symbol].bid_price)
    except Exception:
        # Fallback to position price if available
        try:
            pos = [p for p in trading_client.get_all_positions() if p.symbol.upper() == symbol.upper()]
            if pos:
                last_px = float(pos[0].current_price)
        except Exception:
            last_px = None

    if tp_price is None and last_px:
        tp_price = round(last_px * (1.0 + tp_pct), 2)
    if sl_price is None and last_px:
        sl_price = round(last_px * (1.0 - sl_pct), 2)

    ok_any = False

    # 3) Place TP limit SELL for the full qty_available
    try:
        if tp_price:
            tp_req = LimitOrderRequest(
                symbol=symbol,
                qty=qty_available,
                side=OrderSide.SELL,
                time_in_force=TimeInForce.GTC,
                limit_price=float(tp_price),
            )
            trading_client.submit_order(tp_req)
            print(f"✅ TP limit SELL placed: {qty_available} {symbol} @ {tp_price}")
            ok_any = True
    except Exception as e:
        print(f"⚠️ TP place failed for {symbol}: {e}")

    # 4) Place SL stop SELL for the full qty_available
    try:
        if sl_price:
            sl_req = StopOrderRequest(
                symbol=symbol,
                qty=qty_available,
                side=OrderSide.SELL,
                time_in_force=TimeInForce.GTC,
                stop_price=float(sl_price),
            )
            trading_client.submit_order(sl_req)
            print(f"✅ SL stop SELL placed: {qty_available} {symbol} @ {sl_price}")
            ok_any = True
    except Exception as e:
        print(f"⚠️ SL place failed for {symbol}: {e}")

    return ok_any

# === MAIN GUARD ===
if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    handle_restart_notification()
    with _BACKGROUND_WORKERS_LOCK:
        if not _BACKGROUND_WORKERS_STARTED:
            _BACKGROUND_WORKERS_STARTED = True
            start_autoscan_thread()
            # Add other background workers here (e.g., watchdog, monitors)
            start_eod_close_thread()
            start_weekly_flush_scheduler()
    app.run(host="0.0.0.0", port=port, debug=False)
