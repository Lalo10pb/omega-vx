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
import functools
from typing import Optional
from datetime import datetime, timedelta, time as dt_time
import numpy as np
import pandas as pd
from dotenv import load_dotenv
import json
import base64
from decimal import Decimal, ROUND_HALF_UP

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

# === Load .env and Set Environment Vars ===
from pathlib import Path
ENV_PATH = Path(__file__).parent / ".env"
if not load_dotenv(ENV_PATH):
    print(f"⚠️ Could not load .env at {ENV_PATH} — using environment vars only.", flush=True)


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


# --- Alpaca Data Feed selection (force IEX to avoid SIP permission errors) ---
_DATA_FEED = DataFeed.IEX
try:
    print("📡 Alpaca data feed: iex (forced)")
except Exception:
    pass

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

# === Logging Configuration ===
LOG_DIR = os.path.expanduser("~/omega-vx/logs")
os.makedirs(LOG_DIR, exist_ok=True)

import logging

logging.basicConfig(
    level=logging.INFO,  # Set the desired log level
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "omega_vx_bot.log")),  # Log to a file
        logging.StreamHandler(sys.stdout),  # Log to the console
    ],
)

# Replace print statements with logging.info, logging.warning, logging.error, etc.
logging.info("Application started")
print("✅ Omega-VX live build 08a8d10 (risk guard + PDT safeguards active)")
DAILY_RISK_LIMIT = -10  # optional daily risk guard, currently unused
TRADE_COOLDOWN_SECONDS = 300
MAX_RISK_BASE_PERCENT = _float_env("MAX_RISK_BASE_PERCENT", 1.0)
MAX_RISK_PER_TRADE_PERCENT = MAX_RISK_BASE_PERCENT
MAX_RISK_AUTO_MIN = _float_env("MAX_RISK_AUTO_MIN_PCT", 0.5)
MAX_RISK_AUTO_MAX = _float_env("MAX_RISK_AUTO_MAX_PCT", MAX_RISK_BASE_PERCENT)
MAX_RISK_AUTO_UP_STEP = _float_env("MAX_RISK_AUTO_UP_STEP", 0.5)
MAX_RISK_AUTO_DOWN_STEP = _float_env("MAX_RISK_AUTO_DOWN_STEP", 0.5)
EQUITY_GUARD_MIN_DRAWDOWN = _float_env("EQUITY_GUARD_MIN_DRAWDOWN_PCT", 0.10)
EQUITY_GUARD_STALE_RATIO = _float_env("EQUITY_GUARD_STALE_RATIO", 5.0)
EQUITY_GUARD_MAX_EQUITY_FLOOR = _float_env("EQUITY_GUARD_MAX_EQUITY_FLOOR", 0.0)
MAX_OPEN_POSITIONS_HIGH_EQUITY = _int_env("MAX_OPEN_POSITIONS_HIGH_EQUITY", 8)
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
_symbol_last_trade = {}  # {"SNAP": {"date": date, "exit_price": 8.57, "count": 1}}

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


def _can_trade_symbol_today(symbol: str, entry: float) -> bool:
    today = datetime.now().date()
    record = _symbol_last_trade.get(symbol.upper())

    if not record:
        return True

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

# Watchdog thresholds
WATCHDOG_TRAILING_STOP_PCT = -2.0
WATCHDOG_TAKE_PROFIT_PCT = 5.0
WATCHDOG_HARD_STOP_PCT = -3.5
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
print = functools.partial(print, flush=True)
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
    _pdt_lockouts[symbol.upper()] = until
    return _PDT_LOCKOUT_SEC


def _pdt_lockout_remaining(symbol: str) -> int:
    until = _pdt_lockouts.get(symbol.upper())
    if not until:
        return 0
    remaining = int(max(0, until - monotonic()))
    if remaining == 0:
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
        if equity < 500:
            return 3   # minimum floor raised
        elif equity < 1000:
            return 4
        elif equity < 2000:
            return 5
        elif equity < 5000:
            return 6
        else:
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

def close_position_safely(symbol: str) -> bool:
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
                log_trade(symbol, fqty or pre_qty, pre_avg if pre_avg is not None else 0.0,
                          None, None, status="closed", action="SELL",
                          fill_price=favg, realized_pnl=realized)
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
                prev = _symbol_last_trade.get(key, {})
                current_count = int(prev.get("count", 0) or 0)
                if current_count <= 0:
                    current_count = 1
                _symbol_last_trade[key] = {
                    "date": datetime.now().date(),
                    "exit_price": exit_price,
                    "count": current_count,
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
                log_trade(symbol, fqty, pre_avg if pre_avg is not None else 0.0,
                          None, None, status="closed", action="SELL",
                          fill_price=favg, realized_pnl=realized)
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
                prev = _symbol_last_trade.get(key, {})
                current_count = int(prev.get("count", 0) or 0)
                if current_count <= 0:
                    current_count = 1
                _symbol_last_trade[key] = {
                    "date": datetime.now().date(),
                    "exit_price": exit_price,
                    "count": current_count,
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
    from alpaca.data.requests import StockBarsRequest
    tf = TimeFrame.Minute if interval == '15m' else TimeFrame.Hour
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=2)

    def _request(feed, _tf, _start, _end):
        req = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=_tf,
            start=_start,
            end=_end,
            feed=feed,
        )
        return _fetch_bars_df(symbol, req)

    # 1) Try requested timeframe on IEX
    bars = _fetch_data_with_fallback(
        lambda feed: _request(feed, tf, start, end), symbol, feed=DataFeed.IEX
    )

    # 2) If missing or empty, fall back to DAILY bars
    if bars is None or bars.empty:
        try:
            d_start = end - timedelta(days=20)
            d_tf = TimeFrame.Day
            d_bars = _fetch_data_with_fallback(
                lambda feed: _request(feed, d_tf, d_start, end), symbol, feed=DataFeed.IEX
            )
            if d_bars is None or d_bars.empty:
                print(f"⚠️ No data returned for {symbol} even on daily.")
                return None
            # normalize to single symbol
            try:
                if 'symbol' in d_bars.columns:
                    d_bars = d_bars[d_bars['symbol'] == symbol]
                else:
                    d_bars = d_bars.loc[symbol]
                d_bars = d_bars.reset_index()
            except Exception:
                d_bars = d_bars.reset_index()
            if len(d_bars) < 2:
                print(f"⚠️ Not enough daily data to compute Heikin Ashi for {symbol}.")
                return None
            prev_close = d_bars['close'].iloc[-2]
            curr_open = d_bars['open'].iloc[-1]
            ha_open = (curr_open + prev_close) / 2
            ha_close = (d_bars['open'].iloc[-1] + d_bars['high'].iloc[-1] + d_bars['low'].iloc[-1] + d_bars['close'].iloc[-1]) / 4
            trend = 'bullish' if ha_close > ha_open else ('bearish' if ha_close < ha_open else 'neutral')
            print(f"🕊️ Heikin-Ashi DAILY fallback for {symbol}: {trend}")
            return trend
        except Exception as e:
            print(f"❌ Daily HA fallback failed for {symbol}: {e}")
            return None

    # normalize minutes/hour data
    try:
        if 'symbol' in bars.columns:
            bars = bars[bars['symbol'] == symbol]
        else:
            bars = bars.loc[symbol]
        bars = bars.reset_index()
    except Exception:
        try:
            bars = bars.reset_index()
            bars = bars[bars['symbol'] == symbol]
        except Exception:
            print(f"⚠️ Unexpected bars shape for {symbol}")
            return None

    if len(bars) < lookback + 1:
        print(f"⚠️ Not enough data to compute Heikin Ashi for {symbol}.")
        return None

    ha_candles = []
    for i in range(1, lookback + 1):
        prev_close = bars['close'].iloc[-(i + 1)]
        curr_open = bars['open'].iloc[-i]
        ha_open = (curr_open + prev_close) / 2
        ha_close = (bars['open'].iloc[-i] + bars['high'].iloc[-i] + bars['low'].iloc[-i] + bars['close'].iloc[-i]) / 4
        ha_candles.append({'open': ha_open, 'close': ha_close})

    last = ha_candles[-1]
    if last['close'] > last['open']:
        return 'bullish'
    elif last['close'] < last['open']:
        return 'bearish'
    else:
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

def _best_candidate_from_watchlist(symbols):
    """
    Score each symbol with a simple model:
      +2 if 15m and 1h Heikin‑Ashi are both bullish
      +1 if RSI(15m) is in a neutral (35–65) zone
      −1 if RSI is very extreme (<25 or >75)
    Returns best symbol or None if nothing scores positive.
    """
    ranked = []
    for sym in symbols:
        try:
            s = sym.strip().upper()
            if not s:
                continue
            mtf_bull = is_multi_timeframe_confirmed(s)
            rsi = get_rsi_value(s, interval='15m')
            score = 0
            if mtf_bull:
                score += 2
            if rsi is not None:
                if 35 <= rsi <= 65:
                    score += 1
                elif rsi < 25 or rsi > 75:
                    score -= 1
            ranked.append((score, s))
            print(f"🧪 Score {s}: score={score} (mtf_bull={mtf_bull}, rsi={rsi})")
        except Exception as e:
            print(f"⚠️ scoring {sym} failed: {e}")
            continue
    ranked.sort(reverse=True)
    return ranked[0][1] if ranked and ranked[0][0] > 0 else None

def _compute_entry_tp_sl(symbol: str):
    """
    Pull live quote; compute a conservative TP/SL if not provided:
    entry ~ last ask, SL at −2%, TP at +3%.
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
    sl    = round(entry * 0.98, 2)
    tp    = round(entry * 1.03, 2)
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
            time.sleep(OMEGA_SCAN_INTERVAL_SEC)
    t = threading.Thread(target=_loop, daemon=True)
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
        print(f"📥 Webhook received: {data}", flush=True)

        # 🔐 Validate the secret from HEADER
        header_secret = request.headers.get("X-OMEGA-SECRET")
        env_secret = os.getenv("WEBHOOK_SECRET_TOKEN")  # be explicit
        if header_secret != env_secret:
            print("🚫 Unauthorized webhook attempt blocked.", flush=True)
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
            print(msg, flush=True)
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
        print(f"🔄 Calling submit_order_with_retries for {symbol}", flush=True)

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
            print(msg, flush=True)
            try:
                send_telegram_alert(msg)
            except Exception:
                pass
            return jsonify({"status": "failed", "reason": "exception", "detail": str(trade_error)}), 200

        placed = bool(success)
        print(f"✅ Webhook processed (trade_placed={placed})", flush=True)
        resp = {
            "status": "ok",                      # webhook processed fine
            "trade_placed": placed               # whether an order actually went in
        }
        if not placed:
           resp["reason"] = "submit_returned_false"  # e.g., qty=0 / buying power / weekend

        return jsonify(resp), 200

    except Exception as e:
        print(f"❌ Exception in webhook: {e}", flush=True)
        return jsonify({"status": "failed", "reason": "handler_exception", "detail": str(e)}), 200

@app.route("/ping", methods=["GET"])
def ping():
    return "pong", 200

MAX_EQUITY_FILE = os.path.join(LOG_DIR, "max_equity.txt")
SNAPSHOT_LOG_PATH = os.path.join(LOG_DIR, "last_snapshot.txt")
PORTFOLIO_LOG_PATH = os.path.join(LOG_DIR, "portfolio_log.csv")
TRADE_LOG_PATH = os.path.join(LOG_DIR, "trade_log.csv")
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

    def _request(feed, _tf, _start, _end):
        req = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=_tf,
            start=_start,
            end=_end,
            feed=feed,
        )
        return _fetch_bars_df(symbol, req)

    # 1) Try requested timeframe on IEX
    bars = _fetch_data_with_fallback(
        lambda feed: _request(feed, tf, start, end), symbol, feed=DataFeed.IEX
    )

    # 2) Fallback to DAILY if needed
    used_daily = False
    if bars is None or bars.empty or ('close' not in getattr(bars, 'columns', [])):
        try:
            d_start = end - timedelta(days=100)
            d_tf = TimeFrame.Day
            bars = _fetch_data_with_fallback(
                lambda feed: _request(feed, d_tf, d_start, end), symbol, feed=DataFeed.IEX
            )
            used_daily = True
        except Exception as e2:
            print(f"❌ Daily RSI fallback failed for {symbol}: {e2}")
            return None

    # Normalize to a single‑symbol frame
    try:
        if 'symbol' in bars.columns:
            bars = bars[bars['symbol'] == symbol]
        else:
            bars = bars.loc[symbol]
    except Exception:
        pass

    if 'close' not in bars.columns or len(bars) < period + 1:
        print(f"⚠️ Not enough data to calculate RSI for {symbol}")
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
    # If TP is at/below ~market, bump to +3%
    if abs_tp <= last_px * 0.995:
        abs_tp = round(last_px * 1.03, 2)
    # If SL is at/above ~market, cut to -2%
    if abs_sl >= last_px * 1.005:
        abs_sl = round(last_px * 0.98, 2)
    abs_tp = _quantize_to_tick(abs_tp)
    abs_sl = _quantize_to_tick(abs_sl)
    print(f"🧭 TP/SL sanity → last={last_px:.2f} | TP={abs_tp} | SL={abs_sl}")

    # 🚨 Attach protection immediately (always place TP/SL after buy)
    try:
        place_split_protection(symbol, tp_price=abs_tp, sl_price=abs_sl)
        print(f"🛡️ Protection attached for {symbol}: TP={abs_tp}, SL={abs_sl}")
    except Exception as e:
        print(f"⚠️ Immediate protection attach failed for {symbol}: {e} — watchdog will retry.")

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

        row = [timestamp, equity, cash, portfolio_value]

        # ✅ Log to local CSV file
        file_exists = os.path.exists(PORTFOLIO_LOG_PATH)
        with open(PORTFOLIO_LOG_PATH, mode='a', newline='') as file:
            writer = csv.writer(file)
            if not file_exists:
                writer.writerow(["timestamp", "equity", "cash", "portfolio_value"])
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


def summarize_realized_pnl_for_da<truncated__content/>

# === MAIN GUARD ===
if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    handle_restart_notification()
    with _BACKGROUND_WORKERS_LOCK:
        global _BACKGROUND_WORKERS_STARTED
        if not _BACKGROUND_WORKERS_STARTED:
            _BACKGROUND_WORKERS_STARTED = True
            start_autoscan_thread()
            # Add other background workers here (e.g., watchdog, monitors)
    app.run(host="0.0.0.0", port=port, debug=False)