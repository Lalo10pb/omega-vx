import os
import json
import base64
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def ensure_google_creds_file() -> str:
    """
    Ensures a local Google service-account JSON exists and returns its path.
    Priority:
      1) Existing file at GOOGLE_CREDS_PATH or ./google_credentials.json
      2) GOOGLE_CREDENTIALS_JSON env var (raw JSON)
      3) GOOGLE_CREDENTIALS_B64 env var (base64 of JSON)
    """
    default_path = os.getenv("GOOGLE_CREDS_PATH", "google_credentials.json")
    # 1) If a file already exists, use it
    if os.path.exists(default_path):
        return default_path

    # 2) Raw JSON in env
    raw = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if raw:
        with open(default_path, "w") as f:
            f.write(raw)
        return default_path

    # 3) Base64-encoded JSON in env
    b64 = os.getenv("GOOGLE_CREDENTIALS_B64")
    if b64:
        try:
            data = base64.b64decode(b64).decode("utf-8")
            with open(default_path, "w") as f:
                f.write(data)
            return default_path
        except Exception as e:
            print(f"❌ Failed to decode GOOGLE_CREDENTIALS_B64: {e}")

    # If we got here, we have no credentials
    raise FileNotFoundError("google_credentials.json not found and no GOOGLE_CREDENTIALS_JSON / GOOGLE_CREDENTIALS_B64 provided")

def _debug_log_service_account():
    try:
        path = ensure_google_creds_file()
    except Exception:
        return
    try:
        with open(path, "r") as f:
            j = json.load(f)
        email = j.get("client_email")
        if email:
            print("📧 Google service account:", email)
    except Exception:
        pass

def get_watchlist_from_google_sheet() -> list[str]:
    """
    Returns a list of tickers from the first column of the WATCHLIST sheet.
    Accepts a header row named 'symbol' (case-insensitive) and ignores blanks.
    Env:
      - GOOGLE_SHEET_ID                  (required)
      - WATCHLIST_SHEET_NAME or SHEET_NAME (defaults to 'watchlist')
    Credentials:
      - File at GOOGLE_CREDS_PATH or ./google_credentials.json
      - or GOOGLE_CREDENTIALS_JSON (raw JSON)
      - or GOOGLE_CREDENTIALS_B64 (base64)
    """
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    if not sheet_id:
        print("❌ GOOGLE_SHEET_ID is not set.")
        return []

    sheet_name = (os.getenv("WATCHLIST_SHEET_NAME") or os.getenv("SHEET_NAME") or "watchlist").strip()

    try:
        creds_path = ensure_google_creds_file()
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
        gc = gspread.authorize(creds)

        sh = gc.open_by_key(sheet_id)
        ws = sh.worksheet(sheet_name)
        col = ws.col_values(1)  # first column
        symbols = []
        for s in col:
            if not s:
                continue
            t = s.strip().upper()
            if not t or t == "SYMBOL":
                continue
            symbols.append(t)

        print(f"📄 Watchlist loaded from '{sheet_name}': {symbols}")
        return symbols
    except Exception as e:
        print(f"❌ Failed to fetch watchlist: {e}")
        return []

_debug_log_service_account()
from __future__ import annotations

import os
import json
import base64
import re

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Google API scopes
_SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

# Acceptable ticker pattern (A–Z, 0–9, dot and dash; max 10 chars)
_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")


def _build_credentials():
    """
    Build OAuth2 service-account credentials from one of:
      - GOOGLE_CREDENTIALS_JSON (raw JSON)
      - GOOGLE_CREDENTIALS_B64 (base64 of the JSON)
      - GOOGLE_CREDS_PATH or default 'google_credentials.json' (file path)
    """
    raw_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
    b64_json = os.getenv("GOOGLE_CREDENTIALS_B64")
    path = os.getenv("GOOGLE_CREDS_PATH", "google_credentials.json")

    if raw_json:
        data = json.loads(raw_json)
    elif b64_json:
        data = json.loads(base64.b64decode(b64_json).decode("utf-8"))
    else:
        if not os.path.isabs(path):
            path = os.path.abspath(path)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Credentials file not found at: {path}")
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

    return ServiceAccountCredentials.from_json_keyfile_dict(data, scopes=_SCOPE)


def _gspread_client():
    creds = _build_credentials()
    return gspread.authorize(creds)


def _sanitize_symbols(rows: list[str]) -> list[str]:
    """Uppercase, drop header/empties, de-dup and keep only valid tickers."""
    out: list[str] = []
    for v in rows:
        sym = (v or "").strip().upper()
        if not sym or sym == "SYMBOL":
            continue
        if _TICKER_RE.match(sym) and sym not in out:
            out.append(sym)
    return out


def get_watchlist_from_google_sheet() -> list[str]:
    """
    Read the first column from the configured Google Sheet worksheet and return a list of tickers.

    Required env:
      - GOOGLE_SHEET_ID
    Optional env:
      - WATCHLIST_SHEET_NAME (default: "watchlist")
      - SHEET_NAME (used if WATCHLIST_SHEET_NAME is not set)
      - GOOGLE_CREDS_PATH / GOOGLE_CREDENTIALS_JSON / GOOGLE_CREDENTIALS_B64
    """
    sheet_id = (os.getenv("GOOGLE_SHEET_ID") or "").strip()
    sheet_name = (os.getenv("WATCHLIST_SHEET_NAME") or os.getenv("SHEET_NAME") or "watchlist").strip()

    if not sheet_id:
        print("❌ GOOGLE_SHEET_ID is not set.")
        return []

    try:
        gc = _gspread_client()
        sh = gc.open_by_key(sheet_id)
        ws = sh.worksheet(sheet_name)

        # Pull all values, then take the first column
        values = ws.get_all_values()
        first_col = [row[0] for row in values if row]

        symbols = _sanitize_symbols(first_col)
        print(f"✅ Watchlist loaded from '{sheet_name}' ({len(symbols)} symbols).")
        return symbols

    except gspread.exceptions.SpreadsheetNotFound:
        print("❌ Spreadsheet not found. Check GOOGLE_SHEET_ID and share the sheet with your service account email.")
    except gspread.exceptions.WorksheetNotFound:
        print(f"❌ Worksheet '{sheet_name}' not found. Check WATCHLIST_SHEET_NAME / SHEET_NAME.")
    except Exception as e:
        print(f"❌ get_watchlist_from_google_sheet error: {e}")

    return []