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

_debug_log_service_account