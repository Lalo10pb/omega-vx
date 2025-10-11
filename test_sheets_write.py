import os
from datetime import datetime

import pytest
from dotenv import load_dotenv

load_dotenv()

pytestmark = pytest.mark.skipif(
    os.getenv("OMEGA_RUN_SHEETS_TEST") not in {"1", "true", "yes"},
    reason="Set OMEGA_RUN_SHEETS_TEST=1 for live Google Sheets integration test.",
)

gspread = pytest.importorskip("gspread")
oauth2 = pytest.importorskip("oauth2client.service_account")


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        pytest.skip(f"{name} not configured.")
    return value


def _creds_path() -> str:
    path = os.getenv("GOOGLE_CREDS_PATH", "google_credentials.json")
    if not os.path.exists(path):
        pytest.skip(f"Credentials file missing at {path}.")
    return path


def test_can_append_rows_to_google_sheets():
    sheet_id = _require_env("GOOGLE_SHEET_ID")
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = oauth2.ServiceAccountCredentials.from_json_keyfile_name(_creds_path(), scope)
    gc = gspread.authorize(creds)

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for tab in ["Trade Log", "Portfolio Log"]:
        worksheet = gc.open_by_key(sheet_id).worksheet(tab)
        worksheet.append_row([ts, "SELFTEST", "ok"])
