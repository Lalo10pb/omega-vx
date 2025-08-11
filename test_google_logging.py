import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

# ✅ Google credentials + spreadsheet setup
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
CREDS_FILE = "google_credentials.json"  # Must be in your project folder
SHEET_ID = "1Mi41KWJO6oJxTSJUEfiVslPIM_DEsX39-77_2d5ugMw"
TRADE_SHEET = "Trade Log"
PORTFOLIO_SHEET = "Portfolio Log"

def test_logging():
    try:
        # ✅ Load service account credentials
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, SCOPE)
        client = gspread.authorize(creds)

        # ✅ Open Google Sheet by ID
        sheet = client.open_by_key(SHEET_ID)

        # ✅ Write test data to Trade Log
        trade_ws = sheet.worksheet(TRADE_SHEET)
        trade_ws.append_row(["✅ TEST TRADE", datetime.now().isoformat(), 123.45, "test", "Omega Bot"])

        # ✅ Write test data to Portfolio Log
        portfolio_ws = sheet.worksheet(PORTFOLIO_SHEET)
        portfolio_ws.append_row(["✅ TEST PORTFOLIO", datetime.now().isoformat(), 456.78, 789.01])

        print("✅ Successfully wrote test rows to both sheets.")
    except Exception as e:
        print(f"❌ Failed to write to Google Sheets: {e}")

if __name__ == "__main__":
    test_logging()