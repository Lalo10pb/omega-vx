import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
import os

# ✅ Load spreadsheet and worksheet names from environment
SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_ID")
TRADE_SHEET = os.getenv("TRADE_SHEET_NAME")
PORTFOLIO_SHEET = os.getenv("PORTFOLIO_SHEET_NAME")

# ✅ Path to secret file in Render
SERVICE_ACCOUNT_FILE = "/etc/secrets/omega-vx-service-account.json"

# ✅ Authenticate and connect to Google Sheets
scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scope)
gc = gspread.authorize(creds)
sheet = gc.open_by_key(SPREADSHEET_ID)

# ✅ Upload trade log
def upload_csv_to_sheet(csv_path, worksheet_name):
    try:
        df = pd.read_csv(csv_path)
        worksheet = sheet.worksheet(worksheet_name)
        worksheet.clear()
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())
        print(f"✅ Uploaded {worksheet_name}")
    except Exception as e:
        print(f"❌ Failed to upload {worksheet_name}: {e}")

# ✅ Run uploads
upload_csv_to_sheet("trade_log.csv", TRADE_SHEET)
upload_csv_to_sheet("portfolio_log.csv", PORTFOLIO_SHEET)
