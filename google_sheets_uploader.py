import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

# ✅ Define spreadsheet and worksheet names
SPREADSHEET_NAME = "OMEGA-VX LOGS"
TRADE_SHEET = "Trade Log"
PORTFOLIO_SHEET = "Portfolio Log"

# ✅ Path to your service account key
SERVICE_ACCOUNT_FILE = 'omega-vx-service-account.json'

# ✅ Authenticate and connect to Google Sheets
scope = ["https://www.googleapis.com/auth/spreadsheets",
         "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scope)
gc = gspread.authorize(creds)
SPREADSHEET_ID = "1Mi41KWJO6oJxTSJUEfiVslPIM_DEsX39-77_2d5ugMw"
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
