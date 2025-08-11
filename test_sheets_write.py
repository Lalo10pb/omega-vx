import os, gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
from datetime import datetime
load_dotenv()

SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('google_credentials.json', scope)
gc = gspread.authorize(creds)

ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
for tab in ["Trade Log", "Portfolio Log"]:
    w = gc.open_by_key(SHEET_ID).worksheet(tab)
    w.append_row([ts, "SELFTEST", "ok"])
    print(f"✅ wrote to {tab}")