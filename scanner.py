import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
import os
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()


# 🌐 Google Sheets access
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
CREDS_FILE = "google_credentials.json"  # Replace if you used a different filename

# 📄 Sheet config
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
SHEET_NAME = os.getenv("SHEET_NAME")

# 🌐 Webhook to send alerts to OMEGA
WEBHOOK_URL = os.getenv("WEBHOOK_URL")

# 📩 Telegram optional alert (coming later)
def send_telegram(msg):
    pass  # placeholder

def load_symbols():
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, SCOPE)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SHEET_ID)
    worksheet = sheet.worksheet(SHEET_NAME)
    data = worksheet.get_all_records()
    return [row['symbol'] for row in data if row.get('symbol')]

def scan_symbol(symbol):
    print(f"🔎 Scanning {symbol}...")
    signal = True

    if signal:
        payload = {
            "symbol": symbol,
            "entry": 100.00,
            "stop_loss": 95.00,
            "take_profit": 110.00,
            "use_trailing": True
        }

        headers = {
            "X-OMEGA-SECRET": "VX2025xSecure93"
        }

        print(f"📡 Sending alert for {symbol} to OMEGA...")
        print(f"🧪 Using webhook URL: {WEBHOOK_URL}")

        try:
            r = requests.post(WEBHOOK_URL, json=payload, headers=headers)
            print(f"✅ Webhook response: {r.status_code} {r.text}")
        except Exception as e:
            print(f"❌ Failed to send webhook: {e}")
    else:
        print(f"🚫 No signal for {symbol} (did not match mock condition)")
def main():
    print(f"\n📆 Starting scan at {datetime.now()}")
    try:
        symbols = load_symbols()
        print(f"📊 Loaded symbols: {symbols}")  # ← ADD THIS LINE
        for sym in symbols:
            scan_symbol(sym)
    except Exception as e:
        print(f"❌ Scanner failed: {e}")
if __name__ == "__main__":
    main()