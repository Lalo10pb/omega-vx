import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
import os
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()
# ✅ Get symbols from Google Sheet
def get_symbols_from_sheet(sheet_id, sheet_name):
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('creds.json', scope)
    client = gspread.authorize(creds)

    sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
    symbols = sheet.col_values(1)
    symbols = [s.strip().upper() for s in symbols if s.strip() and s.lower() != "symbol"]  # skip header
    return symbols
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockSnapshotRequest
from alpaca.data.timeframe import TimeFrame

# 🌐 Google Sheets access
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
CREDS_FILE = "google_credentials.json"

# 📄 Sheet config
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
SHEET_NAME = os.getenv("SHEET_NAME")

# 🌐 Webhook to send alerts to OMEGA
WEBHOOK_URL = os.getenv("WEBHOOK_URL")

# ⛓️ Alpaca credentials (loaded from .env)
ALPACA_KEY = os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET = os.getenv("APCA_API_SECRET_KEY")
trading_client = TradingClient(ALPACA_KEY, ALPACA_SECRET, paper=True)
data_client = StockHistoricalDataClient(ALPACA_KEY, ALPACA_SECRET)

def get_latest_price(symbol):
    try:
        snapshot = data_client.get_stock_snapshot(
            StockSnapshotRequest(symbol_or_symbols=symbol)
        )[symbol]
        return snapshot.latest_trade.price
    except Exception as e:
        print(f"❌ Error getting price for {symbol}: {e}")
        return None

def scan_symbol(symbol):
    print(f"🔎 Scanning {symbol}...")

    try:
        price = get_latest_price(symbol)
        if price is None:
            return

        entry = round(price, 2)
        stop_loss = round(entry * 0.97, 2)      # 3% stop loss
        take_profit = round(entry * 1.05, 2)    # 5% take profit

        payload = {
            "symbol": symbol,
            "entry": entry,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "use_trailing": True
        }

        headers = {
            "X-OMEGA-SECRET": os.getenv("WEBHOOK_SECRET_TOKEN")
        }

        print(f"📡 Sending alert for {symbol} to OMEGA...")
        print(f"🧪 Using webhook URL: {WEBHOOK_URL}")
        print(f"📊 Price={price} | Entry={entry} | SL={stop_loss} | TP={take_profit}")

        r = requests.post(WEBHOOK_URL, json=payload, headers=headers)
        print(f"✅ Webhook response: {r.status_code} {r.text}")

    except Exception as e:
        print(f"❌ Error scanning {symbol}: {e}")

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

def main():
    print(f"\n📆 Starting scan at {datetime.now()}")
    try:
        symbols = load_symbols()
        print(f"📊 Loaded symbols: {symbols}")
        for sym in symbols:
            scan_symbol(sym)
    except Exception as e:
        print(f"❌ Scanner failed: {e}")

if __name__ == "__main__":
    main()