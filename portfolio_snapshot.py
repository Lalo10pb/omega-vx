import os
import csv
from datetime import datetime
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient

# Load environment variables
load_dotenv()
API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")
PAPER_MODE = str(os.getenv("ALPACA_PAPER", "true")).strip().lower() in ("1", "true", "yes")

# Connect to Alpaca
client = TradingClient(API_KEY, API_SECRET, paper=PAPER_MODE)

# Get account info
account = client.get_account()
timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
equity = float(account.equity)
cash = float(account.cash)
portfolio_value = float(account.portfolio_value)

# File path
log_file = "portfolio_snapshots.csv"
file_exists = os.path.exists(log_file)

# Write to CSV
with open(log_file, mode='a', newline='') as file:
    writer = csv.writer(file)
    if not file_exists:
        writer.writerow(["timestamp", "equity", "cash", "portfolio_value"])
    writer.writerow([timestamp, equity, cash, portfolio_value])

print("✅ Portfolio snapshot logged:")
print(f"🕒 Time: {timestamp}")
print(f"💼 Equity: ${equity:.2f}")
print(f"💵 Cash: ${cash:.2f}")
print(f"📈 Portfolio Value: ${portfolio_value:.2f}")
