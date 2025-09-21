import os
from alpaca.trading.client import TradingClient
from dotenv import load_dotenv
from datetime import datetime
import pytz

# ✅ Load env
load_dotenv()
API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")
PAPER_MODE = str(os.getenv("ALPACA_PAPER", "true")).strip().lower() in ("1", "true", "yes")

client = TradingClient(API_KEY, API_SECRET, paper=PAPER_MODE)

# ✅ Check time
eastern = pytz.timezone('US/Eastern')
now = datetime.now(eastern)

# Market close is 4:00 PM EST
if now.hour == 15 and now.minute >= 45:
    print("🔔 It's time to close all positions.")
    try:
        positions = client.get_all_positions()
        if not positions:
            print("✅ No open positions.")
        for pos in positions:
            symbol = pos.symbol
            print(f"🧹 Closing {symbol}")
            client.close_position(symbol)
        print("✅ All positions closed before market close.")
    except Exception as e:
        print("❌ Error closing positions:", e)
else:
    print(f"⏳ Current time is {now.strftime('%H:%M:%S')} — not close enough to market close.")
