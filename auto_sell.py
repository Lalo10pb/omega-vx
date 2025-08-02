import os
import alpaca_trade_api as tradeapi
from dotenv import load_dotenv
from datetime import datetime
import pytz

# ✅ Load env
load_dotenv()
API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")
BASE_URL = os.getenv("APCA_API_BASE_URL")

api = tradeapi.REST(API_KEY, API_SECRET, BASE_URL)

# ✅ Check time
eastern = pytz.timezone('US/Eastern')
now = datetime.now(eastern)

# Market close is 4:00 PM EST
if now.hour == 15 and now.minute >= 45:
    print("🔔 It's time to close all positions.")
    try:
        positions = api.list_positions()
        if not positions:
            print("✅ No open positions.")
        for pos in positions:
            symbol = pos.symbol
            print(f"🧹 Closing {symbol}")
            api.close_position(symbol)
        print("✅ All positions closed before market close.")
    except Exception as e:
        print("❌ Error closing positions:", e)
else:
    print(f"⏳ Current time is {now.strftime('%H:%M:%S')} — not close enough to market close.")