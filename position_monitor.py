import time
import alpaca_trade_api as tradeapi
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")
BASE_URL = os.getenv("APCA_API_BASE_URL")

api = tradeapi.REST(API_KEY, API_SECRET, BASE_URL)
# ✅ Thresholds
TAKE_PROFIT_THRESHOLD = 5.0  # +5%
STOP_LOSS_THRESHOLD = -1.5   # -1.5%

def send_telegram_alert(message):
    import requests
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    data = {"chat_id": chat_id, "text": message}
    requests.post(url, data=data)

def monitor_positions():
    try:
        positions = api.list_positions()
        print("🔍 Monitoring open positions...")

        for pos in positions:
            symbol = pos.symbol
            qty = float(pos.qty)
            current_price = float(pos.current_price)
            entry_price = float(pos.avg_entry_price)

            percent_change = ((current_price - entry_price) / entry_price) * 100
            print(f"{symbol}: {qty} shares at ${entry_price:.2f} → {percent_change:.2f}%")

            # 📈 Take Profit if gain >= +5%
            if percent_change >= 5.0:
                print(f"🏆 Closing {symbol} — Profit target hit ({percent_change:.2f}%)")
                api.close_position(symbol)
                send_telegram_alert(f"🏆 {symbol} closed at +{percent_change:.2f}% profit")

            # 📉 Stop Loss if loss <= -3%
            elif percent_change <= -3.0:
                print(f"❌ Closing {symbol} — Stop loss hit ({percent_change:.2f}%)")
                api.close_position(symbol)
                send_telegram_alert(f"❌ {symbol} closed at {percent_change:.2f}% loss")

            # 📉 Auto-close if loss exceeds threshold
            if percent_change <= STOP_LOSS_THRESHOLD:
                print(f"❌ Closing {symbol} due to loss ({percent_change:.2f}%)")
                api.close_position(symbol)
                send_telegram_alert(f"❌ Auto-closed {symbol} due to loss ({percent_change:.2f}%)")

            # ✅ Optional: add take-profit logic here if you want

    except Exception as e:
        print("⚠️ Error in monitor_positions:", e)

def log_closed_trades():
    try:
        activities = api.get_activities(activity_types='FILL')
        now = datetime.utcnow()
        for activity in activities:
            if activity.side == 'sell' and activity.transaction_time:
                sell_time = activity.transaction_time.replace(tzinfo=None)
                time_diff = (now - sell_time).total_seconds()
                if time_diff < 70:  # trade closed in the last cycle
                    symbol = activity.symbol
                    qty = activity.qty
                    price = activity.price
                    profit = activity.net_amount
                    print(f"📉 Closed {symbol} — Qty: {qty} at ${price} → P/L: ${profit}")
                    send_telegram_alert(f"📉 Closed {symbol} — {qty} @ ${price} → P/L: ${profit}")
    except Exception as e:
        print("⚠️ Error logging closed trades:", e)

while True:
    monitor_positions()
    log_closed_trades()
    time.sleep(60)
