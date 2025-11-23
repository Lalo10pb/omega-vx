import time
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import ActivityType
from datetime import datetime
from dotenv import load_dotenv
import os
from dateutil.parser import isoparse

load_dotenv()

API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")
PAPER_MODE = str(os.getenv("ALPACA_PAPER", "true")).strip().lower() in ("1", "true", "yes")

client = TradingClient(API_KEY, API_SECRET, paper=PAPER_MODE)


def _pct_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except Exception:
        return default


# ✅ Thresholds (align with watchdog/fallback envs from omega_vx_bot)
TAKE_PROFIT_THRESHOLD = _pct_env("WATCHDOG_TAKE_PROFIT_PCT", _pct_env("FALLBACK_TAKE_PROFIT_PCT", 5.0))
STOP_LOSS_THRESHOLD = -abs(_pct_env("WATCHDOG_HARD_STOP_PCT", _pct_env("FALLBACK_STOP_LOSS_PCT", 3.0)))

def send_telegram_alert(message):
    import requests
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    data = {"chat_id": chat_id, "text": message}
    requests.post(url, data=data)

def monitor_positions():
    try:
        positions = client.get_all_positions()
        print("🔍 Monitoring open positions...")

        for pos in positions:
            symbol = pos.symbol
            qty = float(pos.qty)
            current_price = float(pos.current_price)
            entry_price = float(pos.avg_entry_price)

            percent_change = ((current_price - entry_price) / entry_price) * 100
            print(f"{symbol}: {qty} shares at ${entry_price:.2f} → {percent_change:.2f}%")

            # 📈 Take Profit if gain >= +5%
            if percent_change >= TAKE_PROFIT_THRESHOLD:
                print(f"🏆 Closing {symbol} — Profit target hit ({percent_change:.2f}%)")
                client.close_position(symbol)
                send_telegram_alert(f"🏆 {symbol} closed at +{percent_change:.2f}% profit")

            # 📉 Auto-close if loss exceeds threshold
            elif percent_change <= STOP_LOSS_THRESHOLD:
                print(f"❌ Closing {symbol} — Stop loss hit ({percent_change:.2f}%)")
                client.close_position(symbol)
                send_telegram_alert(f"❌ Auto-closed {symbol} due to loss ({percent_change:.2f}%)")

            # ✅ Optional: add take-profit logic here if you want

    except Exception as e:
        print("⚠️ Error in monitor_positions:", e)

def log_closed_trades():
    try:
        activities = client.get_activities(activity_type=ActivityType.FILL)
        now = datetime.utcnow()
        for activity in activities:
            if activity.side == 'sell' and getattr(activity, 'transaction_time', None):
                txn = activity.transaction_time
                sell_time = None
                if isinstance(txn, datetime):
                    sell_time = txn.replace(tzinfo=None)
                else:
                    try:
                        sell_time = isoparse(str(txn)).replace(tzinfo=None)
                    except Exception:
                        sell_time = None
                if sell_time is None:
                    continue
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
