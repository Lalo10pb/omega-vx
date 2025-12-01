import time
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import ActivityType
from datetime import datetime
from dotenv import load_dotenv
import os
from dateutil.parser import isoparse
import csv
from pathlib import Path
from typing import Optional

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

DECISION_LOG_PATH = Path(os.getenv("POSITION_MONITOR_LOG", "logs/decision_log.csv"))
DECISION_FIELDS = [
    "timestamp",
    "weekday_utc",
    "hour_utc",
    "symbol",
    "action",
    "exit_reason",
    "qty",
    "entry_price",
    "current_price",
    "percent_change",
    "profit_usd",
    "take_profit_threshold",
    "stop_loss_threshold",
    "paper_mode",
    "risk_off",
    "risk_tag",
]


def _append_decision_log(row: dict) -> None:
    """Append a single decision/outcome row for offline analysis."""
    DECISION_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    file_exists = DECISION_LOG_PATH.exists() and DECISION_LOG_PATH.stat().st_size > 0
    with DECISION_LOG_PATH.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=DECISION_FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def _risk_off_active() -> bool:
    """
    Simple risk-off switch controlled via env or a flag file.
    - RISK_OFF=1 forces risk-off
    - RISK_OFF_FILE=/path sets risk-off if the file exists
    """
    if str(os.getenv("RISK_OFF", "0")).strip().lower() in ("1", "true", "yes"):
        return True
    flag_file = os.getenv("RISK_OFF_FILE")
    if flag_file and Path(flag_file).exists():
        return True
    return False


def _risk_tag() -> Optional[str]:
    """
    Optional marker (e.g., 'high_vol', 'event_day') set via env RISK_TAG
    to trace performance by regime.
    """
    tag = os.getenv("RISK_TAG")
    return tag.strip() if tag else None


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

        risk_off = _risk_off_active()
        risk_tag = _risk_tag()

        for pos in positions:
            symbol = pos.symbol
            qty = float(pos.qty)
            current_price = float(pos.current_price)
            entry_price = float(pos.avg_entry_price)
            profit_usd = (current_price - entry_price) * qty

            percent_change = ((current_price - entry_price) / entry_price) * 100
            print(f"{symbol}: {qty} shares at ${entry_price:.2f} → {percent_change:.2f}%")

            action = "hold"
            exit_reason = "hold"
            # 📈 Take Profit if gain >= +5%
            tp_enabled = not risk_off or str(os.getenv("RISK_OFF_ALLOW_TP", "1")).lower() in ("1", "true", "yes")
            if percent_change >= TAKE_PROFIT_THRESHOLD and tp_enabled:
                print(f"🏆 Closing {symbol} — Profit target hit ({percent_change:.2f}%)")
                client.close_position(symbol)
                send_telegram_alert(f"🏆 {symbol} closed at +{percent_change:.2f}% profit")
                action = "take_profit"
                exit_reason = "tp_threshold"

            # 📉 Auto-close if loss exceeds threshold
            elif percent_change <= STOP_LOSS_THRESHOLD:
                print(f"❌ Closing {symbol} — Stop loss hit ({percent_change:.2f}%)")
                client.close_position(symbol)
                send_telegram_alert(f"❌ Auto-closed {symbol} due to loss ({percent_change:.2f}%)")
                action = "stop_loss"
                exit_reason = "sl_threshold"

            now = datetime.utcnow()

            _append_decision_log(
                {
                    "timestamp": now.isoformat(),
                    "weekday_utc": now.weekday(),
                    "hour_utc": now.hour,
                    "symbol": symbol,
                    "action": action,
                    "exit_reason": exit_reason,
                    "qty": qty,
                    "entry_price": entry_price,
                    "current_price": current_price,
                    "percent_change": percent_change,
                    "profit_usd": profit_usd,
                    "take_profit_threshold": TAKE_PROFIT_THRESHOLD,
                    "stop_loss_threshold": STOP_LOSS_THRESHOLD,
                    "paper_mode": PAPER_MODE,
                    "risk_off": risk_off,
                    "risk_tag": risk_tag or "",
                }
            )

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
