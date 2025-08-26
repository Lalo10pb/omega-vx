# daily_email_report.py
from omega_vx_bot import send_email
import pandas as pd
import os
from datetime import datetime
from alpaca.trading.client import TradingClient

CSV_FILE = os.path.expanduser("~/omega-vx/logs/portfolio_log.csv")

def get_live_account_snapshot():
    paper = str(os.getenv("ALPACA_PAPER", "true")).strip().lower() in ("1","true","yes")
    c = TradingClient(os.getenv("APCA_API_KEY_ID"),
                      os.getenv("APCA_API_SECRET_KEY"),
                      paper=paper)
    a = c.get_account()
    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "equity": float(a.equity),
        "cash": float(a.cash),
        "portfolio_value": float(a.portfolio_value),
        "mode": "PAPER" if paper else "LIVE",
    }

def generate_daily_report():
    # Try live first
    try:
        snap = get_live_account_snapshot()
        return (
            f"📊 *OMEGA-VX Daily Snapshot* ({snap['mode']})\n\n"
            f"🗓️ Date: {snap['timestamp']}\n"
            f"💼 Portfolio Value: ${snap['portfolio_value']:,.2f}\n"
            f"💵 Cash: ${snap['cash']:,.2f}\n"
            f"📈 Equity: ${snap['equity']:,.2f}\n"
        )
    except Exception as e:
        # Fallback to CSV
        if not os.path.exists(CSV_FILE):
            return f"❌ Live fetch failed ({e}) and no portfolio log found."
        df = pd.read_csv(CSV_FILE)
        if df.empty:
            return "📭 No portfolio data yet."
        latest = df.iloc[-1]
        return (
            "📊 *OMEGA-VX Daily Snapshot* (CSV fallback)\n\n"
            f"🗓️ Date: {latest['timestamp']}\n"
            f"💼 Portfolio Value: ${float(latest['portfolio_value']):,.2f}\n"
            f"💵 Cash: ${float(latest['cash']):,.2f}\n"
            f"📈 Equity: ${float(latest['equity']):,.2f}\n"
        )

if __name__ == "__main__":
    body = generate_daily_report()
    send_email("OMEGA-VX Daily Report", body)