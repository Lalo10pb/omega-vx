from omega_vx_bot import send_email
import pandas as pd
import os
from datetime import datetime

CSV_FILE = "portfolio_log.csv"

def generate_daily_report():
    if not os.path.exists(CSV_FILE):
        return "❌ No portfolio log data found."

    df = pd.read_csv(CSV_FILE)
    if df.empty:
        return "📭 No portfolio data yet."

    latest = df.iloc[-1]
    timestamp = latest['timestamp']
    equity = float(latest['equity'])
    cash = float(latest['cash'])
    portfolio = float(latest['portfolio_value'])

    message = (
        f"📊 *OMEGA-VX Daily Snapshot*\n\n"
        f"🗓️ Date: {timestamp}\n"
        f"💼 Portfolio Value: ${portfolio:,.2f}\n"
        f"💵 Cash: ${cash:,.2f}\n"
        f"📈 Equity: ${equity:,.2f}\n"
    )
    return message

if __name__ == "__main__":
    body = generate_daily_report()
    send_email("OMEGA-VX Daily Report", body)
