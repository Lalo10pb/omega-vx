# daily_email_report.py
import os
from datetime import datetime

import pandas as pd

from omega_vx import config as omega_config
from omega_vx.clients import get_trading_client
from omega_vx.notifications import send_email
from account_logging import AccountSnapshot, iter_portfolio_rows

CSV_FILE = os.path.expanduser("~/omega-vx/logs/portfolio_log.csv")

def get_live_account_snapshot():
    c = get_trading_client()
    paper_mode = omega_config.get_bool("ALPACA_PAPER", "1")
    a = c.get_account()
    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "equity": float(a.equity),
        "cash": float(a.cash),
        "portfolio_value": float(a.portfolio_value),
        "mode": "PAPER" if paper_mode else "LIVE",
    }


def _snapshot_from_dict(payload: dict) -> AccountSnapshot:
    return AccountSnapshot(
        timestamp=payload["timestamp"],
        equity=float(payload["equity"]),
        cash=float(payload["cash"]),
        portfolio_value=float(payload["portfolio_value"]),
    )


def _latest_history_pair() -> tuple[AccountSnapshot | None, AccountSnapshot | None]:
    rows = list(iter_portfolio_rows(limit=2))
    if not rows:
        return None, None
    if len(rows) == 1:
        return rows[0], None
    return rows[-1], rows[-2]


def _delta_line(current: AccountSnapshot, previous: AccountSnapshot | None) -> str:
    if not previous:
        return ""
    delta = current.portfolio_value - previous.portfolio_value
    if abs(delta) < 0.01:
        emoji = "➖"
    elif delta > 0:
        emoji = "🔺"
    else:
        emoji = "🔻"
    return f"{emoji} 24h Δ: ${delta:,.2f}\n"

def generate_daily_report():
    # Try live first
    try:
        snap = get_live_account_snapshot()
        current = _snapshot_from_dict(snap)
        history_latest, history_prev = _latest_history_pair()
        prev_for_delta = history_latest if history_latest and history_latest.timestamp != current.timestamp else history_prev
        delta = _delta_line(current, prev_for_delta)
        return (
            f"📊 *OMEGA-VX Daily Snapshot* ({snap['mode']})\n\n"
            f"🗓️ Date: {snap['timestamp']}\n"
            f"💼 Portfolio Value: ${snap['portfolio_value']:,.2f}\n"
            f"💵 Cash: ${snap['cash']:,.2f}\n"
            f"📈 Equity: ${snap['equity']:,.2f}\n"
            f"{delta}"
        )
    except Exception as e:
        # Fallback to CSV
        if not os.path.exists(CSV_FILE):
            return f"❌ Live fetch failed ({e}) and no portfolio log found."
        df = pd.read_csv(CSV_FILE)
        if df.empty:
            return "📭 No portfolio data yet."
        latest = df.iloc[-1]
        prev_row = df.iloc[-2] if len(df.index) > 1 else None
        current = AccountSnapshot(
            timestamp=str(latest["timestamp"]),
            equity=float(latest["equity"]),
            cash=float(latest["cash"]),
            portfolio_value=float(latest["portfolio_value"]),
        )
        prev = (
            AccountSnapshot(
                timestamp=str(prev_row["timestamp"]),
                equity=float(prev_row["equity"]),
                cash=float(prev_row["cash"]),
                portfolio_value=float(prev_row["portfolio_value"]),
            )
            if prev_row is not None
            else None
        )
        delta = _delta_line(current, prev)
        return (
            "📊 *OMEGA-VX Daily Snapshot* (CSV fallback)\n\n"
            f"🗓️ Date: {latest['timestamp']}\n"
            f"💼 Portfolio Value: ${float(latest['portfolio_value']):,.2f}\n"
            f"💵 Cash: ${float(latest['cash']):,.2f}\n"
            f"📈 Equity: ${float(latest['equity']):,.2f}\n"
            f"{delta}"
        )

if __name__ == "__main__":
    body = generate_daily_report()
    send_email("OMEGA-VX Daily Report", body)
