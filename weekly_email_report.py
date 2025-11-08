from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict

import pandas as pd

from omega_vx.notifications import send_email


BASE_DIR = Path(__file__).resolve().parent
FILLED_TRADES = BASE_DIR / "filled_trades.csv"


def _empty_stats() -> Dict[str, float]:
    return {
        "total": 0,
        "wins": 0,
        "losses": 0,
        "breakeven": 0,
        "win_rate": 0.0,
        "net_pl": 0.0,
        "avg_return_pct": 0.0,
        "start": (datetime.now() - timedelta(days=7)).date(),
        "end": datetime.now().date(),
    }


def analyze_weekly_trades(days: int = 7) -> Dict[str, float]:
    if not FILLED_TRADES.exists():
        return _empty_stats()

    try:
        df = pd.read_csv(FILLED_TRADES)
    except Exception as exc:
        print(f"⚠️ Failed to read filled trades: {exc}")
        return _empty_stats()

    if df.empty or "timestamp" not in df.columns:
        return _empty_stats()

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])

    end = datetime.now()
    start = end - timedelta(days=days)
    window = df[df["timestamp"] >= start]

    if window.empty:
        stats = _empty_stats()
        stats["start"] = start.date()
        stats["end"] = end.date()
        return stats

    pnl_dollars = pd.to_numeric(window.get("P/L $", 0), errors="coerce").fillna(0)
    pnl_percent = pd.to_numeric(window.get("P/L %", 0), errors="coerce").fillna(0)

    wins = int((pnl_dollars > 0).sum())
    losses = int((pnl_dollars < 0).sum())
    total = len(window)
    breakeven = max(total - wins - losses, 0)
    win_rate = (wins / total) * 100 if total else 0.0
    net_pl = round(float(pnl_dollars.sum()), 2)
    avg_return_pct = round(float(pnl_percent.mean()), 2) if total else 0.0

    return {
        "total": total,
        "wins": wins,
        "losses": losses,
        "breakeven": breakeven,
        "win_rate": win_rate,
        "net_pl": net_pl,
        "avg_return_pct": avg_return_pct,
        "start": start.date(),
        "end": end.date(),
    }


def send_weekly_report() -> None:
    stats = analyze_weekly_trades()
    subject = "📊 OMEGA-VX Weekly Summary"
    body = f"""
📆 Weekly Trade Summary ({stats['start']} → {stats['end']})

🔢 Total Trades: {stats['total']}
✅ Wins: {stats['wins']}
❌ Losses: {stats['losses']}
➖ Breakeven: {stats['breakeven']}
🏆 Win Rate: {stats['win_rate']:.2f}%
💰 Net P/L: ${stats['net_pl']:.2f}
📈 Avg Return/Trade: {stats['avg_return_pct']:.2f}%

– OMEGA-VX
"""
    send_email(subject, body)


if __name__ == "__main__":
    send_weekly_report()
