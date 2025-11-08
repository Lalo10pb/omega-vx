"""
historical_performance_report.py

Aggregate weekly, monthly, and yearly performance statistics, append them
to logs/performance_history.csv, and email the latest snapshot.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from omega_vx.notifications import send_email


BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
TRADE_LOG = LOG_DIR / "trade_log.csv"
FILLED_TRADES = BASE_DIR / "filled_trades.csv"
PORTFOLIO_SNAPSHOTS = BASE_DIR / "portfolio_snapshots.csv"
HISTORY_FILE = LOG_DIR / "performance_history.csv"
PERIOD_DEFINITIONS = [
    ("weekly", 7),
    ("monthly", 30),
    ("yearly", 365),
]

NOW = datetime.now()


def _load_csv(path: Path, timestamp_col: str) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if not df.size or timestamp_col not in df.columns:
        return pd.DataFrame()
    df[timestamp_col] = pd.to_datetime(df[timestamp_col])
    return df.sort_values(timestamp_col).reset_index(drop=True)


def _load_trade_history() -> pd.DataFrame:
    """Prefer realized fills; fall back to legacy trade log."""
    filled = _load_csv(FILLED_TRADES, "timestamp")
    if not filled.empty:
        df = filled.rename(
            columns={
                "entry_price": "entry",
                "exit_price": "exit",
                "P/L $": "pnl_dollars",
                "P/L %": "pnl_percent",
            }
        )
        for col in ("pnl_dollars", "pnl_percent", "entry", "exit"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df["status"] = "executed"
        return df

    return _load_csv(TRADE_LOG, "timestamp")


def _trade_metrics(
    trades: pd.DataFrame,
    start: datetime,
    end: datetime,
) -> Dict[str, Any]:
    if trades.empty:
        return {
            "total_trades": 0,
            "executed_trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0.0,
            "avg_rr": 0.0,
            "est_gain_score": 0.0,
            "total_pnl_dollars": 0.0,
            "avg_return_pct": 0.0,
        }

    window = trades[(trades["timestamp"] >= start) & (trades["timestamp"] <= end)]
    if "status" in window.columns:
        executed = window[window["status"].astype(str).str.lower() == "executed"]
    else:
        executed = window

    wins = 0
    losses = 0
    rr_values: list[float] = []
    est_gain = 0.0
    total_pnl = 0.0
    avg_return_pct = 0.0

    pnl_available = "pnl_dollars" in executed.columns

    if pnl_available:
        pnl_values = pd.to_numeric(executed["pnl_dollars"], errors="coerce").fillna(0)
        pct_values = (
            pd.to_numeric(executed.get("pnl_percent", 0), errors="coerce").fillna(0)
        )
        wins = int((pnl_values > 0).sum())
        losses = int((pnl_values < 0).sum())
        total_pnl = round(float(pnl_values.sum()), 2)
        avg_return_pct = round(float(pct_values.mean()), 2) if len(pct_values) else 0.0
    else:
        for _, row in executed.iterrows():
            try:
                entry = float(row["entry"])
                stop_loss = float(row["stop_loss"])
                take_profit = float(row["take_profit"])
            except Exception:
                continue

            risk = abs(entry - stop_loss)
            if risk == 0:
                continue

            rr = abs(take_profit - entry) / risk
            rr_values.append(rr)
            if rr >= 1.0:
                wins += 1
                est_gain += rr
            else:
                losses += 1
                est_gain -= 1

    executed_count = len(executed)
    win_rate = (wins / executed_count) * 100 if executed_count else 0.0
    avg_rr = sum(rr_values) / len(rr_values) if rr_values else 0.0

    return {
        "total_trades": len(window),
        "executed_trades": executed_count,
        "wins": wins,
        "losses": losses,
        "win_rate": round(win_rate, 2),
        "avg_rr": round(avg_rr, 2),
        "est_gain_score": round(est_gain, 2),
        "total_pnl_dollars": total_pnl,
        "avg_return_pct": avg_return_pct,
    }


def _portfolio_return(
    snapshots: pd.DataFrame,
    start: datetime,
    end: datetime,
) -> Optional[float]:
    if snapshots.empty:
        return None

    window = snapshots[(snapshots["timestamp"] >= start) & (snapshots["timestamp"] <= end)]
    if len(window) < 2 or "portfolio_value" not in window.columns:
        return None

    start_value = float(window.iloc[0]["portfolio_value"])
    end_value = float(window.iloc[-1]["portfolio_value"])
    if start_value == 0:
        return None

    return round(((end_value - start_value) / start_value) * 100, 2)


def _period_summary(
    label: str,
    days: int,
    trades: pd.DataFrame,
    snapshots: pd.DataFrame,
) -> Dict[str, Any]:
    start = NOW - timedelta(days=days)
    metrics = _trade_metrics(trades, start, NOW)
    portfolio_return = _portfolio_return(snapshots, start, NOW)

    summary = {
        "generated_at": NOW.strftime("%Y-%m-%d %H:%M:%S"),
        "period": label,
        "start_date": start.strftime("%Y-%m-%d"),
        "end_date": NOW.strftime("%Y-%m-%d"),
        **metrics,
        "portfolio_return_pct": portfolio_return if portfolio_return is not None else "",
    }
    return summary


def _append_history(rows: list[Dict[str, Any]]) -> None:
    if not rows:
        return
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    header = not HISTORY_FILE.exists()
    df.to_csv(HISTORY_FILE, mode="a", header=header, index=False)


def _email_body(summaries: list[Dict[str, Any]]) -> str:
    lines = ["📊 OMEGA-VX Historical Performance Update", f"Generated: {NOW:%Y-%m-%d %H:%M:%S}", ""]
    for summary in summaries:
        portfolio_line = (
            f"{summary['portfolio_return_pct']}%"
            if summary["portfolio_return_pct"] != ""
            else "N/A"
        )
        net_pl = summary.get("total_pnl_dollars", 0.0)
        avg_return = summary.get("avg_return_pct", 0.0)
        lines.extend(
            [
                f"🗓️ {summary['period'].title()} ({summary['start_date']} → {summary['end_date']}):",
                f"  • Trades (total/executed): {summary['total_trades']} / {summary['executed_trades']}",
                f"  • Wins / Losses: {summary['wins']} / {summary['losses']}",
                f"  • Win rate: {summary['win_rate']}% | Avg RR: {summary['avg_rr']}",
                f"  • Net P/L: ${net_pl:.2f} | Avg return/trade: {avg_return:.2f}%",
                f"  • Portfolio return: {portfolio_line}",
                "",
            ]
        )
    lines.append(f"History file: {HISTORY_FILE}")
    return "\n".join(lines)


def main() -> None:
    trades = _load_trade_history()
    snapshots = _load_csv(PORTFOLIO_SNAPSHOTS, "timestamp")
    summaries = [
        _period_summary(label, days, trades, snapshots) for label, days in PERIOD_DEFINITIONS
    ]
    _append_history(summaries)
    send_email("📊 OMEGA-VX Weekly / Monthly / Yearly Performance", _email_body(summaries))


if __name__ == "__main__":
    main()
