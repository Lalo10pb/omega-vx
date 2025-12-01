"""
Offline threshold tuner and dataset builder for paper trades.
- Reads decision logs produced by position_monitor.py
- Writes a training-ready CSV with labels
- Emits a JSON suggestion for updated take-profit / stop-loss thresholds
"""

import argparse
import csv
import json
import os
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import List, Dict, Any, Optional, Tuple


DEFAULT_LOG = Path("logs/decision_log.csv")
DEFAULT_DATASET = Path("data/training_dataset.csv")
DEFAULT_SUGGESTIONS = Path("logs/threshold_suggestions.json")


def _quantile(values: List[float], q: float) -> Optional[float]:
    """Compute a simple quantile without external deps."""
    if not values:
        return None
    if q <= 0:
        return min(values)
    if q >= 1:
        return max(values)
    sorted_vals = sorted(values)
    pos = (len(sorted_vals) - 1) * q
    lower = int(pos)
    upper = min(lower + 1, len(sorted_vals) - 1)
    weight = pos - lower
    return sorted_vals[lower] * (1 - weight) + sorted_vals[upper] * weight


def load_log(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"Log not found: {path}")
    rows: List[Dict[str, Any]] = []
    with path.open() as f:
        reader = csv.DictReader(f)
        for r in reader:
            try:
                rows.append(
                    {
                        "timestamp": r.get("timestamp"),
                        "symbol": r.get("symbol"),
                        "action": r.get("action"),
                        "qty": float(r.get("qty", 0) or 0),
                        "entry_price": float(r.get("entry_price", 0) or 0),
                        "current_price": float(r.get("current_price", 0) or 0),
                        "percent_change": float(r.get("percent_change", 0) or 0),
                        "profit_usd": float(r.get("profit_usd", 0) or 0),
                        "take_profit_threshold": float(r.get("take_profit_threshold", 0) or 0),
                        "stop_loss_threshold": float(r.get("stop_loss_threshold", 0) or 0),
                        "paper_mode": str(r.get("paper_mode", "true")).lower() in ("1", "true", "yes"),
                    }
                )
            except Exception:
                # Skip malformed rows so a single bad entry does not halt the job.
                continue
    return rows


def build_dataset(rows: List[Dict[str, Any]], path: Path) -> None:
    """Create a labeled CSV for downstream training."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "timestamp",
        "symbol",
        "action",
        "qty",
        "entry_price",
        "current_price",
        "percent_change",
        "profit_usd",
        "label_good_entry",
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for r in rows:
            if r["action"] not in ("take_profit", "stop_loss"):
                continue  # only label closed decisions
            label = 1 if r["profit_usd"] > 0 else 0
            writer.writerow(
                {
                    "timestamp": r["timestamp"],
                    "symbol": r["symbol"],
                    "action": r["action"],
                    "qty": r["qty"],
                    "entry_price": r["entry_price"],
                    "current_price": r["current_price"],
                    "percent_change": r["percent_change"],
                    "profit_usd": r["profit_usd"],
                    "label_good_entry": label,
                }
            )


def evaluate_policy(rows: List[Dict[str, Any]], tp: float, sl: float) -> Dict[str, Any]:
    """
    Approximate policy eval: use closed trades and ask whether the recorded P/L
    would be classified as TP/SL given the candidate thresholds.
    """
    closed = [r for r in rows if r["action"] in ("take_profit", "stop_loss")]
    if not closed:
        return {"trades": 0}
    considered = []
    for r in closed:
        pct = r["percent_change"]
        action = "hold"
        if pct >= tp:
            action = "take_profit"
        elif pct <= sl:
            action = "stop_loss"
        considered.append((action, r))

    realized = [r for action, r in considered if action in ("take_profit", "stop_loss")]
    if not realized:
        return {"trades": 0}

    profits = [r["profit_usd"] for r in realized]
    pct_changes = [r["percent_change"] for r in realized]
    wins = sum(1 for p in profits if p > 0)
    avg_profit = sum(profits) / len(profits)
    avg_pct = sum(pct_changes) / len(pct_changes)
    return {
        "trades": len(realized),
        "wins": wins,
        "win_rate": round(wins / len(realized), 4),
        "avg_profit_usd": round(avg_profit, 4),
        "avg_profit_pct": round(avg_pct, 4),
        "tp": tp,
        "sl": sl,
    }


def candidate_grid(positives: List[float], negatives: List[float]) -> List[Tuple[float, float]]:
    base_tp = [2, 3, 4, 5, 6, 8, 10]
    base_sl = [-1, -2, -3, -4, -5, -7]
    if positives:
        base_tp.append(round(_quantile(positives, 0.7), 3))
        base_tp.append(round(_quantile(positives, 0.85), 3))
    if negatives:
        base_sl.append(round(_quantile(negatives, 0.3), 3))
        base_sl.append(round(_quantile(negatives, 0.15), 3))
    grid = []
    for tp in sorted({v for v in base_tp if v}):
        for sl in sorted({v for v in base_sl if v}):
            grid.append((tp, sl))
    return grid


def suggest_thresholds(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    closed = [r for r in rows if r["action"] in ("take_profit", "stop_loss")]
    if not closed:
        return {"error": "No closed trades to learn from yet."}

    pct_changes = [r["percent_change"] for r in closed]
    profits = [r["profit_usd"] for r in closed]
    positives = [p for p in pct_changes if p > 0]
    negatives = [p for p in pct_changes if p < 0]

    suggested_take_profit = _quantile(positives, 0.7) if positives else None
    suggested_stop_loss = _quantile(negatives, 0.3) if negatives else None

    stats = {
        "trades_considered": len(closed),
        "win_rate": round(sum(1 for p in profits if p > 0) / len(profits), 4),
        "avg_profit_pct": round(sum(pct_changes) / len(pct_changes), 4),
        "median_profit_pct": round(median(pct_changes), 4),
        "max_profit_pct": round(max(pct_changes), 4),
        "min_profit_pct": round(min(pct_changes), 4),
    }

    leaderboard = []
    for tp, sl in candidate_grid(positives, negatives):
        res = evaluate_policy(closed, tp, sl)
        if res.get("trades", 0) == 0:
            continue
        leaderboard.append(res)
    leaderboard = sorted(leaderboard, key=lambda r: (r.get("avg_profit_usd", 0), r.get("win_rate", 0)), reverse=True)

    best = leaderboard[0] if leaderboard else None

    drift_alert = None
    min_trades = 5
    min_win = float(os.getenv("TUNER_MIN_WIN_RATE", 0.45))
    if stats["trades_considered"] >= min_trades and stats["win_rate"] < min_win:
        drift_alert = f"Win rate {stats['win_rate']} below guard {min_win}; pause promotions."

    suggestion = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "suggested_take_profit_pct": round(suggested_take_profit, 3) if suggested_take_profit else None,
        "suggested_stop_loss_pct": round(suggested_stop_loss, 3) if suggested_stop_loss else None,
        "reference_trades": stats,
        "leaderboard": leaderboard[:10],
        "best_candidate": best,
        "drift_alert": drift_alert,
        "note": "These are based on observed exits; validate with backtests before applying.",
    }

    # Canary recommendation: start with smaller size and limited symbols if provided
    canary_symbols = os.getenv("CANARY_SYMBOLS")
    suggestion["canary_recommendation"] = {
        "size_multiplier": 0.5,
        "symbols": [s.strip() for s in canary_symbols.split(",")] if canary_symbols else [],
        "comment": "Run candidate thresholds on reduced size first; compare vs control before full rollout.",
    }
    return suggestion


def write_suggestions(suggestion: Dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump(suggestion, f, indent=2)


def main():
    parser = argparse.ArgumentParser(description="Offline tuner for TP/SL thresholds.")
    parser.add_argument("--log", type=Path, default=DEFAULT_LOG, help="Path to decision_log.csv")
    parser.add_argument("--dataset", type=Path, default=DEFAULT_DATASET, help="Where to write labeled dataset CSV")
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_SUGGESTIONS,
        help="Where to write threshold_suggestions.json",
    )
    args = parser.parse_args()

    rows = load_log(args.log)
    build_dataset(rows, args.dataset)
    suggestion = suggest_thresholds(rows)
    write_suggestions(suggestion, args.out)

    print(f"Wrote labeled dataset to {args.dataset}")
    print(f"Wrote threshold suggestions to {args.out}")
    if "error" in suggestion:
        print(f"Suggestion warning: {suggestion['error']}")
    else:
        print(f"Suggested take-profit: {suggestion['suggested_take_profit_pct']}%")
        print(f"Suggested stop-loss: {suggestion['suggested_stop_loss_pct']}%")


if __name__ == "__main__":
    main()
