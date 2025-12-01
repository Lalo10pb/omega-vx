"""
Offline threshold tuner and dataset builder for paper trades.
- Reads decision logs produced by position_monitor.py
- Writes a training-ready CSV with labels
- Emits a JSON suggestion for updated take-profit / stop-loss thresholds
"""

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import List, Dict, Any, Optional


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

    suggestion = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "suggested_take_profit_pct": round(suggested_take_profit, 3) if suggested_take_profit else None,
        "suggested_stop_loss_pct": round(suggested_stop_loss, 3) if suggested_stop_loss else None,
        "reference_trades": stats,
        "note": "These are based on observed exits; validate with backtests before applying.",
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
