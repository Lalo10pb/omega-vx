from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Iterable, Tuple


BASE_DIR = Path(__file__).resolve().parent
TRADE_LOG = BASE_DIR / "logs" / "trade_log.csv"
FILLED_TRADES = BASE_DIR / "filled_trades.csv"
FILLED_HEADER = [
    "timestamp",
    "symbol",
    "qty",
    "entry_price",
    "exit_price",
    "P/L $",
    "P/L %",
    "exit_reason",
    "activity_id",
]


def _to_float(value) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def _load_existing_keys() -> set[Tuple[str, str, float]]:
    keys: set[Tuple[str, str, float]] = set()
    if not FILLED_TRADES.exists():
        return keys
    with open(FILLED_TRADES, newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            try:
                keys.add(
                    (
                        row.get("timestamp", ""),
                        row.get("symbol", "").upper(),
                        float(row.get("exit_price") or 0),
                    )
                )
            except Exception:
                continue
    return keys


def _yield_sell_rows() -> Iterable[dict]:
    if not TRADE_LOG.exists():
        raise FileNotFoundError(f"{TRADE_LOG} not found.")
    with open(TRADE_LOG, newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            status = (row.get("status") or "").lower()
            action = (row.get("action") or "").lower()
            realized = row.get("realized_pnl")
            if action == "sell" or status in {"closed", "sell"} or realized not in ("", None):
                yield row


def _compute_row(row: dict) -> dict | None:
    timestamp = row.get("timestamp") or datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    symbol = (row.get("symbol") or "").upper()
    if not symbol:
        return None
    qty = _to_float(row.get("qty")) or 0.0
    entry = _to_float(row.get("entry"))
    exit_price = _to_float(row.get("fill_price")) or _to_float(row.get("take_profit")) or entry
    realized = _to_float(row.get("realized_pnl"))

    if realized is None and None not in (entry, exit_price):
        realized = (exit_price - entry) * qty
    pnl_percent = 0.0
    if entry not in (None, 0) and exit_price is not None:
        pnl_percent = ((exit_price - entry) / entry) * 100

    return {
        "timestamp": timestamp,
        "symbol": symbol,
        "qty": qty,
        "entry_price": "" if entry is None else round(entry, 4),
        "exit_price": "" if exit_price is None else round(exit_price, 4),
        "P/L $": 0.0 if realized is None else round(realized, 2),
        "P/L %": round(pnl_percent, 2),
        "exit_reason": row.get("close_reason") or row.get("hold_reason") or row.get("status") or "SELL",
        "activity_id": "",
    }


def backfill() -> int:
    existing_keys = _load_existing_keys()
    new_rows = []
    for row in _yield_sell_rows():
        normalized = _compute_row(row)
        if not normalized:
            continue
        key = (
            normalized["timestamp"],
            normalized["symbol"],
            float(normalized.get("exit_price") or 0.0),
        )
        if key in existing_keys:
            continue
        existing_keys.add(key)
        new_rows.append(normalized)

    if not new_rows:
        return 0

    file_exists = FILLED_TRADES.exists()
    with open(FILLED_TRADES, mode="a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FILLED_HEADER)
        if not file_exists:
            writer.writeheader()
        for row in new_rows:
            writer.writerow(row)
    return len(new_rows)


if __name__ == "__main__":
    added = backfill()
    print(f"✅ Backfill complete. Added {added} rows to {FILLED_TRADES}.")
