"""
sync_alpaca_fills.py

Fetch fill activities from Alpaca, reconstruct realized P/L using FIFO cost basis,
and append any missing SELL fills to filled_trades.csv. Persists state so it can
run incrementally as a cron/Render job.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import requests
from dateutil import parser as date_parser

from omega_vx import config as omega_config


BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"
omega_config.load_environment(ENV_PATH)

LOG_DIR = Path(omega_config.LOG_DIR)
FILLED_TRADES = BASE_DIR / "filled_trades.csv"
STATE_FILE = LOG_DIR / "fill_sync_state.json"

APCA_API_KEY_ID = omega_config.get_env("APCA_API_KEY_ID")
APCA_API_SECRET_KEY = omega_config.get_env("APCA_API_SECRET_KEY")
APCA_API_BASE_URL = omega_config.get_env("APCA_API_BASE_URL") or "https://paper-api.alpaca.markets"

HEADERS = {
    "APCA-API-KEY-ID": APCA_API_KEY_ID or "",
    "APCA-API-SECRET-KEY": APCA_API_SECRET_KEY or "",
}

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

EPS = 1e-6


def _isoformat(dt: datetime) -> str:
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_ts(value: str) -> datetime:
    dt = date_parser.isoparse(value)
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _load_state(rebuild: bool) -> Dict:
    if rebuild or not STATE_FILE.exists():
        return {"last_transaction_time": None, "inventory": {}}
    with open(STATE_FILE, "r") as handle:
        return json.load(handle)


def _save_state(state: Dict) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w") as handle:
        json.dump(state, handle, indent=2)


def _load_existing_records() -> Tuple[set, List[dict]]:
    ids: set[str] = set()
    rows: List[dict] = []
    if not FILLED_TRADES.exists():
        return ids, rows
    with open(FILLED_TRADES, newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            activity_id = (row.get("activity_id") or "").strip()
            if activity_id:
                ids.add(activity_id)
            ts_raw = row.get("timestamp")
            try:
                timestamp = datetime.strptime(ts_raw, "%Y-%m-%d %H:%M:%S")
            except Exception:
                try:
                    timestamp = datetime.fromisoformat(ts_raw)
                except Exception:
                    timestamp = None
            exit_price = row.get("exit_price")
            try:
                exit_price = float(exit_price)
            except Exception:
                exit_price = None
            rows.append(
                {
                    "symbol": (row.get("symbol") or "").upper(),
                    "timestamp": timestamp,
                    "exit_price": exit_price,
                }
            )
    return ids, rows


def _determine_after(state: Dict, lookback_days: int) -> datetime:
    last_ts = state.get("last_transaction_time")
    if last_ts:
        try:
            dt = datetime.fromisoformat(last_ts)
            return dt - timedelta(seconds=1)
        except Exception:
            pass
    return datetime.utcnow() - timedelta(days=lookback_days)


def _fetch_fill_activities(after: datetime) -> List[dict]:
    if not APCA_API_KEY_ID or not APCA_API_SECRET_KEY:
        raise RuntimeError("Missing Alpaca API credentials.")

    url = f"{APCA_API_BASE_URL.rstrip('/')}/v2/account/activities/FILL"
    params = {
        "direction": "asc",
        "page_size": 100,
        "after": _isoformat(after),
    }
    activities: List[dict] = []
    while True:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=15)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        activities.extend(batch)
        page_token = resp.headers.get("x-next-page-token")
        if not page_token:
            break
        params["page_token"] = page_token
        params.pop("after", None)
    activities.sort(key=lambda a: a.get("transaction_time") or "")
    return activities


def _consume_lots(lots: List[dict], qty: float, exit_price: float) -> Tuple[float, float, float]:
    realized = 0.0
    entry_total = 0.0
    matched = 0.0
    while qty > EPS and lots:
        lot = lots[0]
        take = min(qty, lot["qty"])
        realized += (exit_price - lot["price"]) * take
        entry_total += lot["price"] * take
        matched += take
        lot["qty"] -= take
        if lot["qty"] <= EPS:
            lots.pop(0)
        qty -= take
    return realized, entry_total, matched


def _inventory_from_state(state: Dict) -> Dict[str, List[dict]]:
    inventory = defaultdict(list)
    for symbol, lots in state.get("inventory", {}).items():
        for lot in lots:
            qty = float(lot.get("qty") or 0)
            price = float(lot.get("price") or 0)
            if qty > EPS:
                inventory[symbol.upper()].append({"qty": qty, "price": price})
    return inventory


def _serialize_inventory(inventory: Dict[str, List[dict]]) -> Dict[str, List[dict]]:
    result = {}
    for symbol, lots in inventory.items():
        cleaned = [{"qty": round(lot["qty"], 6), "price": round(lot["price"], 6)} for lot in lots if lot["qty"] > EPS]
        if cleaned:
            result[symbol] = cleaned
    return result


def _is_duplicate(candidate: dict, existing_ids: set[str], existing_rows: List[dict]) -> bool:
    activity_id = candidate.get("activity_id")
    if activity_id and activity_id in existing_ids:
        return True
    symbol = candidate["symbol"]
    exit_price = candidate.get("exit_price")
    timestamp = datetime.strptime(candidate["timestamp"], "%Y-%m-%d %H:%M:%S")
    for row in existing_rows:
        if row["symbol"] != symbol:
            continue
        if row["timestamp"] and abs((timestamp - row["timestamp"]).total_seconds()) > 180:
            continue
        if row["exit_price"] is not None and exit_price is not None:
            if abs(row["exit_price"] - exit_price) > 0.01:
                continue
        return True
    return False


def _append_rows(rows: List[dict]) -> None:
    if not rows:
        return
    file_exists = FILLED_TRADES.exists()
    with open(FILLED_TRADES, mode="a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FILLED_HEADER)
        if not file_exists:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)


def sync_fills(lookback_days: int, dry_run: bool, rebuild_state: bool) -> int:
    state = _load_state(rebuild_state)
    inventory = _inventory_from_state(state)
    existing_ids, existing_rows = _load_existing_records()
    after = _determine_after(state, lookback_days)
    activities = _fetch_fill_activities(after)
    if not activities:
        print("ℹ️ No fill activities returned.")
        return 0

    rows_to_append: List[dict] = []
    last_processed_time = state.get("last_transaction_time")

    for activity in activities:
        try:
            txn_time = _parse_ts(activity["transaction_time"])
        except Exception:
            continue
        last_processed_time = txn_time.isoformat()
        side = (activity.get("side") or "").lower()
        symbol = (activity.get("symbol") or "").upper()
        if not symbol or side not in {"buy", "sell"}:
            continue
        qty = float(activity.get("qty") or 0)
        price = float(activity.get("price") or 0)
        if qty <= EPS:
            continue
        lots = inventory[symbol]
        if side == "buy":
            lots.append({"qty": qty, "price": price})
            inventory[symbol] = lots
            continue

        realized, entry_total, matched = _consume_lots(lots, qty, price)
        if matched <= EPS:
            entry_price = price
            pnl_pct = 0.0
        else:
            entry_price = entry_total / matched
            pnl_pct = ((price - entry_price) / entry_price) * 100 if entry_price else 0.0
        row = {
            "timestamp": txn_time.strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": symbol,
            "qty": round(matched or qty, 6),
            "entry_price": round(entry_price, 4) if entry_price else "",
            "exit_price": round(price, 4),
            "P/L $": round(realized, 2),
            "P/L %": round(pnl_pct, 2),
            "exit_reason": "alpaca_fill",
            "activity_id": activity.get("id") or "",
        }
        if not _is_duplicate(row, existing_ids, existing_rows):
            rows_to_append.append(row)
            existing_rows.append(
                {
                    "symbol": symbol,
                    "timestamp": datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S"),
                    "exit_price": round(price, 4),
                }
            )
            if row["activity_id"]:
                existing_ids.add(row["activity_id"])

    if dry_run:
        print(f"📝 Dry run: {len(rows_to_append)} rows would be added.")
    else:
        _append_rows(rows_to_append)
        state["inventory"] = _serialize_inventory(inventory)
        state["last_transaction_time"] = last_processed_time
        _save_state(state)
        print(f"✅ Sync complete. Added {len(rows_to_append)} rows.")
    return len(rows_to_append)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Alpaca fill activities into filled_trades.csv")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=int(os.getenv("FILL_SYNC_LOOKBACK_DAYS", "30")),
        help="Days to look back when no prior state exists (default: 30).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Compute rows but do not modify files.")
    parser.add_argument("--rebuild-state", action="store_true", help="Ignore saved state and rebuild from lookback window.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    sync_fills(args.lookback_days, args.dry_run, args.rebuild_state)
