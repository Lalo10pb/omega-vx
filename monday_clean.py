#!/usr/bin/env python3
"""
monday_clean.py
- Cancel all OPEN orders (handles pending_cancel by polling)
- Optionally close positions EXCEPT a keep/watchlist
- Defaults to DRY-RUN (no trading) unless --live is passed

Usage:
  python3 monday_clean.py                   # dry-run
  python3 monday_clean.py --live            # execute actions
  python3 monday_clean.py --live --keep AAPL,TSLA,NVDA
"""

import os, sys, time
from dotenv import load_dotenv
from typing import Set, List

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetOrdersRequest
from alpaca.trading.enums import QueryOrderStatus, OrderSide, TimeInForce
from alpaca.trading.requests import MarketOrderRequest


# ---------- Config (safe defaults) ----------
POLL_SECONDS = 2
MAX_POLLS = 120           # ~4 minutes total wait for pending_cancel to clear
DEFAULT_KEEP = {"AAPL", "TSLA", "NVDA"}  # edit or override via --keep
# -------------------------------------------


def parse_args():
    live = "--live" in sys.argv
    keep_arg = None
    for i, a in enumerate(sys.argv):
        if a == "--keep" and i + 1 < len(sys.argv):
            keep_arg = sys.argv[i + 1]
    keep = {s.strip().upper() for s in keep_arg.split(",")} if keep_arg else set(DEFAULT_KEEP)
    return live, keep


def connect_client() -> TradingClient:
    load_dotenv(".env")
    key = os.getenv("APCA_API_KEY_ID")
    sec = os.getenv("APCA_API_SECRET_KEY")
    if not key or not sec:
        print("❌ Missing APCA_API_KEY_ID / APCA_API_SECRET_KEY in .env")
        sys.exit(1)
    paper = os.getenv("ALPACA_PAPER", "false").lower() == "true"
    return TradingClient(key, sec, paper=paper)


def list_open_orders(tc: TradingClient):
    req = GetOrdersRequest(status=QueryOrderStatus.OPEN)
    return tc.get_orders(filter=req)


def cancel_all_open_orders(tc: TradingClient, live: bool):
    orders = list_open_orders(tc)
    if not orders:
        print("✅ No open orders to cancel.")
        return

    print(f"🧹 Found {len(orders)} open orders → canceling...")
    for o in orders:
        try:
            if live:
                tc.cancel_order(o.id)
            print(f"  - {'(LIVE) ' if live else '(DRY) '}Cancel {o.symbol} {o.id}")
        except Exception as e:
            print(f"  ⚠️ Cancel error for {o.symbol} {o.id}: {e}")


def wait_until_no_open_orders(tc: TradingClient):
    for i in range(MAX_POLLS):
        open_syms = [o.symbol for o in list_open_orders(tc)]
        if not open_syms:
            print("✅ Open orders cleared.")
            return True
        if i % 5 == 0:
            print(f"⏳ Still open ({len(open_syms)}): {open_syms}")
        time.sleep(POLL_SECONDS)
    print("⚠️ Timed out waiting for open orders to clear. Some may still be pending_cancel.")
    return False


def close_positions_except(tc: TradingClient, keep: Set[str], live: bool):
    poss = tc.get_all_positions()
    if not poss:
        print("✅ No positions to close.")
        return

    to_close = [p for p in poss if p.symbol.upper() not in keep]
    if not to_close:
        print(f"✅ All positions are in keep list: {sorted(keep)}")
        return

    print("🔒 Keep list:", sorted(keep))
    print(f"🚪 Positions to close: {[p.symbol for p in to_close]}")

    for p in to_close:
        sym = p.symbol
        qty = abs(int(float(p.qty)))
        if qty <= 0:
            continue
        try:
            if live:
                # Market close to flatten quickly (DAY is fine premarket closed—Alpaca queues)
                req = MarketOrderRequest(symbol=sym, qty=qty, side=OrderSide.SELL, time_in_force=TimeInForce.DAY)
                tc.submit_order(req)
            print(f"  - {'(LIVE) ' if live else '(DRY) '}Submit MARKET SELL {sym} x {qty}")
        except Exception as e:
            print(f"  ⚠️ Close submit failed for {sym}: {e}")


def show_account(tc: TradingClient):
    acct = tc.get_account()
    poss = tc.get_all_positions()
    print(f"👤 Equity: {acct.equity} | BP: {acct.buying_power} | Cash: {acct.cash}")
    print("📦 Positions:", [(p.symbol, p.qty) for p in poss])


def main():
    live, keep = parse_args()
    tc = connect_client()

    print("======================================")
    print(" Omega-VX Monday Cleanup (minimal) ")
    print("======================================")
    print("Mode:", "LIVE (actions WILL execute)" if live else "DRY-RUN (no trading)")

    # 1) Snapshot before
    print("\n🔍 Before snapshot:")
    show_account(tc)

    # 2) Cancel open orders and wait
    print("\n🧹 Cancel open orders...")
    cancel_all_open_orders(tc, live=live)
    print("⏳ Waiting for pending_cancel to clear...")
    wait_until_no_open_orders(tc)

    # 3) Close positions not in keep list
    print("\n🚪 Close positions NOT in keep list...")
    close_positions_except(tc, keep=keep, live=live)

    # 4) Snapshot after (may still show queued orders if market closed)
    print("\n📸 After snapshot:")
    show_account(tc)

    print("\n✅ Done.")


if __name__ == "__main__":
    main()