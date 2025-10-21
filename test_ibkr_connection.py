"""
Quick IBKR Gateway connection check using ib_insync.

Run with: python3 test_ibkr_connection.py
"""
import os
import sys

from dotenv import load_dotenv
from ib_insync import IB


SUMMARY_TAGS = [
    "NetLiquidation",
    "TotalCashValue",
    "BuyingPower",
    "AvailableFunds",
    "ExcessLiquidity",
    "FullMaintMarginReq",
]


def main() -> int:
    load_dotenv()

    host = os.getenv("IBKR_HOST", "127.0.0.1")
    port = int(os.getenv("IBKR_PORT", "7497"))
    client_id = int(os.getenv("IBKR_CLIENT_ID", "1"))
    mode = os.getenv("IBKR_MODE", "paper")

    ib = IB()
    print(f"Connecting to IBKR Gateway at {host}:{port} (clientId={client_id}, mode={mode}) …")

    exit_code = 0

    try:
        try:
            ib.connect(host, port, clientId=client_id, timeout=5.0)
        except Exception as exc:  # pragma: no cover - defensive logging
            print(f"❌ Connection failed: {exc}")
            exit_code = 1
        else:
            if not ib.isConnected():
                print("❌ Connection failed: IBKR session not established.")
                exit_code = 1
            else:
                accounts = ib.managedAccounts()
                if not accounts:
                    print("ℹ️ Connected but no managed accounts returned. Verify IB Gateway settings.")
                    account = ""
                else:
                    account = accounts[0]
                    print(f"✅ Connected to IBKR account: {account}")

                if exit_code == 0:
                    try:
                        summary_items = ib.accountSummary(account)
                    except Exception as exc:  # pragma: no cover - defensive logging
                        print(f"⚠️ Failed to fetch account summary: {exc}")
                        exit_code = 1
                    else:
                        summary_index = {(item.account, item.tag): item for item in summary_items}

                        print("\nAccount Summary Metrics:")
                        if summary_items:
                            fallback_account = summary_items[0].account
                        else:
                            fallback_account = account

                        for tag in SUMMARY_TAGS:
                            key = (account or fallback_account, tag)
                            item = summary_index.get(key)
                            if item:
                                print(f"- {tag}: {item.value} {item.currency}")
                            else:
                                print(f"- {tag}: N/A")
    finally:
        if ib.isConnected():
            ib.disconnect()
            print("\nDisconnected from IBKR.")

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
