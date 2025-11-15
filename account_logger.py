from __future__ import annotations

import argparse
import time

from account_logging import record_account_snapshot


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Log portfolio + equity snapshots outside the main bot runtime.",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=900,
        help="Seconds between snapshots (ignored when --once is used).",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Log a single snapshot and exit.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    interval = max(60, args.interval)
    while True:
        try:
            snap = record_account_snapshot()
            print(
                f"Logged snapshot @ {snap.timestamp} — Equity ${snap.equity:.2f}, "
                f"Cash ${snap.cash:.2f}, Value ${snap.portfolio_value:.2f}"
            )
        except Exception as exc:
            print(f"⚠️ Snapshot error: {exc}")
        if args.once:
            break
        time.sleep(interval)


if __name__ == "__main__":
    main()
