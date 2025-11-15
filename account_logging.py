from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

from omega_vx import config as omega_config
from omega_vx.clients import get_trading_client

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"
omega_config.load_environment(ENV_PATH)

LOG_DIR = Path(omega_config.LOG_DIR)
PORTFOLIO_LOG_PATH = LOG_DIR / "portfolio_log.csv"
EQUITY_CURVE_LOG_PATH = LOG_DIR / "equity_curve.log"
PORTFOLIO_LOG_HEADER = ["timestamp", "equity", "cash", "portfolio_value"]


@dataclass
class AccountSnapshot:
    timestamp: str
    equity: float
    cash: float
    portfolio_value: float

    @classmethod
    def from_equity(cls, equity: float, *, timestamp: str | None = None) -> "AccountSnapshot":
        ts = timestamp or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        val = round(float(equity), 2)
        return cls(timestamp=ts, equity=val, cash=val, portfolio_value=val)

    def as_row(self) -> list[str | float]:
        return [self.timestamp, self.equity, self.cash, self.portfolio_value]


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _repair_portfolio_log_header(path: Path = PORTFOLIO_LOG_PATH) -> None:
    """Make sure the CSV begins with the expected header row."""
    if not path.exists():
        return
    try:
        with path.open("r", newline="") as src:
            first_line = src.readline().strip().lower()
            if first_line.startswith("timestamp"):
                return
        with path.open("r", newline="") as src:
            rows = [row for row in csv.reader(src) if row]
    except Exception:
        return
    try:
        with path.open("w", newline="") as dest:
            writer = csv.writer(dest)
            writer.writerow(PORTFOLIO_LOG_HEADER)
            writer.writerows(rows)
    except Exception:
        pass


def fetch_account_snapshot() -> AccountSnapshot:
    """Fetch live account balances from the configured broker."""
    client = get_trading_client()
    account = client.get_account()
    return AccountSnapshot(
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        equity=round(float(account.equity), 2),
        cash=round(float(account.cash), 2),
        portfolio_value=round(float(account.portfolio_value), 2),
    )


def append_portfolio_snapshot(snapshot: AccountSnapshot) -> Path:
    """Append a snapshot row to portfolio_log.csv (writing the header if needed)."""
    _ensure_parent(PORTFOLIO_LOG_PATH)
    needs_header = not PORTFOLIO_LOG_PATH.exists() or PORTFOLIO_LOG_PATH.stat().st_size == 0
    if not needs_header:
        _repair_portfolio_log_header()
    with PORTFOLIO_LOG_PATH.open("a", newline="") as handle:
        writer = csv.writer(handle)
        if needs_header:
            writer.writerow(PORTFOLIO_LOG_HEADER)
        writer.writerow(snapshot.as_row())
    return PORTFOLIO_LOG_PATH


def append_equity_curve(
    snapshot: AccountSnapshot | None = None,
    *,
    timestamp: str | None = None,
    equity: float | None = None,
) -> Path:
    """Append a `[timestamp] EQUITY: $123.45` line to equity_curve.log."""
    snap = snapshot
    if snap is None:
        if equity is not None:
            snap = AccountSnapshot.from_equity(equity, timestamp=timestamp)
        else:
            snap = fetch_account_snapshot()
    _ensure_parent(EQUITY_CURVE_LOG_PATH)
    with EQUITY_CURVE_LOG_PATH.open("a") as handle:
        handle.write(f"[{snap.timestamp}] EQUITY: ${snap.equity:.2f}\n")
    return EQUITY_CURVE_LOG_PATH


def record_account_snapshot() -> AccountSnapshot:
    """Fetch balances and write both portfolio_log.csv and equity_curve.log."""
    snapshot = fetch_account_snapshot()
    append_portfolio_snapshot(snapshot)
    append_equity_curve(snapshot)
    return snapshot


def iter_portfolio_rows(limit: int = 10) -> Iterable[AccountSnapshot]:
    """Yield the most recent snapshot rows (up to `limit`)."""
    if not PORTFOLIO_LOG_PATH.exists():
        return []
    rows: list[AccountSnapshot] = []
    with PORTFOLIO_LOG_PATH.open("r", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            try:
                rows.append(
                    AccountSnapshot(
                        timestamp=row["timestamp"],
                        equity=float(row["equity"]),
                        cash=float(row["cash"]),
                        portfolio_value=float(row["portfolio_value"]),
                    )
                )
            except Exception:
                continue
    return rows[-limit:]


def latest_snapshot() -> Optional[AccountSnapshot]:
    rows = list(iter_portfolio_rows(limit=1))
    return rows[-1] if rows else None
